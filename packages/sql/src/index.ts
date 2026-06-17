import {
  type AggregateOp,
  type AggregateSpec,
  type Expr,
  LakeqlError,
  type OrderByTerm,
  type PathQueryInit,
  type Scalar,
} from "lakeql-core";
import { parse } from "pgsql-ast-parser";

export interface SqlQueryAst extends PathQueryInit {
  groupBy?: string[];
  aggregates?: AggregateSpec;
  having?: Expr;
  join?: SqlJoinAst;
  subqueryJoin?: SqlSubqueryJoinAst;
  cte?: SqlCteAst;
  scalarSubqueries?: Record<string, SqlScalarSubqueryAst>;
}

export type SqlStatementAst = SqlQueryAst | SqlDescribeAst;

export interface SqlDescribeAst {
  type: "describe";
  source: string;
}

export type SqlParameterValue = Scalar;

export interface SqlParseOptions {
  parameters?: readonly SqlParameterValue[];
}

export interface SqlJoinAst {
  leftAlias: string;
  source: string;
  alias: string;
  type: "inner" | "left";
  leftKey: string[];
  rightKey: string[];
}

export interface SqlSubqueryJoinAst {
  source: string;
  type: "semi" | "anti";
  leftKey: string[];
  rightKey: string[];
  where?: Expr;
}

export interface SqlCteAst {
  name: string;
  query: SqlQueryAst;
}

export interface SqlScalarSubqueryAst {
  query: SqlQueryAst;
  column: string;
}

const MAX_SQL_LENGTH = 128_000;
const MAX_AST_DEPTH = 128;

type PgNode = Record<string, unknown>;

interface SqlParseContext {
  scalarSubqueries: Record<string, SqlScalarSubqueryAst>;
  nextScalarSubqueryId: number;
  parameters?: readonly SqlParameterValue[];
}

export function parseSql(sql: string, options: SqlParseOptions = {}): SqlQueryAst {
  if (sql.length > MAX_SQL_LENGTH) {
    throwParse(`SQL input length exceeds ${MAX_SQL_LENGTH}`);
  }

  let statements: unknown[];
  try {
    statements = parse(sql) as unknown[];
  } catch (error) {
    if (error instanceof Error) throwParse(error.message);
    throwParse("Invalid SQL");
  }

  if (statements.length !== 1) throwUnsupported("Only one SELECT statement is supported");
  const statement = asNode(statements[0], "statement");
  assertAstDepth(statement);
  const context = newSqlParseContext(options);
  if (statement.type === "with") return withStatementToAst(statement, context);
  if (statement.type !== "select") throwUnsupported("Only SELECT statements are supported");

  return selectStatementToAst(statement, context);
}

export function parseSqlStatement(sql: string, options: SqlParseOptions = {}): SqlStatementAst {
  const describe = parseDescribeStatement(sql);
  if (describe !== undefined) return describe;
  return parseSql(sql, options);
}

export function formatSql(ast: SqlQueryAst): string {
  const select = [...(ast.select ?? [])];
  for (const [alias, expr] of Object.entries(ast.projections ?? {})) {
    select.push(`${formatExpr(expr, ast)} as ${formatIdentifier(alias)}`);
  }
  for (const [alias, aggregate] of Object.entries(ast.aggregates ?? {})) {
    select.push(`${formatAggregate(aggregate)} as ${formatIdentifier(alias)}`);
  }

  const clauses = [
    `select ${ast.distinct === true ? "distinct " : ""}${select.length > 0 ? select.join(", ") : "*"}`,
  ];
  clauses.push(`from ${formatIdentifier(ast.source)}${formatJoin(ast.source, ast.join)}`);
  const where = formatWhere(ast.where, ast.subqueryJoin, ast);
  if (where !== undefined) clauses.push(`where ${where}`);
  if (ast.groupBy && ast.groupBy.length > 0) {
    clauses.push(`group by ${ast.groupBy.map(formatIdentifier).join(", ")}`);
  }
  if (ast.having) clauses.push(`having ${formatExpr(ast.having, ast)}`);
  if (ast.orderBy && ast.orderBy.length > 0) {
    clauses.push(`order by ${ast.orderBy.map(formatOrderByTerm).join(", ")}`);
  }
  if (ast.limit !== undefined) clauses.push(`limit ${ast.limit}`);
  if (ast.offset !== undefined) clauses.push(`offset ${ast.offset}`);
  const sql = clauses.join("\n");
  if (ast.cte === undefined) return sql;
  return `with ${formatIdentifier(ast.cte.name)} as (${formatSql(ast.cte.query)})\n${sql}`;
}

function parseDescribeStatement(sql: string): SqlDescribeAst | undefined {
  if (sql.length > MAX_SQL_LENGTH) {
    throwParse(`SQL input length exceeds ${MAX_SQL_LENGTH}`);
  }
  const match = /^\s*describe\s+([A-Za-z_][A-Za-z0-9_]*|"(?:""|[^"])+")\s*;?\s*$/iu.exec(sql);
  if (match === null) return undefined;
  const source = match[1];
  if (source === undefined) throwParse("DESCRIBE requires a table name");
  return { type: "describe", source: unquoteIdentifier(source) };
}

function unquoteIdentifier(value: string): string {
  if (!value.startsWith('"')) return value;
  return value.slice(1, -1).replaceAll('""', '"');
}

function withStatementToAst(statement: PgNode, context: SqlParseContext): SqlQueryAst {
  const bindings = optionalArray(statement.bind);
  if (bindings.length !== 1) throwUnsupported("Only one CTE is supported");
  const binding = asNode(bindings[0], "CTE binding");
  const name = nameNodeToString(binding.alias);
  const inner = asNode(binding.statement, "CTE statement");
  if (inner.type !== "select") throwUnsupported("Only SELECT CTEs are supported");
  const cteQuery = selectStatementToAst(inner, context);
  validateCteQuery(cteQuery);
  const outer = asNode(statement.in, "CTE outer query");
  if (outer.type !== "select") throwUnsupported("Only SELECT after WITH is supported");
  const ast = selectStatementToAst(outer, context);
  ast.cte = { name, query: cteQuery };
  return ast;
}

function validateCteQuery(ast: SqlQueryAst): void {
  if (
    ast.join !== undefined ||
    ast.subqueryJoin !== undefined ||
    ast.scalarSubqueries !== undefined ||
    ast.cte !== undefined
  ) {
    throwUnsupported("Nested CTE joins and subqueries are not supported");
  }
}

function selectStatementToAst(
  statement: PgNode,
  context: SqlParseContext = newSqlParseContext(),
): SqlQueryAst {
  rejectPresent(statement, "with", "CTEs are not supported");
  rejectPresent(statement, "windows", "Window functions are not supported");

  const from = optionalArray(statement.from);
  if (from.length !== 1 && from.length !== 2) {
    throwUnsupported("SELECT must have exactly one FROM table or one bounded JOIN");
  }
  const leftSource = sourceTable(from[0]);
  const ast: SqlQueryAst = { source: leftSource.source };
  const scope = new SqlScope(leftSource);
  if (from.length === 2) {
    const join = joinToAst(from[1], leftSource);
    ast.join = join;
    scope.add({ source: join.source, alias: join.alias });
  }
  if (statement.distinct !== undefined) {
    if (statement.distinct !== "distinct") {
      throwUnsupported("Only SELECT DISTINCT is supported");
    }
    ast.distinct = true;
  }

  const columns = optionalArray(statement.columns);
  if (columns.length === 0) throwUnsupported("SELECT requires at least one projection");
  const select: string[] = [];
  const projections: Record<string, Expr> = {};
  const aggregates: AggregateSpec = {};

  for (const column of columns) {
    const item = asNode(column, "select item");
    const expr = asNode(item.expr, "select expression");
    if (isWildcard(expr)) {
      if (item.alias !== undefined) throwUnsupported("Aliases on SELECT * are not supported");
      select.push("*");
      continue;
    }
    if (expr.type === "call" && isAggregateCall(expr)) {
      const alias = aliasName(item.alias) ?? functionName(expr.function);
      aggregates[alias] = aggregateCallToSpec(expr, scope, context);
      continue;
    }
    if (expr.type === "ref") {
      const name = scope.refName(expr);
      const alias = aliasName(item.alias);
      if (alias === undefined || alias === name) select.push(name);
      else projections[alias] = { kind: "column", name };
      continue;
    }
    const alias = aliasName(item.alias);
    if (alias === undefined) {
      throwUnsupported("Computed projections require an explicit alias");
    }
    projections[alias] = exprToLakeql(expr, scope, context);
  }

  if (select.length > 0) ast.select = select;
  if (Object.keys(projections).length > 0) ast.projections = projections;
  if (Object.keys(aggregates).length > 0) ast.aggregates = aggregates;

  if (statement.where !== undefined) {
    const where = whereToAst(asNode(statement.where, "WHERE"), scope, context);
    if (where.where !== undefined) ast.where = where.where;
    if (where.subqueryJoin !== undefined) ast.subqueryJoin = where.subqueryJoin;
  }
  if (statement.groupBy !== undefined) {
    ast.groupBy = optionalArray(statement.groupBy).map((expr) =>
      scope.refName(asNode(expr, "GROUP BY expression")),
    );
  }
  if (statement.having !== undefined) {
    ast.having = exprToLakeql(asNode(statement.having, "HAVING"), scope, context);
  }
  if (statement.orderBy !== undefined) {
    ast.orderBy = optionalArray(statement.orderBy).map((term) => orderByToTerm(term, scope));
  }

  const limit = statement.limit;
  if (limit !== undefined) {
    const node = asNode(limit, "LIMIT");
    if (node.limit !== undefined) ast.limit = nonNegativeInteger(node.limit, "LIMIT", context);
    if (node.offset !== undefined) ast.offset = nonNegativeInteger(node.offset, "OFFSET", context);
  }
  if (Object.keys(context.scalarSubqueries).length > 0) {
    ast.scalarSubqueries = context.scalarSubqueries;
  }

  return ast;
}

function newSqlParseContext(options: SqlParseOptions = {}): SqlParseContext {
  return {
    scalarSubqueries: {},
    nextScalarSubqueryId: 0,
    ...(options.parameters === undefined ? {} : { parameters: options.parameters }),
  };
}

function exprToLakeql(
  expr: PgNode,
  scope = SqlScope.empty(),
  context: SqlParseContext = newSqlParseContext(),
): Expr {
  switch (expr.type) {
    case "ref":
      return { kind: "column", name: scope.refName(expr) };
    case "string":
    case "integer":
    case "numeric":
    case "boolean":
      return literal(expr.value as string | number | boolean);
    case "null":
      return literal(null);
    case "parameter":
      return literal(parameterValue(expr, context));
    case "unary":
      return unaryToExpr(expr, scope, context);
    case "binary":
      return binaryToExpr(expr, scope, context);
    case "ternary":
      return ternaryToExpr(expr, scope, context);
    case "case":
      return caseToExpr(expr, scope, context);
    case "select":
      return scalarSubqueryToExpr(expr, context);
    case "call":
      if ("over" in expr && expr.over !== undefined) {
        throwUnsupported("Window functions are not supported");
      }
      return {
        kind: "call",
        fn: functionName(expr.function),
        args: optionalArray(expr.args).map((arg) =>
          exprToLakeql(asNode(arg, "function argument"), scope, context),
        ),
      };
    default:
      throwUnsupported(`Unsupported SQL expression ${String(expr.type)}`);
  }
}

function binaryToExpr(
  expr: PgNode,
  scope = SqlScope.empty(),
  context: SqlParseContext = newSqlParseContext(),
): Expr {
  const op = String(expr.op).toUpperCase();
  const left = exprToLakeql(asNode(expr.left, "left expression"), scope, context);
  const right = asNode(expr.right, "right expression");

  if (op === "AND" || op === "OR") {
    const operands = flattenLogical(
      op.toLowerCase() as "and" | "or",
      left,
      exprToLakeql(right, scope, context),
    );
    return { kind: "logical", op: op.toLowerCase() as "and" | "or", operands };
  }
  if (op === "IN" || op === "NOT IN") {
    if (right.type !== "list") throwUnsupported("IN subqueries are not supported");
    return {
      kind: "in",
      negated: op === "NOT IN",
      target: left,
      values: optionalArray(right.expressions).map((value) =>
        exprToLakeql(asNode(value, "IN value"), scope, context),
      ),
    };
  }
  if (op === "LIKE" || op === "NOT LIKE" || op === "ILIKE" || op === "NOT ILIKE") {
    const pattern = exprToLakeql(right, scope, context);
    if (pattern.kind !== "literal" || typeof pattern.value !== "string") {
      throwUnsupported("LIKE pattern must be a string literal");
    }
    const like: Expr = {
      kind: "like",
      caseInsensitive: op.includes("ILIKE"),
      target: left,
      pattern: pattern.value,
    };
    return op.startsWith("NOT ") ? { kind: "not", operand: like } : like;
  }
  if (["+", "-", "*", "/", "%"].includes(op)) {
    return {
      kind: "arithmetic",
      op: arithmeticOp(op),
      left,
      right: exprToLakeql(right, scope, context),
    };
  }

  return { kind: "compare", op: compareOp(op), left, right: exprToLakeql(right, scope, context) };
}

function whereToAst(
  expr: PgNode,
  scope: SqlScope,
  context: SqlParseContext,
): { where?: Expr; subqueryJoin?: SqlSubqueryJoinAst } {
  const predicates = flattenWhereConjuncts(expr);
  let subqueryJoin: SqlSubqueryJoinAst | undefined;
  const residual: PgNode[] = [];
  for (const predicate of predicates) {
    const extracted = maybeSubqueryJoin(predicate, scope);
    if (extracted === undefined) {
      residual.push(predicate);
      continue;
    }
    if (subqueryJoin !== undefined) throwUnsupported("Only one IN subquery is supported");
    subqueryJoin = extracted;
  }
  const out: { where?: Expr; subqueryJoin?: SqlSubqueryJoinAst } = {};
  if (residual.length === 1) out.where = exprToLakeql(residual[0] as PgNode, scope, context);
  else if (residual.length > 1) {
    out.where = {
      kind: "logical",
      op: "and",
      operands: residual.map((predicate) => exprToLakeql(predicate, scope, context)),
    };
  }
  if (subqueryJoin !== undefined) out.subqueryJoin = subqueryJoin;
  return out;
}

function maybeSubqueryJoin(expr: PgNode, scope: SqlScope): SqlSubqueryJoinAst | undefined {
  if (expr.type !== "binary") return undefined;
  const op = String(expr.op).toUpperCase();
  if (op !== "IN" && op !== "NOT IN") return undefined;
  const right = asNode(expr.right, "IN right side");
  if (right.type !== "select") return undefined;
  return subqueryJoinToAst(asNode(expr.left, "IN left side"), right, scope, op === "NOT IN");
}

function subqueryJoinToAst(
  left: PgNode,
  subquery: PgNode,
  outerScope: SqlScope,
  negated: boolean,
): SqlSubqueryJoinAst {
  rejectPresent(subquery, "groupBy", "Grouped IN subqueries are not supported");
  rejectPresent(subquery, "having", "HAVING in IN subqueries is not supported");
  rejectPresent(subquery, "orderBy", "ORDER BY in IN subqueries is not supported");
  rejectPresent(subquery, "limit", "LIMIT in IN subqueries is not supported");
  const from = optionalArray(subquery.from);
  if (from.length !== 1) throwUnsupported("IN subqueries must select from one table");
  const source = sourceTable(from[0]);
  const subqueryScope = new SqlScope(source);
  const leftKey = subqueryLeftKeys(left, outerScope);
  const rightKey = optionalArray(subquery.columns).map((column) => {
    const item = asNode(column, "IN subquery select item");
    return subqueryScope.refName(asNode(item.expr, "IN subquery key"));
  });
  if (leftKey.length !== rightKey.length || leftKey.length === 0) {
    throwUnsupported("IN subquery key counts must match");
  }
  const out: SqlSubqueryJoinAst = {
    source: source.source,
    type: negated ? "anti" : "semi",
    leftKey,
    rightKey,
  };
  if (subquery.where !== undefined) {
    out.where = exprToLakeql(asNode(subquery.where, "IN subquery WHERE"), subqueryScope);
  }
  return out;
}

function subqueryLeftKeys(expr: PgNode, scope: SqlScope): string[] {
  if (expr.type === "list") {
    return optionalArray(expr.expressions).map((value) =>
      scope.refName(asNode(value, "IN left key")),
    );
  }
  return [scope.refName(expr)];
}

function flattenWhereConjuncts(expr: PgNode): PgNode[] {
  if (expr.type === "binary" && String(expr.op).toUpperCase() === "AND") {
    return [
      ...flattenWhereConjuncts(asNode(expr.left, "WHERE predicate")),
      ...flattenWhereConjuncts(asNode(expr.right, "WHERE predicate")),
    ];
  }
  return [expr];
}

function scalarSubqueryToExpr(subquery: PgNode, context: SqlParseContext): Expr {
  const query = selectStatementToAst(subquery, context);
  const outputColumns = scalarSubqueryOutputColumns(query);
  if (outputColumns.length !== 1) {
    throwUnsupported("Scalar subqueries must return exactly one column");
  }
  if (query.aggregates === undefined && query.limit !== 1) {
    throwUnsupported("Scalar subqueries must be aggregate queries or use LIMIT 1");
  }
  const id = `scalar_${context.nextScalarSubqueryId}`;
  context.nextScalarSubqueryId += 1;
  context.scalarSubqueries[id] = { query, column: outputColumns[0] as string };
  return { kind: "call", fn: "__lakeql_scalar_subquery", args: [{ kind: "literal", value: id }] };
}

function scalarSubqueryOutputColumns(query: SqlQueryAst): string[] {
  return [
    ...(query.select ?? []).filter((column) => column !== "*"),
    ...Object.keys(query.projections ?? {}),
    ...Object.keys(query.aggregates ?? {}),
  ];
}

function unaryToExpr(
  expr: PgNode,
  scope = SqlScope.empty(),
  context: SqlParseContext = newSqlParseContext(),
): Expr {
  const op = String(expr.op).toUpperCase();
  const operand = exprToLakeql(asNode(expr.operand, "unary operand"), scope, context);
  if (op === "NOT") return { kind: "not", operand };
  if (op === "IS NULL") return { kind: "null-check", negated: false, target: operand };
  if (op === "IS NOT NULL") return { kind: "null-check", negated: true, target: operand };
  if (op === "-") {
    return { kind: "arithmetic", op: "mul", left: literal(-1), right: operand };
  }
  throwUnsupported(`Unsupported unary operator ${op}`);
}

function caseToExpr(
  expr: PgNode,
  scope = SqlScope.empty(),
  context: SqlParseContext = newSqlParseContext(),
): Expr {
  if (expr.value !== null && expr.value !== undefined) {
    throwUnsupported("Simple CASE expressions are not supported yet");
  }
  const whens = optionalArray(expr.whens).map((branch) => {
    const node = asNode(branch, "CASE branch");
    return {
      when: exprToLakeql(asNode(node.when, "CASE WHEN expression"), scope, context),
      value: exprToLakeql(asNode(node.value, "CASE THEN expression"), scope, context),
    };
  });
  const out: Expr = { kind: "case", whens };
  if (expr.else !== undefined) {
    out.else = exprToLakeql(asNode(expr.else, "CASE ELSE expression"), scope, context);
  }
  return out;
}

function ternaryToExpr(
  expr: PgNode,
  scope = SqlScope.empty(),
  context: SqlParseContext = newSqlParseContext(),
): Expr {
  const op = String(expr.op).toUpperCase();
  if (op !== "BETWEEN" && op !== "NOT BETWEEN") {
    throwUnsupported(`Unsupported ternary operator ${op}`);
  }
  const between: Expr = {
    kind: "between",
    target: exprToLakeql(asNode(expr.value, "BETWEEN target"), scope, context),
    low: exprToLakeql(asNode(expr.lo, "BETWEEN low value"), scope, context),
    high: exprToLakeql(asNode(expr.hi, "BETWEEN high value"), scope, context),
  };
  return op === "NOT BETWEEN" ? { kind: "not", operand: between } : between;
}

function aggregateCallToSpec(
  expr: PgNode,
  scope = SqlScope.empty(),
  context: SqlParseContext = newSqlParseContext(),
): AggregateSpec[string] {
  if ("over" in expr && expr.over !== undefined) {
    throwUnsupported("Window functions are not supported");
  }
  let op = aggregateOp(functionName(expr.function));
  const args = optionalArray(expr.args);
  if (expr.distinct !== undefined) {
    if (expr.distinct !== "distinct")
      throwUnsupported("Only DISTINCT aggregate arguments are supported");
    if (op !== "count") throwUnsupported("Only COUNT(DISTINCT x) is supported");
    if (args.length !== 1 || isWildcard(asNode(args[0], "COUNT DISTINCT argument"))) {
      throwUnsupported("COUNT(DISTINCT *) is not supported");
    }
    op = "count_distinct";
  }
  if (op === "quantile") return quantileAggregateCallToSpec(args, scope, context);
  if (
    op === "count" &&
    (args.length === 0 || (args.length === 1 && isWildcard(asNode(args[0], "COUNT argument"))))
  ) {
    return { op };
  }
  if (args.length !== 1) throwUnsupported(`${op} requires exactly one argument`);
  const arg = asNode(args[0], "aggregate argument");
  if (arg.type === "ref") return { op, column: scope.refName(arg) };
  return { op, expr: exprToLakeql(arg, scope, context) };
}

function quantileAggregateCallToSpec(
  args: unknown[],
  scope: SqlScope,
  context: SqlParseContext,
): AggregateSpec[string] {
  if (args.length !== 2) throwUnsupported("quantile_cont requires value and percentile arguments");
  const value = asNode(args[0], "quantile_cont value argument");
  const percentile = requiredQuantilePercentile(
    exprToLakeql(asNode(args[1], "quantile_cont percentile argument"), scope, context),
  );
  if (value.type === "ref") {
    return { op: "quantile", column: scope.refName(value), quantile: percentile };
  }
  return { op: "quantile", expr: exprToLakeql(value, scope, context), quantile: percentile };
}

function requiredQuantilePercentile(expr: Expr): number {
  if (
    expr.kind !== "literal" ||
    typeof expr.value !== "number" ||
    !Number.isFinite(expr.value) ||
    expr.value < 0 ||
    expr.value > 1
  ) {
    throwType("quantile_cont percentile must be a numeric literal between 0 and 1", {
      function: "quantile_cont",
    });
  }
  return expr.value;
}

function isAggregateCall(expr: PgNode): boolean {
  if (expr.type !== "call") return false;
  return aggregateOpOrUndefined(functionName(expr.function)) !== undefined;
}

function isAggregateOp(op: string): op is AggregateOp {
  return [
    "count",
    "sum",
    "avg",
    "var_samp",
    "var_pop",
    "stddev_samp",
    "stddev_pop",
    "median",
    "min",
    "max",
    "count_distinct",
    "approx_count_distinct",
    "mode",
    "first",
    "last",
    "any",
  ].includes(op);
}

function orderByToTerm(value: unknown, scope = SqlScope.empty()): OrderByTerm {
  const node = asNode(value, "ORDER BY term");
  const term: OrderByTerm = { column: scope.refName(asNode(node.by, "ORDER BY expression")) };
  if (node.order !== undefined) {
    const order = String(node.order).toLowerCase();
    if (order !== "asc" && order !== "desc") throwUnsupported(`Unsupported ORDER BY ${order}`);
    term.direction = order;
  }
  if (node.nulls !== undefined) {
    const nulls = String(node.nulls).toLowerCase();
    if (nulls !== "first" && nulls !== "last") {
      throwUnsupported(`Unsupported ORDER BY NULLS ${nulls}`);
    }
    term.nulls = nulls;
  }
  return term;
}

interface SourceTable {
  source: string;
  alias: string;
}

class SqlScope {
  private readonly aliases = new Map<string, SourceTable>();

  static empty(): SqlScope {
    return new SqlScope();
  }

  constructor(source?: SourceTable) {
    if (source !== undefined) this.add(source);
  }

  add(source: SourceTable): void {
    this.aliases.set(source.source, source);
    this.aliases.set(source.alias, source);
  }

  refName(expr: PgNode): string {
    if (expr.type !== "ref" || typeof expr.name !== "string") {
      throwUnsupported("Only column references are supported here");
    }
    if (expr.table === undefined) return expr.name;
    const qualifier = nameNodeToString(expr.table);
    const source = this.aliases.get(qualifier);
    if (source === undefined) throwUnsupported(`Unknown SQL table qualifier ${qualifier}`);
    if (this.aliases.size <= 2) return expr.name;
    return `${source.alias}.${expr.name}`;
  }
}

function sourceTable(value: unknown): SourceTable {
  const node = asNode(value, "FROM item");
  if (node.type !== "table") throwUnsupported("Only table FROM sources are supported");
  if (node.join !== undefined) throwUnsupported("Unexpected JOIN position");
  const source = nameNodeToString(node.name);
  return { source, alias: aliasName(node.name) ?? source };
}

function joinToAst(value: unknown, left: SourceTable): SqlJoinAst {
  const node = asNode(value, "JOIN source");
  if (node.type !== "table") throwUnsupported("Only table JOIN sources are supported");
  const join = asNode(node.join, "JOIN");
  const joinType = String(join.type).toUpperCase();
  if (joinType !== "INNER JOIN" && joinType !== "LEFT JOIN") {
    throwUnsupported(`Unsupported JOIN type ${joinType}`);
  }
  const right = sourceTable({ ...node, join: undefined });
  if (join.using !== undefined) {
    const using = optionalArray(join.using);
    if (using.length === 0) throwUnsupported("JOIN USING requires at least one column");
    const keys = using.map(nameNodeToString);
    return {
      source: right.source,
      leftAlias: left.alias,
      alias: right.alias,
      type: joinType === "LEFT JOIN" ? "left" : "inner",
      leftKey: keys.map((key) => `${left.alias}.${key}`),
      rightKey: keys.map((key) => `${right.alias}.${key}`),
    };
  }
  const on = asNode(join.on, "JOIN ON");
  const { leftKey, rightKey } = joinKeysFromPredicate(on, left, right);
  return {
    source: right.source,
    leftAlias: left.alias,
    alias: right.alias,
    type: joinType === "LEFT JOIN" ? "left" : "inner",
    leftKey,
    rightKey,
  };
}

function qualifiedJoinKey(expr: PgNode, left: SourceTable, right: SourceTable): string {
  if (expr.type !== "ref" || typeof expr.name !== "string" || expr.table === undefined) {
    throwUnsupported("JOIN keys must be qualified column references");
  }
  const qualifier = nameNodeToString(expr.table);
  if (qualifier === left.alias || qualifier === left.source) return `${left.alias}.${expr.name}`;
  if (qualifier === right.alias || qualifier === right.source) return `${right.alias}.${expr.name}`;
  throwUnsupported(`Unknown JOIN qualifier ${qualifier}`);
}

function joinKeysFromPredicate(
  expr: PgNode,
  left: SourceTable,
  right: SourceTable,
): { leftKey: string[]; rightKey: string[] } {
  const conjuncts = flattenJoinConjuncts(expr);
  const leftKey: string[] = [];
  const rightKey: string[] = [];
  for (const conjunct of conjuncts) {
    if (String(conjunct.op) !== "=") throwUnsupported("Only equi-joins are supported");
    const first = qualifiedJoinKey(asNode(conjunct.left, "JOIN left key"), left, right);
    const second = qualifiedJoinKey(asNode(conjunct.right, "JOIN right key"), left, right);
    if (first.startsWith(`${left.alias}.`) && second.startsWith(`${right.alias}.`)) {
      leftKey.push(first);
      rightKey.push(second);
    } else if (second.startsWith(`${left.alias}.`) && first.startsWith(`${right.alias}.`)) {
      leftKey.push(second);
      rightKey.push(first);
    } else {
      throwUnsupported("JOIN ON must compare left table key to right table key");
    }
  }
  return { leftKey, rightKey };
}

function flattenJoinConjuncts(expr: PgNode): PgNode[] {
  if (expr.type === "binary" && String(expr.op).toUpperCase() === "AND") {
    return [
      ...flattenJoinConjuncts(asNode(expr.left, "JOIN predicate")),
      ...flattenJoinConjuncts(asNode(expr.right, "JOIN predicate")),
    ];
  }
  return [expr];
}

function nameNodeToString(value: unknown): string {
  if (typeof value === "string") return value;
  const node = asNode(value, "name");
  const name = node.name;
  if (typeof name !== "string") throwUnsupported("Unsupported qualified name");
  return name;
}

function aliasName(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    const node = value as Record<string, unknown>;
    if (typeof node.alias === "string") return node.alias;
  }
  return nameNodeToString(value);
}

function functionName(value: unknown): string {
  return nameNodeToString(value).toLowerCase();
}

function isWildcard(expr: PgNode): boolean {
  return expr.type === "ref" && expr.name === "*";
}

function optionalArray(value: unknown): unknown[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) throwUnsupported("Expected SQL AST array");
  return value;
}

function nonNegativeInteger(
  value: unknown,
  label: string,
  context: SqlParseContext = newSqlParseContext(),
): number {
  const node = asNode(value, label);
  const parsed = node.type === "parameter" ? parameterValue(node, context) : node.value;
  if (typeof parsed !== "number" || !Number.isInteger(parsed) || parsed < 0) {
    throwUnsupported(`${label} must be a non-negative integer`);
  }
  return parsed;
}

function flattenLogical(op: "and" | "or", left: Expr, right: Expr): Expr[] {
  return [
    ...(left.kind === "logical" && left.op === op ? left.operands : [left]),
    ...(right.kind === "logical" && right.op === op ? right.operands : [right]),
  ];
}

function compareOp(op: string): "eq" | "ne" | "lt" | "lte" | "gt" | "gte" {
  switch (op) {
    case "=":
      return "eq";
    case "!=":
    case "<>":
      return "ne";
    case "<":
      return "lt";
    case "<=":
      return "lte";
    case ">":
      return "gt";
    case ">=":
      return "gte";
    default:
      throwUnsupported(`Unsupported comparison operator ${op}`);
  }
}

function arithmeticOp(op: string): "add" | "sub" | "mul" | "div" | "mod" {
  switch (op) {
    case "+":
      return "add";
    case "-":
      return "sub";
    case "*":
      return "mul";
    case "/":
      return "div";
    case "%":
      return "mod";
    default:
      throwUnsupported(`Unsupported arithmetic operator ${op}`);
  }
}

function aggregateOp(op: string): AggregateOp {
  const aggregate = aggregateOpOrUndefined(op);
  if (aggregate !== undefined) return aggregate;
  throwUnsupported(`Unsupported aggregate ${op}`);
}

function aggregateOpOrUndefined(op: string): AggregateOp | undefined {
  if (isAggregateOp(op)) return op;
  if (op === "var" || op === "variance") return "var_samp";
  if (op === "stddev") return "stddev_samp";
  if (op === "quantile_cont") return "quantile";
  return undefined;
}

function literal(value: Scalar): Expr {
  return { kind: "literal", value };
}

function parameterValue(expr: PgNode, context: SqlParseContext): SqlParameterValue {
  const index = parameterIndex(expr.name);
  const value = context.parameters?.[index - 1];
  if (value === undefined) {
    throwUnsupported(`SQL parameter $${index} has no bound value`);
  }
  if (isScalar(value)) return value;
  throwType(`SQL parameter $${index} must be a scalar value`, { parameter: index });
}

function parameterIndex(name: unknown): number {
  if (typeof name !== "string" || !/^\$[1-9][0-9]*$/u.test(name)) {
    throwUnsupported(`Unsupported SQL parameter ${String(name)}`);
  }
  return Number(name.slice(1));
}

function isScalar(value: unknown): value is Scalar {
  return (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    typeof value === "bigint" ||
    (typeof value === "number" && Number.isFinite(value))
  );
}

function formatExpr(expr: Expr, ast?: SqlQueryAst): string {
  switch (expr.kind) {
    case "literal":
      return formatLiteral(expr.value);
    case "column":
      return formatIdentifier(expr.name);
    case "compare":
      return `${formatExpr(expr.left, ast)} ${formatCompareOp(expr.op)} ${formatExpr(expr.right, ast)}`;
    case "between":
      return `${formatExpr(expr.target, ast)} between ${formatExpr(expr.low, ast)} and ${formatExpr(expr.high, ast)}`;
    case "in":
      return `${formatExpr(expr.target, ast)}${expr.negated ? " not" : ""} in (${expr.values.map((value) => formatExpr(value, ast)).join(", ")})`;
    case "null-check":
      return `${formatExpr(expr.target, ast)} is${expr.negated ? " not" : ""} null`;
    case "logical":
      return expr.operands.map((operand) => `(${formatExpr(operand, ast)})`).join(` ${expr.op} `);
    case "not":
      return `not (${formatExpr(expr.operand, ast)})`;
    case "like":
      return `${formatExpr(expr.target)} ${expr.caseInsensitive ? "ilike" : "like"} ${formatLiteral(expr.pattern)}`;
    case "call":
      if (expr.fn === "__lakeql_scalar_subquery") return formatScalarSubqueryExpr(expr, ast);
      return `${formatIdentifier(expr.fn)}(${expr.args.map((arg) => formatExpr(arg, ast)).join(", ")})`;
    case "arithmetic":
      return `${formatExpr(expr.left, ast)} ${formatArithmeticOp(expr.op)} ${formatExpr(expr.right, ast)}`;
    case "case":
      return `case ${expr.whens
        .map(
          (branch) => `when ${formatExpr(branch.when, ast)} then ${formatExpr(branch.value, ast)}`,
        )
        .join(" ")}${expr.else === undefined ? "" : ` else ${formatExpr(expr.else, ast)}`} end`;
  }
}

function formatScalarSubqueryExpr(
  expr: Extract<Expr, { kind: "call" }>,
  ast: SqlQueryAst | undefined,
): string {
  const id = expr.args[0];
  if (id?.kind !== "literal" || typeof id.value !== "string") {
    throwUnsupported("Invalid scalar subquery placeholder");
  }
  const subquery = ast?.scalarSubqueries?.[id.value];
  if (subquery === undefined) throwUnsupported("Missing scalar subquery metadata");
  return `(${formatSql(subquery.query)})`;
}

function formatOrderByTerm(term: OrderByTerm): string {
  const parts = [formatIdentifier(term.column)];
  if (term.direction) parts.push(term.direction);
  if (term.nulls) parts.push("nulls", term.nulls);
  return parts.join(" ");
}

function formatJoin(leftSource: string, join: SqlJoinAst | undefined): string {
  if (join === undefined) return "";
  const leftAlias = join.leftAlias === leftSource ? "" : ` ${formatIdentifier(join.leftAlias)}`;
  const source = formatIdentifier(join.source);
  const alias = join.alias === join.source ? "" : ` ${formatIdentifier(join.alias)}`;
  const type = join.type === "left" ? "left join" : "join";
  const on = join.leftKey
    .map((leftKey, index) => {
      const rightKey = join.rightKey[index];
      if (rightKey === undefined) throwUnsupported("JOIN key counts must match");
      return `${formatIdentifier(leftKey)} = ${formatIdentifier(rightKey)}`;
    })
    .join(" and ");
  return `${leftAlias} ${type} ${source}${alias} on ${on}`;
}

function formatWhere(
  where: Expr | undefined,
  subqueryJoin: SqlSubqueryJoinAst | undefined,
  ast?: SqlQueryAst,
): string | undefined {
  const parts: string[] = [];
  if (where !== undefined) parts.push(formatExpr(where, ast));
  if (subqueryJoin !== undefined) parts.push(formatSubqueryJoin(subqueryJoin));
  return parts.length === 0 ? undefined : parts.map((part) => `(${part})`).join(" and ");
}

function formatSubqueryJoin(subqueryJoin: SqlSubqueryJoinAst): string {
  const left =
    subqueryJoin.leftKey.length === 1
      ? formatIdentifier(subqueryJoin.leftKey[0] ?? "")
      : `(${subqueryJoin.leftKey.map(formatIdentifier).join(", ")})`;
  const right =
    subqueryJoin.rightKey.length === 1
      ? formatIdentifier(subqueryJoin.rightKey[0] ?? "")
      : subqueryJoin.rightKey.map(formatIdentifier).join(", ");
  const where = subqueryJoin.where === undefined ? "" : ` where ${formatExpr(subqueryJoin.where)}`;
  return `${left} ${subqueryJoin.type === "anti" ? "not in" : "in"} (select ${right} from ${formatIdentifier(subqueryJoin.source)}${where})`;
}

function formatAggregate(aggregate: AggregateSpec[string]): string {
  const arg =
    aggregate.expr !== undefined
      ? formatExpr(aggregate.expr)
      : aggregate.column === undefined
        ? "*"
        : formatIdentifier(aggregate.column);
  if (aggregate.op === "count_distinct") return `count(distinct ${arg})`;
  if (aggregate.op === "quantile") return `quantile_cont(${arg}, ${aggregate.quantile})`;
  return `${aggregate.op}(${arg})`;
}

function formatCompareOp(op: "eq" | "ne" | "lt" | "lte" | "gt" | "gte"): string {
  switch (op) {
    case "eq":
      return "=";
    case "ne":
      return "!=";
    case "lt":
      return "<";
    case "lte":
      return "<=";
    case "gt":
      return ">";
    case "gte":
      return ">=";
  }
}

function formatArithmeticOp(op: "add" | "sub" | "mul" | "div" | "mod"): string {
  switch (op) {
    case "add":
      return "+";
    case "sub":
      return "-";
    case "mul":
      return "*";
    case "div":
      return "/";
    case "mod":
      return "%";
  }
}

function formatLiteral(value: string | number | boolean | bigint | null): string {
  if (value === null) return "null";
  if (typeof value === "string") return `'${value.replaceAll("'", "''")}'`;
  if (typeof value === "bigint") return value.toString();
  return String(value);
}

function formatIdentifier(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_.*:/=-]*$/u.test(value)) {
    throwUnsupported(`Identifier ${value} cannot be represented in the SQL dialect`);
  }
  return value;
}

function assertAstDepth(value: unknown, depth = 0): void {
  if (depth > MAX_AST_DEPTH) throwParse(`SQL AST nesting exceeds ${MAX_AST_DEPTH}`);
  if (value === null || typeof value !== "object") return;
  if (Array.isArray(value)) {
    for (const item of value) assertAstDepth(item, depth + 1);
    return;
  }
  for (const child of Object.values(value as Record<string, unknown>)) {
    assertAstDepth(child, depth + 1);
  }
}

function rejectPresent(node: PgNode, key: string, message: string): void {
  if (node[key] !== undefined) throwUnsupported(message);
}

function asNode(value: unknown, label: string): PgNode {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throwUnsupported(`Expected ${label}`);
  }
  return value as PgNode;
}

function throwParse(message: string): never {
  throw new LakeqlError("LAKEQL_PARSE_ERROR", message);
}

function throwUnsupported(message: string): never {
  throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", message);
}

function throwType(message: string, details?: Record<string, unknown>): never {
  throw new LakeqlError("LAKEQL_TYPE_ERROR", message, details);
}
