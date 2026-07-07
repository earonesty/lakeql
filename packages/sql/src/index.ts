import {
  type AggregateOp,
  type AggregateSpec,
  type Expr,
  intervalToString,
  intervalValue,
  isIntervalValue,
  isTimestampValue,
  LakeqlError,
  type OrderByTerm,
  type PathQueryInit,
  type Scalar,
  type WindowExpr,
  type WindowFrame,
  type WindowFrameBound,
  type WindowOrderTerm,
} from "lakeql-core";
import { parse } from "pgsql-ast-parser";
import { extractTopLevelQualify } from "./qualify-prepass.js";
import { findTopLevelKeyword } from "./sql-scan.js";
import { extractWindowFrames, type SqlWindowPrepassFrame } from "./window-prepass.js";

export interface SqlQueryAst extends PathQueryInit {
  groupBy?: string[];
  aggregates?: AggregateSpec;
  hiddenAggregates?: string[];
  having?: Expr;
  join?: SqlJoinAst;
  joins?: SqlJoinAst[];
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
  type: "inner" | "left" | "right" | "full" | "cross";
  leftKey: string[];
  rightKey: string[];
  predicate?: Expr;
}

export interface SqlSubqueryJoinAst {
  source: string;
  type: "semi" | "anti";
  leftKey: string[];
  rightKey: string[];
  groupBy?: string[];
  aggregates?: AggregateSpec;
  hiddenAggregates?: string[];
  having?: Expr;
  leftAlias?: string;
  alias?: string;
  predicate?: Expr;
  where?: Expr;
  orderBy?: OrderByTerm[];
  limit?: number;
  offset?: number;
}

export interface SqlCteAst {
  name: string;
  query: SqlQueryAst;
}

export interface SqlScalarSubqueryAst {
  query: SqlQueryAst;
  column: string;
  mode?: "scalar" | "exists";
}

const MAX_SQL_LENGTH = 128_000;
const MAX_AST_DEPTH = 128;
const PROTOTYPE_MUTATION_KEYS = new Set(["__proto__", "constructor", "prototype"]);

type PgNode = Record<string, unknown>;

interface SqlParseContext {
  scalarSubqueries: Record<string, SqlScalarSubqueryAst>;
  nextScalarSubqueryId: number;
  windows: Record<string, WindowExpr>;
  nextWindowId: number;
  nextHavingAggregateId: number;
  havingAggregates?: HavingAggregateContext;
  windowFrames: (SqlWindowPrepassFrame | undefined)[];
  nextWindowFrameId: number;
  qualifySql?: string;
  parameters?: readonly SqlParameterValue[];
}

interface HavingAggregateContext {
  aggregates: AggregateSpec;
  hiddenAggregates: Set<string>;
  outputAliases: Set<string>;
}

export function parseSql(sql: string, options: SqlParseOptions = {}): SqlQueryAst {
  if (sql.length > MAX_SQL_LENGTH) {
    throwParse(`SQL input length exceeds ${MAX_SQL_LENGTH}`);
  }
  rejectRecursiveCte(sql);
  const windowPrepass = extractWindowFrames(sql);
  const prepass = extractTopLevelQualify(windowPrepass.sql);

  let statements: unknown[];
  try {
    statements = parse(prepass.sql) as unknown[];
  } catch (error) {
    if (error instanceof Error) throwParse(error.message);
    throwParse("Invalid SQL");
  }

  if (statements.length !== 1) throwUnsupported("Only one SELECT statement is supported");
  const statement = asNode(statements[0], "statement");
  assertAstDepth(statement);
  const context = newSqlParseContext(options);
  context.windowFrames = windowPrepass.frames;
  if (prepass.qualify !== undefined) context.qualifySql = prepass.qualify;
  if (statement.type === "with") return withStatementToAst(statement, context);
  if (statement.type !== "select") throwUnsupported("Only SELECT statements are supported");

  return selectStatementToAst(statement, context, context.qualifySql);
}

function rejectRecursiveCte(sql: string): void {
  const withIndex = findTopLevelKeyword(sql, "with", 0);
  if (withIndex === -1) return;
  const recursiveIndex = findTopLevelKeyword(sql, "recursive", withIndex + "with".length);
  if (recursiveIndex === -1) return;
  const between = sql.slice(withIndex + "with".length, recursiveIndex);
  if (between.trim().length > 0) return;
  throwUnsupported("Recursive CTEs are not supported");
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
  const hiddenAggregates = new Set(ast.hiddenAggregates ?? []);
  for (const [alias, aggregate] of Object.entries(ast.aggregates ?? {})) {
    if (hiddenAggregates.has(alias)) continue;
    select.push(`${formatAggregate(aggregate)} as ${formatIdentifier(alias)}`);
  }

  const clauses = [
    `select ${ast.distinct === true ? "distinct " : ""}${select.length > 0 ? select.join(", ") : "*"}`,
  ];
  clauses.push(`from ${formatSqlSource(ast)}${formatJoins(ast.source, sqlJoins(ast))}`);
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

function formatSqlSource(ast: SqlQueryAst): string {
  const source = formatIdentifier(ast.source);
  const alias = ast.subqueryJoin?.predicate === undefined ? undefined : ast.subqueryJoin.leftAlias;
  return alias === undefined || alias === ast.source
    ? source
    : `${source} ${formatIdentifier(alias)}`;
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
  if (inner.type !== "select" && inner.type !== "with") {
    throwUnsupported("Only SELECT CTEs are supported");
  }
  const cteQuery =
    inner.type === "with"
      ? withStatementToAst(inner, context)
      : selectStatementToAst(inner, context);
  const outer = asNode(statement.in, "CTE outer query");
  if (outer.type !== "select") throwUnsupported("Only SELECT after WITH is supported");
  const ast = selectStatementToAst(outer, context, context.qualifySql);
  if (ast.cte !== undefined) throwUnsupported("WITH queries cannot also use derived tables");
  ast.cte = { name, query: cteQuery };
  return ast;
}

function selectStatementToAst(
  statement: PgNode,
  context: SqlParseContext = newSqlParseContext(),
  qualifySql?: string,
): SqlQueryAst {
  const previousWindows = context.windows;
  const previousNextWindowId = context.nextWindowId;
  const previousNextHavingAggregateId = context.nextHavingAggregateId;
  const previousQualifySql = context.qualifySql;
  context.windows = {};
  context.nextWindowId = 0;
  context.nextHavingAggregateId = 0;
  if (qualifySql === undefined) delete context.qualifySql;
  else context.qualifySql = qualifySql;
  const ast = selectStatementToAstInScope(statement, context);
  context.windows = previousWindows;
  context.nextWindowId = previousNextWindowId;
  context.nextHavingAggregateId = previousNextHavingAggregateId;
  if (previousQualifySql === undefined) delete context.qualifySql;
  else context.qualifySql = previousQualifySql;
  return ast;
}

function selectStatementToAstInScope(statement: PgNode, context: SqlParseContext): SqlQueryAst {
  rejectPresent(statement, "with", "CTEs are not supported");
  rejectPresent(statement, "windows", "Window functions are not supported");

  const from = optionalArray(statement.from);
  if (from.length < 1) {
    throwUnsupported("SELECT must have a FROM table");
  }
  const leftSource = sourceTable(from[0], context);
  const ast: SqlQueryAst = { source: leftSource.source };
  if (leftSource.cte !== undefined) ast.cte = leftSource.cte;
  const scope = new SqlScope(leftSource);
  const joinedSources = [leftSource];
  const joins: SqlJoinAst[] = [];
  for (const source of from.slice(1)) {
    const { join, right } = joinToAst(source, joinedSources);
    joins.push(join);
    joinedSources.push(right);
    scope.add(right);
  }
  if (joins.length > 0) {
    const firstJoin = joins[0];
    if (firstJoin === undefined) throwUnsupported("JOIN chain must contain at least one join");
    ast.join = firstJoin;
    ast.joins = joins;
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
  const outputAliases = new Set<string>();
  const hiddenAggregates = new Set<string>();

  for (const column of columns) {
    const item = asNode(column, "select item");
    const expr = asNode(item.expr, "select expression");
    if (isWildcard(expr)) {
      if (item.alias !== undefined) throwUnsupported("Aliases on SELECT * are not supported");
      select.push("*");
      continue;
    }
    if (expr.type === "call" && hasWindowOver(expr)) {
      const alias = aliasName(item.alias) ?? syntheticWindowAlias(context);
      ensureUniqueOutputAlias(alias, outputAliases);
      context.windows[alias] = windowCallToExpr(expr, scope, context);
      select.push(alias);
      continue;
    }
    if (expr.type === "call" && isAggregateCall(expr)) {
      const alias = aliasName(item.alias) ?? functionName(expr.function);
      ensureUniqueOutputAlias(alias, outputAliases);
      aggregates[alias] = aggregateCallToSpec(expr, scope, context);
      continue;
    }
    if (expr.type === "ref") {
      const name = scope.refName(expr);
      const alias = aliasName(item.alias);
      const output = alias === undefined ? name : alias;
      ensureUniqueOutputAlias(output, outputAliases);
      if (alias === undefined || alias === name) select.push(name);
      else projections[alias] = { kind: "column", name };
      continue;
    }
    const alias = aliasName(item.alias);
    if (alias === undefined) {
      throwUnsupported("Computed projections require an explicit alias");
    }
    ensureUniqueOutputAlias(alias, outputAliases);
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
    ast.having = havingToExpr(
      asNode(statement.having, "HAVING"),
      scope,
      context,
      aggregates,
      hiddenAggregates,
      outputAliases,
    );
    if (Object.keys(aggregates).length > 0) ast.aggregates = aggregates;
    if (hiddenAggregates.size > 0) ast.hiddenAggregates = [...hiddenAggregates];
  }
  if (context.qualifySql !== undefined) {
    ast.qualify = expandProjectionAliasesInExpr(
      qualifyToExpr(context.qualifySql, scope, context),
      projections,
      context.windows,
    );
  }
  if (Object.keys(context.windows).length > 0) ast.windows = context.windows;
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

function qualifyToExpr(sql: string, scope: SqlScope, context: SqlParseContext): Expr {
  let statements: unknown[];
  try {
    statements = parse(`select * from __lakeql_qualify where ${sql}`) as unknown[];
  } catch (error) {
    if (error instanceof Error) throwParse(error.message);
    throwParse("Invalid QUALIFY clause");
  }
  const statement = asNode(statements[0], "QUALIFY wrapper");
  const where = asNode(statement.where, "QUALIFY predicate");
  return exprToLakeql(where, scope, context);
}

function expandProjectionAliasesInExpr(
  expr: Expr,
  projections: Record<string, Expr>,
  windows: Record<string, WindowExpr>,
): Expr {
  switch (expr.kind) {
    case "column":
      return expr.name in windows ? expr : (projections[expr.name] ?? expr);
    case "compare":
      return {
        ...expr,
        left: expandProjectionAliasesInExpr(expr.left, projections, windows),
        right: expandProjectionAliasesInExpr(expr.right, projections, windows),
      };
    case "between":
      return {
        ...expr,
        target: expandProjectionAliasesInExpr(expr.target, projections, windows),
        low: expandProjectionAliasesInExpr(expr.low, projections, windows),
        high: expandProjectionAliasesInExpr(expr.high, projections, windows),
      };
    case "in":
      return {
        ...expr,
        target: expandProjectionAliasesInExpr(expr.target, projections, windows),
        values: expr.values.map((value) =>
          expandProjectionAliasesInExpr(value, projections, windows),
        ),
      };
    case "null-check":
      return { ...expr, target: expandProjectionAliasesInExpr(expr.target, projections, windows) };
    case "logical":
      return {
        ...expr,
        operands: expr.operands.map((operand) =>
          expandProjectionAliasesInExpr(operand, projections, windows),
        ),
      };
    case "not":
      return {
        ...expr,
        operand: expandProjectionAliasesInExpr(expr.operand, projections, windows),
      };
    case "like":
      return { ...expr, target: expandProjectionAliasesInExpr(expr.target, projections, windows) };
    case "call":
      return {
        ...expr,
        args: expr.args.map((arg) => expandProjectionAliasesInExpr(arg, projections, windows)),
      };
    case "arithmetic":
      return {
        ...expr,
        left: expandProjectionAliasesInExpr(expr.left, projections, windows),
        right: expandProjectionAliasesInExpr(expr.right, projections, windows),
      };
    case "case":
      return {
        ...expr,
        whens: expr.whens.map((branch) => ({
          when: expandProjectionAliasesInExpr(branch.when, projections, windows),
          value: expandProjectionAliasesInExpr(branch.value, projections, windows),
        })),
        ...(expr.else === undefined
          ? {}
          : { else: expandProjectionAliasesInExpr(expr.else, projections, windows) }),
      };
    case "literal":
      return expr;
  }
}

function havingToExpr(
  expr: PgNode,
  scope: SqlScope,
  context: SqlParseContext,
  aggregates: AggregateSpec,
  hiddenAggregates: Set<string>,
  outputAliases: Set<string>,
): Expr {
  const previous = context.havingAggregates;
  context.havingAggregates = { aggregates, hiddenAggregates, outputAliases };
  try {
    return exprToLakeql(expr, scope, context);
  } finally {
    if (previous === undefined) delete context.havingAggregates;
    else context.havingAggregates = previous;
  }
}

function registerHavingAggregate(expr: PgNode, scope: SqlScope, context: SqlParseContext): string {
  const havingAggregates = context.havingAggregates;
  if (havingAggregates === undefined) throwUnsupported("HAVING aggregate context is missing");
  const spec = aggregateCallToSpec(expr, scope, context);
  for (const [alias, aggregate] of Object.entries(havingAggregates.aggregates)) {
    if (JSON.stringify(aggregate) === JSON.stringify(spec)) return alias;
  }
  let alias = `__lakeql_having_${context.nextHavingAggregateId}`;
  context.nextHavingAggregateId += 1;
  while (
    havingAggregates.outputAliases.has(alias) ||
    havingAggregates.aggregates[alias] !== undefined
  ) {
    alias = `__lakeql_having_${context.nextHavingAggregateId}`;
    context.nextHavingAggregateId += 1;
  }
  havingAggregates.aggregates[alias] = spec;
  havingAggregates.hiddenAggregates.add(alias);
  return alias;
}

function newSqlParseContext(options: SqlParseOptions = {}): SqlParseContext {
  return {
    scalarSubqueries: {},
    nextScalarSubqueryId: 0,
    windows: {},
    nextWindowId: 0,
    nextHavingAggregateId: 0,
    windowFrames: [],
    nextWindowFrameId: 0,
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
    case "cast":
      return castToExpr(expr, scope, context);
    case "call":
      if (context.havingAggregates !== undefined && isAggregateCall(expr)) {
        return { kind: "column", name: registerHavingAggregate(expr, scope, context) };
      }
      if ("over" in expr && expr.over !== undefined) {
        return { kind: "column", name: registerSyntheticWindow(expr, scope, context) };
      }
      if (functionName(expr.function) === "exists") {
        return existsSubqueryToExpr(expr, context);
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

function castToExpr(
  expr: PgNode,
  scope = SqlScope.empty(),
  context: SqlParseContext = newSqlParseContext(),
): Expr {
  const target = String(asNode(expr.to, "cast target").name ?? "").toLowerCase();
  if (target === "interval") {
    const operand = exprToLakeql(asNode(expr.operand, "interval literal"), scope, context);
    if (operand.kind !== "literal" || typeof operand.value !== "string") {
      throwType("INTERVAL requires a string literal", { target });
    }
    return literal(intervalValue(operand.value));
  }
  throwUnsupported(`Unsupported SQL cast to ${target}`);
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
    const extracted = maybeSubqueryJoin(predicate, scope, context);
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

function maybeSubqueryJoin(
  expr: PgNode,
  scope: SqlScope,
  context: SqlParseContext,
): SqlSubqueryJoinAst | undefined {
  if (expr.type === "binary") {
    const op = String(expr.op).toUpperCase();
    if (op !== "IN" && op !== "NOT IN") return undefined;
    const right = asNode(expr.right, "IN right side");
    if (right.type !== "select") return undefined;
    return subqueryJoinToAst(
      asNode(expr.left, "IN left side"),
      right,
      scope,
      op === "NOT IN",
      context,
    );
  }
  const exists = existsCallFromPredicate(expr);
  if (exists === undefined) return undefined;
  return existsSubqueryJoinToAst(exists.call, scope, exists.negated, context);
}

function subqueryJoinToAst(
  left: PgNode,
  subquery: PgNode,
  outerScope: SqlScope,
  negated: boolean,
  context: SqlParseContext,
): SqlSubqueryJoinAst {
  const from = optionalArray(subquery.from);
  if (from.length !== 1) throwUnsupported("IN subqueries must select from one table");
  const source = sourceTable(from[0]);
  const subqueryScope = new SqlScope(source);
  const aggregates: AggregateSpec = {};
  const hiddenAggregates = new Set<string>();
  const outputAliases = new Set<string>();
  const leftKey = subqueryLeftKeys(left, outerScope);
  const directLeftKeyCount = leftKey.length;
  const rightKey = optionalArray(subquery.columns).map((column) => {
    const item = asNode(column, "IN subquery select item");
    const key = subqueryScope.refName(asNode(item.expr, "IN subquery key"));
    ensureUniqueOutputAlias(aliasName(item.alias) ?? key, outputAliases);
    return key;
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
  applyCorrelatedSubqueryWhere(out, subquery, outerScope, subqueryScope, context);
  if (
    (subquery.groupBy !== undefined || subquery.having !== undefined) &&
    out.predicate !== undefined
  ) {
    throwUnsupported(
      "Correlated grouped IN subqueries with non-equality predicates are not supported",
    );
  }
  if (subquery.groupBy !== undefined) {
    out.groupBy = optionalArray(subquery.groupBy).map((expr) =>
      subqueryScope.refName(asNode(expr, "IN subquery GROUP BY expression")),
    );
  }
  if (subquery.groupBy !== undefined || subquery.having !== undefined) {
    out.groupBy = appendUnique(out.groupBy ?? [], out.rightKey.slice(directLeftKeyCount));
  }
  if (subquery.having !== undefined) {
    out.having = havingToExpr(
      asNode(subquery.having, "IN subquery HAVING"),
      subqueryScope,
      context,
      aggregates,
      hiddenAggregates,
      outputAliases,
    );
  }
  if (Object.keys(aggregates).length > 0) out.aggregates = aggregates;
  if (hiddenAggregates.size > 0) out.hiddenAggregates = [...hiddenAggregates];
  if (subquery.orderBy !== undefined) {
    out.orderBy = optionalArray(subquery.orderBy).map((term) => orderByToTerm(term, subqueryScope));
  }
  const limit = subquery.limit;
  if (limit !== undefined) {
    const node = asNode(limit, "IN subquery LIMIT");
    if (node.limit !== undefined) out.limit = nonNegativeInteger(node.limit, "LIMIT", context);
    if (node.offset !== undefined) out.offset = nonNegativeInteger(node.offset, "OFFSET", context);
  }
  return out;
}

function existsCallFromPredicate(expr: PgNode): { call: PgNode; negated: boolean } | undefined {
  if (expr.type === "call" && functionName(expr.function) === "exists") {
    return { call: expr, negated: false };
  }
  if (expr.type !== "unary" || String(expr.op).toUpperCase() !== "NOT") return undefined;
  const operand = asNode(expr.operand, "NOT operand");
  if (operand.type !== "call" || functionName(operand.function) !== "exists") return undefined;
  return { call: operand, negated: true };
}

function existsSubqueryJoinToAst(
  expr: PgNode,
  outerScope: SqlScope,
  negated: boolean,
  context: SqlParseContext,
): SqlSubqueryJoinAst | undefined {
  const subquery = existsSubquery(expr);
  rejectPresent(subquery, "orderBy", "ORDER BY in EXISTS subqueries is not supported");
  rejectPresent(subquery, "limit", "LIMIT in EXISTS subqueries is not supported");
  const from = optionalArray(subquery.from);
  if (from.length !== 1) throwUnsupported("EXISTS subqueries must select from one table");
  const source = sourceTable(from[0]);
  const subqueryScope = new SqlScope(source);
  const aggregates: AggregateSpec = {};
  const hiddenAggregates = new Set<string>();
  const outputAliases = new Set<string>();
  const out: SqlSubqueryJoinAst = {
    source: source.source,
    type: negated ? "anti" : "semi",
    leftKey: [],
    rightKey: [],
  };
  applyCorrelatedSubqueryWhere(out, subquery, outerScope, subqueryScope, context);
  if (subquery.groupBy !== undefined || subquery.having !== undefined) {
    if (out.predicate !== undefined) {
      throwUnsupported(
        "Correlated grouped EXISTS subqueries with non-equality predicates are not supported",
      );
    }
    if (out.leftKey.length === 0) return undefined;
    if (subquery.groupBy !== undefined) {
      out.groupBy = optionalArray(subquery.groupBy).map((expr) =>
        subqueryScope.refName(asNode(expr, "EXISTS subquery GROUP BY expression")),
      );
    }
    out.groupBy = appendUnique(out.groupBy ?? [], out.rightKey);
    if (subquery.having !== undefined) {
      out.having = havingToExpr(
        asNode(subquery.having, "EXISTS subquery HAVING"),
        subqueryScope,
        context,
        aggregates,
        hiddenAggregates,
        outputAliases,
      );
    }
    if (Object.keys(aggregates).length > 0) out.aggregates = aggregates;
    if (hiddenAggregates.size > 0) out.hiddenAggregates = [...hiddenAggregates];
    return out;
  }
  return out.leftKey.length === 0 && out.predicate === undefined ? undefined : out;
}

function appendUnique(values: string[], additions: string[]): string[] {
  const out = [...values];
  const seen = new Set(out);
  for (const value of additions) {
    if (seen.has(value)) continue;
    seen.add(value);
    out.push(value);
  }
  return out;
}

function existsSubquery(expr: PgNode): PgNode {
  const args = optionalArray(expr.args);
  if (args.length !== 1) throwUnsupported("EXISTS requires exactly one subquery");
  const subquery = asNode(args[0], "EXISTS subquery");
  if (subquery.type !== "select") throwUnsupported("EXISTS requires a SELECT subquery");
  return subquery;
}

function applyCorrelatedSubqueryWhere(
  out: SqlSubqueryJoinAst,
  subquery: PgNode,
  outerScope: SqlScope,
  subqueryScope: SqlScope,
  context: SqlParseContext,
): void {
  if (subquery.where === undefined) return;
  const residual: PgNode[] = [];
  const correlated: PgNode[] = [];
  for (const predicate of flattenWhereConjuncts(asNode(subquery.where, "subquery WHERE"))) {
    const pair = correlatedJoinKeyPair(predicate, outerScope, subqueryScope);
    if (pair === undefined) {
      if (predicateReferencesOuterScope(predicate, outerScope)) {
        correlated.push(predicate);
        continue;
      }
      residual.push(predicate);
      continue;
    }
    out.leftKey.push(pair.leftKey);
    out.rightKey.push(pair.rightKey);
  }
  if (correlated.length === 1) {
    const leftAlias = outerScope.primaryAlias();
    if (leftAlias !== undefined) out.leftAlias = leftAlias;
    const alias = subqueryScope.primaryAlias();
    if (alias !== undefined) out.alias = alias;
    out.predicate = exprToLakeql(
      correlated[0] as PgNode,
      combinedScope(outerScope, subqueryScope),
      context,
    );
  } else if (correlated.length > 1) {
    const leftAlias = outerScope.primaryAlias();
    if (leftAlias !== undefined) out.leftAlias = leftAlias;
    const alias = subqueryScope.primaryAlias();
    if (alias !== undefined) out.alias = alias;
    out.predicate = {
      kind: "logical",
      op: "and",
      operands: correlated.map((predicate) =>
        exprToLakeql(predicate, combinedScope(outerScope, subqueryScope), context),
      ),
    };
  }
  if (residual.length === 1) {
    out.where = exprToLakeql(residual[0] as PgNode, subqueryScope, context);
  } else if (residual.length > 1) {
    out.where = {
      kind: "logical",
      op: "and",
      operands: residual.map((predicate) => exprToLakeql(predicate, subqueryScope, context)),
    };
  }
}

function combinedScope(left: SqlScope, right: SqlScope): SqlScope {
  const scope = SqlScope.empty();
  for (const source of left.sources()) scope.add(source);
  for (const source of right.sources()) scope.add(source);
  return scope;
}

function predicateReferencesOuterScope(predicate: PgNode, outerScope: SqlScope): boolean {
  let found = false;
  visitPgRefs(predicate, (ref) => {
    if (outerScope.qualifiedRefName(ref) !== undefined) found = true;
  });
  return found;
}

function visitPgRefs(value: unknown, visit: (ref: PgNode) => void): void {
  if (!isRecord(value)) return;
  if (value.type === "ref") visit(value as PgNode);
  for (const child of Object.values(value)) {
    if (Array.isArray(child)) {
      for (const item of child) visitPgRefs(item, visit);
    } else {
      visitPgRefs(child, visit);
    }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function correlatedJoinKeyPair(
  predicate: PgNode,
  outerScope: SqlScope,
  subqueryScope: SqlScope,
): { leftKey: string; rightKey: string } | undefined {
  if (predicate.type !== "binary" || String(predicate.op) !== "=") return undefined;
  const left = asNode(predicate.left, "correlated left key");
  const right = asNode(predicate.right, "correlated right key");
  const leftOuter = outerScope.qualifiedRefName(left);
  const leftSubquery = subqueryScope.qualifiedRefName(left);
  const rightOuter = outerScope.qualifiedRefName(right);
  const rightSubquery = subqueryScope.qualifiedRefName(right);
  if (leftOuter !== undefined && rightSubquery !== undefined) {
    return { leftKey: leftOuter, rightKey: rightSubquery };
  }
  if (rightOuter !== undefined && leftSubquery !== undefined) {
    return { leftKey: rightOuter, rightKey: leftSubquery };
  }
  return undefined;
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
  return registerScalarSubquery(query, outputColumns[0] as string, context);
}

function existsSubqueryToExpr(expr: PgNode, context: SqlParseContext): Expr {
  const subquery = existsSubquery(expr);
  rejectPresent(subquery, "orderBy", "ORDER BY in EXISTS subqueries is not supported");
  rejectPresent(subquery, "limit", "LIMIT in EXISTS subqueries is not supported");
  rejectPresent(subquery, "windows", "Window functions in EXISTS subqueries are not supported");
  const from = optionalArray(subquery.from);
  if (from.length !== 1) throwUnsupported("EXISTS subqueries must select from one table");
  const source = sourceTable(from[0]);
  if (subquery.groupBy !== undefined || subquery.having !== undefined) {
    return registerExistsSubquery(groupedExistsSubqueryToAst(subquery, source, context), context);
  }
  const query: SqlQueryAst = {
    source: source.source,
    aggregates: { __lakeql_exists_count: { op: "count" } },
  };
  if (subquery.where !== undefined) {
    query.where = exprToLakeql(
      asNode(subquery.where, "EXISTS subquery WHERE"),
      new SqlScope(source),
      context,
    );
  }
  const count = registerScalarSubquery(query, "__lakeql_exists_count", context);
  return { kind: "compare", op: "gt", left: count, right: literal(0) };
}

function groupedExistsSubqueryToAst(
  subquery: PgNode,
  source: SourceTable,
  context: SqlParseContext,
): SqlQueryAst {
  const scope = new SqlScope(source);
  const query: SqlQueryAst = { source: source.source };
  if (subquery.where !== undefined) {
    query.where = exprToLakeql(asNode(subquery.where, "EXISTS subquery WHERE"), scope, context);
  }
  if (subquery.groupBy !== undefined) {
    query.groupBy = optionalArray(subquery.groupBy).map((expr) =>
      scope.refName(asNode(expr, "EXISTS subquery GROUP BY expression")),
    );
    query.select = query.groupBy;
  }
  const aggregates: AggregateSpec = {};
  const hiddenAggregates = new Set<string>();
  const outputAliases = new Set<string>(query.select ?? []);
  if (subquery.having !== undefined) {
    query.having = havingToExpr(
      asNode(subquery.having, "EXISTS subquery HAVING"),
      scope,
      context,
      aggregates,
      hiddenAggregates,
      outputAliases,
    );
  }
  if (Object.keys(aggregates).length > 0) query.aggregates = aggregates;
  if (hiddenAggregates.size > 0) query.hiddenAggregates = [...hiddenAggregates];
  return query;
}

function registerScalarSubquery(
  query: SqlQueryAst,
  column: string,
  context: SqlParseContext,
): Expr {
  const id = `scalar_${context.nextScalarSubqueryId}`;
  context.nextScalarSubqueryId += 1;
  context.scalarSubqueries[id] = { query, column };
  return { kind: "call", fn: "__lakeql_scalar_subquery", args: [{ kind: "literal", value: id }] };
}

function registerExistsSubquery(query: SqlQueryAst, context: SqlParseContext): Expr {
  const id = `scalar_${context.nextScalarSubqueryId}`;
  context.nextScalarSubqueryId += 1;
  context.scalarSubqueries[id] = { query, column: "__lakeql_exists", mode: "exists" };
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
  const caseValue =
    expr.value === null || expr.value === undefined
      ? undefined
      : exprToLakeql(asNode(expr.value, "CASE expression"), scope, context);
  const whens: Extract<Expr, { kind: "case" }>["whens"] = optionalArray(expr.whens).map(
    (branch) => {
      const node = asNode(branch, "CASE branch");
      const when = exprToLakeql(asNode(node.when, "CASE WHEN expression"), scope, context);
      return {
        when:
          caseValue === undefined
            ? when
            : { kind: "compare", op: "eq", left: caseValue, right: when },
        value: exprToLakeql(asNode(node.value, "CASE THEN expression"), scope, context),
      };
    },
  );
  const out: Extract<Expr, { kind: "case" }> = { kind: "case", whens };
  if (expr.else !== undefined) {
    out.else = exprToLakeql(asNode(expr.else, "CASE ELSE expression"), scope, context);
  }
  validateCaseResultTypes(out);
  return out;
}

type StaticScalarType =
  | "binary"
  | "bigint"
  | "boolean"
  | "interval"
  | "number"
  | "string"
  | "timestamp";

function validateCaseResultTypes(expr: Extract<Expr, { kind: "case" }>): void {
  const resultTypes = caseResultTypes(expr);
  if (resultTypes.size <= 1) return;
  throwType("CASE result branches must have compatible literal types", {
    types: [...resultTypes].sort(),
  });
}

function caseResultTypes(expr: Extract<Expr, { kind: "case" }>): Set<StaticScalarType> {
  const types = new Set<StaticScalarType>();
  for (const branch of expr.whens) {
    const type = staticExprResultType(branch.value);
    if (type !== undefined) types.add(type);
  }
  if (expr.else !== undefined) {
    const type = staticExprResultType(expr.else);
    if (type !== undefined) types.add(type);
  }
  return types;
}

function staticExprResultType(expr: Expr): StaticScalarType | undefined {
  switch (expr.kind) {
    case "literal":
      return staticScalarType(expr.value);
    case "compare":
    case "between":
    case "in":
    case "like":
    case "logical":
    case "not":
    case "null-check":
      return "boolean";
    case "arithmetic":
      return "number";
    case "case": {
      const resultTypes = caseResultTypes(expr);
      return resultTypes.size === 1 ? [...resultTypes][0] : undefined;
    }
    case "call":
    case "column":
      return undefined;
  }
}

function staticScalarType(value: Scalar): StaticScalarType | undefined {
  if (value === null) return undefined;
  if (value instanceof Uint8Array) return "binary";
  if (isTimestampValue(value)) return "timestamp";
  if (isIntervalValue(value)) return "interval";
  if (typeof value === "boolean") return "boolean";
  if (typeof value === "bigint") return "bigint";
  if (typeof value === "number") return "number";
  return "string";
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

function hasWindowOver(expr: PgNode): boolean {
  return "over" in expr && expr.over !== undefined;
}

function registerSyntheticWindow(expr: PgNode, scope: SqlScope, context: SqlParseContext): string {
  const alias = syntheticWindowAlias(context);
  context.windows[alias] = windowCallToExpr(expr, scope, context);
  return alias;
}

function syntheticWindowAlias(context: SqlParseContext): string {
  const alias = `__lakeql_window_${context.nextWindowId}`;
  context.nextWindowId += 1;
  return alias;
}

function ensureUniqueOutputAlias(alias: string, aliases: Set<string>): void {
  if (PROTOTYPE_MUTATION_KEYS.has(alias)) throwUnsupported(`Unsafe SELECT output alias ${alias}`);
  if (aliases.has(alias)) throwUnsupported(`Duplicate SELECT output alias ${alias}`);
  aliases.add(alias);
}

function windowCallToExpr(expr: PgNode, scope: SqlScope, context: SqlParseContext): WindowExpr {
  if (!hasWindowOver(expr)) throwUnsupported("Window call requires OVER");
  const args = optionalArray(expr.args);
  const fnName = functionName(expr.function);
  let aggregate = aggregateOpOrUndefined(fnName);
  if (expr.distinct !== undefined) {
    if (expr.distinct !== "distinct") {
      throwUnsupported("Only DISTINCT window arguments are supported");
    }
    if (aggregate !== "count")
      throwUnsupported("Only COUNT(DISTINCT x) window arguments are supported");
    aggregate = "count_distinct";
  }
  const windowFn: WindowExpr["fn"] =
    aggregate === undefined ? windowFunctionName(fnName) : { aggregate };
  const over = callOverToWindowSpec(asNode(expr.over, "OVER clause"), scope, context);
  const out: WindowExpr = {
    fn: windowFn,
    args: args
      .filter((arg) => !isWildcard(asNode(arg, "window argument")))
      .map((arg) => exprToLakeql(asNode(arg, "window argument"), scope, context)),
    over: over.spec,
  };
  if (over.ignoreNulls !== undefined) out.ignoreNulls = over.ignoreNulls;
  if (expr.filter !== undefined) {
    out.filter = exprToLakeql(asNode(expr.filter, "window FILTER predicate"), scope, context);
  }
  return out;
}

function windowFunctionName(name: string): Extract<WindowExpr["fn"], string> {
  switch (name.toLowerCase()) {
    case "row_number":
    case "rank":
    case "dense_rank":
    case "percent_rank":
    case "cume_dist":
    case "ntile":
    case "lag":
    case "lead":
    case "first_value":
    case "last_value":
    case "nth_value":
      return name.toLowerCase() as Extract<WindowExpr["fn"], string>;
    default:
      throwUnsupported(`Unsupported window function ${name}`);
  }
}

function callOverToWindowSpec(
  over: PgNode,
  scope: SqlScope,
  context: SqlParseContext,
): { spec: WindowExpr["over"]; ignoreNulls?: boolean } {
  rejectPresent(over, "name", "Named windows require the window pre-pass");
  const prepass = nextWindowFrame(context);
  const spec: WindowExpr["over"] = {
    partitionBy: optionalArray(over.partitionBy).map((expr) =>
      exprToLakeql(asNode(expr, "window PARTITION BY expression"), scope, context),
    ),
    orderBy: optionalArray(over.orderBy).map((term) => windowOrderByToTerm(term, scope, context)),
  };
  if (prepass?.text !== undefined) spec.frame = frameTextToSpec(prepass.text, scope, context);
  return prepass?.ignoreNulls === undefined ? { spec } : { spec, ignoreNulls: prepass.ignoreNulls };
}

function nextWindowFrame(context: SqlParseContext): SqlWindowPrepassFrame | undefined {
  const frame = context.windowFrames[context.nextWindowFrameId];
  context.nextWindowFrameId += 1;
  return frame;
}

function frameTextToSpec(text: string, scope: SqlScope, context: SqlParseContext): WindowFrame {
  const { body, exclude } = splitFrameExclude(text);
  const modeMatch = /^(rows|range|groups)\b/iu.exec(body.trim());
  if (modeMatch === null || modeMatch[1] === undefined) {
    throwParse("Window frame must start with ROWS, RANGE, or GROUPS");
  }
  const mode = modeMatch[1].toLowerCase() as WindowFrame["mode"];
  const rest = body.trim().slice(modeMatch[0].length).trim();
  const bounds = frameBounds(rest, scope, context);
  return { mode, ...bounds, exclude };
}

function splitFrameExclude(text: string): {
  body: string;
  exclude: WindowFrame["exclude"];
} {
  const match = /\bexclude\s+(no\s+others|current\s+row|group|ties)\s*$/iu.exec(text);
  if (match === null) return { body: text, exclude: "no-others" };
  const value = String(match[1]).toLowerCase().replace(/\s+/gu, "-");
  const exclude =
    value === "no-others" || value === "current-row" || value === "group" || value === "ties"
      ? value
      : "no-others";
  return { body: text.slice(0, match.index).trim(), exclude };
}

function frameBounds(
  text: string,
  scope: SqlScope,
  context: SqlParseContext,
): Pick<WindowFrame, "start" | "end"> {
  const trimmed = text.trim();
  if (trimmed.length === 0) {
    return { start: { kind: "unbounded-preceding" }, end: { kind: "current-row" } };
  }
  if (/^between\b/iu.test(trimmed)) {
    const between = trimmed.replace(/^between\b/iu, "").trim();
    const andIndex = findTopLevelAnd(between);
    if (andIndex === -1) throwParse("Window frame BETWEEN requires AND");
    return {
      start: frameBound(between.slice(0, andIndex).trim(), scope, context),
      end: frameBound(between.slice(andIndex + "and".length).trim(), scope, context),
    };
  }
  return {
    start: frameBound(trimmed, scope, context),
    end: { kind: "current-row" },
  };
}

function frameBound(text: string, scope: SqlScope, context: SqlParseContext): WindowFrameBound {
  if (/^unbounded\s+preceding$/iu.test(text)) return { kind: "unbounded-preceding" };
  if (/^unbounded\s+following$/iu.test(text)) return { kind: "unbounded-following" };
  if (/^current\s+row$/iu.test(text)) return { kind: "current-row" };
  const preceding = /\bpreceding$/iu.exec(text);
  if (preceding !== null) {
    return {
      kind: "preceding",
      offset: parseFrameOffset(text.slice(0, preceding.index).trim(), scope, context),
    };
  }
  const following = /\bfollowing$/iu.exec(text);
  if (following !== null) {
    return {
      kind: "following",
      offset: parseFrameOffset(text.slice(0, following.index).trim(), scope, context),
    };
  }
  throwParse(`Unsupported window frame bound ${text}`);
}

function parseFrameOffset(text: string, scope: SqlScope, context: SqlParseContext): Expr {
  if (text.length === 0) throwParse("Window frame offset is required");
  let statements: unknown[];
  try {
    statements = parse(`select ${text} as frame_offset from __lakeql_frame`) as unknown[];
  } catch (error) {
    if (error instanceof Error) throwParse(error.message);
    throwParse("Invalid window frame offset");
  }
  const statement = asNode(statements[0], "frame offset wrapper");
  const column = asNode(optionalArray(statement.columns)[0], "frame offset column");
  return exprToLakeql(asNode(column.expr, "frame offset expression"), scope, context);
}

function findTopLevelAnd(text: string): number {
  let depth = 0;
  let quote: "'" | '"' | "`" | undefined;
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];
    if (quote !== undefined) {
      if (char === quote) {
        if (next === quote) {
          index += 1;
          continue;
        }
        quote = undefined;
      }
      continue;
    }
    if (char === "'" || char === '"' || char === "`") {
      quote = char;
      continue;
    }
    if (char === "(") depth += 1;
    else if (char === ")") depth = Math.max(0, depth - 1);
    if (
      depth === 0 &&
      text.slice(index, index + "and".length).toLowerCase() === "and" &&
      /(?:^|[^A-Za-z0-9_])$/u.test(text.slice(0, index)) &&
      !/[A-Za-z0-9_]/u.test(text[index + "and".length] ?? "")
    ) {
      return index;
    }
  }
  return -1;
}

function windowOrderByToTerm(
  value: unknown,
  scope: SqlScope,
  context: SqlParseContext,
): WindowOrderTerm {
  const node = asNode(value, "window ORDER BY term");
  const term: WindowOrderTerm = {
    expr: exprToLakeql(asNode(node.by, "window ORDER BY expression"), scope, context),
  };
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
  if (hasWindowOver(expr)) return false;
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
  cte?: SqlCteAst;
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

  sources(): SourceTable[] {
    return [...new Set(this.aliases.values())];
  }

  primaryAlias(): string | undefined {
    return this.sources()[0]?.alias;
  }

  refName(expr: PgNode): string {
    const name = this.resolveQualifiedRefName(expr, true);
    if (name !== undefined) return name;
    if (expr.type !== "ref" || typeof expr.name !== "string") {
      throwUnsupported("Only column references are supported here");
    }
    const qualifier = expr.table === undefined ? "" : nameNodeToString(expr.table);
    throwUnsupported(`Unknown SQL table qualifier ${qualifier}`);
  }

  qualifiedRefName(expr: PgNode): string | undefined {
    if (expr.type !== "ref" || typeof expr.name !== "string") return undefined;
    return this.resolveQualifiedRefName(expr, false);
  }

  private resolveQualifiedRefName(expr: PgNode, allowUnqualified: boolean): string | undefined {
    if (expr.type !== "ref" || typeof expr.name !== "string") {
      throwUnsupported("Only column references are supported here");
    }
    if (expr.table === undefined) return allowUnqualified ? expr.name : undefined;
    const qualifier = nameNodeToString(expr.table);
    const source = this.aliases.get(qualifier);
    if (source === undefined) return undefined;
    if (this.aliases.size <= 2) return expr.name;
    return `${source.alias}.${expr.name}`;
  }
}

function sourceTable(value: unknown, context?: SqlParseContext): SourceTable {
  const node = asNode(value, "FROM item");
  if (node.type === "call") return readParquetSourceTable(node);
  if (node.type === "statement") return derivedSourceTable(node, context);
  if (node.type !== "table") throwUnsupported("Only table FROM sources are supported");
  if (node.join !== undefined) throwUnsupported("Unexpected JOIN position");
  const source = nameNodeToString(node.name);
  return { source, alias: aliasName(node.alias) ?? aliasName(node.name) ?? source };
}

function derivedSourceTable(node: PgNode, context: SqlParseContext | undefined): SourceTable {
  if (context === undefined) throwUnsupported("Derived tables are not supported here");
  const alias = aliasName(node.alias);
  if (alias === undefined) throwUnsupported("Derived tables require an alias");
  const statement = asNode(node.statement, "derived table SELECT");
  if (statement.type !== "select") throwUnsupported("Derived tables must be SELECT statements");
  const query = selectStatementToAst(statement, context);
  return { source: alias, alias, cte: { name: alias, query } };
}

function readParquetSourceTable(node: PgNode): SourceTable {
  const functionName = nameNodeToString(node.function).toLowerCase();
  if (functionName !== "read_parquet") {
    throwUnsupported("Only read_parquet FROM functions are supported");
  }
  const args = optionalArray(node.args);
  if (args.length !== 1) throwUnsupported("read_parquet requires exactly one path argument");
  const arg = asNode(args[0], "read_parquet argument");
  if (arg.type !== "string" || typeof arg.value !== "string") {
    throwUnsupported("read_parquet path must be a string literal");
  }
  return { source: arg.value, alias: aliasName(node.alias) ?? arg.value };
}

function joinToAst(
  value: unknown,
  joinedSources: readonly SourceTable[],
): { join: SqlJoinAst; right: SourceTable } {
  const node = asNode(value, "JOIN source");
  if (node.type !== "table" && node.type !== "call") {
    throwUnsupported("Only table JOIN sources are supported");
  }
  const join = node.join === undefined ? undefined : asNode(node.join, "JOIN");
  const joinType = join === undefined ? "" : String(join.type).toUpperCase();
  if (join === undefined || joinType === "CROSS JOIN") {
    const right = sourceTable({ ...node, join: undefined });
    const left = joinedSources[0];
    if (left === undefined) throwUnsupported("JOIN requires a left source");
    return {
      right,
      join: {
        source: right.source,
        leftAlias: left.alias,
        alias: right.alias,
        type: "cross",
        leftKey: [],
        rightKey: [],
      },
    };
  }
  if (
    joinType !== "INNER JOIN" &&
    joinType !== "LEFT JOIN" &&
    joinType !== "RIGHT JOIN" &&
    joinType !== "FULL JOIN"
  ) {
    throwUnsupported(`Unsupported JOIN type ${joinType}`);
  }
  const right = sourceTable({ ...node, join: undefined });
  const left = joinedSources[0];
  if (left === undefined) throwUnsupported("JOIN requires a left source");
  if (join.using !== undefined) {
    const using = optionalArray(join.using);
    if (using.length === 0) throwUnsupported("JOIN USING requires at least one column");
    const keys = using.map(nameNodeToString);
    return {
      right,
      join: {
        source: right.source,
        leftAlias: left.alias,
        alias: right.alias,
        type: sqlJoinType(joinType),
        leftKey: keys.map((key) => `${left.alias}.${key}`),
        rightKey: keys.map((key) => `${right.alias}.${key}`),
      },
    };
  }
  const on = asNode(join.on, "JOIN ON");
  const keys = joinKeysFromPredicate(on, joinedSources, right);
  return {
    right,
    join: {
      source: right.source,
      leftAlias: left.alias,
      alias: right.alias,
      type: sqlJoinType(joinType),
      leftKey: keys?.leftKey ?? [],
      rightKey: keys?.rightKey ?? [],
      ...(keys === undefined
        ? { predicate: exprToLakeql(on, joinScope(joinedSources, right)) }
        : {}),
    },
  };
}

function sqlJoinType(joinType: string): SqlJoinAst["type"] {
  if (joinType === "LEFT JOIN") return "left";
  if (joinType === "RIGHT JOIN") return "right";
  if (joinType === "FULL JOIN") return "full";
  return "inner";
}

function maybeQualifiedJoinKey(
  expr: PgNode,
  joinedSources: readonly SourceTable[],
  right: SourceTable,
): { side: "left" | "right"; key: string } | undefined {
  if (expr.type !== "ref" || typeof expr.name !== "string" || expr.table === undefined)
    return undefined;
  const qualifier = nameNodeToString(expr.table);
  if (qualifier === right.alias || qualifier === right.source) {
    return { side: "right", key: `${right.alias}.${expr.name}` };
  }
  const left = joinedSources.find(
    (source) => qualifier === source.alias || qualifier === source.source,
  );
  if (left !== undefined) return { side: "left", key: `${left.alias}.${expr.name}` };
  return undefined;
}

function joinKeysFromPredicate(
  expr: PgNode,
  joinedSources: readonly SourceTable[],
  right: SourceTable,
): { leftKey: string[]; rightKey: string[] } | undefined {
  const conjuncts = flattenJoinConjuncts(expr);
  const leftKey: string[] = [];
  const rightKey: string[] = [];
  for (const conjunct of conjuncts) {
    if (conjunct.type !== "binary" || String(conjunct.op) !== "=") return undefined;
    const first = maybeQualifiedJoinKey(
      asNode(conjunct.left, "JOIN left key"),
      joinedSources,
      right,
    );
    const second = maybeQualifiedJoinKey(
      asNode(conjunct.right, "JOIN right key"),
      joinedSources,
      right,
    );
    if (first === undefined || second === undefined) return undefined;
    if (first.side === "left" && second.side === "right") {
      leftKey.push(first.key);
      rightKey.push(second.key);
    } else if (second.side === "left" && first.side === "right") {
      leftKey.push(second.key);
      rightKey.push(first.key);
    } else {
      throwUnsupported("JOIN ON must compare left table key to right table key");
    }
  }
  return { leftKey, rightKey };
}

function joinScope(joinedSources: readonly SourceTable[], right: SourceTable): SqlScope {
  const scope = SqlScope.empty();
  for (const source of joinedSources) scope.add(source);
  scope.add(right);
  return scope;
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
    isTimestampValue(value) ||
    isIntervalValue(value) ||
    (typeof value === "number" && Number.isFinite(value))
  );
}

function formatExpr(expr: Expr, ast?: SqlQueryAst): string {
  switch (expr.kind) {
    case "literal":
      return formatLiteral(expr.value);
    case "column":
      if (ast?.hiddenAggregates?.includes(expr.name) === true) {
        const aggregate = ast.aggregates?.[expr.name];
        if (aggregate !== undefined) return formatAggregate(aggregate);
      }
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
  if (subquery.mode === "exists") return `exists (${formatSql(subquery.query)})`;
  return `(${formatSql(subquery.query)})`;
}

function formatOrderByTerm(term: OrderByTerm): string {
  const parts = [formatIdentifier(term.column)];
  if (term.direction) parts.push(term.direction);
  if (term.nulls) parts.push("nulls", term.nulls);
  return parts.join(" ");
}

function sqlJoins(ast: SqlQueryAst): SqlJoinAst[] {
  if (ast.joins !== undefined) return ast.joins;
  return ast.join === undefined ? [] : [ast.join];
}

function formatJoins(leftSource: string, joins: readonly SqlJoinAst[]): string {
  return joins.map((join, index) => formatJoin(leftSource, join, index === 0)).join("");
}

function formatJoin(leftSource: string, join: SqlJoinAst, includeLeftAlias: boolean): string {
  const leftAlias =
    includeLeftAlias && join.leftAlias !== leftSource ? ` ${formatIdentifier(join.leftAlias)}` : "";
  const source = formatIdentifier(join.source);
  const alias = join.alias === join.source ? "" : ` ${formatIdentifier(join.alias)}`;
  const type =
    join.type === "left"
      ? "left join"
      : join.type === "right"
        ? "right join"
        : join.type === "full"
          ? "full join"
          : "join";
  if (join.type === "cross") {
    return `${leftAlias} cross join ${source}${alias}`;
  }
  if (join.predicate !== undefined) {
    return `${leftAlias} ${type} ${source}${alias} on ${formatExpr(join.predicate)}`;
  }
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
  if (subqueryJoin.predicate !== undefined) {
    const existsWhere = [
      ...subqueryJoin.leftKey.map((leftKey, index) => {
        const rightKey = subqueryJoin.rightKey[index];
        if (rightKey === undefined) throwUnsupported("IN subquery key counts must match");
        return `${formatSubqueryColumn(rightKey, subqueryJoin.alias)} = ${formatSubqueryColumn(leftKey, subqueryJoin.leftAlias)}`;
      }),
      formatExpr(subqueryJoin.predicate),
      ...(subqueryJoin.where === undefined ? [] : [formatExpr(subqueryJoin.where)]),
    ].join(" and ");
    const where = existsWhere.length === 0 ? "" : ` where ${existsWhere}`;
    const source =
      subqueryJoin.alias === undefined || subqueryJoin.alias === subqueryJoin.source
        ? formatIdentifier(subqueryJoin.source)
        : `${formatIdentifier(subqueryJoin.source)} ${formatIdentifier(subqueryJoin.alias)}`;
    return `${subqueryJoin.type === "anti" ? "not " : ""}exists (select 1 from ${source}${where})`;
  }
  const left =
    subqueryJoin.leftKey.length === 1
      ? formatIdentifier(subqueryJoin.leftKey[0] ?? "")
      : `(${subqueryJoin.leftKey.map(formatIdentifier).join(", ")})`;
  const right =
    subqueryJoin.rightKey.length === 1
      ? formatIdentifier(subqueryJoin.rightKey[0] ?? "")
      : subqueryJoin.rightKey.map(formatIdentifier).join(", ");
  const where = subqueryJoin.where === undefined ? "" : ` where ${formatExpr(subqueryJoin.where)}`;
  const groupBy =
    subqueryJoin.groupBy === undefined || subqueryJoin.groupBy.length === 0
      ? ""
      : ` group by ${subqueryJoin.groupBy.map(formatIdentifier).join(", ")}`;
  const having =
    subqueryJoin.having === undefined
      ? ""
      : ` having ${formatExpr(subqueryJoin.having, subqueryFormatAst(subqueryJoin))}`;
  const orderBy =
    subqueryJoin.orderBy === undefined || subqueryJoin.orderBy.length === 0
      ? ""
      : ` order by ${subqueryJoin.orderBy.map(formatOrderByTerm).join(", ")}`;
  const limit = subqueryJoin.limit === undefined ? "" : ` limit ${subqueryJoin.limit}`;
  const offset = subqueryJoin.offset === undefined ? "" : ` offset ${subqueryJoin.offset}`;
  return `${left} ${subqueryJoin.type === "anti" ? "not in" : "in"} (select ${right} from ${formatIdentifier(subqueryJoin.source)}${where}${groupBy}${having}${orderBy}${limit}${offset})`;
}

function subqueryFormatAst(subqueryJoin: SqlSubqueryJoinAst): SqlQueryAst {
  return {
    source: subqueryJoin.source,
    ...(subqueryJoin.aggregates === undefined ? {} : { aggregates: subqueryJoin.aggregates }),
    ...(subqueryJoin.hiddenAggregates === undefined
      ? {}
      : { hiddenAggregates: subqueryJoin.hiddenAggregates }),
  };
}

function formatSubqueryColumn(column: string, alias: string | undefined): string {
  return alias === undefined
    ? formatIdentifier(column)
    : `${formatIdentifier(alias)}.${formatIdentifier(column)}`;
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

function formatLiteral(value: Scalar): string {
  if (value === null) return "null";
  if (isTimestampValue(value)) return `'${value.toJSON()}'`;
  if (isIntervalValue(value)) return `interval '${intervalToString(value).replaceAll("'", "''")}'`;
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
