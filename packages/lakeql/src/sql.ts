import {
  type AggregateSpec,
  broadcastJoin,
  type Expr,
  evaluate,
  LakeqlError,
  matches,
  type ObjectStore,
  type QueryBuilder,
  type Row,
} from "lakeql-core";
import { createParquetLake, type ParquetLakeConfig, writePartitionedParquet } from "lakeql-parquet";
import { parseSql, type SqlParameterValue, type SqlQueryAst } from "lakeql-sql";

export interface SqlQueryOptions {
  path?: string;
  tables?: Record<string, string>;
  parameters?: readonly SqlParameterValue[];
  joinMaxRightRows?: number;
}

export type SqlLake = ReturnType<typeof createParquetLake> & {
  sql(sql: string, options?: SqlQueryOptions): SqlQueryResult;
};

type ParquetLake = ReturnType<typeof createParquetLake>;

const textEncoder = new TextEncoder();
let sqlTempOrdinal = 0;

export function createLake(config: ParquetLakeConfig): SqlLake {
  const lake = createParquetLake(config) as SqlLake;
  lake.sql = (sql, options = {}) => querySql(lake, sql, options);
  return lake;
}

export function querySql(
  lake: ParquetLake,
  sql: string,
  options: SqlQueryOptions = {},
): SqlQueryResult {
  return new SqlQueryResult(() => executeSql(lake, sql, options));
}

export class SqlQueryResult {
  private readonly execute: () => Promise<Row[]>;
  private rowsPromise: Promise<Row[]> | undefined;

  constructor(execute: () => Promise<Row[]>) {
    this.execute = execute;
  }

  async toArray(): Promise<Row[]> {
    this.rowsPromise ??= this.execute();
    return await this.rowsPromise;
  }

  async first(): Promise<Row | undefined> {
    return (await this.toArray())[0];
  }

  async count(): Promise<number> {
    return (await this.toArray()).length;
  }

  async *rows(): AsyncIterable<Row> {
    for (const row of await this.toArray()) yield row;
  }

  async *batches(): AsyncIterable<Row[]> {
    const rows = await this.toArray();
    if (rows.length > 0) yield rows;
  }

  streamJson(): ReadableStream<Uint8Array> {
    return streamText(async () => jsonStringify(await this.toArray()));
  }

  streamNdjson(): ReadableStream<Uint8Array> {
    return streamText(async () =>
      (await this.toArray()).map((row) => jsonStringify(row)).join("\n"),
    );
  }

  streamCsv(): ReadableStream<Uint8Array> {
    return streamText(async () => rowsToCsv(await this.toArray()));
  }
}

async function executeSql(
  lake: ParquetLake,
  sql: string,
  options: SqlQueryOptions,
): Promise<Row[]> {
  const ast = sqlAst(sql, options);
  const bound = bindSources(ast, options);
  const materialized = await materializeCteIfNeeded(lake.store, lake, bound);
  try {
    const resolved = await resolveScalarSubqueries(lake, materialized.ast);
    if (resolved.subqueryJoin !== undefined)
      return subqueryJoinRowsFromAst(lake, resolved, options);
    if (resolved.join !== undefined) return joinRowsFromAst(lake, resolved, options);
    if (hasAggregation(resolved)) return aggregateRowsFromAst(lake.path(resolved.source), resolved);
    return await builderFromAst(lake.path(resolved.source), resolved).toArray();
  } finally {
    await materialized.cleanup();
  }
}

function sqlAst(sql: string, options: SqlQueryOptions): SqlQueryAst {
  const trimmed = sql.trim();
  const sourceSql = /\bfrom\b/iu.test(trimmed) ? trimmed : addDefaultFrom(trimmed);
  return options.parameters === undefined
    ? parseSql(sourceSql)
    : parseSql(sourceSql, { parameters: options.parameters });
}

function bindSources(ast: SqlQueryAst, options: SqlQueryOptions): SqlQueryAst {
  const tables = options.tables ?? {};
  const bindSource = (source: string): string => {
    if (source === "input" && options.path !== undefined) return options.path;
    return tables[source] ?? source;
  };
  return mapAstSources(ast, bindSource);
}

function mapAstSources(ast: SqlQueryAst, bindSource: (source: string) => string): SqlQueryAst {
  const out: SqlQueryAst = { ...ast, source: bindSource(ast.source) };
  if (ast.join !== undefined) out.join = { ...ast.join, source: bindSource(ast.join.source) };
  if (ast.subqueryJoin !== undefined) {
    out.subqueryJoin = {
      ...ast.subqueryJoin,
      source: bindSource(ast.subqueryJoin.source),
    };
  }
  if (ast.cte !== undefined) {
    out.cte = { ...ast.cte, query: mapAstSources(ast.cte.query, bindSource) };
  }
  if (ast.scalarSubqueries !== undefined) {
    out.scalarSubqueries = Object.fromEntries(
      Object.entries(ast.scalarSubqueries).map(([id, subquery]) => [
        id,
        { ...subquery, query: mapAstSources(subquery.query, bindSource) },
      ]),
    );
  }
  return out;
}

function builderFromAst(builder: QueryBuilder, ast: SqlQueryAst): QueryBuilder {
  let next = builder;
  for (const [alias, expr] of Object.entries(ast.windows ?? {})) next = next.window(alias, expr);
  if (ast.select) next = next.select(ast.select);
  if (ast.projections) next = next.project(ast.projections);
  if (ast.where) next = next.where(ast.where);
  if (ast.qualify) next = next.qualify(ast.qualify);
  if (ast.distinct === true) next = next.distinct();
  if (ast.orderBy) next = next.orderBy(ast.orderBy);
  if (ast.offset !== undefined) next = next.offset(ast.offset);
  if (ast.limit !== undefined) next = next.limit(ast.limit);
  return next;
}

function hasAggregation(ast: SqlQueryAst): boolean {
  return (
    ast.aggregates !== undefined ||
    (ast.groupBy !== undefined && ast.groupBy.length > 0) ||
    ast.having !== undefined
  );
}

function hasWindowSemantics(ast: SqlQueryAst): boolean {
  return (
    (ast.windows !== undefined && Object.keys(ast.windows).length > 0) || ast.qualify !== undefined
  );
}

async function materializeCteIfNeeded(
  store: ObjectStore,
  lake: ParquetLake,
  ast: SqlQueryAst,
): Promise<{ ast: SqlQueryAst; cleanup: () => Promise<void> }> {
  if (ast.cte === undefined) return { ast, cleanup: async () => {} };
  if (ast.source !== ast.cte.name) {
    throw new LakeqlError(
      "LAKEQL_SQL_UNSUPPORTED",
      "CTEs are only supported as the outer FROM source",
    );
  }
  if (ast.join !== undefined || ast.subqueryJoin !== undefined) {
    throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "CTEs inside JOINs are not supported yet");
  }
  const cteRows = hasAggregation(ast.cte.query)
    ? await aggregateRowsFromAst(lake.path(ast.cte.query.source), ast.cte.query)
    : await builderFromAst(lake.path(ast.cte.query.source), ast.cte.query).toArray();
  if (cteRows.length === 0) {
    throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Empty CTE results are not supported yet");
  }
  const prefix = `__lakeql_sql_cte/${safeTempName(ast.cte.name)}/${nextSqlTempId()}`;
  const written = await writePartitionedParquet(store, prefix, {
    rows: cteRows,
    maxRowsPerFile: cteRows.length,
  });
  const { cte: _cte, ...rest } = ast;
  return {
    ast: { ...rest, source: `${prefix}/*.parquet` },
    cleanup: async () => {
      await Promise.all(written.files.map((file) => store.delete(file.path)));
    },
  };
}

function nextSqlTempId(): string {
  const randomUuid = globalThis.crypto?.randomUUID?.();
  if (randomUuid !== undefined) return randomUuid;
  sqlTempOrdinal += 1;
  return `${Date.now().toString(36)}-${sqlTempOrdinal.toString(36)}`;
}

function safeTempName(name: string): string {
  return name.replaceAll(/[^A-Za-z0-9_-]/gu, "_");
}

async function resolveScalarSubqueries(lake: ParquetLake, ast: SqlQueryAst): Promise<SqlQueryAst> {
  if (ast.scalarSubqueries === undefined) return ast;
  const values = new Map<string, unknown>();
  for (const [id, subquery] of Object.entries(ast.scalarSubqueries)) {
    const rows = hasAggregation(subquery.query)
      ? await aggregateRowsFromAst(lake.path(subquery.query.source), subquery.query)
      : await builderFromAst(lake.path(subquery.query.source), subquery.query).toArray();
    if (rows.length > 1) {
      throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Scalar subquery returned more than one row");
    }
    values.set(id, rows.length === 0 ? null : (rows[0]?.[subquery.column] ?? null));
  }
  const out = { ...ast };
  if (ast.where !== undefined) out.where = replaceScalarSubqueryExpr(ast.where, values);
  if (ast.having !== undefined) out.having = replaceScalarSubqueryExpr(ast.having, values);
  if (ast.projections !== undefined) {
    out.projections = Object.fromEntries(
      Object.entries(ast.projections).map(([alias, expr]) => [
        alias,
        replaceScalarSubqueryExpr(expr, values),
      ]),
    );
  }
  delete out.scalarSubqueries;
  return out;
}

function replaceScalarSubqueryExpr(expr: Expr, values: Map<string, unknown>): Expr {
  switch (expr.kind) {
    case "call":
      if (expr.fn === "__lakeql_scalar_subquery") {
        const id = expr.args[0];
        if (id?.kind !== "literal" || typeof id.value !== "string" || !values.has(id.value)) {
          throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Invalid scalar subquery placeholder");
        }
        return { kind: "literal", value: values.get(id.value) as string | number | boolean | null };
      }
      return { ...expr, args: expr.args.map((arg) => replaceScalarSubqueryExpr(arg, values)) };
    case "compare":
      return {
        ...expr,
        left: replaceScalarSubqueryExpr(expr.left, values),
        right: replaceScalarSubqueryExpr(expr.right, values),
      };
    case "between":
      return {
        ...expr,
        target: replaceScalarSubqueryExpr(expr.target, values),
        low: replaceScalarSubqueryExpr(expr.low, values),
        high: replaceScalarSubqueryExpr(expr.high, values),
      };
    case "in":
      return {
        ...expr,
        target: replaceScalarSubqueryExpr(expr.target, values),
        values: expr.values.map((value) => replaceScalarSubqueryExpr(value, values)),
      };
    case "logical":
      return {
        ...expr,
        operands: expr.operands.map((operand) => replaceScalarSubqueryExpr(operand, values)),
      };
    case "not":
      return { ...expr, operand: replaceScalarSubqueryExpr(expr.operand, values) };
    case "null-check":
      return { ...expr, target: replaceScalarSubqueryExpr(expr.target, values) };
    case "like":
      return { ...expr, target: replaceScalarSubqueryExpr(expr.target, values) };
    case "arithmetic":
      return {
        ...expr,
        left: replaceScalarSubqueryExpr(expr.left, values),
        right: replaceScalarSubqueryExpr(expr.right, values),
      };
    case "case":
      return {
        ...expr,
        whens: expr.whens.map((branch) => ({
          when: replaceScalarSubqueryExpr(branch.when, values),
          value: replaceScalarSubqueryExpr(branch.value, values),
        })),
        ...(expr.else === undefined ? {} : { else: replaceScalarSubqueryExpr(expr.else, values) }),
      };
    case "column":
    case "literal":
      return expr;
  }
}

async function joinRowsFromAst(
  lake: ParquetLake,
  ast: SqlQueryAst,
  options: SqlQueryOptions,
): Promise<Row[]> {
  if (ast.join === undefined) throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing SQL JOIN");
  const join = ast.join;
  if (hasWindowSemantics(ast)) {
    throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Window SQL over JOIN is not supported");
  }
  if (hasAggregation(ast)) {
    throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Aggregate SQL over JOIN is not supported yet");
  }
  const leftAlias = ast.join.leftAlias;
  const plan = planJoinSides(ast, leftAlias, join.alias);
  let leftBuilder = lake.path(ast.source);
  let rightBuilder = lake.path(join.source);
  if (plan.leftWhere !== undefined) leftBuilder = leftBuilder.where(plan.leftWhere);
  if (plan.rightWhere !== undefined) rightBuilder = rightBuilder.where(plan.rightWhere);
  if (plan.leftColumns !== undefined) leftBuilder = leftBuilder.select(plan.leftColumns);
  if (plan.rightColumns !== undefined) rightBuilder = rightBuilder.select(plan.rightColumns);
  const leftRows = (await leftBuilder.toArray()).map((row) => qualifyRow(row, leftAlias));
  const rightRows = (await rightBuilder.toArray()).map((row) => qualifyRow(row, join.alias));
  let rows = await broadcastJoin(leftRows, rightRows, {
    leftKey: join.leftKey,
    rightKey: join.rightKey,
    type: join.type,
    rightPrefix: `${join.alias}.`,
    maxRightRows: options.joinMaxRightRows ?? 100_000,
  });
  if (join.type === "left") {
    rows = fillLeftJoinNulls(rows, join.alias, leftJoinRightColumns(ast, join.alias, rightRows));
  }
  if (plan.residualWhere !== undefined)
    rows = rows.filter((row) => matches(plan.residualWhere, row));
  if (ast.orderBy !== undefined) rows = sortRows(rows, ast.orderBy);
  rows = projectRows(rows, ast);
  if (ast.distinct === true) rows = distinctRows(rows);
  return offsetLimitRows(rows, ast);
}

interface JoinSidePlan {
  leftWhere?: Expr;
  rightWhere?: Expr;
  residualWhere?: Expr;
  leftColumns?: string[];
  rightColumns?: string[];
}

function planJoinSides(ast: SqlQueryAst, leftAlias: string, rightAlias: string): JoinSidePlan {
  if (ast.join === undefined) throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing SQL JOIN");
  const leftPredicates: Expr[] = [];
  const rightPredicates: Expr[] = [];
  const residualPredicates: Expr[] = [];
  for (const predicate of splitAndPredicate(ast.where)) {
    const columns = exprColumns(predicate);
    if (columns.length > 0 && columns.every((column) => isQualifiedBy(column, leftAlias))) {
      leftPredicates.push(stripQualifiedExpr(predicate, leftAlias));
    } else if (
      ast.join.type === "inner" &&
      columns.length > 0 &&
      columns.every((column) => isQualifiedBy(column, rightAlias))
    ) {
      rightPredicates.push(stripQualifiedExpr(predicate, rightAlias));
    } else {
      residualPredicates.push(predicate);
    }
  }

  const plan: JoinSidePlan = {};
  const leftWhere = combineAndPredicate(leftPredicates);
  const rightWhere = combineAndPredicate(rightPredicates);
  const residualWhere = combineAndPredicate(residualPredicates);
  if (leftWhere !== undefined) plan.leftWhere = leftWhere;
  if (rightWhere !== undefined) plan.rightWhere = rightWhere;
  if (residualWhere !== undefined) plan.residualWhere = residualWhere;
  if (canProjectJoinSides(ast, leftAlias, rightAlias)) {
    plan.leftColumns = joinSideColumns(ast, leftAlias, ast.join.leftKey);
    plan.rightColumns = joinSideColumns(ast, rightAlias, ast.join.rightKey);
  }
  return plan;
}

function splitAndPredicate(expr: Expr | undefined): Expr[] {
  if (expr === undefined) return [];
  if (expr.kind === "logical" && expr.op === "and") return expr.operands.flatMap(splitAndPredicate);
  return [expr];
}

function combineAndPredicate(predicates: Expr[]): Expr | undefined {
  if (predicates.length === 0) return undefined;
  if (predicates.length === 1) return predicates[0];
  return { kind: "logical", op: "and", operands: predicates };
}

function isQualifiedBy(column: string, alias: string): boolean {
  return column.startsWith(`${alias}.`) && column.length > alias.length + 1;
}

function stripQualifiedColumn(column: string, alias: string): string {
  return isQualifiedBy(column, alias) ? column.slice(alias.length + 1) : column;
}

function stripQualifiedExpr(expr: Expr, alias: string): Expr {
  switch (expr.kind) {
    case "column":
      return { ...expr, name: stripQualifiedColumn(expr.name, alias) };
    case "compare":
      return {
        ...expr,
        left: stripQualifiedExpr(expr.left, alias),
        right: stripQualifiedExpr(expr.right, alias),
      };
    case "between":
      return {
        ...expr,
        target: stripQualifiedExpr(expr.target, alias),
        low: stripQualifiedExpr(expr.low, alias),
        high: stripQualifiedExpr(expr.high, alias),
      };
    case "in":
      return {
        ...expr,
        target: stripQualifiedExpr(expr.target, alias),
        values: expr.values.map((value) => stripQualifiedExpr(value, alias)),
      };
    case "logical":
      return {
        ...expr,
        operands: expr.operands.map((operand) => stripQualifiedExpr(operand, alias)),
      };
    case "not":
      return { ...expr, operand: stripQualifiedExpr(expr.operand, alias) };
    case "null-check":
      return { ...expr, target: stripQualifiedExpr(expr.target, alias) };
    case "like":
      return { ...expr, target: stripQualifiedExpr(expr.target, alias) };
    case "call":
      return { ...expr, args: expr.args.map((arg) => stripQualifiedExpr(arg, alias)) };
    case "arithmetic":
      return {
        ...expr,
        left: stripQualifiedExpr(expr.left, alias),
        right: stripQualifiedExpr(expr.right, alias),
      };
    case "case":
      return {
        ...expr,
        whens: expr.whens.map((branch) => ({
          when: stripQualifiedExpr(branch.when, alias),
          value: stripQualifiedExpr(branch.value, alias),
        })),
        ...(expr.else === undefined ? {} : { else: stripQualifiedExpr(expr.else, alias) }),
      };
    case "literal":
      return expr;
  }
}

function exprColumns(expr: Expr): string[] {
  const columns = new Set<string>();
  collectExprColumns(expr, columns);
  return [...columns];
}

function collectExprColumns(expr: Expr, columns: Set<string>): void {
  switch (expr.kind) {
    case "column":
      columns.add(expr.name);
      return;
    case "compare":
      collectExprColumns(expr.left, columns);
      collectExprColumns(expr.right, columns);
      return;
    case "between":
      collectExprColumns(expr.target, columns);
      collectExprColumns(expr.low, columns);
      collectExprColumns(expr.high, columns);
      return;
    case "in":
      collectExprColumns(expr.target, columns);
      for (const value of expr.values) collectExprColumns(value, columns);
      return;
    case "logical":
      for (const operand of expr.operands) collectExprColumns(operand, columns);
      return;
    case "not":
      collectExprColumns(expr.operand, columns);
      return;
    case "null-check":
      collectExprColumns(expr.target, columns);
      return;
    case "like":
      collectExprColumns(expr.target, columns);
      return;
    case "call":
      for (const arg of expr.args) collectExprColumns(arg, columns);
      return;
    case "arithmetic":
      collectExprColumns(expr.left, columns);
      collectExprColumns(expr.right, columns);
      return;
    case "case":
      for (const branch of expr.whens) {
        collectExprColumns(branch.when, columns);
        collectExprColumns(branch.value, columns);
      }
      if (expr.else !== undefined) collectExprColumns(expr.else, columns);
      return;
    case "literal":
      return;
  }
}

function canProjectJoinSides(ast: SqlQueryAst, leftAlias: string, rightAlias: string): boolean {
  if (ast.select?.includes("*") ?? false) return false;
  return referencedJoinColumns(ast).every(
    (column) => isQualifiedBy(column, leftAlias) || isQualifiedBy(column, rightAlias),
  );
}

function referencedJoinColumns(ast: SqlQueryAst): string[] {
  const columns = new Set<string>();
  for (const select of ast.select ?? []) {
    const { column } = selectColumn(select);
    columns.add(column);
  }
  for (const expr of Object.values(ast.projections ?? {})) collectExprColumns(expr, columns);
  if (ast.where !== undefined) collectExprColumns(ast.where, columns);
  for (const term of ast.orderBy ?? []) columns.add(term.column);
  return [...columns].filter((column) => column !== "*");
}

function joinSideColumns(ast: SqlQueryAst, alias: string, joinKey: string | string[]): string[] {
  const columns = new Set(
    (Array.isArray(joinKey) ? joinKey : [joinKey]).map((column) =>
      stripQualifiedColumn(column, alias),
    ),
  );
  for (const column of referencedJoinColumns(ast)) {
    if (isQualifiedBy(column, alias)) columns.add(stripQualifiedColumn(column, alias));
  }
  return [...columns];
}

function leftJoinRightColumns(ast: SqlQueryAst, rightAlias: string, rightRows: Row[]): string[] {
  const columns = new Set<string>();
  if (ast.select?.includes("*") ?? false) {
    for (const row of rightRows) {
      for (const column of Object.keys(row)) {
        if (isQualifiedBy(column, rightAlias))
          columns.add(stripQualifiedColumn(column, rightAlias));
      }
    }
  }
  for (const column of referencedJoinColumns(ast)) {
    if (isQualifiedBy(column, rightAlias)) columns.add(stripQualifiedColumn(column, rightAlias));
  }
  return [...columns];
}

function fillLeftJoinNulls(rows: Row[], rightAlias: string, rightColumns: string[]): Row[] {
  if (rightColumns.length === 0) return rows;
  return rows.map((row) => {
    let out: Row | undefined;
    for (const column of rightColumns) {
      const qualified = `${rightAlias}.${column}`;
      if (qualified in row) continue;
      out ??= { ...row };
      out[qualified] = null;
    }
    return out ?? row;
  });
}

async function subqueryJoinRowsFromAst(
  lake: ParquetLake,
  ast: SqlQueryAst,
  options: SqlQueryOptions,
): Promise<Row[]> {
  if (ast.subqueryJoin === undefined) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing SQL IN subquery");
  }
  if (hasWindowSemantics(ast)) {
    throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Window SQL over IN subquery is not supported");
  }
  if (hasAggregation(ast)) {
    throw new LakeqlError(
      "LAKEQL_SQL_UNSUPPORTED",
      "Aggregate SQL over IN subquery is not supported yet",
    );
  }
  const join = ast.subqueryJoin;
  let rightRows = await lake.path(join.source).toArray();
  if (join.where !== undefined) rightRows = rightRows.filter((row) => matches(join.where, row));
  let rows = await broadcastJoin(await lake.path(ast.source).toArray(), rightRows, {
    leftKey: join.leftKey,
    rightKey: join.rightKey,
    type: join.type,
    maxRightRows: options.joinMaxRightRows ?? 100_000,
  });
  if (ast.where !== undefined) rows = rows.filter((row) => matches(ast.where, row));
  if (ast.orderBy !== undefined) rows = sortRows(rows, ast.orderBy);
  rows = projectRows(rows, ast);
  if (ast.distinct === true) rows = distinctRows(rows);
  return offsetLimitRows(rows, ast);
}

function qualifyRow(row: Row, alias: string): Row {
  const out: Row = { ...row };
  for (const [key, value] of Object.entries(row)) out[`${alias}.${key}`] = value;
  return out;
}

function projectRows(rows: Row[], ast: SqlQueryAst): Row[] {
  const hasWildcardProjection = ast.select?.includes("*") ?? false;
  return rows.map((row) => {
    if (hasWildcardProjection && ast.projections === undefined) return row;
    const out: Row = {};
    if (hasWildcardProjection) {
      for (const [column, value] of Object.entries(row)) out[column] = value;
    } else {
      for (const select of ast.select ?? []) {
        const { column, alias } = selectColumn(select);
        out[alias] = row[column];
      }
    }
    for (const [alias, expr] of Object.entries(ast.projections ?? {})) {
      out[alias] = evaluate(expr, row);
    }
    return out;
  });
}

async function aggregateRowsFromAst(builder: QueryBuilder, ast: SqlQueryAst): Promise<Row[]> {
  if (hasWindowSemantics(ast)) {
    throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Window SQL over aggregates is not supported");
  }
  validateAggregateProjection(ast);
  let next = builder;
  if (ast.where) next = next.where(ast.where);
  const aggregates = ast.aggregates ?? {};
  const hiddenCountAlias = hiddenAggregateAlias(ast);
  const aggregateSpec: AggregateSpec =
    Object.keys(aggregates).length > 0 ? aggregates : { [hiddenCountAlias]: { op: "count" } };
  let rows = await next.groupBy(ast.groupBy ?? []).aggregate(aggregateSpec, {
    ...(ast.having !== undefined ? { having: ast.having } : {}),
    ...(ast.orderBy !== undefined ? { orderBy: ast.orderBy } : {}),
    ...(ast.distinct === true ? {} : ast.limit !== undefined ? { limit: ast.limit } : {}),
    ...(ast.distinct === true ? {} : ast.offset !== undefined ? { offset: ast.offset } : {}),
  });
  rows = projectAggregateRows(rows, ast, hiddenCountAlias);
  if (ast.distinct === true) rows = distinctRows(rows);
  if (ast.distinct === true) return offsetLimitRows(rows, ast);
  return rows;
}

function validateAggregateProjection(ast: SqlQueryAst): void {
  const groupColumns = new Set(ast.groupBy ?? []);
  for (const select of ast.select ?? []) {
    const { column } = selectColumn(select);
    if (column === "*") continue;
    if (!groupColumns.has(column)) {
      throw new LakeqlError(
        "LAKEQL_SQL_UNSUPPORTED",
        `Aggregate SQL can only select grouped columns or aggregate expressions, not ${column}`,
      );
    }
  }
}

function projectAggregateRows(rows: Row[], ast: SqlQueryAst, hiddenCountAlias: string): Row[] {
  const aggregates = Object.entries(ast.aggregates ?? {});
  const hasWildcardProjection = ast.select?.includes("*") ?? false;
  return rows.map((row) => {
    const out: Row = {};
    if (hasWildcardProjection) {
      for (const [column, value] of Object.entries(row)) {
        if (column !== hiddenCountAlias) out[column] = value;
      }
    } else if (ast.select !== undefined) {
      for (const select of ast.select ?? []) {
        const { column, alias } = selectColumn(select);
        out[alias] = row[column];
      }
    }
    for (const [alias] of aggregates) out[alias] = row[alias];
    for (const [alias, expr] of Object.entries(ast.projections ?? {})) {
      out[alias] = evaluate(expr, row);
    }
    return out;
  });
}

function hiddenAggregateAlias(ast: SqlQueryAst): string {
  const reserved = new Set([...(ast.select ?? []), ...Object.keys(ast.aggregates ?? {})]);
  let alias = "__lakeql_group_count";
  while (reserved.has(alias)) alias = `_${alias}`;
  return alias;
}

function selectColumn(select: string): { column: string; alias: string } {
  const match = /^(.+?)\s+as\s+(.+)$/iu.exec(select);
  if (match === null) return { column: select, alias: select };
  const [, column, alias] = match;
  return { column: column ?? select, alias: alias ?? select };
}

function distinctRows(rows: Row[]): Row[] {
  const seen = new Set<string>();
  const out: Row[] = [];
  for (const row of rows) {
    const key = JSON.stringify(row);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function sortRows(rows: Row[], orderBy: NonNullable<SqlQueryAst["orderBy"]>): Row[] {
  return [...rows].sort((a, b) => {
    for (const term of orderBy) {
      const av = a[term.column] ?? null;
      const bv = b[term.column] ?? null;
      if (av === bv) continue;
      if (av === null) return term.nulls === "first" ? -1 : 1;
      if (bv === null) return term.nulls === "first" ? 1 : -1;
      const direction = term.direction === "desc" ? -1 : 1;
      return (av < bv ? -1 : 1) * direction;
    }
    return 0;
  });
}

function offsetLimitRows(rows: Row[], ast: SqlQueryAst): Row[] {
  const offset = ast.offset ?? 0;
  if (ast.limit !== undefined) return rows.slice(offset, offset + ast.limit);
  return offset > 0 ? rows.slice(offset) : rows;
}

function rowsToCsv(rows: Row[]): string {
  const columns = Object.keys(rows[0] ?? {});
  const lines = [columns.join(",")];
  for (const row of rows) {
    lines.push(columns.map((column) => csvCell(row[column])).join(","));
  }
  return `${lines.join("\n")}\n`;
}

function csvCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  const text = String(value);
  return /[",\n\r]/u.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function jsonStringify(value: unknown): string {
  return JSON.stringify(value, (_key, item) => (typeof item === "bigint" ? item.toString() : item));
}

function streamText(read: () => Promise<string>): ReadableStream<Uint8Array> {
  let done = false;
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (done) return;
      done = true;
      controller.enqueue(textEncoder.encode(await read()));
      controller.close();
    },
  });
}

function addDefaultFrom(sql: string): string {
  if (!/^\s*select\b/iu.test(sql)) {
    throw new LakeqlError("LAKEQL_PARSE_ERROR", "SQL must start with SELECT");
  }
  const clause = /\b(where|group\s+by|having|order\s+by|limit|offset)\b/iu.exec(sql);
  if (clause === null || clause.index === undefined) return `${sql} from input`;
  return `${sql.slice(0, clause.index)}from input ${sql.slice(clause.index)}`;
}
