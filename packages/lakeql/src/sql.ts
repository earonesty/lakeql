import {
  type AggregateSpec,
  broadcastJoin,
  crossJoin,
  type Expr,
  evaluate,
  LakeqlError,
  matches,
  type ObjectStore,
  type QueryBuilder,
  type Row,
} from "lakeql-core";
import type { IcebergReadMode, PlanIcebergFilesOptions } from "lakeql-iceberg";
import { createParquetLake, type ParquetLakeConfig, writePartitionedParquet } from "lakeql-parquet";
import { parseSql, type SqlParameterValue, type SqlQueryAst } from "lakeql-sql";
import { loadTable, planFiles, scanRows } from "./engine.js";

export interface SqlIcebergTableOptions {
  metadataPath: string;
  snapshotId?: number;
  asOfTimestampMs?: number;
  ref?: string;
  readMode?: IcebergReadMode;
  maxRowsPerFile?: number;
}

export interface SqlQueryOptions {
  path?: string;
  tables?: Record<string, string>;
  icebergTables?: Record<string, SqlIcebergTableOptions>;
  parameters?: readonly SqlParameterValue[];
  joinMaxRightRows?: number;
  joinMaxOutputRows?: number;
}

export interface SqlCsvOptions {
  /**
   * Prefix cells that spreadsheet apps may interpret as formulae. Defaults to
   * false so CSV remains a faithful data export unless a spreadsheet-targeted
   * response opts in.
   */
  preventFormulae?: boolean;
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

  streamCsv(options: SqlCsvOptions = {}): ReadableStream<Uint8Array> {
    return streamText(async () => rowsToCsv(await this.toArray(), options));
  }
}

async function executeSql(
  lake: ParquetLake,
  sql: string,
  options: SqlQueryOptions,
): Promise<Row[]> {
  const ast = sqlAst(sql, options);
  validateSourceBindings(options);
  const bound = bindSources(ast, options);
  const pushed = pushdownIcebergPredicates(bound, options);
  const materializedIceberg = await materializeIcebergTablesIfNeeded(
    lake.store,
    pushed.ast,
    options,
    pushed.predicates,
  );
  try {
    return await executeRowsFromAst(lake, materializedIceberg.ast, options);
  } finally {
    await materializedIceberg.cleanup();
  }
}

async function executeRowsFromAst(
  lake: ParquetLake,
  ast: SqlQueryAst,
  options: SqlQueryOptions,
): Promise<Row[]> {
  const materialized = await materializeCteIfNeeded(lake.store, lake, ast, options);
  try {
    const resolved = await resolveScalarSubqueries(lake, materialized.ast);
    if (resolved.subqueryJoin !== undefined)
      return await subqueryJoinRowsFromAst(lake, resolved, options);
    if (sqlJoins(resolved).length > 0) return await joinRowsFromAst(lake, resolved, options);
    if (hasAggregation(resolved))
      return await aggregateRowsFromAst(lake.path(resolved.source), resolved);
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
  const icebergTables = options.icebergTables ?? {};
  const bindSource = (source: string): string => {
    if (source === "input" && options.path !== undefined) return options.path;
    if (icebergTables[source] !== undefined) return source;
    return tables[source] ?? source;
  };
  return mapAstSources(ast, bindSource);
}

function validateSourceBindings(options: SqlQueryOptions): void {
  const tables = options.tables ?? {};
  const icebergTables = options.icebergTables ?? {};
  for (const name of Object.keys(icebergTables)) {
    if (tables[name] === undefined) continue;
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      `SQL source "${name}" is bound as both Parquet and Iceberg`,
    );
  }
}

function mapAstSources(
  ast: SqlQueryAst,
  bindSource: (source: string) => string,
  seen = new WeakSet<SqlQueryAst>(),
): SqlQueryAst {
  const out: SqlQueryAst = { ...ast, source: bindSource(ast.source) };
  if (seen.has(ast)) {
    delete out.scalarSubqueries;
    return out;
  }
  seen.add(ast);
  if (ast.join !== undefined) out.join = { ...ast.join, source: bindSource(ast.join.source) };
  if (ast.joins !== undefined) {
    out.joins = ast.joins.map((join) => ({ ...join, source: bindSource(join.source) }));
    const firstJoin = out.joins[0];
    if (firstJoin !== undefined) out.join = firstJoin;
  }
  if (ast.subqueryJoin !== undefined) {
    out.subqueryJoin = {
      ...ast.subqueryJoin,
      source: bindSource(ast.subqueryJoin.source),
    };
  }
  if (ast.cte !== undefined) {
    out.cte = { ...ast.cte, query: mapAstSources(ast.cte.query, bindSource, seen) };
  }
  if (ast.scalarSubqueries !== undefined) {
    out.scalarSubqueries = Object.fromEntries(
      Object.entries(ast.scalarSubqueries).map(([id, subquery]) => [
        id,
        { ...subquery, query: mapAstSources(subquery.query, bindSource, seen) },
      ]),
    );
  }
  return out;
}

async function mapAstSourcesAsync(
  ast: SqlQueryAst,
  bindSource: (source: string) => Promise<string>,
  seen = new WeakSet<SqlQueryAst>(),
): Promise<SqlQueryAst> {
  const out: SqlQueryAst = { ...ast, source: await bindSource(ast.source) };
  if (seen.has(ast)) {
    delete out.scalarSubqueries;
    return out;
  }
  seen.add(ast);
  if (ast.join !== undefined) {
    out.join = { ...ast.join, source: await bindSource(ast.join.source) };
  }
  if (ast.joins !== undefined) {
    out.joins = await Promise.all(
      ast.joins.map(async (join) => ({ ...join, source: await bindSource(join.source) })),
    );
    const firstJoin = out.joins[0];
    if (firstJoin !== undefined) out.join = firstJoin;
  }
  if (ast.subqueryJoin !== undefined) {
    out.subqueryJoin = {
      ...ast.subqueryJoin,
      source: await bindSource(ast.subqueryJoin.source),
    };
  }
  if (ast.cte !== undefined) {
    out.cte = { ...ast.cte, query: await mapAstSourcesAsync(ast.cte.query, bindSource, seen) };
  }
  if (ast.scalarSubqueries !== undefined) {
    out.scalarSubqueries = Object.fromEntries(
      await Promise.all(
        Object.entries(ast.scalarSubqueries).map(async ([id, subquery]) => [
          id,
          { ...subquery, query: await mapAstSourcesAsync(subquery.query, bindSource, seen) },
        ]),
      ),
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
  options: SqlQueryOptions,
): Promise<{ ast: SqlQueryAst; cleanup: () => Promise<void> }> {
  if (ast.cte === undefined) return { ast, cleanup: async () => {} };
  const cteRows = await executeRowsFromAst(lake, ast.cte.query, options);
  if (cteRows.length === 0) {
    throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Empty CTE results are not supported yet");
  }
  const prefix = `__lakeql_sql_cte/${safeTempName(ast.cte.name)}/${nextSqlTempId()}`;
  const written = await writePartitionedParquet(store, prefix, {
    rows: cteRows,
    maxRowsPerFile: cteRows.length,
  });
  const { cte: _cte, ...rest } = ast;
  const ctePath = `${prefix}/*.parquet`;
  return {
    ast: mapAstSources(rest, (source) => (source === ast.cte?.name ? ctePath : source)),
    cleanup: async () => {
      await Promise.all(written.files.map((file) => store.delete(file.path)));
    },
  };
}

async function materializeIcebergTablesIfNeeded(
  store: ObjectStore,
  ast: SqlQueryAst,
  options: SqlQueryOptions,
  predicates: Map<string, Expr> = new Map(),
): Promise<{ ast: SqlQueryAst; cleanup: () => Promise<void> }> {
  const icebergTables = options.icebergTables ?? {};
  if (Object.keys(icebergTables).length === 0) return { ast, cleanup: async () => {} };

  const materialized = new Map<string, { path: string; files: string[] }>();
  const requiredColumns = collectSourceColumns(ast);
  const materializeSource = async (source: string): Promise<string> => {
    const icebergTable = icebergTables[source];
    if (icebergTable === undefined) return source;
    const cached = materialized.get(source);
    if (cached !== undefined) return cached.path;

    const table = await loadTable({
      format: "iceberg",
      store,
      metadataPath: icebergTable.metadataPath,
    });
    const planOptions = icebergPlanOptions(icebergTable);
    const sourceColumns = requiredColumns.get(source);
    if (sourceColumns !== undefined) planOptions.select = [...sourceColumns];
    const predicate = predicates.get(source);
    if (predicate !== undefined) planOptions.where = predicate;
    const plan = planFiles(table, planOptions);
    const rows: Row[] = [];
    for await (const row of scanRows(plan)) rows.push(row);
    if (rows.length === 0) {
      throw new LakeqlError(
        "LAKEQL_SQL_UNSUPPORTED",
        `Iceberg SQL source "${source}" produced no rows`,
      );
    }

    const prefix = `__lakeql_sql_iceberg/${safeTempName(source)}/${nextSqlTempId()}`;
    const written = await writePartitionedParquet(store, prefix, {
      rows,
      maxRowsPerFile: icebergTable.maxRowsPerFile ?? rows.length,
    });
    const result = {
      path: `${prefix}/*.parquet`,
      files: written.files.map((file) => file.path),
    };
    materialized.set(source, result);
    return result.path;
  };

  const mapped = await mapAstSourcesAsync(ast, materializeSource);
  return {
    ast: mapped,
    cleanup: async () => {
      await Promise.all(
        [...materialized.values()]
          .flatMap((table) => table.files)
          .map((path) => store.delete(path)),
      );
    },
  };
}

function pushdownIcebergPredicates(
  ast: SqlQueryAst,
  options: SqlQueryOptions,
): { ast: SqlQueryAst; predicates: Map<string, Expr> } {
  const icebergTables = options.icebergTables ?? {};
  if (Object.keys(icebergTables).length === 0) return { ast, predicates: new Map() };
  const sourceCounts = countAstSources(ast);
  const predicates = new Map<string, Expr>();
  return {
    ast: pushdownIcebergPredicatesFromAst(ast, icebergTables, sourceCounts, predicates),
    predicates,
  };
}

function pushdownIcebergPredicatesFromAst(
  ast: SqlQueryAst,
  icebergTables: Record<string, SqlIcebergTableOptions>,
  sourceCounts: Map<string, number>,
  predicates: Map<string, Expr>,
  seen = new WeakSet<SqlQueryAst>(),
): SqlQueryAst {
  if (seen.has(ast)) return ast;
  seen.add(ast);

  const aliases = sourceAliases(ast);
  const out: SqlQueryAst = { ...ast };
  if (ast.where !== undefined) {
    const remaining: Expr[] = [];
    for (const predicate of splitAndPredicates(ast.where)) {
      const source = predicateSource(predicate, aliases);
      if (
        source !== undefined &&
        icebergTables[source] !== undefined &&
        sourceCounts.get(source) === 1
      ) {
        addPushdownPredicate(predicates, source, stripExprSource(predicate, aliases, source));
      } else {
        remaining.push(predicate);
      }
    }
    const where = combineAndPredicates(remaining);
    if (where === undefined) delete out.where;
    else out.where = where;
  }
  if (ast.subqueryJoin?.where !== undefined) {
    const source = ast.subqueryJoin.source;
    if (icebergTables[source] !== undefined && sourceCounts.get(source) === 1) {
      const subqueryAliases = new Map([[source, source]]);
      const remaining: Expr[] = [];
      for (const predicate of splitAndPredicates(ast.subqueryJoin.where)) {
        const predicateTable = predicateSource(predicate, subqueryAliases);
        if (predicateTable === source) {
          addPushdownPredicate(
            predicates,
            source,
            stripExprSource(predicate, subqueryAliases, source),
          );
        } else {
          remaining.push(predicate);
        }
      }
      const where = combineAndPredicates(remaining);
      out.subqueryJoin = { ...ast.subqueryJoin };
      if (where === undefined) delete out.subqueryJoin.where;
      else out.subqueryJoin.where = where;
    }
  }
  if (ast.cte !== undefined) {
    out.cte = {
      ...ast.cte,
      query: pushdownIcebergPredicatesFromAst(
        ast.cte.query,
        icebergTables,
        sourceCounts,
        predicates,
        seen,
      ),
    };
  }
  if (ast.scalarSubqueries !== undefined) {
    out.scalarSubqueries = Object.fromEntries(
      Object.entries(ast.scalarSubqueries).map(([id, subquery]) => [
        id,
        {
          ...subquery,
          query: pushdownIcebergPredicatesFromAst(
            subquery.query,
            icebergTables,
            sourceCounts,
            predicates,
            seen,
          ),
        },
      ]),
    );
  }
  return out;
}

function countAstSources(
  ast: SqlQueryAst,
  counts = new Map<string, number>(),
  seen = new WeakSet<SqlQueryAst>(),
): Map<string, number> {
  if (seen.has(ast)) return counts;
  seen.add(ast);
  incrementCount(counts, ast.source);
  for (const join of sqlJoins(ast)) incrementCount(counts, join.source);
  if (ast.subqueryJoin !== undefined) incrementCount(counts, ast.subqueryJoin.source);
  if (ast.cte !== undefined) countAstSources(ast.cte.query, counts, seen);
  for (const subquery of Object.values(ast.scalarSubqueries ?? {})) {
    countAstSources(subquery.query, counts, seen);
  }
  return counts;
}

function incrementCount(counts: Map<string, number>, source: string): void {
  counts.set(source, (counts.get(source) ?? 0) + 1);
}

function splitAndPredicates(expr: Expr): Expr[] {
  if (expr.kind !== "logical" || expr.op !== "and") return [expr];
  return expr.operands.flatMap(splitAndPredicates);
}

function combineAndPredicates(predicates: readonly Expr[]): Expr | undefined {
  if (predicates.length === 0) return undefined;
  if (predicates.length === 1) return predicates[0];
  return { kind: "logical", op: "and", operands: [...predicates] };
}

function addPushdownPredicate(
  predicates: Map<string, Expr>,
  source: string,
  predicate: Expr,
): void {
  const existing = predicates.get(source);
  predicates.set(
    source,
    existing === undefined
      ? predicate
      : { kind: "logical", op: "and", operands: [existing, predicate] },
  );
}

function predicateSource(expr: Expr, aliases: Map<string, string>): string | undefined {
  const columns = new Set<string>();
  collectExprColumns(expr, columns);
  let source: string | undefined;
  for (const column of columns) {
    const columnSource = columnSourceName(column, aliases);
    if (columnSource === undefined) return undefined;
    if (source === undefined) source = columnSource;
    else if (source !== columnSource) return undefined;
  }
  return source;
}

function columnSourceName(column: string, aliases: Map<string, string>): string | undefined {
  const dot = column.indexOf(".");
  if (dot > 0) return aliases.get(column.slice(0, dot));
  if (aliases.size !== 1) return undefined;
  return aliases.values().next().value as string | undefined;
}

function stripExprSource(expr: Expr, aliases: Map<string, string>, source: string): Expr {
  switch (expr.kind) {
    case "column":
      return { ...expr, name: stripSourceColumn(expr.name, aliases, source) };
    case "compare":
      return {
        ...expr,
        left: stripExprSource(expr.left, aliases, source),
        right: stripExprSource(expr.right, aliases, source),
      };
    case "between":
      return {
        ...expr,
        target: stripExprSource(expr.target, aliases, source),
        low: stripExprSource(expr.low, aliases, source),
        high: stripExprSource(expr.high, aliases, source),
      };
    case "in":
      return {
        ...expr,
        target: stripExprSource(expr.target, aliases, source),
        values: expr.values.map((value) => stripExprSource(value, aliases, source)),
      };
    case "logical":
      return {
        ...expr,
        operands: expr.operands.map((operand) => stripExprSource(operand, aliases, source)),
      };
    case "not":
      return { ...expr, operand: stripExprSource(expr.operand, aliases, source) };
    case "null-check":
      return { ...expr, target: stripExprSource(expr.target, aliases, source) };
    case "like":
      return { ...expr, target: stripExprSource(expr.target, aliases, source) };
    case "call":
      return { ...expr, args: expr.args.map((arg) => stripExprSource(arg, aliases, source)) };
    case "arithmetic":
      return {
        ...expr,
        left: stripExprSource(expr.left, aliases, source),
        right: stripExprSource(expr.right, aliases, source),
      };
    case "case":
      return {
        ...expr,
        whens: expr.whens.map((branch) => ({
          when: stripExprSource(branch.when, aliases, source),
          value: stripExprSource(branch.value, aliases, source),
        })),
        ...(expr.else === undefined ? {} : { else: stripExprSource(expr.else, aliases, source) }),
      };
    case "literal":
      return expr;
  }
}

function stripSourceColumn(column: string, aliases: Map<string, string>, source: string): string {
  const dot = column.indexOf(".");
  if (dot <= 0) return column;
  const qualifier = column.slice(0, dot);
  return aliases.get(qualifier) === source ? column.slice(dot + 1) : column;
}

function collectSourceColumns(ast: SqlQueryAst): Map<string, Set<string>> {
  const columns = new Map<string, Set<string>>();
  collectSourceColumnsFromAst(ast, columns);
  return columns;
}

function collectSourceColumnsFromAst(
  ast: SqlQueryAst,
  columns: Map<string, Set<string>>,
  seen = new WeakSet<SqlQueryAst>(),
): void {
  if (seen.has(ast)) return;
  seen.add(ast);

  const aliases = sourceAliases(ast);
  const referenced = new Set<string>();
  for (const select of ast.select ?? []) referenced.add(select);
  for (const expr of Object.values(ast.projections ?? {})) collectExprColumns(expr, referenced);
  for (const aggregate of Object.values(ast.aggregates ?? {})) {
    if (aggregate.column !== undefined) referenced.add(aggregate.column);
    if (aggregate.expr !== undefined) collectExprColumns(aggregate.expr, referenced);
  }
  for (const column of ast.groupBy ?? []) referenced.add(column);
  for (const term of ast.orderBy ?? []) referenced.add(term.column);
  if (ast.where !== undefined) collectExprColumns(ast.where, referenced);
  if (ast.having !== undefined) collectExprColumns(ast.having, referenced);
  if (ast.qualify !== undefined) collectExprColumns(ast.qualify, referenced);
  for (const window of Object.values(ast.windows ?? {})) {
    for (const expr of window.args) collectExprColumns(expr, referenced);
    for (const expr of window.over.partitionBy) collectExprColumns(expr, referenced);
    for (const term of window.over.orderBy) collectExprColumns(term.expr, referenced);
    if (window.over.frame?.start.offset !== undefined)
      collectExprColumns(window.over.frame.start.offset, referenced);
    if (window.over.frame?.end.offset !== undefined)
      collectExprColumns(window.over.frame.end.offset, referenced);
    if (window.filter !== undefined) collectExprColumns(window.filter, referenced);
  }
  for (const join of sqlJoins(ast)) {
    for (const column of join.leftKey) referenced.add(column);
    for (const column of join.rightKey) referenced.add(column);
    if (join.predicate !== undefined) collectExprColumns(join.predicate, referenced);
  }
  if (ast.subqueryJoin !== undefined) {
    for (const column of ast.subqueryJoin.leftKey) referenced.add(column);
    for (const column of ast.subqueryJoin.rightKey) referenced.add(column);
    if (ast.subqueryJoin.where !== undefined)
      collectExprColumns(ast.subqueryJoin.where, referenced);
    for (const term of ast.subqueryJoin.orderBy ?? []) referenced.add(term.column);
  }

  for (const column of referenced) addSourceColumn(columns, aliases, column);
  if (ast.cte !== undefined) collectSourceColumnsFromAst(ast.cte.query, columns, seen);
  for (const subquery of Object.values(ast.scalarSubqueries ?? {})) {
    collectSourceColumnsFromAst(subquery.query, columns, seen);
  }
}

function sourceAliases(ast: SqlQueryAst): Map<string, string> {
  const aliases = new Map<string, string>([[ast.source, ast.source]]);
  const joins = sqlJoins(ast);
  const firstJoin = joins[0];
  if (firstJoin !== undefined) aliases.set(firstJoin.leftAlias, ast.source);
  for (const join of joins) {
    aliases.set(join.source, join.source);
    aliases.set(join.alias, join.source);
  }
  return aliases;
}

function addSourceColumn(
  columns: Map<string, Set<string>>,
  aliases: Map<string, string>,
  column: string,
): void {
  if (column === "*" || column.endsWith(".*")) return;
  const dot = column.indexOf(".");
  if (dot > 0) {
    const source = aliases.get(column.slice(0, dot));
    if (source === undefined) return;
    addColumn(columns, source, column.slice(dot + 1));
    return;
  }
  if (aliases.size !== 1) return;
  const source = aliases.values().next().value as string | undefined;
  if (source !== undefined) addColumn(columns, source, column);
}

function addColumn(columns: Map<string, Set<string>>, source: string, column: string): void {
  let sourceColumns = columns.get(source);
  if (sourceColumns === undefined) {
    sourceColumns = new Set<string>();
    columns.set(source, sourceColumns);
  }
  sourceColumns.add(column);
}

function icebergPlanOptions(table: SqlIcebergTableOptions): PlanIcebergFilesOptions {
  const options: PlanIcebergFilesOptions = {};
  if (table.snapshotId !== undefined) options.snapshotId = table.snapshotId;
  if (table.asOfTimestampMs !== undefined) options.asOfTimestampMs = table.asOfTimestampMs;
  if (table.ref !== undefined) options.ref = table.ref;
  if (table.readMode !== undefined) options.readMode = table.readMode;
  return options;
}

function nextSqlTempId(): string {
  const randomUuid = globalThis.crypto?.randomUUID?.();
  /* v8 ignore next -- runtime fallback for environments without crypto.randomUUID */
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
  if (ast.joins !== undefined) {
    out.joins = ast.joins.map((join) =>
      join.predicate === undefined
        ? join
        : { ...join, predicate: replaceScalarSubqueryExpr(join.predicate, values) },
    );
    const firstJoin = out.joins[0];
    if (firstJoin !== undefined) out.join = firstJoin;
  } else if (ast.join?.predicate !== undefined) {
    out.join = { ...ast.join, predicate: replaceScalarSubqueryExpr(ast.join.predicate, values) };
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
  const joins = sqlJoins(ast);
  const firstJoin = joins[0];
  /* v8 ignore next -- executeSql calls this only after joins are present */
  if (firstJoin === undefined) throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing SQL JOIN");
  if (hasWindowSemantics(ast)) {
    throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Window SQL over JOIN is not supported");
  }
  if (hasAggregation(ast)) {
    throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Aggregate SQL over JOIN is not supported yet");
  }
  let rows = (await lake.path(ast.source).toArray()).map((row) =>
    qualifyRow(row, firstJoin.leftAlias),
  );
  for (const join of joins) {
    const rightRows = (await lake.path(join.source).toArray()).map((row) =>
      qualifyOnlyRow(row, join.alias),
    );
    if (join.type === "right") {
      const leftRows = rows;
      rows = await broadcastJoin(rightRows, leftRows, {
        leftKey: join.rightKey,
        rightKey: join.leftKey,
        type: "left",
        rightPrefix: `${join.leftAlias}.`,
        maxRightRows: options.joinMaxRightRows ?? 100_000,
      });
      rows = fillRightJoinNulls(rows, ast, join.alias, leftRows);
    } else if (join.type === "full") {
      const leftRows = rows;
      const leftJoined = await broadcastJoin(leftRows, rightRows, {
        leftKey: join.leftKey,
        rightKey: join.rightKey,
        type: "left",
        rightPrefix: `${join.alias}.`,
        maxRightRows: options.joinMaxRightRows ?? 100_000,
      });
      const unmatchedRight = await broadcastJoin(rightRows, leftRows, {
        leftKey: join.rightKey,
        rightKey: join.leftKey,
        type: "anti",
        rightPrefix: `${join.leftAlias}.`,
        maxRightRows: options.joinMaxRightRows ?? 100_000,
      });
      rows = [
        ...fillLeftJoinNulls(
          leftJoined,
          join.alias,
          leftJoinRightColumns(ast, join.alias, rightRows),
        ),
        ...fillRightJoinNulls(unmatchedRight, ast, join.alias, leftRows),
      ];
    } else {
      if (join.predicate !== undefined) {
        rows = (
          await crossJoin(rows, rightRows, {
            rightPrefix: `${join.alias}.`,
            maxRightRows: options.joinMaxRightRows ?? 100_000,
            maxOutputRows: options.joinMaxOutputRows ?? 100_000,
          })
        ).filter((row) => matches(join.predicate as Expr, row));
      } else {
        rows =
          join.type === "cross"
            ? await crossJoin(rows, rightRows, {
                rightPrefix: `${join.alias}.`,
                maxRightRows: options.joinMaxRightRows ?? 100_000,
                maxOutputRows: options.joinMaxOutputRows ?? 100_000,
              })
            : await broadcastJoin(rows, rightRows, {
                leftKey: join.leftKey,
                rightKey: join.rightKey,
                type: join.type,
                rightPrefix: `${join.alias}.`,
                maxRightRows: options.joinMaxRightRows ?? 100_000,
              });
      }
    }
    if (join.type === "left") {
      rows = fillLeftJoinNulls(rows, join.alias, leftJoinRightColumns(ast, join.alias, rightRows));
    }
  }
  if (ast.where !== undefined) rows = rows.filter((row) => matches(ast.where, row));
  if (ast.orderBy !== undefined) rows = sortRows(rows, ast.orderBy);
  rows = projectRows(rows, ast);
  if (ast.distinct === true) rows = distinctRows(rows);
  return offsetLimitRows(rows, ast);
}

function sqlJoins(ast: SqlQueryAst): NonNullable<SqlQueryAst["joins"]> {
  if (ast.joins !== undefined) return ast.joins;
  return ast.join === undefined ? [] : [ast.join];
}

function isQualifiedBy(column: string, alias: string): boolean {
  return column.startsWith(`${alias}.`) && column.length > alias.length + 1;
}

function stripQualifiedColumn(column: string, alias: string): string {
  return isQualifiedBy(column, alias) ? column.slice(alias.length + 1) : column;
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

function fillRightJoinNulls(
  rows: Row[],
  ast: SqlQueryAst,
  preservedAlias: string,
  leftRows: Row[],
): Row[] {
  const columns = rightJoinLeftColumns(ast, preservedAlias, leftRows);
  if (columns.length === 0) return rows;
  return rows.map((row) => {
    let out: Row | undefined;
    for (const column of columns) {
      if (column in row) continue;
      out ??= { ...row };
      out[column] = null;
    }
    return out ?? row;
  });
}

function rightJoinLeftColumns(ast: SqlQueryAst, preservedAlias: string, leftRows: Row[]): string[] {
  const columns = new Set<string>();
  if (ast.select?.includes("*") ?? false) {
    for (const row of leftRows) {
      for (const column of Object.keys(row)) {
        if (!isQualifiedBy(column, preservedAlias)) columns.add(column);
      }
    }
  }
  for (const column of referencedJoinColumns(ast)) {
    if (!isQualifiedBy(column, preservedAlias)) columns.add(column);
  }
  return [...columns];
}

async function subqueryJoinRowsFromAst(
  lake: ParquetLake,
  ast: SqlQueryAst,
  options: SqlQueryOptions,
): Promise<Row[]> {
  /* v8 ignore next -- executeSql calls this only after ast.subqueryJoin is present */
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
  if (join.orderBy !== undefined) rightRows = sortRows(rightRows, join.orderBy);
  rightRows = offsetLimitSubqueryRows(rightRows, join);
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

function offsetLimitSubqueryRows(
  rows: Row[],
  subquery: Pick<NonNullable<SqlQueryAst["subqueryJoin"]>, "limit" | "offset">,
): Row[] {
  const offset = subquery.offset ?? 0;
  if (subquery.limit !== undefined) return rows.slice(offset, offset + subquery.limit);
  return offset > 0 ? rows.slice(offset) : rows;
}

function qualifyRow(row: Row, alias: string): Row {
  const out: Row = { ...row };
  for (const [key, value] of Object.entries(row)) out[`${alias}.${key}`] = value;
  return out;
}

function qualifyOnlyRow(row: Row, alias: string): Row {
  return Object.fromEntries(Object.entries(row).map(([key, value]) => [`${alias}.${key}`, value]));
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
  const preProjections = aggregatePreProjections(ast);
  if (preProjections !== undefined) next = next.select(["*"]).project(preProjections);
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
      out[alias] = aggregateProjectionValue(alias, expr, row, ast);
    }
    return out;
  });
}

function aggregatePreProjections(ast: SqlQueryAst): Record<string, Expr> | undefined {
  const groupBy = new Set(ast.groupBy ?? []);
  const entries = Object.entries(ast.projections ?? {}).filter(([alias]) => groupBy.has(alias));
  return entries.length === 0 ? undefined : Object.fromEntries(entries);
}

function aggregateProjectionValue(alias: string, expr: Expr, row: Row, ast: SqlQueryAst): unknown {
  if (ast.groupBy?.includes(alias) && alias in row) return row[alias];
  return evaluate(expr, row);
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
  /* v8 ignore next -- regex capture groups are present when match is not null */
  return { column: column ?? select, alias: alias ?? select };
}

function distinctRows(rows: Row[]): Row[] {
  const seen = new Set<string>();
  const out: Row[] = [];
  for (const row of rows) {
    const key = jsonStringify(row);
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
    /* v8 ignore next -- comparator equality fallback after all ORDER BY terms match */
    return 0;
  });
}

function offsetLimitRows(rows: Row[], ast: SqlQueryAst): Row[] {
  const offset = ast.offset ?? 0;
  if (ast.limit !== undefined) return rows.slice(offset, offset + ast.limit);
  return offset > 0 ? rows.slice(offset) : rows;
}

function rowsToCsv(rows: Row[], options: SqlCsvOptions): string {
  const columns = Object.keys(rows[0] ?? {});
  const lines = [columns.join(",")];
  for (const row of rows) {
    lines.push(columns.map((column) => csvCell(row[column], options)).join(","));
  }
  return `${lines.join("\n")}\n`;
}

function csvCell(value: unknown, options: SqlCsvOptions): string {
  if (value === null || value === undefined) return "";
  const raw = String(value);
  const text = options.preventFormulae === true && /^[=+\-@]/u.test(raw) ? `'${raw}` : raw;
  return /[",\n\r]/u.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function jsonStringify(value: unknown): string {
  return JSON.stringify(value, (_key, item) => (typeof item === "bigint" ? item.toString() : item));
}

function streamText(read: () => Promise<string>): ReadableStream<Uint8Array> {
  let done = false;
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      /* v8 ignore next -- defensive guard for repeated pull after close */
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
