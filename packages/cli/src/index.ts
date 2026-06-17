import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import {
  type AggregateSpec,
  broadcastJoin,
  createOutputManifest,
  type Expr,
  evaluate,
  fingerprint,
  LakeqlError,
  type MemoryObjectStore,
  matches,
  memoryStore,
  type QueryBuilder,
  type Row,
  writeOutputManifest,
} from "lakeql-core";
import {
  createParquetLake,
  partitionedParquetOutputEntries,
  readParquetMetadata,
  type WriteParquetRowsOptions,
  writePartitionedParquet,
} from "lakeql-parquet";
import { parseSql, parseSqlStatement } from "lakeql-sql";

export const COMMANDS = ["query", "explain", "inspect", "write", "compact", "schema"] as const;

export type Command = (typeof COMMANDS)[number];

export interface CliResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

interface ParsedArgs {
  command?: string;
  path?: string;
  tables?: Record<string, string>;
  output?: string;
  sql?: string;
  format?: "csv" | "json" | "ndjson";
  partitionBy?: string[];
  maxRowsPerFile?: number;
  joinMaxRightRows?: number;
  manifest?: string;
  jobId?: string;
  help: boolean;
}

export async function runCli(argv: string[]): Promise<CliResult> {
  try {
    const args = parseArgs(argv);
    if (args.help || !args.command) return ok(`${usage()}\n`);
    switch (args.command) {
      case "compact":
        return ok(await compact(args));
      case "query":
        return ok(await query(args));
      case "explain":
        return ok(await explain(args));
      case "inspect":
        return ok(await inspect(args));
      case "write":
        return ok(await write(args));
      case "schema":
        return ok(await schema(args));
      default:
        return fail(`Command ${args.command} is not implemented yet\n${usage()}\n`, 2);
    }
  } catch (error) {
    if (error instanceof LakeqlError) return fail(`${error.code}: ${error.message}\n`, 1);
    if (error instanceof Error) return fail(`${error.message}\n`, 1);
    return fail("Unknown CLI error\n", 1);
  }
}

export function usage(): string {
  const reserved = COMMANDS.filter(
    (command) => !["compact", "query", "explain", "inspect", "write", "schema"].includes(command),
  );
  const lines = [
    "usage: lakeql <command> [options]",
    "",
    "commands:",
    "  compact --path <file.parquet> --output <prefix> [--max-rows-per-file n]",
    "  query   --path <file.parquet> --sql <query> [--format csv|json|ndjson]",
    "  query   --table name=file.parquet [--table name=file.parquet ...] --sql <join-query> [--join-max-right-rows n]",
    "  explain --path <file.parquet> --sql <query>",
    "  inspect --path <file.parquet>",
    "  write   --path <file.parquet> --sql <query> --output <prefix> [--partition-by a,b] [--max-rows-per-file n] [--manifest <path>] [--job-id id]",
    "  schema  --path <file.parquet>",
  ];
  /* v8 ignore next 3 -- COMMANDS only contains implemented commands today. */
  if (reserved.length > 0) {
    lines.push("", `other commands reserved by the build plan: ${reserved.join(", ")}`);
  }
  return lines.join("\n");
}

async function query(args: ParsedArgs): Promise<string> {
  const sql = requireOption(args.sql, "--sql");
  const { store, key, preserveSqlSource } = await queryStore(args);
  const statement = parseCliSqlStatement(sql, key, preserveSqlSource);
  if (isDescribeStatement(statement)) {
    return formatRows([await schemaSummary(store, statement.source)], args.format);
  }
  const ast = statement;
  const lake = createParquetLake({ store });
  const executableAst = await materializeCteIfNeeded(store, lake, ast);
  const executableLake = createParquetLake({ store });
  const resolvedAst = await resolveScalarSubqueries(executableLake, executableAst);
  if (resolvedAst.subqueryJoin !== undefined) {
    const rows = await subqueryJoinRowsFromAst(executableLake, resolvedAst, args);
    if (args.format === "json") return `${JSON.stringify(rows)}\n`;
    if (args.format === "csv") return rowsToCsv(rows);
    return rows.map((row) => JSON.stringify(row)).join("\n") + (rows.length > 0 ? "\n" : "");
  }
  if (resolvedAst.join !== undefined) {
    const rows = await joinRowsFromAst(executableLake, resolvedAst, args);
    if (args.format === "json") return `${JSON.stringify(rows)}\n`;
    if (args.format === "csv") return rowsToCsv(rows);
    return rows.map((row) => JSON.stringify(row)).join("\n") + (rows.length > 0 ? "\n" : "");
  }
  const result = builderFromAst(executableLake.path(resolvedAst.source), resolvedAst);
  if (hasAggregation(resolvedAst)) {
    const rows = await aggregateRowsFromAst(executableLake.path(resolvedAst.source), resolvedAst);
    if (args.format === "json") return `${JSON.stringify(rows)}\n`;
    if (args.format === "csv") return rowsToCsv(rows);
    return rows.map((row) => JSON.stringify(row)).join("\n") + (rows.length > 0 ? "\n" : "");
  }
  if (args.format === "json") return `${JSON.stringify(await result.toArray())}\n`;
  if (args.format === "csv") return new Response(result.streamCsv()).text();
  let out = "";
  for await (const row of result.rows()) out += `${JSON.stringify(row)}\n`;
  return out;
}

async function explain(args: ParsedArgs): Promise<string> {
  const path = requireOption(args.path, "--path");
  const sql = requireOption(args.sql, "--sql");
  const { store, key } = await localStore(path);
  const lake = createParquetLake({ store });
  const ast = parseCliSql(sql, key);
  if (hasAggregation(ast)) {
    throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "EXPLAIN for aggregate SQL is not supported");
  }
  return `${(await builderFromAst(lake.path(key), ast).explain()).text}\n`;
}

async function compact(args: ParsedArgs): Promise<string> {
  const inputPath = requireOption(args.path, "--path");
  const outputPrefix = requireOption(args.output, "--output");
  const { store, key } = await localStore(inputPath);
  const lake = createParquetLake({ store });
  const rows = await lake.path(key).toArray();
  return writeRows(outputPrefix, rows, args);
}

async function schema(args: ParsedArgs): Promise<string> {
  const path = requireOption(args.path, "--path");
  const { store, key } = await localStore(path);
  return `${JSON.stringify(await schemaSummary(store, key, path))}\n`;
}

async function inspect(args: ParsedArgs): Promise<string> {
  const path = requireOption(args.path, "--path");
  const { store, key } = await localStore(path);
  const metadata = await readParquetMetadata(store, key);
  return `${JSON.stringify({
    path,
    rows: Number(totalRows(metadata.row_groups)),
    rowGroups: metadata.row_groups.length,
    columns: metadata.schema.filter((field) => field.name !== "root").length,
  })}\n`;
}

async function write(args: ParsedArgs): Promise<string> {
  const outputPrefix = requireOption(args.output, "--output");
  const sql = requireOption(args.sql, "--sql");
  const { store, key, preserveSqlSource } = await queryStore(args);
  const ast = parseCliSql(sql, key, preserveSqlSource);
  const lake = createParquetLake({ store });
  const executableAst = await materializeCteIfNeeded(store, lake, ast);
  const executableLake = createParquetLake({ store });
  const resolvedAst = await resolveScalarSubqueries(executableLake, executableAst);
  const rows =
    resolvedAst.subqueryJoin !== undefined
      ? await subqueryJoinRowsFromAst(executableLake, resolvedAst, args)
      : resolvedAst.join !== undefined
        ? await joinRowsFromAst(executableLake, resolvedAst, args)
        : hasAggregation(resolvedAst)
          ? await aggregateRowsFromAst(executableLake.path(resolvedAst.source), resolvedAst)
          : await builderFromAst(executableLake.path(resolvedAst.source), resolvedAst).toArray();
  return writeRows(outputPrefix, rows, args);
}

async function writeRows(outputPrefix: string, rows: Row[], args: ParsedArgs): Promise<string> {
  const outStore = memoryStore();
  const writeOptions: WriteParquetRowsOptions = {
    rows,
  };
  if (args.partitionBy !== undefined) writeOptions.partitionBy = args.partitionBy;
  if (args.maxRowsPerFile !== undefined) writeOptions.maxRowsPerFile = args.maxRowsPerFile;
  const result = await writePartitionedParquet(outStore, outputPrefix, writeOptions);

  for (const file of result.files) {
    const bytes = await outStore.get(file.path);
    if (bytes === null) {
      throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No generated output at ${file.path}`, {
        path: file.path,
      });
    }
    await mkdir(dirname(file.path), { recursive: true });
    await writeFile(file.path, bytes);
  }

  const body: { files: typeof result.files; manifest?: string } = { files: result.files };
  if (args.manifest !== undefined) {
    const jobId = args.jobId ?? "cli";
    const manifest = createOutputManifest({
      jobId,
      planFingerprint: fingerprint({
        command: args.command,
        path: args.path,
        sql: args.sql,
        outputPrefix,
        partitionBy: args.partitionBy ?? [],
        maxRowsPerFile: args.maxRowsPerFile ?? null,
      }),
      entries: partitionedParquetOutputEntries(result, {
        taskId: `${jobId}-task-000000`,
      }),
    });
    await writeOutputManifest(outStore, args.manifest, manifest);
    const bytes = await outStore.get(args.manifest);
    if (bytes === null) {
      throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No generated output at ${args.manifest}`, {
        path: args.manifest,
      });
    }
    await mkdir(dirname(args.manifest), { recursive: true });
    await writeFile(args.manifest, bytes);
    body.manifest = args.manifest;
  }

  return `${JSON.stringify(body)}\n`;
}

function builderFromAst(builder: QueryBuilder, ast: ReturnType<typeof parseSql>): QueryBuilder {
  let next = builder;
  if (ast.select) next = next.select(ast.select);
  if (ast.projections) next = next.project(ast.projections);
  if (ast.where) next = next.where(ast.where);
  if (ast.distinct === true) next = next.distinct();
  if (ast.orderBy) next = next.orderBy(ast.orderBy);
  if (ast.offset !== undefined) next = next.offset(ast.offset);
  if (ast.limit !== undefined) next = next.limit(ast.limit);
  return next;
}

function hasAggregation(ast: ReturnType<typeof parseSql>): boolean {
  return (
    ast.aggregates !== undefined ||
    (ast.groupBy !== undefined && ast.groupBy.length > 0) ||
    ast.having !== undefined
  );
}

async function materializeCteIfNeeded(
  store: MemoryObjectStore,
  lake: ReturnType<typeof createParquetLake>,
  ast: ReturnType<typeof parseSql>,
): Promise<ReturnType<typeof parseSql>> {
  if (ast.cte === undefined) return ast;
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
  const prefix = `__lakeql_cte/${ast.cte.name}`;
  await writePartitionedParquet(store, prefix, {
    rows: cteRows,
    maxRowsPerFile: cteRows.length,
  });
  const { cte: _cte, ...rest } = ast;
  return { ...rest, source: `${prefix}/*.parquet` };
}

async function resolveScalarSubqueries(
  lake: ReturnType<typeof createParquetLake>,
  ast: ReturnType<typeof parseSql>,
): Promise<ReturnType<typeof parseSql>> {
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
  lake: ReturnType<typeof createParquetLake>,
  ast: ReturnType<typeof parseSql>,
  args: ParsedArgs,
): Promise<Row[]> {
  if (ast.join === undefined) throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing SQL JOIN");
  const join = ast.join;
  if (hasAggregation(ast)) {
    throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", "Aggregate SQL over JOIN is not supported yet");
  }
  const leftAlias = leftJoinAlias(ast);
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
    maxRightRows: args.joinMaxRightRows ?? 100_000,
  });
  if (join.type === "left") {
    rows = fillLeftJoinNulls(rows, join.alias, leftJoinRightColumns(ast, join.alias, rightRows));
  }
  if (plan.residualWhere !== undefined)
    rows = rows.filter((row) => matches(plan.residualWhere, row));
  if (ast.orderBy !== undefined) rows = sortRows(rows, ast.orderBy);
  rows = projectRows(rows, ast);
  if (ast.distinct === true) rows = distinctRows(rows);
  const offset = ast.offset ?? 0;
  if (ast.limit !== undefined) rows = rows.slice(offset, offset + ast.limit);
  else if (offset > 0) rows = rows.slice(offset);
  return rows;
}

interface JoinSidePlan {
  leftWhere?: Expr;
  rightWhere?: Expr;
  residualWhere?: Expr;
  leftColumns?: string[];
  rightColumns?: string[];
}

function planJoinSides(
  ast: ReturnType<typeof parseSql>,
  leftAlias: string,
  rightAlias: string,
): JoinSidePlan {
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

function canProjectJoinSides(
  ast: ReturnType<typeof parseSql>,
  leftAlias: string,
  rightAlias: string,
): boolean {
  if (ast.select?.includes("*") ?? false) return false;
  return referencedJoinColumns(ast).every(
    (column) => isQualifiedBy(column, leftAlias) || isQualifiedBy(column, rightAlias),
  );
}

function referencedJoinColumns(ast: ReturnType<typeof parseSql>): string[] {
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

function joinSideColumns(
  ast: ReturnType<typeof parseSql>,
  alias: string,
  joinKey: string | string[],
): string[] {
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

function leftJoinRightColumns(
  ast: ReturnType<typeof parseSql>,
  rightAlias: string,
  rightRows: Row[],
): string[] {
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
  lake: ReturnType<typeof createParquetLake>,
  ast: ReturnType<typeof parseSql>,
  args: ParsedArgs,
): Promise<Row[]> {
  if (ast.subqueryJoin === undefined) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing SQL IN subquery");
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
    maxRightRows: args.joinMaxRightRows ?? 100_000,
  });
  if (ast.where !== undefined) rows = rows.filter((row) => matches(ast.where, row));
  if (ast.orderBy !== undefined) rows = sortRows(rows, ast.orderBy);
  rows = projectRows(rows, ast);
  if (ast.distinct === true) rows = distinctRows(rows);
  const offset = ast.offset ?? 0;
  if (ast.limit !== undefined) rows = rows.slice(offset, offset + ast.limit);
  else if (offset > 0) rows = rows.slice(offset);
  return rows;
}

function leftJoinAlias(ast: ReturnType<typeof parseSql>): string {
  if (ast.join === undefined) return ast.source;
  return ast.join.leftAlias;
}

function qualifyRow(row: Row, alias: string): Row {
  const out: Row = { ...row };
  for (const [key, value] of Object.entries(row)) out[`${alias}.${key}`] = value;
  return out;
}

function projectRows(rows: Row[], ast: ReturnType<typeof parseSql>): Row[] {
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

async function aggregateRowsFromAst(
  builder: QueryBuilder,
  ast: ReturnType<typeof parseSql>,
): Promise<Row[]> {
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
  if (ast.distinct === true) {
    const offset = ast.offset ?? 0;
    if (ast.limit !== undefined) rows = rows.slice(offset, offset + ast.limit);
    else if (offset > 0) rows = rows.slice(offset);
  }
  return rows;
}

function validateAggregateProjection(ast: ReturnType<typeof parseSql>): void {
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

function projectAggregateRows(
  rows: Row[],
  ast: ReturnType<typeof parseSql>,
  hiddenCountAlias: string,
): Row[] {
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

function hiddenAggregateAlias(ast: ReturnType<typeof parseSql>): string {
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

function sortRows(
  rows: Row[],
  orderBy: NonNullable<ReturnType<typeof parseSql>["orderBy"]>,
): Row[] {
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

function rowsToCsv(rows: Row[]): string {
  const columns = Object.keys(rows[0] ?? {});
  const lines = [columns.join(",")];
  for (const row of rows) {
    lines.push(columns.map((column) => csvCell(row[column])).join(","));
  }
  return `${lines.join("\n")}\n`;
}

function formatRows(rows: Row[], format: ParsedArgs["format"]): string {
  if (format === "json") return `${JSON.stringify(rows)}\n`;
  if (format === "csv") return rowsToCsv(rows);
  return rows.map((row) => JSON.stringify(row)).join("\n") + (rows.length > 0 ? "\n" : "");
}

async function schemaSummary(
  store: MemoryObjectStore,
  key: string,
  displayPath = key,
): Promise<Row> {
  const metadata = await readParquetMetadata(store, key);
  const columns = metadata.schema
    .filter((field) => field.name !== "root")
    .map((field) => ({ name: field.name, type: field.type ?? field.converted_type ?? "group" }));
  return { path: displayPath, rows: Number(totalRows(metadata.row_groups)), columns };
}

function csvCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  const text = String(value);
  return /[",\n\r]/u.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function parseCliSql(
  sql: string,
  defaultSource: string,
  preserveSqlSource = false,
): ReturnType<typeof parseSql> {
  const trimmed = sql.trim();
  if (/\bfrom\b/iu.test(trimmed)) {
    const ast = parseSql(trimmed);
    return preserveSqlSource ? ast : applyDefaultSource(ast, defaultSource);
  }
  return { ...parseSql(addDefaultFrom(trimmed)), source: defaultSource };
}

function parseCliSqlStatement(
  sql: string,
  defaultSource: string,
  preserveSqlSource = false,
): ReturnType<typeof parseSql> | ReturnType<typeof parseSqlStatement> {
  const trimmed = sql.trim();
  if (/^\s*describe\b/iu.test(trimmed)) {
    const statement = parseSqlStatement(trimmed);
    if (!isDescribeStatement(statement)) return statement;
    if (preserveSqlSource || statement.source !== "input") return statement;
    return { ...statement, source: defaultSource };
  }
  return parseCliSql(sql, defaultSource, preserveSqlSource);
}

function isDescribeStatement(
  statement: ReturnType<typeof parseSql> | ReturnType<typeof parseSqlStatement>,
): statement is Extract<ReturnType<typeof parseSqlStatement>, { type: "describe" }> {
  return "type" in statement && statement.type === "describe";
}

function applyDefaultSource(
  ast: ReturnType<typeof parseSql>,
  defaultSource: string,
  includeScalarSubqueries = true,
  seen = new WeakSet<object>(),
): ReturnType<typeof parseSql> {
  if (seen.has(ast)) return ast.cte === undefined ? { ...ast, source: defaultSource } : ast;
  seen.add(ast);
  const out = { ...ast };
  if (out.cte !== undefined) {
    out.cte = {
      ...out.cte,
      query: applyDefaultSource(out.cte.query, defaultSource, includeScalarSubqueries, seen),
    };
  } else {
    out.source = defaultSource;
  }
  if (out.subqueryJoin !== undefined)
    out.subqueryJoin = { ...out.subqueryJoin, source: defaultSource };
  if (includeScalarSubqueries && out.scalarSubqueries !== undefined) {
    out.scalarSubqueries = Object.fromEntries(
      Object.entries(out.scalarSubqueries).map(([id, subquery]) => [
        id,
        { ...subquery, query: applyDefaultSource(subquery.query, defaultSource, false, seen) },
      ]),
    );
  }
  return out;
}

function addDefaultFrom(sql: string): string {
  if (!/^\s*select\b/iu.test(sql)) {
    throw new LakeqlError("LAKEQL_PARSE_ERROR", "SQL must start with SELECT");
  }
  const clause = /\b(where|group\s+by|having|order\s+by|limit|offset)\b/iu.exec(sql);
  if (clause === null || clause.index === undefined) return `${sql} from input`;
  return `${sql.slice(0, clause.index)}from input ${sql.slice(clause.index)}`;
}

async function localStore(path: string): Promise<{ store: MemoryObjectStore; key: string }> {
  const store = memoryStore();
  const bytes = await readFile(path);
  await store.put(path, new Uint8Array(bytes));
  return { store, key: path };
}

async function queryStore(args: ParsedArgs): Promise<{
  store: MemoryObjectStore;
  key: string;
  preserveSqlSource: boolean;
}> {
  if (args.tables !== undefined && Object.keys(args.tables).length > 0) {
    const store = memoryStore();
    for (const [name, path] of Object.entries(args.tables)) {
      const bytes = await readFile(path);
      await store.put(name, new Uint8Array(bytes));
    }
    return { store, key: "input", preserveSqlSource: true };
  }
  const path = requireOption(args.path, "--path");
  return { ...(await localStore(path)), preserveSqlSource: false };
}

function parseArgs(argv: string[]): ParsedArgs {
  const args: ParsedArgs = { help: false };
  const [command, ...rest] = argv;
  if (command === "--help" || command === "-h") args.help = true;
  else if (command !== undefined) args.command = command;
  for (let index = 0; index < rest.length; index += 1) {
    const arg = rest[index];
    if (arg === "--help" || arg === "-h") args.help = true;
    else if (arg === "--path") {
      index += 1;
      args.path = requireValue(rest, index, arg);
    } else if (arg === "--table") {
      index += 1;
      const { name, path } = parseTableArg(requireValue(rest, index, arg));
      args.tables = { ...(args.tables ?? {}), [name]: path };
    } else if (arg === "--output") {
      index += 1;
      args.output = requireValue(rest, index, arg);
    } else if (arg === "--sql") {
      index += 1;
      args.sql = requireValue(rest, index, arg);
    } else if (arg === "--format") {
      index += 1;
      args.format = parseFormat(requireValue(rest, index, arg));
    } else if (arg === "--partition-by") {
      index += 1;
      args.partitionBy = parseCsv(requireValue(rest, index, arg), arg);
    } else if (arg === "--max-rows-per-file") {
      index += 1;
      args.maxRowsPerFile = parsePositiveInt(requireValue(rest, index, arg), arg);
    } else if (arg === "--join-max-right-rows") {
      index += 1;
      args.joinMaxRightRows = parsePositiveInt(requireValue(rest, index, arg), arg);
    } else if (arg === "--manifest") {
      index += 1;
      args.manifest = requireValue(rest, index, arg);
    } else if (arg === "--job-id") {
      index += 1;
      args.jobId = requireValue(rest, index, arg);
    } else throw new LakeqlError("LAKEQL_PARSE_ERROR", `Unknown argument ${arg}`);
  }
  return args;
}

function parseTableArg(value: string): { name: string; path: string } {
  const separator = value.indexOf("=");
  if (separator <= 0 || separator === value.length - 1) {
    throw new LakeqlError("LAKEQL_PARSE_ERROR", "--table must be name=path.parquet");
  }
  const name = value.slice(0, separator);
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(name)) {
    throw new LakeqlError("LAKEQL_PARSE_ERROR", "--table name must be a SQL identifier");
  }
  return { name, path: value.slice(separator + 1) };
}

function parseFormat(value: string): "csv" | "json" | "ndjson" {
  if (value === "csv" || value === "json" || value === "ndjson") return value;
  throw new LakeqlError("LAKEQL_PARSE_ERROR", "--format must be csv, json, or ndjson");
}

function requireValue(args: string[], index: number, flag: string): string {
  const value = args[index];
  if (value === undefined || value.startsWith("--")) {
    throw new LakeqlError("LAKEQL_PARSE_ERROR", `${flag} requires a value`);
  }
  return value;
}

function requireOption(value: string | undefined, flag: string): string {
  if (value === undefined) throw new LakeqlError("LAKEQL_PARSE_ERROR", `${flag} is required`);
  return value;
}

function parseCsv(value: string, flag: string): string[] {
  const values = value
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  if (values.length === 0) throw new LakeqlError("LAKEQL_PARSE_ERROR", `${flag} must not be empty`);
  return values;
}

function parsePositiveInt(value: string, flag: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new LakeqlError("LAKEQL_PARSE_ERROR", `${flag} must be a positive integer`);
  }
  return parsed;
}

function totalRows(rowGroups: { num_rows: bigint | number }[]): number {
  return rowGroups.reduce((sum, rowGroup) => sum + Number(rowGroup.num_rows), 0);
}

function ok(stdout: string): CliResult {
  return { stdout, stderr: "", exitCode: 0 };
}

function fail(stderr: string, exitCode: number): CliResult {
  return { stdout: "", stderr, exitCode };
}
