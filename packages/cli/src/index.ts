import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import {
  LaQLError,
  type MemoryObjectStore,
  memoryStore,
  type QueryBuilder,
  type Row,
} from "@laql/core";
import {
  createParquetLake,
  readParquetMetadata,
  type WriteParquetRowsOptions,
  writePartitionedParquet,
} from "@laql/parquet";
import { parseSql } from "@laql/sql";

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
  output?: string;
  sql?: string;
  format?: "json" | "ndjson";
  partitionBy?: string[];
  maxRowsPerFile?: number;
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
    if (error instanceof LaQLError) return fail(`${error.code}: ${error.message}\n`, 1);
    if (error instanceof Error) return fail(`${error.message}\n`, 1);
    return fail("Unknown CLI error\n", 1);
  }
}

export function usage(): string {
  const reserved = COMMANDS.filter(
    (command) => !["compact", "query", "explain", "inspect", "write", "schema"].includes(command),
  );
  const lines = [
    "usage: laql <command> [options]",
    "",
    "commands:",
    "  compact --path <file.parquet> --output <prefix> [--max-rows-per-file n]",
    "  query   --path <file.parquet> --sql <query> [--format json|ndjson]",
    "  explain --path <file.parquet> --sql <query>",
    "  inspect --path <file.parquet>",
    "  write   --path <file.parquet> --sql <query> --output <prefix> [--partition-by a,b] [--max-rows-per-file n]",
    "  schema  --path <file.parquet>",
  ];
  if (reserved.length > 0) {
    lines.push("", `other commands reserved by the build plan: ${reserved.join(", ")}`);
  }
  return lines.join("\n");
}

async function query(args: ParsedArgs): Promise<string> {
  const path = requireOption(args.path, "--path");
  const sql = requireOption(args.sql, "--sql");
  const { store, key } = await localStore(path);
  const ast = parseCliSql(sql, key);
  const lake = createParquetLake({ store });
  const builder = lake.path(ast.source);
  const result = builderFromAst(builder, ast);
  if (args.format === "json") return `${JSON.stringify(await result.toArray())}\n`;
  let out = "";
  for await (const row of result.rows()) out += `${JSON.stringify(row)}\n`;
  return out;
}

async function explain(args: ParsedArgs): Promise<string> {
  const path = requireOption(args.path, "--path");
  const sql = requireOption(args.sql, "--sql");
  const { store, key } = await localStore(path);
  const lake = createParquetLake({ store });
  return `${(await builderFromAst(lake.path(key), parseCliSql(sql, key)).explain()).text}\n`;
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
  const metadata = await readParquetMetadata(store, key);
  const columns = metadata.schema
    .filter((field) => field.name !== "root")
    .map((field) => ({ name: field.name, type: field.type ?? field.converted_type ?? "group" }));
  return `${JSON.stringify({ path, rows: Number(totalRows(metadata.row_groups)), columns })}\n`;
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
  const inputPath = requireOption(args.path, "--path");
  const outputPrefix = requireOption(args.output, "--output");
  const sql = requireOption(args.sql, "--sql");
  const { store, key } = await localStore(inputPath);
  const ast = parseCliSql(sql, key);
  const lake = createParquetLake({ store });
  const rows = await builderFromAst(lake.path(ast.source), ast).toArray();
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
      throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `No generated output at ${file.path}`, {
        path: file.path,
      });
    }
    await mkdir(dirname(file.path), { recursive: true });
    await writeFile(file.path, bytes);
  }

  return `${JSON.stringify({ files: result.files })}\n`;
}

function builderFromAst(builder: QueryBuilder, ast: ReturnType<typeof parseSql>): QueryBuilder {
  let next = builder;
  if (ast.select) next = next.select(ast.select);
  if (ast.where) next = next.where(ast.where);
  if (ast.orderBy) next = next.orderBy(ast.orderBy);
  if (ast.offset !== undefined) next = next.offset(ast.offset);
  if (ast.limit !== undefined) next = next.limit(ast.limit);
  return next;
}

function parseCliSql(sql: string, defaultSource: string): ReturnType<typeof parseSql> {
  const trimmed = sql.trim();
  if (/^from\s/iu.test(trimmed)) return { ...parseSql(trimmed), source: defaultSource };
  return { ...parseSql(`from input ${trimmed}`), source: defaultSource };
}

async function localStore(path: string): Promise<{ store: MemoryObjectStore; key: string }> {
  const store = memoryStore();
  const bytes = await readFile(path);
  await store.put(path, new Uint8Array(bytes));
  return { store, key: path };
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
    } else throw new LaQLError("LAQL_PARSE_ERROR", `Unknown argument ${arg}`);
  }
  return args;
}

function parseFormat(value: string): "json" | "ndjson" {
  if (value === "json" || value === "ndjson") return value;
  throw new LaQLError("LAQL_PARSE_ERROR", "--format must be json or ndjson");
}

function requireValue(args: string[], index: number, flag: string): string {
  const value = args[index];
  if (value === undefined || value.startsWith("--")) {
    throw new LaQLError("LAQL_PARSE_ERROR", `${flag} requires a value`);
  }
  return value;
}

function requireOption(value: string | undefined, flag: string): string {
  if (value === undefined) throw new LaQLError("LAQL_PARSE_ERROR", `${flag} is required`);
  return value;
}

function parseCsv(value: string, flag: string): string[] {
  const values = value
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  if (values.length === 0) throw new LaQLError("LAQL_PARSE_ERROR", `${flag} must not be empty`);
  return values;
}

function parsePositiveInt(value: string, flag: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new LaQLError("LAQL_PARSE_ERROR", `${flag} must be a positive integer`);
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
