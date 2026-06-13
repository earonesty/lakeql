import {
  type Expr,
  Lake,
  type LakeConfig,
  LaQLError,
  type ObjectStore,
  type OutputManifestEntry,
  type Row,
  type ScanAdapter,
  type ScanOptions,
} from "@laql/core";
import type { RowGroup } from "hyparquet";
import { parquetMetadataAsync, parquetReadObjects } from "hyparquet";
import type { BasicType, ColumnSource, ParquetWriteOptions } from "hyparquet-writer";
import { parquetWriteBuffer } from "hyparquet-writer";

export interface ReadParquetOptions {
  /** Columns to project; all columns when omitted. */
  columns?: string[];
  rowStart?: number;
  rowEnd?: number;
}

export interface WriteParquetOptions extends Omit<ParquetWriteOptions, "writer" | "columnData"> {
  columnData: ColumnSource[];
  contentType?: string;
}

export interface WriteParquetRowsOptions
  extends Omit<WriteParquetOptions, "columnData" | "schema"> {
  rows: Row[];
  partitionBy?: string[];
  maxRowsPerFile?: number;
  maxBytesPerFile?: number;
  jobId?: string;
  columnTypes?: Record<string, BasicType>;
}

export interface WritePartitionedParquetFile {
  path: string;
  byteSize: number;
  etag?: string;
  rowCount: number;
  partitionValues: Record<string, string>;
}

export interface WritePartitionedParquetResult {
  files: WritePartitionedParquetFile[];
}

export interface PartitionedParquetOutputEntryOptions {
  taskId: string | ((file: WritePartitionedParquetFile, index: number) => string);
}

export interface ParquetLakeConfig extends Omit<LakeConfig, "scanner"> {
  batchSize?: number;
}

/**
 * Bridge an ObjectStore path to hyparquet's AsyncBuffer (length + ranged slice).
 */
export async function asyncBufferFromStore(
  store: ObjectStore,
  path: string,
  options: ScanOptions | undefined = undefined,
) {
  const head = await store.head(path);
  if (!head) {
    throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `No object at ${path}`, { path });
  }
  return {
    byteLength: head.size,
    slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
      const length = (end ?? head.size) - start;
      if (options) {
        options.stats.rangeRequests += 1;
        options.stats.bytesRequested += length;
      }
      const bytes = await store.getRange(path, { offset: start, length });
      const out = new ArrayBuffer(bytes.byteLength);
      new Uint8Array(out).set(bytes);
      return out;
    },
  };
}

/**
 * Read rows from a Parquet object. Early scaffold: full planner-driven
 * row-group pruning and batch streaming land in phase 1-2 (see BUILD_PLAN.md).
 */
export async function readParquetObjects(
  store: ObjectStore,
  path: string,
  options: ReadParquetOptions = {},
): Promise<Record<string, unknown>[]> {
  const file = await asyncBufferFromStore(store, path);
  try {
    const readOptions: Parameters<typeof parquetReadObjects>[0] = { file };
    if (options.columns) readOptions.columns = options.columns;
    if (options.rowStart !== undefined) readOptions.rowStart = options.rowStart;
    if (options.rowEnd !== undefined) readOptions.rowEnd = options.rowEnd;
    return await parquetReadObjects(readOptions);
  } catch (cause) {
    throw new LaQLError("LAQL_PARQUET_READ_ERROR", `Failed to read ${path}`, { path, cause });
  }
}

/** Read Parquet footer metadata (row groups, schema, stats). */
export async function readParquetMetadata(store: ObjectStore, path: string) {
  const file = await asyncBufferFromStore(store, path);
  return parquetMetadataAsync(file);
}

export async function writeParquet(
  store: ObjectStore,
  path: string,
  options: WriteParquetOptions,
): Promise<{ path: string; byteSize: number; etag?: string }> {
  try {
    const { contentType, ...writeOptions } = options;
    return await putParquetBytes(store, path, encodeParquetBytes(writeOptions), contentType);
  } catch (cause) {
    throw new LaQLError("LAQL_PARQUET_WRITE_ERROR", `Failed to write ${path}`, { path, cause });
  }
}

export async function writePartitionedParquet(
  store: ObjectStore,
  prefix: string,
  options: WriteParquetRowsOptions,
): Promise<WritePartitionedParquetResult> {
  const maxRowsPerFile = options.maxRowsPerFile ?? options.rows.length;
  validatePartitionedWriteOptions(prefix, options, maxRowsPerFile);

  const normalizedPrefix = prefix.replace(/\/+$/u, "");
  const partitionBy = options.partitionBy ?? [];
  const {
    rows: _rows,
    partitionBy: _partitionBy,
    maxRowsPerFile: _maxRowsPerFile,
    maxBytesPerFile,
    jobId,
    columnTypes,
    contentType,
    ...writeOptions
  } = options;
  const partitions = partitionRows(options.rows, partitionBy);
  const files: WritePartitionedParquetFile[] = [];
  let ordinal = 0;

  for (const partition of partitions) {
    for (let start = 0; start < partition.rows.length; start += maxRowsPerFile) {
      const chunk = partition.rows.slice(start, start + maxRowsPerFile);
      const encodedChunks = splitRowsForFileSize(
        chunk,
        partitionBy,
        columnTypes ?? {},
        writeOptions,
        maxBytesPerFile,
      );
      for (const encodedChunk of encodedChunks) {
        const path = partitionOutputPath(
          normalizedPrefix,
          partition.values,
          partitionBy,
          jobId,
          ordinal,
        );
        const written = await writeEncodedParquet(store, path, encodedChunk.bytes, contentType);
        const result: WritePartitionedParquetFile = {
          path: written.path,
          byteSize: written.byteSize,
          rowCount: encodedChunk.rows.length,
          partitionValues: partition.values,
        };
        if (written.etag !== undefined) result.etag = written.etag;
        files.push(result);
        ordinal += 1;
      }
    }
  }

  return { files };
}

export function partitionedParquetOutputEntries(
  result: WritePartitionedParquetResult,
  options: PartitionedParquetOutputEntryOptions,
): OutputManifestEntry[] {
  return result.files.map((file, index) => {
    const entry: OutputManifestEntry = {
      taskId: typeof options.taskId === "function" ? options.taskId(file, index) : options.taskId,
      outputPath: file.path,
      partitionValues: sortStringRecord(file.partitionValues),
      rowCount: file.rowCount,
      byteSize: file.byteSize,
    };
    if (file.etag !== undefined) entry.etag = file.etag;
    return entry;
  });
}

export class ParquetScanAdapter implements ScanAdapter {
  private readonly store: ObjectStore;
  private readonly defaultBatchSize: number;

  constructor(store: ObjectStore, options: { batchSize?: number } = {}) {
    this.store = store;
    this.defaultBatchSize = options.batchSize ?? 4096;
  }

  async *scan(path: string, options: ScanOptions): AsyncIterable<Row[]> {
    const batchSize = options.batchSize || this.defaultBatchSize;
    const file = await asyncBufferFromStore(this.store, path, options);
    const metadata = await parquetMetadataAsync(file);
    const readColumns = options.columns;
    if (readColumns) {
      const known = new Set(options.stats.columnsRead);
      for (const column of readColumns) {
        if (!known.has(column)) {
          known.add(column);
          options.stats.columnsRead.push(column);
        }
      }
      options.stats.columnsRead.sort();
    }

    let rowGroupStart = 0;
    for (const rowGroup of metadata.row_groups) {
      const rowGroupEnd = rowGroupStart + Number(rowGroup.num_rows);
      if (!rowGroupMayMatch(rowGroup, options.where)) {
        options.stats.rowGroupsSkipped += 1;
        rowGroupStart = rowGroupEnd;
        continue;
      }
      options.stats.rowGroupsRead += 1;
      for (let rowStart = rowGroupStart; rowStart < rowGroupEnd; rowStart += batchSize) {
        const rowEnd = Math.min(rowStart + batchSize, rowGroupEnd);
        const readOptions: Parameters<typeof parquetReadObjects>[0] = {
          file,
          metadata,
          rowFormat: "object",
          rowStart,
          rowEnd,
        };
        if (readColumns) readOptions.columns = readColumns;
        try {
          yield await parquetReadObjects(readOptions);
        } catch (cause) {
          throw new LaQLError("LAQL_PARQUET_READ_ERROR", `Failed to read ${path}`, { path, cause });
        }
      }
      rowGroupStart = rowGroupEnd;
    }
  }
}

interface RowPartition {
  key: string;
  values: Record<string, string>;
  rows: Row[];
}

type ColumnValue = string | number | boolean | bigint | null;

interface EncodedRowChunk {
  rows: Row[];
  bytes: Uint8Array;
}

function validatePartitionedWriteOptions(
  prefix: string,
  options: WriteParquetRowsOptions,
  maxRowsPerFile: number,
): void {
  if (!prefix || prefix.replace(/\/+$/u, "") === "") {
    throw new LaQLError("LAQL_TYPE_ERROR", "Parquet output prefix must be non-empty");
  }
  if (options.rows.length === 0) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "Cannot write an empty row set");
  }
  if (!Number.isInteger(maxRowsPerFile) || maxRowsPerFile < 1) {
    throw new LaQLError("LAQL_TYPE_ERROR", "maxRowsPerFile must be a positive integer", {
      maxRowsPerFile,
    });
  }
  if (
    options.maxBytesPerFile !== undefined &&
    (!Number.isInteger(options.maxBytesPerFile) || options.maxBytesPerFile < 1)
  ) {
    throw new LaQLError("LAQL_TYPE_ERROR", "maxBytesPerFile must be a positive integer", {
      maxBytesPerFile: options.maxBytesPerFile,
    });
  }
  const partitionBy = options.partitionBy ?? [];
  const uniquePartitions = new Set(partitionBy);
  if (uniquePartitions.size !== partitionBy.length) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "partitionBy columns must be unique", {
      partitionBy,
    });
  }
}

function partitionRows(rows: Row[], partitionBy: string[]): RowPartition[] {
  const byKey = new Map<string, RowPartition>();
  for (const row of rows) {
    const values: Record<string, string> = {};
    for (const column of partitionBy) {
      const raw = row[column];
      if (!isPartitionValue(raw)) {
        throw new LaQLError("LAQL_VALIDATION_ERROR", "Partition values must be scalar", {
          column,
        });
      }
      values[column] = String(raw);
    }
    const key = partitionBy.map((column) => `${column}=${values[column]}`).join("/");
    const existing = byKey.get(key);
    if (existing) {
      existing.rows.push(row);
    } else {
      byKey.set(key, { key, values, rows: [row] });
    }
  }
  return [...byKey.values()].sort((left, right) => left.key.localeCompare(right.key));
}

function partitionOutputPath(
  prefix: string,
  partitionValues: Record<string, string>,
  partitionBy: string[],
  jobId: string | undefined,
  ordinal: number,
): string {
  const segments = [prefix];
  for (const column of partitionBy) {
    segments.push(`${column}=${encodeURIComponent(partitionValues[column] ?? "")}`);
  }
  const safeJobId = jobId ?? "data";
  segments.push(`part-${safeJobId}-${String(ordinal).padStart(5, "0")}.parquet`);
  return segments.join("/");
}

function sortStringRecord(record: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const key of Object.keys(record).sort()) out[key] = record[key] ?? "";
  return out;
}

function splitRowsForFileSize(
  rows: Row[],
  partitionBy: string[],
  columnTypes: Record<string, BasicType>,
  writeOptions: Omit<WriteParquetOptions, "columnData" | "contentType">,
  maxBytesPerFile: number | undefined,
): EncodedRowChunk[] {
  const bytes = encodeRows(rows, partitionBy, columnTypes, writeOptions);
  if (maxBytesPerFile === undefined || bytes.byteLength <= maxBytesPerFile || rows.length === 1) {
    return [{ rows, bytes }];
  }
  const mid = Math.ceil(rows.length / 2);
  return [
    ...splitRowsForFileSize(
      rows.slice(0, mid),
      partitionBy,
      columnTypes,
      writeOptions,
      maxBytesPerFile,
    ),
    ...splitRowsForFileSize(
      rows.slice(mid),
      partitionBy,
      columnTypes,
      writeOptions,
      maxBytesPerFile,
    ),
  ];
}

function encodeRows(
  rows: Row[],
  partitionBy: string[],
  columnTypes: Record<string, BasicType>,
  writeOptions: Omit<WriteParquetOptions, "columnData" | "contentType">,
): Uint8Array {
  return encodeParquetBytes({
    ...writeOptions,
    columnData: rowsToColumnData(rows, partitionBy, columnTypes),
  });
}

function rowsToColumnData(
  rows: Row[],
  partitionBy: string[],
  columnTypes: Record<string, BasicType>,
): ColumnSource[] {
  const partitionColumns = new Set(partitionBy);
  const columns = [
    ...new Set(rows.flatMap((row) => Object.keys(row).filter((key) => !partitionColumns.has(key)))),
  ].sort();
  if (columns.length === 0) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "At least one non-partition column is required");
  }

  return columns.map((name) => {
    const data = rows.map((row) => normalizeColumnValue(row[name], name));
    return {
      name,
      data,
      type: columnTypes[name] ?? inferColumnType(name, data),
      nullable: data.some((value) => value === null),
    };
  });
}

function normalizeColumnValue(value: unknown, column: string): ColumnValue {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" || typeof value === "boolean" || typeof value === "bigint") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Numeric column values must be finite", {
        column,
      });
    }
    return value;
  }
  throw new LaQLError("LAQL_VALIDATION_ERROR", "Column values must be scalar", { column });
}

function inferColumnType(column: string, data: ColumnValue[]): BasicType {
  const values = data.filter((value) => value !== null);
  if (values.length === 0) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "Cannot infer type for all-null column", {
      column,
    });
  }
  const kinds = new Set(values.map((value) => typeof value));
  if (kinds.size !== 1) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "Column values must have one scalar type", {
      column,
    });
  }
  const kind = kinds.values().next().value;
  switch (kind) {
    case "boolean":
      return "BOOLEAN";
    case "bigint":
      return "INT64";
    case "string":
      return "STRING";
    case "number":
      return data.every((value) => value === null || isInt32Value(value)) ? "INT32" : "DOUBLE";
    default:
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Unsupported column value type", { column });
  }
}

function isInt32Value(value: ColumnValue): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= -2147483648 &&
    value <= 2147483647
  );
}

function isPartitionValue(value: unknown): value is string | number | boolean | bigint {
  return (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  );
}

function encodeParquetBytes(options: Omit<WriteParquetOptions, "contentType">): Uint8Array {
  return new Uint8Array(parquetWriteBuffer(options));
}

async function writeEncodedParquet(
  store: ObjectStore,
  path: string,
  bytes: Uint8Array,
  contentType: string | undefined,
): Promise<{ path: string; byteSize: number; etag?: string }> {
  try {
    return await putParquetBytes(store, path, bytes, contentType);
  } catch (cause) {
    throw new LaQLError("LAQL_PARQUET_WRITE_ERROR", `Failed to write ${path}`, { path, cause });
  }
}

async function putParquetBytes(
  store: ObjectStore,
  path: string,
  bytes: Uint8Array,
  contentType: string | undefined,
): Promise<{ path: string; byteSize: number; etag?: string }> {
  await store.put(path, bytes, {
    contentType: contentType ?? "application/vnd.apache.parquet",
  });
  const head = await store.head(path);
  const result: { path: string; byteSize: number; etag?: string } = {
    path,
    byteSize: head?.size ?? bytes.byteLength,
  };
  if (head?.etag !== undefined) result.etag = head.etag;
  return result;
}

type StatsValue = string | number | bigint | boolean;

/** @internal Exposed for pruning tests; not part of the stable public API. */
export function rowGroupMayMatch(rowGroup: RowGroup, expr: Expr | undefined): boolean {
  if (!expr) return true;
  switch (expr.kind) {
    case "literal":
    case "column":
    case "null-check":
    case "like":
    case "call":
      return true;
    case "not":
      return true;
    case "logical":
      if (expr.op === "and")
        return expr.operands.every((operand) => rowGroupMayMatch(rowGroup, operand));
      return expr.operands.some((operand) => rowGroupMayMatch(rowGroup, operand));
    case "compare":
      return compareMayMatch(rowGroup, expr);
    case "in":
      return inMayMatch(rowGroup, expr);
    case "between":
      return betweenMayMatch(rowGroup, expr);
  }
}

function compareMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "compare" }>): boolean {
  const pair = columnLiteralPair(expr.left, expr.right);
  if (!pair) return true;
  const stats = columnStats(rowGroup, pair.column);
  if (!stats) return true;
  const { min, max } = stats;
  const value = pair.value;
  if (!sameComparableType(min, value) || !sameComparableType(max, value)) return true;
  switch (expr.op) {
    case "eq":
      return compareValues(value, min) >= 0 && compareValues(value, max) <= 0;
    case "ne":
      return !(compareValues(min, value) === 0 && compareValues(max, value) === 0);
    case "lt":
      return compareValues(min, value) < 0;
    case "lte":
      return compareValues(min, value) <= 0;
    case "gt":
      return compareValues(max, value) > 0;
    case "gte":
      return compareValues(max, value) >= 0;
  }
}

function inMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "in" }>): boolean {
  if (expr.negated) return true;
  if (expr.target.kind !== "column") return true;
  const stats = columnStats(rowGroup, expr.target.name);
  if (!stats) return true;
  return expr.values.some((valueExpr) => {
    if (valueExpr.kind !== "literal" || valueExpr.value === null) return true;
    const value = valueExpr.value;
    if (
      !isStatsValue(value) ||
      !sameComparableType(stats.min, value) ||
      !sameComparableType(stats.max, value)
    ) {
      return true;
    }
    return compareValues(value, stats.min) >= 0 && compareValues(value, stats.max) <= 0;
  });
}

function betweenMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "between" }>): boolean {
  if (
    expr.target.kind !== "column" ||
    expr.low.kind !== "literal" ||
    expr.high.kind !== "literal"
  ) {
    return true;
  }
  if (!isStatsValue(expr.low.value) || !isStatsValue(expr.high.value)) return true;
  const stats = columnStats(rowGroup, expr.target.name);
  if (!stats) return true;
  if (
    !sameComparableType(stats.min, expr.low.value) ||
    !sameComparableType(stats.max, expr.high.value)
  ) {
    return true;
  }
  return (
    compareValues(stats.max, expr.low.value) >= 0 && compareValues(stats.min, expr.high.value) <= 0
  );
}

function columnLiteralPair(
  left: Expr,
  right: Expr,
): { column: string; value: StatsValue } | undefined {
  if (left.kind === "column" && right.kind === "literal" && isStatsValue(right.value)) {
    return { column: left.name, value: right.value };
  }
  if (right.kind === "column" && left.kind === "literal" && isStatsValue(left.value)) {
    return { column: right.name, value: left.value };
  }
  return undefined;
}

function columnStats(
  rowGroup: RowGroup,
  column: string,
): { min: StatsValue; max: StatsValue } | undefined {
  for (const chunk of rowGroup.columns) {
    const metadata = chunk.meta_data;
    if (!metadata || metadata.path_in_schema.join(".") !== column) continue;
    const min = metadata.statistics?.min_value;
    const max = metadata.statistics?.max_value;
    if (isStatsValue(min) && isStatsValue(max)) return { min, max };
  }
  return undefined;
}

function isStatsValue(value: unknown): value is StatsValue {
  return (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  );
}

function sameComparableType(left: StatsValue, right: StatsValue): boolean {
  if (typeof left === typeof right) return true;
  return isNumberLike(left) && isNumberLike(right);
}

function isNumberLike(value: StatsValue): value is number | bigint {
  return typeof value === "number" || typeof value === "bigint";
}

function compareValues(left: StatsValue, right: StatsValue): number {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

export function parquetScanner(
  store: ObjectStore,
  options: { batchSize?: number } = {},
): ScanAdapter {
  return new ParquetScanAdapter(store, options);
}

export function createParquetLake(config: ParquetLakeConfig): Lake {
  const scannerOptions: { batchSize?: number } = {};
  if (config.batchSize !== undefined) scannerOptions.batchSize = config.batchSize;
  return new Lake({
    ...config,
    scanner: parquetScanner(config.store, scannerOptions),
  });
}
