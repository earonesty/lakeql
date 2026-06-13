import {
  type Expr,
  Lake,
  type LakeConfig,
  LaQLError,
  type ObjectStore,
  type Row,
  type ScanAdapter,
  type ScanOptions,
} from "@laql/core";
import type { RowGroup } from "hyparquet";
import { parquetMetadataAsync, parquetReadObjects } from "hyparquet";

export interface ReadParquetOptions {
  /** Columns to project; all columns when omitted. */
  columns?: string[];
  rowStart?: number;
  rowEnd?: number;
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
