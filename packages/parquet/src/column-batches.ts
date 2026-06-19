import type { ColumnData } from "hyparquet";
import { parquetRead, parquetSchema } from "hyparquet";
import { getSchemaPath } from "hyparquet/src/schema.js";
import { type Batch, batchFromColumns } from "lakeql-core";
import { decodedColumnCacheKey } from "./decoded-column-cache.js";
import { lakeqlParquetParsers } from "./parsers.js";
import {
  recordReadColumns,
  recordRowGroupRead,
  recordRowGroupSkipped,
  recordRowsDecoded,
} from "./read-metrics.js";
import { rowGroupMayMatch, rowGroupMustMatch } from "./row-group-pruning.js";
import type {
  ParquetColumnBatch,
  ParquetMetadata,
  ReadParquetBatchOptions,
  StoreAsyncBuffer,
} from "./types.js";

export async function* readParquetColumnBatchesFromFile(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  options: ReadParquetBatchOptions,
): AsyncIterable<ParquetColumnBatch> {
  const batchSize = options.batchSize ?? 4096;
  const requestedStart = options.rowStart ?? 0;
  const requestedEnd = options.rowEnd ?? Number(metadata.num_rows);
  const readColumns = options.columns ?? parquetTopLevelColumns(metadata);
  recordReadColumns(options.stats, readColumns);
  let rowGroupStart = 0;
  for (const rowGroup of metadata.row_groups) {
    const rowGroupEnd = rowGroupStart + Number(rowGroup.num_rows);
    if (
      rowGroupEnd <= requestedStart ||
      rowGroupStart >= requestedEnd ||
      !rowGroupMayMatch(rowGroup, options.where)
    ) {
      recordRowGroupSkipped(options.stats);
      rowGroupStart = rowGroupEnd;
      continue;
    }
    recordRowGroupRead(options.stats);
    const residualPredicateSatisfied = rowGroupMustMatch(rowGroup, options.where);
    const start = Math.max(rowGroupStart, requestedStart);
    const end = Math.min(rowGroupEnd, requestedEnd);
    for (let rowStart = start; rowStart < end; rowStart += batchSize) {
      const rowEnd = Math.min(rowStart + batchSize, end);
      const cache = options.decodedColumnCache;
      const key =
        cache === undefined || options.decodedColumnCacheKey === undefined
          ? undefined
          : decodedColumnCacheKey({
              path: options.decodedColumnCacheKey,
              byteLength: file.byteLength,
              ...(file.etag === undefined ? {} : { etag: file.etag }),
              columns: readColumns,
              rowStart,
              rowEnd,
            });
      const cached = key === undefined || cache === undefined ? undefined : cache.get(key);
      let batch: Batch;
      if (cached !== undefined) {
        batch = cached;
      } else {
        batch = await readParquetColumnBatch(file, metadata, readColumns, rowStart, rowEnd);
        if (key !== undefined && cache !== undefined) cache.set(key, batch);
      }
      if (key !== undefined && options.stats !== undefined) {
        if (cached === undefined) options.stats.cacheMisses += 1;
        else options.stats.cacheHits += 1;
      }
      recordRowsDecoded(options.stats, batch.rowCount);
      yield { rowOffset: rowStart, batch, residualPredicateSatisfied };
    }
    rowGroupStart = rowGroupEnd;
  }
}

export async function readParquetColumnBatch(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  columns: string[],
  rowStart: number,
  rowEnd: number,
): Promise<Batch> {
  const columnValues: Record<string, ArrayLike<unknown>> = Object.fromEntries(
    columns.map((column) => [column, []]),
  );
  const readOptions: Parameters<typeof parquetRead>[0] = {
    file,
    metadata,
    columns,
    rowStart,
    rowEnd,
    parsers: lakeqlParquetParsers,
    onChunk(chunk) {
      appendColumnChunk(columnValues, chunk, rowStart, rowEnd);
    },
  };
  await parquetRead(readOptions);
  return batchFromColumns(normalizeParquetColumnValues(metadata, columns, columnValues));
}

function normalizeParquetColumnValues(
  metadata: ParquetMetadata,
  columns: readonly string[],
  columnValues: Record<string, ArrayLike<unknown>>,
): Record<string, ArrayLike<unknown>> {
  const normalized = { ...columnValues };
  for (const column of columns) {
    const values = normalized[column];
    if (values === undefined || !isParquetMapColumn(metadata, column)) continue;
    normalized[column] = Array.from(values, parquetMapValue);
  }
  return normalized;
}

function isParquetMapColumn(metadata: ParquetMetadata, column: string): boolean {
  try {
    const element = getSchemaPath(metadata.schema, [column]).at(-1)?.element;
    const logicalType = element?.logical_type;
    return (
      element?.converted_type === "MAP" ||
      element?.converted_type === "MAP_KEY_VALUE" ||
      logicalTypeName(logicalType) === "MAP"
    );
  } catch {
    return false;
  }
}

function logicalTypeName(logicalType: unknown): string | undefined {
  if (typeof logicalType === "string") return logicalType;
  if (typeof logicalType !== "object" || logicalType === null) return undefined;
  if ("type" in logicalType && typeof logicalType.type === "string") return logicalType.type;
  for (const key of ["LIST", "MAP"]) {
    if (key in logicalType) return key;
  }
  return undefined;
}

function parquetMapValue(value: unknown): unknown {
  if (value === null || value === undefined || value instanceof Map) return value;
  if (Array.isArray(value)) return new Map(value as Iterable<readonly [unknown, unknown]>);
  if (typeof value === "object") return new Map(Object.entries(value));
  return value;
}

function appendColumnChunk(
  columnValues: Record<string, ArrayLike<unknown>>,
  chunk: ColumnData,
  rowStart: number,
  rowEnd: number,
): void {
  const values = columnValues[chunk.columnName];
  if (values === undefined) return;
  if (chunk.rowStart === rowStart && chunk.rowEnd === rowEnd) {
    columnValues[chunk.columnName] = chunk.columnData as ArrayLike<unknown>;
    return;
  }
  const start = Math.max(rowStart, chunk.rowStart);
  const end = Math.min(rowEnd, chunk.rowEnd);
  const out = values as unknown[];
  for (let row = start; row < end; row += 1) {
    out[row - rowStart] = chunk.columnData[row - chunk.rowStart];
  }
}

function parquetTopLevelColumns(metadata: ParquetMetadata): string[] {
  return parquetSchema(metadata).children.map((child) => child.element.name);
}
