import type { ColumnData } from "hyparquet";
import { parquetRead, parquetSchema } from "hyparquet";
import {
  type Batch,
  batchFromColumns,
  type MemoryBudget,
  type MemoryScope,
  type OwnedMemory,
} from "lakeql-core";
import { parquetBatchSize } from "./batch-size.js";
import { lakeqlParquetCompressors, scopedParquetCompressors } from "./compressors.js";
import { normalizeDecimalColumns, rawDecimalMetadata } from "./decimal.js";
import { decodedColumnCacheKey } from "./decoded-column-cache.js";
import { nestedValueNormalizer } from "./nested-values.js";
import { parquetParsersForMetadata } from "./parsers.js";
import {
  recordReadColumns,
  recordRowGroupRead,
  recordRowGroupSkipped,
  recordRowsDecoded,
} from "./read-metrics.js";
import { ParquetReadScope } from "./read-scope.js";
import { rowGroupMayMatch, rowGroupMustMatch } from "./row-group-pruning.js";
import { schemaPathIndex } from "./schema-path-index.js";
import type {
  ParquetColumnBatch,
  ParquetMetadata,
  ReadParquetBatchOptions,
  StoreAsyncBuffer,
} from "./types.js";

interface ColumnBatchPlan {
  readColumns: string[];
  rowStart: number;
  rowEnd: number;
  residualPredicateSatisfied: boolean;
}

export async function* readParquetColumnBatchesFromFile(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  options: ReadParquetBatchOptions,
): AsyncIterable<ParquetColumnBatch> {
  for (const plan of columnBatchPlans(metadata, options)) {
    yield await readColumnBatchPlan(file, metadata, options, plan);
  }
}

/** Each fallback batch owns only its own input/decode stage, not previous batches. */
export async function* readOwnedParquetColumnBatches(
  source: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  budget: MemoryBudget,
  options: ReadParquetBatchOptions,
  signals: readonly AbortSignal[] = [],
): AsyncIterable<OwnedMemory<ParquetColumnBatch>> {
  for (const plan of columnBatchPlans(metadata, options)) {
    const stage = new ParquetReadScope(source, budget, signals);
    try {
      const output = await readColumnBatchPlan(
        stage.file,
        metadata,
        { ...options, allocationScope: stage.allocations },
        plan,
      );
      yield stage.retainInputBacking(output);
    } finally {
      stage.dispose();
    }
  }
}

function* columnBatchPlans(
  metadata: ParquetMetadata,
  options: ReadParquetBatchOptions,
): Iterable<ColumnBatchPlan> {
  const batchSize = parquetBatchSize(options.batchSize);
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
      yield {
        readColumns,
        rowStart,
        rowEnd: Math.min(rowStart + batchSize, end),
        residualPredicateSatisfied,
      };
    }
    rowGroupStart = rowGroupEnd;
  }
}

async function readColumnBatchPlan(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  options: ReadParquetBatchOptions,
  plan: ColumnBatchPlan,
): Promise<ParquetColumnBatch> {
  const { readColumns, rowStart, rowEnd, residualPredicateSatisfied } = plan;
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
  const cached =
    key === undefined || cache === undefined ? undefined : cache.get(key, options.allocationScope);
  let batch: Batch;
  if (cached !== undefined) {
    batch = cached;
  } else {
    batch = await readParquetColumnBatch(
      file,
      metadata,
      readColumns,
      rowStart,
      rowEnd,
      options.allocationScope ?? options.decompressionScope,
    );
    if (key !== undefined && cache !== undefined) cache.set(key, batch, options.allocationScope);
  }
  if (key !== undefined && options.stats !== undefined) {
    if (cached === undefined) options.stats.cacheMisses += 1;
    else options.stats.cacheHits += 1;
  }
  recordRowsDecoded(options.stats, batch.rowCount);
  return { rowOffset: rowStart, batch, residualPredicateSatisfied };
}

export async function readParquetColumnBatch(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  columns: string[],
  rowStart: number,
  rowEnd: number,
  decompressionScope?: MemoryScope,
): Promise<Batch> {
  const columnValues: Record<string, ArrayLike<unknown>> = Object.fromEntries(
    columns.map((column) => [column, []]),
  );
  const readOptions: Parameters<typeof parquetRead>[0] = {
    file,
    metadata: rawDecimalMetadata(metadata, columns),
    columns,
    rowStart,
    rowEnd,
    compressors:
      decompressionScope === undefined
        ? lakeqlParquetCompressors
        : scopedParquetCompressors(decompressionScope),
    parsers: parquetParsersForMetadata(metadata),
    utf8: false,
    onChunk(chunk) {
      appendColumnChunk(columnValues, chunk, rowStart, rowEnd);
    },
  };
  await parquetRead(readOptions);
  return batchFromColumns(
    normalizeParquetColumnValues(
      metadata,
      columns,
      normalizeDecimalColumns(metadata, columns, columnValues),
    ),
    decompressionScope,
  );
}

function normalizeParquetColumnValues(
  metadata: ParquetMetadata,
  columns: readonly string[],
  columnValues: Record<string, ArrayLike<unknown>>,
): Record<string, ArrayLike<unknown>> {
  const normalized = { ...columnValues };
  const lookup = schemaPathIndex(metadata.schema);
  for (const column of columns) {
    const values = normalized[column];
    if (values === undefined) continue;
    const schema = lookup([column]).at(-1);
    const normalize = schema === undefined ? undefined : nestedValueNormalizer(schema);
    if (normalize !== undefined) normalized[column] = Array.from(values, normalize);
  }
  return normalized;
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
