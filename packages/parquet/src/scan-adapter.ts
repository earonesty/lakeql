import { parquetReadObjects } from "hyparquet";
import { PageTypes } from "hyparquet/src/constants.js";
import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { convertMetadata } from "hyparquet/src/metadata.js";
import { deserializeTCompactProtocol } from "hyparquet/src/thrift.js";
import {
  type Batch,
  type CacheAdapter,
  LakeqlError,
  type ObjectStore,
  type Row,
  type ScanAdapter,
  type ScanColumnBatch,
  type ScanDataRange,
  type ScanDataRangePlan,
  type ScanOptions,
  type ScanStatsValue,
  type ScanTaskPlan,
  type ScanTaskPlanOptions,
  throwIfAborted,
} from "lakeql-core";
import { readParquetColumnBatchesFromFile } from "./column-batches.js";
import type { DecodedColumnCache } from "./decoded-column-cache.js";
import { normalizeDecodedRows } from "./decoded-rows.js";
import { readCachedParquetMetadata } from "./metadata-cache.js";
import { cachedRangeBuffer, type RangeCacheOptions } from "./range-cache.js";
import { recordReadColumns } from "./read-metrics.js";
import { planRowGroupsFromMetadata } from "./row-group-plan.js";
import { rowGroupMayMatch } from "./row-group-pruning.js";
import { rejectUnsupportedParquetSchema } from "./schema.js";
import { asyncBufferFromObjectInfo, asyncBufferFromStore } from "./store-buffer.js";
import type { ParquetMetadata, StoreAsyncBuffer } from "./types.js";

export class ParquetScanAdapter implements ScanAdapter {
  private readonly store: ObjectStore;
  private readonly defaultBatchSize: number;
  private readonly metadataCache: CacheAdapter<ParquetMetadata> | undefined;
  private readonly decodedColumnCache: DecodedColumnCache | undefined;
  private readonly scanRangeCache: RangeCacheOptions | undefined;

  constructor(
    store: ObjectStore,
    options: {
      batchSize?: number;
      metadataCache?: CacheAdapter<ParquetMetadata>;
      decodedColumnCache?: DecodedColumnCache;
      scanRangeCache?: RangeCacheOptions;
    } = {},
  ) {
    this.store = store;
    this.defaultBatchSize = options.batchSize ?? 4096;
    this.metadataCache = options.metadataCache;
    this.decodedColumnCache = options.decodedColumnCache;
    this.scanRangeCache = options.scanRangeCache;
  }

  async *scan(path: string, options: ScanOptions): AsyncIterable<Row[]> {
    const batchSize = options.batchSize || this.defaultBatchSize;
    const file = this.scanBuffer(await asyncBufferFromStore(this.store, path, options));
    const metadata = await this.metadata(path, file, options);
    rejectUnsupportedParquetSchema(metadata);
    const readColumns = options.columns;
    if (readColumns) {
      recordReadColumns(options.stats, readColumns);
    }

    let rowGroupStart = 0;
    for (const rowGroup of metadata.row_groups) {
      throwIfAborted(options.budget.signal);
      const rowGroupEnd = rowGroupStart + Number(rowGroup.num_rows);
      if (!rowGroupMayMatch(rowGroup, options.where)) {
        options.stats.rowGroupsSkipped += 1;
        rowGroupStart = rowGroupEnd;
        continue;
      }
      options.stats.rowGroupsRead += 1;
      for (let rowStart = rowGroupStart; rowStart < rowGroupEnd; rowStart += batchSize) {
        throwIfAborted(options.budget.signal);
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
          yield normalizeDecodedRows(await parquetReadObjects(readOptions));
        } catch (cause) {
          throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Failed to read ${path}`, {
            path,
            cause,
          });
        }
      }
      rowGroupStart = rowGroupEnd;
    }
  }

  async *scanColumns(path: string, options: ScanOptions): AsyncIterable<Batch> {
    for await (const columnBatch of this.scanColumnBatches(path, options)) {
      yield columnBatch.batch;
    }
  }

  async *scanColumnBatches(path: string, options: ScanOptions): AsyncIterable<ScanColumnBatch> {
    const file = this.scanBuffer(await asyncBufferFromStore(this.store, path, options));
    const metadata = await this.metadata(path, file, options);
    rejectUnsupportedParquetSchema(metadata);
    try {
      for await (const columnBatch of readParquetColumnBatchesFromFile(file, metadata, {
        batchSize: options.batchSize || this.defaultBatchSize,
        ...(options.rowStart === undefined ? {} : { rowStart: options.rowStart }),
        ...(options.rowEnd === undefined ? {} : { rowEnd: options.rowEnd }),
        ...(options.columns === undefined ? {} : { columns: options.columns }),
        ...(options.where === undefined ? {} : { where: options.where }),
        ...(this.decodedColumnCache === undefined
          ? {}
          : {
              decodedColumnCache: this.decodedColumnCache,
              decodedColumnCacheKey: path,
            }),
        stats: options.stats,
      })) {
        throwIfAborted(options.budget.signal);
        yield columnBatch;
      }
    } catch (cause) {
      if (cause instanceof LakeqlError) throw cause;
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Failed to read ${path}`, {
        path,
        cause,
      });
    }
  }

  async planTask(path: string, options: ScanTaskPlanOptions): Promise<ScanTaskPlan> {
    const file =
      options.object === undefined
        ? await asyncBufferFromStore(this.store, path)
        : asyncBufferFromObjectInfo(this.store, options.object);
    const metadata = await this.metadata(path, file);
    return {
      rowGroupCount: metadata.row_groups.length,
      rowGroupRanges: planRowGroupsFromMetadata(metadata, options.where).rowGroupRanges,
    };
  }

  async planDataRanges(path: string, options: ScanTaskPlanOptions): Promise<ScanDataRangePlan> {
    const file =
      options.object === undefined
        ? await asyncBufferFromStore(this.store, path)
        : asyncBufferFromObjectInfo(this.store, options.object);
    const metadata = await this.metadata(path, file);
    const plan = planRowGroupsFromMetadata(metadata, options.where);
    const pageColumn = options.columns?.[0];
    if (pageColumn !== undefined) {
      const pageRanges = await planPageDataRanges(file, metadata, plan.rowGroups, pageColumn);
      if (pageRanges.length > 0) return { ranges: pageRanges };
    }
    return {
      ranges: plan.rowGroups.map((rowGroup) => ({
        rowStart: rowGroup.rowStart,
        rowEnd: rowGroup.rowStart + rowGroup.rowCount,
        ...(rowGroup.stats === undefined ? {} : { stats: rowGroup.stats }),
      })),
    };
  }

  private async metadata(
    path: string,
    file: StoreAsyncBuffer,
    options?: ScanOptions,
  ): Promise<ParquetMetadata> {
    const { metadata, cached } = await readCachedParquetMetadata(path, file, this.metadataCache);
    if (cached) {
      if (options !== undefined) options.stats.cacheHits += 1;
    } else if (options !== undefined) {
      options.stats.cacheMisses += 1;
    }
    return metadata;
  }

  private scanBuffer(file: StoreAsyncBuffer): StoreAsyncBuffer {
    return this.scanRangeCache === undefined ? file : cachedRangeBuffer(file, this.scanRangeCache);
  }
}

async function planPageDataRanges(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  rowGroups: readonly { index: number; rowStart: number; rowCount: number }[],
  column: string,
): Promise<ScanDataRange[]> {
  const ranges: ScanDataRange[] = [];
  let foundPages = false;
  for (const planned of rowGroups) {
    const rowGroup = metadata.row_groups[planned.index];
    const chunk = rowGroup?.columns.find(
      (candidate) => candidate.meta_data?.path_in_schema.join(".") === column,
    );
    const columnMetadata = chunk?.meta_data;
    if (rowGroup === undefined || columnMetadata === undefined) {
      ranges.push({ rowStart: planned.rowStart, rowEnd: planned.rowStart + planned.rowCount });
      continue;
    }
    const schema = metadata.schema.find((element) => element.name === column);
    if (schema === undefined) {
      ranges.push({ rowStart: planned.rowStart, rowEnd: planned.rowStart + planned.rowCount });
      continue;
    }
    const start = safeNumber(
      columnMetadata.dictionary_page_offset ?? columnMetadata.data_page_offset,
    );
    const compressedSize = safeNumber(columnMetadata.total_compressed_size);
    if (start === undefined || compressedSize === undefined || compressedSize <= 0) {
      ranges.push({ rowStart: planned.rowStart, rowEnd: planned.rowStart + planned.rowCount });
      continue;
    }
    const pageRanges = await columnPageDataRanges(
      file,
      start,
      start + compressedSize,
      planned.rowStart,
      planned.rowCount,
      column,
      schema,
    );
    if (pageRanges.length === 0) {
      ranges.push({ rowStart: planned.rowStart, rowEnd: planned.rowStart + planned.rowCount });
      continue;
    }
    foundPages = true;
    ranges.push(...pageRanges);
  }
  return foundPages ? ranges : [];
}

async function columnPageDataRanges(
  file: StoreAsyncBuffer,
  start: number,
  end: number,
  rowGroupStart: number,
  rowGroupRows: number,
  column: string,
  schema: ParquetMetadata["schema"][number],
): Promise<ScanDataRange[]> {
  const buffer = await file.slice(start, end);
  const reader = { view: new DataView(buffer), offset: 0 };
  const ranges: ScanDataRange[] = [];
  let pageRowStart = 0;
  while (reader.offset < reader.view.byteLength - 1 && pageRowStart < rowGroupRows) {
    const header = deserializeTCompactProtocol(reader);
    const type = PageTypes[header.field_1 as keyof typeof PageTypes];
    const compressedPageSize = safeNumber(header.field_3);
    if (compressedPageSize === undefined) break;
    if (type === "DATA_PAGE") {
      const dataHeader = header.field_5;
      const rowCount = safeNumber(dataHeader?.field_1);
      if (rowCount === undefined) break;
      ranges.push(
        pageRange(rowGroupStart, pageRowStart, rowCount, column, schema, dataHeader?.field_5),
      );
      pageRowStart += rowCount;
    } else if (type === "DATA_PAGE_V2") {
      const dataHeader = header.field_8;
      const rowCount = safeNumber(dataHeader?.field_3);
      if (rowCount === undefined) break;
      ranges.push(
        pageRange(rowGroupStart, pageRowStart, rowCount, column, schema, dataHeader?.field_8),
      );
      pageRowStart += rowCount;
    }
    reader.offset += compressedPageSize;
  }
  return ranges;
}

function pageRange(
  rowGroupStart: number,
  pageRowStart: number,
  rowCount: number,
  column: string,
  schema: ParquetMetadata["schema"][number],
  stats: unknown,
): ScanDataRange {
  const range: ScanDataRange = {
    rowStart: rowGroupStart + pageRowStart,
    rowEnd: rowGroupStart + pageRowStart + rowCount,
  };
  const columnStats = pageColumnStats(stats, schema);
  if (columnStats !== undefined) range.stats = { [column]: columnStats };
  return range;
}

function pageColumnStats(
  stats: unknown,
  schema: ParquetMetadata["schema"][number],
): { min: ScanStatsValue; max: ScanStatsValue; hasNoNulls: boolean } | undefined {
  if (stats === undefined || stats === null || typeof stats !== "object") return undefined;
  const record = stats as Record<string, unknown>;
  const minBytes = uint8StatsValue(record.field_6 ?? record.field_2);
  const maxBytes = uint8StatsValue(record.field_5 ?? record.field_1);
  const min = scanStatsValue(convertMetadata(minBytes, schema, DEFAULT_PARSERS));
  const max = scanStatsValue(convertMetadata(maxBytes, schema, DEFAULT_PARSERS));
  if (min === undefined || max === undefined) return undefined;
  return {
    min,
    max,
    hasNoNulls: record.field_3 === 0n || record.field_3 === 0,
  };
}

function uint8StatsValue(value: unknown): Uint8Array | undefined {
  return value instanceof Uint8Array ? value : undefined;
}

function scanStatsValue(value: unknown): ScanStatsValue | undefined {
  if (typeof value === "string") return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "bigint") return value;
  if (typeof value === "boolean") return value;
  return undefined;
}

function safeNumber(value: bigint | number | undefined): number | undefined {
  if (typeof value === "number" && Number.isSafeInteger(value)) return value;
  if (typeof value === "bigint") {
    const numberValue = Number(value);
    if (Number.isSafeInteger(numberValue) && BigInt(numberValue) === value) return numberValue;
  }
  return undefined;
}
