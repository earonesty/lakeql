import { parquetReadObjects } from "hyparquet";
import {
  type Batch,
  type CacheAdapter,
  LakeqlError,
  type ObjectStore,
  type Row,
  type ScanAdapter,
  type ScanColumnBatch,
  type ScanOptions,
  type ScanTaskPlan,
  type ScanTaskPlanOptions,
  type ScanVectorBatch,
  type TaskInput,
  throwIfAborted,
  type WindowExpr,
  type WindowTaskExecutionOptions,
} from "lakeql-core";
import { readParquetColumnBatchesFromFile } from "./column-batches.js";
import { lakeqlParquetCompressors } from "./compressors.js";
import type { DecodedColumnCache } from "./decoded-column-cache.js";
import { normalizeDecodedRows } from "./decoded-rows.js";
import { readCachedParquetMetadata } from "./metadata-cache.js";
import { lakeqlParquetParsers } from "./parsers.js";
import { cachedRangeBuffer, type RangeCacheOptions } from "./range-cache.js";
import { recordReadColumns } from "./read-metrics.js";
import { planRowGroupsFromMetadata } from "./row-group-plan.js";
import { rowGroupMayMatch } from "./row-group-pruning.js";
import { rejectUnsupportedParquetSchema } from "./schema.js";
import { asyncBufferFromObjectInfo, asyncBufferFromStore } from "./store-buffer.js";
import type { ParquetMetadata, StoreAsyncBuffer } from "./types.js";
import { canReadParquetVectorBatches, readParquetVectorBatchesFromFile } from "./vector-batches.js";
import { windowParquetTasks } from "./window-task.js";

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
    const file = this.scanBuffer(path, await asyncBufferFromStore(this.store, path, options));
    const metadata = await this.metadata(path, file, options);
    rejectUnsupportedParquetSchema(metadata, { columns: options.columns });
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
          compressors: lakeqlParquetCompressors,
          parsers: lakeqlParquetParsers,
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
    for await (const vectorBatch of this.scanVectorBatches(path, options)) {
      yield vectorBatch.batch;
    }
  }

  async *scanColumnBatches(path: string, options: ScanOptions): AsyncIterable<ScanColumnBatch> {
    yield* this.scanVectorBatches(path, options);
  }

  async *scanVectorBatches(path: string, options: ScanOptions): AsyncIterable<ScanVectorBatch> {
    const file = this.scanBuffer(path, await asyncBufferFromStore(this.store, path, options));
    const metadata = await this.metadata(path, file, options);
    rejectUnsupportedParquetSchema(metadata, { columns: options.columns });
    try {
      const vectorOptions = {
        batchSize: options.batchSize || this.defaultBatchSize,
        ...(options.rowStart === undefined ? {} : { rowStart: options.rowStart }),
        ...(options.rowEnd === undefined ? {} : { rowEnd: options.rowEnd }),
        ...(options.columns === undefined ? {} : { columns: options.columns }),
        ...(options.where === undefined ? {} : { where: options.where }),
        ...(options.canStopEarly === undefined ? {} : { canStopEarly: options.canStopEarly }),
        ...(this.decodedColumnCache === undefined
          ? {}
          : {
              decodedColumnCache: this.decodedColumnCache,
              decodedColumnCacheKey: path,
            }),
        stats: options.stats,
      };
      if (canReadParquetVectorBatches(metadata, vectorOptions)) {
        for await (const vectorBatch of readParquetVectorBatchesFromFile(
          file,
          metadata,
          vectorOptions,
        )) {
          throwIfAborted(options.budget.signal);
          yield vectorBatch;
        }
        return;
      }
      for await (const columnBatch of readParquetColumnBatchesFromFile(file, metadata, {
        batchSize: options.batchSize || this.defaultBatchSize,
        ...(options.rowStart === undefined ? {} : { rowStart: options.rowStart }),
        ...(options.rowEnd === undefined ? {} : { rowEnd: options.rowEnd }),
        ...(options.columns === undefined ? {} : { columns: options.columns }),
        ...(options.where === undefined ? {} : { where: options.where }),
        ...(options.canStopEarly === undefined ? {} : { canStopEarly: options.canStopEarly }),
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

  async executeWindowTasks(
    tasks: TaskInput[],
    windows: Record<string, WindowExpr>,
    options: WindowTaskExecutionOptions,
  ): Promise<Row[]> {
    return windowParquetTasks(this.store, tasks, windows, {
      batchSize: options.batchSize,
      budget: options.budget,
      stats: options.stats,
      ...(this.metadataCache === undefined ? {} : { metadataCache: this.metadataCache }),
      ...(options.maxConcurrentTasks === undefined
        ? {}
        : { maxConcurrentTasks: options.maxConcurrentTasks }),
      ...(options.maxBufferedPartials === undefined
        ? {}
        : { maxBufferedPartials: options.maxBufferedPartials }),
    });
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

  private scanBuffer(path: string, file: StoreAsyncBuffer): StoreAsyncBuffer {
    return this.scanRangeCache === undefined
      ? file
      : cachedRangeBuffer(file, this.scanRangeCache, path);
  }
}
