import { parquetReadObjects } from "hyparquet";
import {
  type Batch,
  type CacheAdapter,
  LakeqlError,
  type ObjectStore,
  type Row,
  type ScanAdapter,
  type ScanOptions,
  type ScanTaskPlan,
  type ScanTaskPlanOptions,
  throwIfAborted,
} from "lakeql-core";
import { readParquetColumnBatchesFromFile } from "./column-batches.js";
import type { DecodedColumnCache } from "./decoded-column-cache.js";
import { normalizeDecodedRows } from "./decoded-rows.js";
import { readCachedParquetMetadata } from "./metadata-cache.js";
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

  constructor(
    store: ObjectStore,
    options: {
      batchSize?: number;
      metadataCache?: CacheAdapter<ParquetMetadata>;
      decodedColumnCache?: DecodedColumnCache;
    } = {},
  ) {
    this.store = store;
    this.defaultBatchSize = options.batchSize ?? 4096;
    this.metadataCache = options.metadataCache;
    this.decodedColumnCache = options.decodedColumnCache;
  }

  async *scan(path: string, options: ScanOptions): AsyncIterable<Row[]> {
    const batchSize = options.batchSize || this.defaultBatchSize;
    const file = await asyncBufferFromStore(this.store, path, options);
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
    const file = await asyncBufferFromStore(this.store, path, options);
    const metadata = await this.metadata(path, file, options);
    rejectUnsupportedParquetSchema(metadata);
    try {
      for await (const columnBatch of readParquetColumnBatchesFromFile(file, metadata, {
        batchSize: options.batchSize || this.defaultBatchSize,
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
        yield columnBatch.batch;
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
    const file = asyncBufferFromObjectInfo(this.store, options.object);
    const metadata = await this.metadata(path, file);
    return {
      rowGroupCount: metadata.row_groups.length,
      rowGroupRanges: planRowGroupsFromMetadata(metadata, options.where).rowGroupRanges,
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
}
