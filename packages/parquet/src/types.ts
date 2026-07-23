import type { parquetMetadataAsync } from "hyparquet";
import type { Batch, Expr, QueryStats, Row } from "lakeql-core";
import type { DecodedColumnCache } from "./decoded-column-cache.js";

export interface ReadParquetOptions {
  /** Columns to project; all columns when omitted. */
  columns?: string[];
  rowStart?: number;
  rowEnd?: number;
}

export interface ReadParquetBatchOptions extends ReadParquetOptions {
  batchSize?: number;
  where?: Expr;
  canStopEarly?: boolean;
  /**
   * Number of selected row groups whose physical column ranges may be fetched
   * ahead of decoding. Read concurrency remains enforced by the ObjectStore.
   */
  maxConcurrentReads?: number;
  now?: () => number;
  stats?: QueryStats;
  decodedColumnCache?: DecodedColumnCache;
  decodedColumnCacheKey?: string;
}

export interface ParquetRowBatch {
  rowOffset: number;
  rows: Row[];
}

export interface ParquetColumnBatch {
  rowOffset: number;
  batch: Batch;
  residualPredicateSatisfied?: boolean;
}

export type ParquetMetadata = Awaited<ReturnType<typeof parquetMetadataAsync>>;

export interface StoreAsyncBuffer {
  byteLength: number;
  etag?: string;
  slice(start: number, end?: number): Promise<ArrayBuffer>;
  /**
   * Populate a bounded range cache before decoding. Implementations may ignore
   * ranges that cannot be retained within their cache budget.
   */
  prefetch?(ranges: readonly { start: number; end: number }[]): Promise<void>;
}
