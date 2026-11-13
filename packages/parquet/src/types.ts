import type { parquetMetadataAsync } from "hyparquet";
import type { Batch, Expr, MemoryScope, OwnedMemory, QueryStats, Row } from "lakeql-core";
import type { DecodedColumnCache } from "./decoded-column-cache.js";

export interface ReadParquetOptions {
  /** Columns to project; all columns when omitted. */
  columns?: string[];
  rowStart?: number;
  rowEnd?: number;
}

export interface ReadParquetBatchOptions extends ReadParquetOptions {
  /** Observed transport cost: emulated ranges transfer the complete object. */
  rangeReadMode?: "native" | "emulated";
  /** Admits engine-owned typed vector buffers and decompression outputs before allocation. */
  allocationScope?: MemoryScope;
  /** Iteration mode releases page backing on advance; retain from allocationScope before advancing.
   * Scope mode (default) preserves borrowed vectors until caller scope disposal.
   */
  pageAllocationLifetime?: "scope" | "iteration";
  /** Retains admitted decompression outputs through the caller-controlled decode lifetime. */
  decompressionScope?: MemoryScope;
  batchSize?: number;
  where?: Expr;
  canStopEarly?: boolean;
  /** Owned scan policy: use page windows for large chunks under shared-budget pressure. */
  adaptiveReadWindows?: boolean;
  /**
   * Number of selected row groups whose physical column ranges may be fetched
   * ahead of decoding. Read concurrency remains enforced by the ObjectStore.
   */
  maxConcurrentReads?: number;
  /** Internal demand-page concurrency, independent of row-group prefetch windows. */
  maxConcurrentPageReads?: number;
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

/** Random-access metadata source; consumers must retain its backing lifetime. */
export interface ParquetRowGroupSource {
  readonly metadata: Omit<ParquetMetadata, "row_groups">;
  readonly rowGroupCount: number;
  readRowGroup(index: number): ParquetMetadata["row_groups"][number];
  /** Independently owned decoded metadata; release after the group consumer exits. */
  acquireRowGroup?(index: number): OwnedMemory<ParquetMetadata["row_groups"][number]>;
}

export interface StoreAsyncBuffer {
  byteLength: number;
  etag?: string;
  slice(start: number, end?: number): Promise<ArrayBuffer>;
  /** Allocation-aware slice; the caller releases the returned backing ownership. */
  sliceOwned?(start: number, end: number, scope: MemoryScope): Promise<OwnedMemory<ArrayBuffer>>;
  /** Total new encoded-buffer capacity for an exact-range read, including adapter
   * copies/workspace. Unknown adapters omit this capability. Responses exceeding
   * the declared exact-range extent must fail within the admitted scope.
   */
  ownedReadAllocationBytes?(start: number, end: number): number;
  /**
   * Populate a bounded range cache before decoding. Implementations may ignore
   * ranges that cannot be retained within their cache budget.
   */
  prefetchOwned?(
    ranges: readonly { start: number; end: number }[],
    scope: MemoryScope,
  ): Promise<void>;
  prefetch?(ranges: readonly { start: number; end: number }[]): Promise<void>;
}
