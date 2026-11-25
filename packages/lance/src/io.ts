import {
  LakeqlError,
  type ObjectStore,
  type QueryBudget,
  throwIfAborted,
  withObjectStoreReadControls,
} from "lakeql";

export interface ByteRange {
  offset: number;
  length: number;
}

export type LanceReadCategory = "snapshot" | "file_metadata" | "data";

export interface MutableLanceReadStats {
  snapshotMetadataBytes: number;
  dataMetadataBytes: number;
  logicalBytesRequested: number;
  physicalBytesRequested: number;
  rangeRequests: number;
  cacheHits: number;
  cacheMisses: number;
  peakMemoryBytes: number;
  rowsDecoded: number;
  fragments: Set<number>;
  pages: Set<string>;
}

export interface LanceRangePlanningOptions {
  coalesceGapBytes: number;
  maxCoalescedRangeBytes: number;
}

export class LanceReadContext {
  readonly store: ObjectStore;
  private memoryBytes = 0;
  private decodedMemoryBytes = 0;
  private readonly metadataLeases: RangeLease[] = [];
  private readonly sharedMetadata = new Map<string, Promise<RangeLease>>();

  constructor(
    store: ObjectStore,
    readonly budget: QueryBudget,
    readonly stats: MutableLanceReadStats,
    readonly startedAt: number,
    readonly now: () => number,
    readonly planning: LanceRangePlanningOptions,
  ) {
    this.store = withObjectStoreReadControls(store, {
      maxConcurrentReads: budget.maxConcurrentReads ?? 4,
      ...(budget.signal === undefined ? {} : { signal: budget.signal }),
    });
  }

  /** Headroom for planning bounded waves; every allocation still requires admission. */
  get availableMemoryBytes(): number {
    return (this.budget.maxMemoryBytes ?? Infinity) - this.memoryBytes;
  }

  /** Share immutable page directories within an operation, including pending reads. */
  async readSharedMetadata(path: string, range: ByteRange, fileSize: number): Promise<RangeLease> {
    const key = JSON.stringify([path, range.offset, range.length]);
    let pending = this.sharedMetadata.get(key);
    if (pending === undefined) {
      this.accountDecodedMemory(64 + key.length * 2);
      pending = this.readRange(path, range, "file_metadata", fileSize).then((lease) => {
        this.metadataLeases.push(lease);
        return lease;
      });
      this.sharedMetadata.set(key, pending);
    }
    const lease = await pending;
    return new RangeLease([{ ...range, bytes: lease.slice(range) }], () => {});
  }

  /** Drain started work before releasing operation memory, including on failure. */
  async runTasks<T>(
    values: readonly T[],
    run: (value: T, index: number) => Promise<void>,
    concurrency = this.budget.maxConcurrentReads ?? 4,
  ): Promise<void> {
    let cursor = 0;
    let failed = false;
    let failure: unknown;
    await Promise.all(
      Array.from({ length: Math.min(values.length, concurrency) }, async () => {
        while (!failed && cursor < values.length) {
          const index = cursor++;
          try {
            this.check();
            await run(values[index] as T, index);
          } catch (error) {
            if (!failed) failure = error;
            failed = true;
          }
        }
      }),
    );
    if (failed) throw failure;
  }

  async readRange(
    path: string,
    range: ByteRange,
    category: LanceReadCategory,
    fileSize?: number,
  ): Promise<RangeLease> {
    return await this.readRanges(path, [range], category, fileSize, {
      coalesceGapBytes: 0,
      maxCoalescedRangeBytes: range.length,
    });
  }

  reserveDecodedRows(count: number): void {
    if (!Number.isSafeInteger(count) || count < 0) {
      throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", "Invalid decoded row count", { count });
    }
    const total = this.stats.rowsDecoded + count;
    if (this.budget.maxRowsDecoded !== undefined && total > this.budget.maxRowsDecoded) {
      throw new LakeqlError(
        "LAKEQL_BUDGET_EXCEEDED",
        `Lance read exceeded decoded row budget (${total} > ${this.budget.maxRowsDecoded})`,
        {
          metric: "rows decoded",
          limit: this.budget.maxRowsDecoded,
          actual: total,
        },
      );
    }
    this.stats.rowsDecoded = total;
  }

  async readRanges(
    path: string,
    ranges: readonly ByteRange[],
    category: LanceReadCategory,
    fileSize?: number,
    planning = this.planning,
  ): Promise<RangeLease> {
    this.check();
    const logical = mergeRanges(ranges, 0, Number.MAX_SAFE_INTEGER);
    const physical = mergeRanges(
      logical,
      planning.coalesceGapBytes,
      planning.maxCoalescedRangeBytes,
    );
    const logicalBytes = totalBytes(logical);
    const physicalBytes = totalBytes(physical);
    if (
      category === "data" &&
      fileSize !== undefined &&
      physical.some((range) => range.offset === 0 && range.length >= fileSize)
    ) {
      throw new LakeqlError(
        "LAKEQL_UNSUPPORTED_LANCE_FEATURE",
        "Lance data reads may not load an entire data file",
        { path, fileSize },
      );
    }
    this.reserveReads(physical.length, physicalBytes, logicalBytes);
    this.reserveMemory(physicalBytes);
    try {
      const values: (ByteRange & { bytes: Uint8Array })[] = [];
      await this.runTasks(physical, async (range, index) => {
        const bytes = await this.store.getRange(path, range);
        if (bytes.byteLength !== range.length) {
          throw new LakeqlError(
            "LAKEQL_LANCE_READ_ERROR",
            "Lance object store returned a truncated range",
            {
              path,
              offset: range.offset,
              expectedLength: range.length,
              actualLength: bytes.byteLength,
            },
          );
        }
        this.check();
        values[index] = { ...range, bytes };
      });
      if (category === "snapshot") this.stats.snapshotMetadataBytes += physicalBytes;
      else if (category === "file_metadata") this.stats.dataMetadataBytes += physicalBytes;
      return new RangeLease(values, () => this.releaseMemory(physicalBytes));
    } catch (cause) {
      this.releaseMemory(physicalBytes);
      throw cause;
    }
  }

  check(): void {
    throwIfAborted(this.budget.signal);
    const elapsedMs = this.now() - this.startedAt;
    if (this.budget.maxElapsedMs !== undefined && elapsedMs > this.budget.maxElapsedMs) {
      budgetExceeded("elapsed milliseconds", this.budget.maxElapsedMs, elapsedMs, this.stats);
    }
  }

  accountDecodedMemory(bytes: number): void {
    this.reserveMemory(bytes);
    this.decodedMemoryBytes += bytes;
  }

  leaseDecodedMemory(bytes: number): MemoryLease {
    this.reserveMemory(bytes);
    return new MemoryLease(() => this.releaseMemory(bytes));
  }

  /**
   * A charge that grows with its owner: call `grow(bytes)` before each allocation the owner
   * makes, and `release()` once the owner is dropped. Use it for plans, batches and maps whose
   * size is only known as they are built, so they are admitted entry by entry instead of
   * being measured after the fact.
   */
  openReservation(): MemoryReservation {
    return new MemoryReservation(
      (bytes) => this.reserveMemory(bytes),
      (bytes) => this.releaseMemory(bytes),
    );
  }

  releaseDecodedMemory(): void {
    for (const lease of this.metadataLeases) lease.release();
    this.metadataLeases.length = 0;
    this.sharedMetadata.clear();
    this.releaseMemory(this.decodedMemoryBytes);
    this.decodedMemoryBytes = 0;
  }

  private reserveReads(requests: number, physicalBytes: number, logicalBytes: number): void {
    const nextRequests = this.stats.rangeRequests + requests;
    const nextBytes = this.stats.physicalBytesRequested + physicalBytes;
    if (this.budget.maxRangeRequests !== undefined && nextRequests > this.budget.maxRangeRequests) {
      budgetExceeded("range requests", this.budget.maxRangeRequests, nextRequests, this.stats);
    }
    if (this.budget.maxBytes !== undefined && nextBytes > this.budget.maxBytes) {
      budgetExceeded("physical bytes", this.budget.maxBytes, nextBytes, this.stats);
    }
    this.stats.rangeRequests = nextRequests;
    this.stats.physicalBytesRequested = nextBytes;
    this.stats.logicalBytesRequested += logicalBytes;
  }

  private reserveMemory(bytes: number): void {
    if (!Number.isSafeInteger(bytes) || bytes < 0) {
      throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", "Invalid Lance memory reservation", {
        bytes,
      });
    }
    const next = this.memoryBytes + bytes;
    if (this.budget.maxMemoryBytes !== undefined && next > this.budget.maxMemoryBytes) {
      budgetExceeded("memory bytes", this.budget.maxMemoryBytes, next, this.stats);
    }
    this.memoryBytes = next;
    this.stats.peakMemoryBytes = Math.max(this.stats.peakMemoryBytes, next);
  }

  private releaseMemory(bytes: number): void {
    this.memoryBytes = Math.max(0, this.memoryBytes - bytes);
  }
}

export class MemoryLease {
  private released = false;

  constructor(private readonly onRelease: () => void) {}

  release(): void {
    if (this.released) return;
    this.released = true;
    this.onRelease();
  }
}

export class MemoryReservation {
  private held = 0;

  constructor(
    private readonly reserve: (bytes: number) => void,
    private readonly free: (bytes: number) => void,
  ) {}

  /** Bytes currently charged to the operation by this reservation. */
  get bytes(): number {
    return this.held;
  }

  /** Admit `bytes` more; throws (charging nothing) when they do not fit the budget. */
  grow(bytes: number): void {
    this.reserve(bytes);
    this.held += bytes;
  }

  release(): void {
    const bytes = this.held;
    this.held = 0;
    if (bytes > 0) this.free(bytes);
  }
}

export class RangeLease {
  private released = false;

  constructor(
    private readonly ranges: readonly (ByteRange & { bytes: Uint8Array })[],
    private readonly onRelease: () => void,
  ) {}

  slice(range: ByteRange): Uint8Array {
    const physical = this.ranges.find(
      (candidate) =>
        range.offset >= candidate.offset &&
        range.offset + range.length <= candidate.offset + candidate.length,
    );
    if (physical === undefined) {
      throw new LakeqlError(
        "LAKEQL_LANCE_READ_ERROR",
        "Planned Lance range is not present in the physical response",
        { range },
      );
    }
    const start = range.offset - physical.offset;
    return physical.bytes.subarray(start, start + range.length);
  }

  release(): void {
    if (this.released) return;
    this.released = true;
    this.onRelease();
  }
}

export function mergeRanges(
  ranges: readonly ByteRange[],
  gapBytes: number,
  maxRangeBytes: number,
): ByteRange[] {
  const sorted = ranges
    .map(validateRange)
    .filter((range) => range.length > 0)
    .sort((left, right) => left.offset - right.offset || left.length - right.length);
  const merged: ByteRange[] = [];
  for (const range of sorted) {
    const previous = merged.at(-1);
    if (previous === undefined) {
      merged.push({ ...range });
      continue;
    }
    const previousEnd = previous.offset + previous.length;
    const rangeEnd = range.offset + range.length;
    const combinedEnd = Math.max(previousEnd, rangeEnd);
    if (range.offset - previousEnd <= gapBytes && combinedEnd - previous.offset <= maxRangeBytes) {
      previous.length = combinedEnd - previous.offset;
    } else {
      merged.push({ ...range });
    }
  }
  return merged;
}

export function totalBytes(ranges: readonly ByteRange[]): number {
  return ranges.reduce((total, range) => total + range.length, 0);
}

function validateRange(range: ByteRange): ByteRange {
  if (
    !Number.isSafeInteger(range.offset) ||
    !Number.isSafeInteger(range.length) ||
    range.offset < 0 ||
    range.length < 0 ||
    !Number.isSafeInteger(range.offset + range.length)
  ) {
    throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", "Invalid Lance byte range", { range });
  }
  return range;
}

function budgetExceeded(
  metric: string,
  limit: number,
  actual: number,
  stats: MutableLanceReadStats,
): never {
  throw new LakeqlError(
    "LAKEQL_BUDGET_EXCEEDED",
    `Lance read exceeded ${metric} budget (${actual} > ${limit})`,
    {
      metric,
      limit,
      actual,
      physicalBytesRequested: stats.physicalBytesRequested,
      rangeRequests: stats.rangeRequests,
    },
  );
}
