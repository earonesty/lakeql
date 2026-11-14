import {
  LakeqlError,
  MemoryBudget,
  MemoryScope,
  mapConcurrent,
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
  private readonly decodedMemory: MemoryLease[] = [];
  readonly memoryBudget: MemoryBudget;

  constructor(
    store: ObjectStore,
    readonly budget: QueryBudget,
    readonly stats: MutableLanceReadStats,
    readonly startedAt: number,
    readonly now: () => number,
    readonly planning: LanceRangePlanningOptions,
    memoryPool?: MemoryBudget,
  ) {
    const limit = budget.maxAllocatedMemoryBytes ?? Number.MAX_SAFE_INTEGER;
    this.memoryBudget = memoryPool?.child(limit) ?? new MemoryBudget(limit);
    this.store = withObjectStoreReadControls(store, {
      ...(budget.maxConcurrentReads === undefined
        ? {}
        : { maxConcurrentReads: budget.maxConcurrentReads }),
      ...(budget.signal === undefined ? {} : { signal: budget.signal }),
    });
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
    let physical = mergeRanges(logical, planning.coalesceGapBytes, planning.maxCoalescedRangeBytes);
    const logicalBytes = totalBytes(logical);
    let physicalBytes = totalBytes(physical);
    const headroom = Math.min(
      this.memoryBudget.availableBytes,
      (this.budget.maxMemoryBytes ?? Infinity) - this.memoryBytes,
      (this.budget.maxBytes ?? Infinity) - this.stats.physicalBytesRequested,
    );
    // Gap reads are optional: preserve exact requested ranges when coalescing
    // would consume unavailable capacity or turn a partial read into a full file.
    if (
      physicalBytes > logicalBytes &&
      (physicalBytes > headroom ||
        (category === "data" &&
          fileSize !== undefined &&
          physical.some((range) => range.offset === 0 && range.length >= fileSize)))
    ) {
      physical = logical;
      physicalBytes = logicalBytes;
    }
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
    const ownedRead = this.store.getRangeOwned?.bind(this.store);
    // Owned transports admit their actual backing allocations. Keep the legacy
    // logical-byte limit without reserving those same buffers a second time.
    const memory = this.reserveMemory(physicalBytes, ownedRead === undefined);
    const scope =
      ownedRead === undefined
        ? undefined
        : new MemoryScope(
            this.memoryBudget,
            this.budget.signal === undefined ? [] : [this.budget.signal],
          );
    const release = () => {
      scope?.dispose();
      memory.release();
    };
    try {
      const values = await mapConcurrent(
        physical,
        this.budget.maxConcurrentReads ?? 4,
        async (range) => {
          this.check();
          const owner =
            ownedRead !== undefined && scope !== undefined
              ? await ownedRead(path, range, scope)
              : undefined;
          if (owner !== undefined) scope?.adopt(owner);
          const bytes = owner?.value ?? (await this.store.getRange(path, range));
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
          return { ...range, bytes };
        },
      );
      if (category === "snapshot") this.stats.snapshotMetadataBytes += physicalBytes;
      else if (category === "file_metadata") this.stats.dataMetadataBytes += physicalBytes;
      return new RangeLease(values, release);
    } catch (cause) {
      release();
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
    this.decodedMemory.push(this.reserveMemory(bytes));
  }

  leaseDecodedMemory(bytes: number): MemoryLease {
    return this.reserveMemory(bytes);
  }

  releaseDecodedMemory(): void {
    for (const lease of this.decodedMemory) lease.release();
    this.decodedMemory.length = 0;
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

  private reserveMemory(bytes: number, chargeAllocation = true): MemoryLease {
    if (!Number.isSafeInteger(bytes) || bytes < 0) {
      throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", "Invalid Lance memory reservation", {
        bytes,
      });
    }
    const next = this.memoryBytes + bytes;
    if (this.budget.maxMemoryBytes !== undefined && next > this.budget.maxMemoryBytes) {
      budgetExceeded("memory bytes", this.budget.maxMemoryBytes, next, this.stats);
    }
    const reservation = chargeAllocation
      ? this.memoryBudget.reserve(bytes, "Lance read allocation")
      : undefined;
    this.memoryBytes = next;
    this.stats.peakMemoryBytes = Math.max(this.stats.peakMemoryBytes, next);
    return new MemoryLease(() => {
      reservation?.release();
      this.releaseMemory(bytes);
    });
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
