import {
  type CachePolicy,
  MATERIALIZATION_CHARGES as c,
  jsonNumberCharacters,
  LakeqlError,
  type MemoryReservation,
  mapConcurrent,
  type ObjectStoreCacheOptions,
  SharedMemoryCache,
  sharedCacheMetadataBytes,
} from "lakeql-core";
import {
  borrowedRangeKeyBytes,
  borrowedRangeKeyCharacters,
  fileCacheKey,
} from "./file-cache-key.js";
import { withOwnedRangeCache } from "./range-cache-owned.js";
import type { StoreAsyncBuffer } from "./types.js";

const DEFAULT_MAX_ENTRY_BYTES = 16 * 1024 * 1024;
const DEFAULT_COALESCE_BYTES = 256 * 1024;

export interface RangeCacheOptions {
  /** Maximum retained payload plus entry metadata. Workspace also obeys its allocation pool. */
  maxBytes: number;
  maxEntryBytes?: number;
  /** Pending read-ahead callbacks per invocation. Defaults to four. */
  maxConcurrentPrefetchReads?: number;
  /**
   * Fetch small adjacent slices through aligned windows of this size. This turns
   * Parquet page-header/page-body read pairs into one object-store range read.
   */
  coalesceBytes?: number;
  sharedCache?: SharedMemoryCache;
  cacheOptions?: ObjectStoreCacheOptions;
}

interface RangeCacheEntry {
  bytes: ArrayBuffer;
  byteLength: number;
  start: number;
  end: number;
  release?: () => void;
}

export function cachedRangeBuffer(
  file: StoreAsyncBuffer,
  options: RangeCacheOptions,
  cacheKey: string,
): StoreAsyncBuffer {
  const concurrency = options.maxConcurrentPrefetchReads ?? 4;
  if (!Number.isSafeInteger(concurrency) || concurrency < 1) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "maxConcurrentPrefetchReads must be a positive safe integer",
    );
  }
  const cached = borrowedCachedRangeBuffer(file, options, cacheKey);
  return withOwnedRangeCache(file, cached, options, cacheKey);
}

function borrowedCachedRangeBuffer(
  file: StoreAsyncBuffer,
  options: RangeCacheOptions,
  cacheKey: string,
): StoreAsyncBuffer {
  const maxBytes = options.maxBytes;
  const maxEntryBytes = options.maxEntryBytes ?? DEFAULT_MAX_ENTRY_BYTES;
  const coalesceBytes = options.coalesceBytes ?? DEFAULT_COALESCE_BYTES;
  if (maxBytes <= 0 || maxEntryBytes <= 0) return file;
  return sharedCachedRangeBuffer(
    file,
    // A private cache already identifies its file; do not duplicate the path in
    // every entry. Shared caches require the complete cross-wrapper identity.
    options.sharedCache === undefined ? "" : cacheKey,
    options.sharedCache ?? new SharedMemoryCache({ ...options.cacheOptions, maxBytes }),
    options.cacheOptions ?? {},
    {
      maxBytes,
      maxEntryBytes,
      coalesceBytes,
      maxConcurrentPrefetchReads: options.maxConcurrentPrefetchReads ?? 4,
    },
  );
}

function sharedCachedRangeBuffer(
  file: StoreAsyncBuffer,
  cacheKey: string,
  cache: SharedMemoryCache,
  cacheOptions: ObjectStoreCacheOptions,
  options: {
    maxBytes: number;
    maxEntryBytes: number;
    coalesceBytes: number;
    maxConcurrentPrefetchReads: number;
  },
): StoreAsyncBuffer {
  // The returned buffer has one fixed file identity. Measure its static fields
  // once; warm page/header lookups only size their two varying numeric offsets.
  const { byteLength, etag } = file;
  const baseCharacters = borrowedRangeKeyCharacters(cacheKey, byteLength, etag, 0, 0) - 2;
  const baseWorkspace =
    borrowedRangeKeyBytes(cacheKey, byteLength, etag, 0, 0) - 2 * c.stringCodeUnit;
  const offsetCharacters = (start: number, end: number) =>
    jsonNumberCharacters(start) + jsonNumberCharacters(end);
  const workspaceBytes = (start: number, end: number) =>
    baseWorkspace + offsetCharacters(start, end) * c.stringCodeUnit;
  const pending = new Map<string, Promise<ArrayBuffer>>();
  async function ensureRange(
    start: number,
    end: number,
    demand = false,
  ): Promise<RangeCacheEntry | undefined> {
    const normalizedEnd = end;
    const request = coalescedRange(byteLength, start, normalizedEnd, options.coalesceBytes);
    const workspace = cache.tryReserveKeyWorkspace(workspaceBytes(request.start, request.end));
    if (workspace === undefined) {
      if (!demand) return undefined;
      const exact = coalescedRange(byteLength, start, end, 0);
      const bytes = await file.slice(exact.start, exact.end);
      return { bytes, byteLength: bytes.byteLength, ...exact };
    }
    try {
      const key = fileCacheKey(
        "scan-range-v2",
        cacheKey,
        byteLength,
        etag,
        request.start,
        request.end,
      );
      const cached = cache.acquire<ArrayBuffer>(key);
      if (cached !== undefined) {
        return {
          bytes: cached.value,
          byteLength: cached.value.byteLength,
          ...request,
          release: cached.release,
        };
      }
      const inflight = pending.get(key);
      if (inflight !== undefined) {
        const bytes = await inflight;
        const lease = cache.acquire<ArrayBuffer>(key);
        return {
          bytes,
          byteLength: bytes.byteLength,
          ...request,
          ...(lease === undefined ? {} : { release: lease.release }),
        };
      }
      const read = file.slice(request.start, request.end);
      pending.set(key, read);
      let bytes: ArrayBuffer;
      try {
        bytes = await read;
      } finally {
        pending.delete(key);
      }
      if (bytes.byteLength <= options.maxEntryBytes && bytes.byteLength <= options.maxBytes) {
        cache.set(key, bytes, bytes.byteLength, {
          priority: scanRangePriority(cacheOptions.policy ?? "balanced"),
        });
      }
      const lease = cache.acquire<ArrayBuffer>(key);
      return {
        bytes,
        byteLength: bytes.byteLength,
        ...request,
        ...(lease === undefined ? {} : { release: lease.release }),
      };
    } finally {
      // A hit no longer references the key after its synchronous lookup. Pending
      // reads keep this lease through await/settlement, without extra hit promises.
      workspace.release();
    }
  }
  return {
    byteLength: byteLength,
    ...(etag === undefined ? {} : { etag: etag }),
    async slice(start, end = byteLength) {
      const entry = await ensureRange(start, end, true);
      if (entry === undefined) throw new Error("Demanded range missing");
      return sliceEntry(entry, start, end);
    },
    prefetch: (ranges) =>
      prefetchRetainableRanges(
        ensureRange,
        ranges,
        byteLength,
        options,
        cache,
        workspaceBytes(byteLength, byteLength),
        cache.availableAdmissionBytes(scanRangePriority(cacheOptions.policy ?? "balanced")),
        (start, end) =>
          sharedCacheMetadataBytes("") +
          (baseCharacters + offsetCharacters(start, end)) * c.stringCodeUnit,
      ),
  };
}

async function prefetchRetainableRanges(
  ensureRange: (start: number, end: number) => Promise<RangeCacheEntry | undefined>,
  ranges: readonly { start: number; end: number }[],
  fileLength: number,
  options: {
    maxBytes: number;
    maxEntryBytes: number;
    coalesceBytes: number;
    maxConcurrentPrefetchReads: number;
  },
  cache: SharedMemoryCache,
  readKeyBytes: number,
  cacheCapacity: number,
  entryMetadataBytes: (start: number, end: number) => number,
): Promise<void> {
  const directoryBytes = 2 * c.array + c.object + 2 * c.property;
  const selectionBytes =
    c.object + 3 * c.property + c.string + 65 * c.stringCodeUnit + 4 * c.arraySlot;
  const initialWorkspace = cache.availableKeyWorkspaceBytes;
  if (directoryBytes + selectionBytes + readKeyBytes > initialWorkspace) return;
  const directory = cache.tryReserveKeyWorkspace(directoryBytes);
  if (directory === undefined) return;
  const selections: Pick<MemoryReservation, "release">[] = [];
  try {
    const retained: { start: number; end: number }[] = [];
    const seen = new Set<string>();
    let retainedBytes = 0;
    let retainedMetadata = 0;
    for (const range of ranges) {
      const request = coalescedRange(fileLength, range.start, range.end, options.coalesceBytes);
      const bytes = request.end - request.start;
      const metadata = entryMetadataBytes(request.start, request.end);
      const storageBytes = retainedBytes + bytes + retainedMetadata + metadata;
      const planningBytes = directoryBytes + (retained.length + 1) * selectionBytes;
      const activeBytes =
        Math.min(retained.length + 1, options.maxConcurrentPrefetchReads) * readKeyBytes;
      if (
        planningBytes + activeBytes + storageBytes > initialWorkspace ||
        storageBytes > cacheCapacity ||
        selectionBytes + activeBytes > cache.availableKeyWorkspaceBytes
      )
        continue;
      const selection = cache.tryReserveKeyWorkspace(selectionBytes);
      if (selection === undefined) continue;
      let keep = false;
      try {
        const key = `${request.start}:${request.end}`;
        if (
          bytes <= 0 ||
          bytes > options.maxEntryBytes ||
          retainedBytes + bytes > options.maxBytes ||
          seen.has(key)
        ) {
          continue;
        }
        seen.add(key);
        retained.push(request);
        retainedBytes += bytes;
        retainedMetadata += metadata;
        selections.push(selection);
        keep = true;
      } finally {
        if (!keep) selection.release();
      }
    }
    await mapConcurrent(retained, options.maxConcurrentPrefetchReads, async (range) => {
      const entry = await ensureRange(range.start, range.end);
      entry?.release?.();
    });
  } finally {
    for (const selection of selections) selection.release();
    directory.release();
  }
}

export function coalescedRange(
  fileLength: number,
  start: number,
  end: number,
  coalesceBytes: number,
): { start: number; end: number } {
  const normalizedStart = Math.max(0, start);
  const normalizedEnd = Math.min(fileLength, Math.max(normalizedStart, end));
  if (coalesceBytes <= 0 || normalizedEnd - normalizedStart >= coalesceBytes) {
    return { start: normalizedStart, end: normalizedEnd };
  }
  const coalescedStart = Math.floor(normalizedStart / coalesceBytes) * coalesceBytes;
  const coalescedEnd = Math.min(fileLength, coalescedStart + coalesceBytes);
  if (normalizedEnd <= coalescedEnd) return { start: coalescedStart, end: coalescedEnd };
  return { start: normalizedStart, end: normalizedEnd };
}

function sliceEntry(entry: RangeCacheEntry, start: number, end: number): ArrayBuffer {
  try {
    return sliceBuffer(entry.bytes, start - entry.start, end - entry.start);
  } finally {
    entry.release?.();
  }
}

function sliceBuffer(bytes: ArrayBuffer, start: number, end: number): ArrayBuffer {
  return bytes.slice(start, end);
}

function scanRangePriority(policy: CachePolicy): number {
  if (policy === "io") return 4;
  if (policy === "latency") return 1;
  return 3;
}
