import {
  type CachePolicy,
  LakeqlError,
  mapConcurrent,
  type ObjectStoreCacheOptions,
  type SharedMemoryCache,
} from "lakeql-core";
import { withOwnedRangeCache } from "./range-cache-owned.js";
import type { StoreAsyncBuffer } from "./types.js";

const DEFAULT_MAX_ENTRY_BYTES = 16 * 1024 * 1024;
const DEFAULT_COALESCE_BYTES = 256 * 1024;

export interface RangeCacheOptions {
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
  const cached = legacyCachedRangeBuffer(file, options, cacheKey);
  return withOwnedRangeCache(file, cached, options, cacheKey);
}

function legacyCachedRangeBuffer(
  file: StoreAsyncBuffer,
  options: RangeCacheOptions,
  cacheKey: string,
): StoreAsyncBuffer {
  const maxBytes = options.maxBytes;
  const maxEntryBytes = options.maxEntryBytes ?? DEFAULT_MAX_ENTRY_BYTES;
  const coalesceBytes = options.coalesceBytes ?? DEFAULT_COALESCE_BYTES;
  if (maxBytes <= 0 || maxEntryBytes <= 0) return file;
  if (options.sharedCache !== undefined) {
    return sharedCachedRangeBuffer(
      file,
      cacheKey,
      options.sharedCache,
      options.cacheOptions ?? {},
      {
        maxBytes,
        maxEntryBytes,
        coalesceBytes,
        maxConcurrentPrefetchReads: options.maxConcurrentPrefetchReads ?? 4,
      },
    );
  }
  const cache = new Map<string, RangeCacheEntry>();
  const pending = new Map<string, Promise<RangeCacheEntry>>();
  let cachedBytes = 0;

  async function ensureRange(start: number, end: number): Promise<RangeCacheEntry> {
    const normalizedEnd = end;
    const request = coalescedRange(file.byteLength, start, normalizedEnd, coalesceBytes);
    const key = `${request.start}:${request.end}`;
    const cached = cache.get(key);
    if (cached !== undefined) {
      cache.delete(key);
      cache.set(key, cached);
      return cached;
    }
    const inflight = pending.get(key);
    if (inflight !== undefined) {
      return inflight;
    }

    const read = file.slice(request.start, request.end).then((bytes) => ({
      bytes,
      byteLength: bytes.byteLength,
      start: request.start,
      end: request.end,
    }));
    pending.set(key, read);
    let entry: RangeCacheEntry;
    try {
      entry = await read;
    } finally {
      pending.delete(key);
    }
    if (entry.byteLength <= maxEntryBytes && entry.byteLength <= maxBytes) {
      cache.set(key, entry);
      cachedBytes += entry.byteLength;
      while (cachedBytes > maxBytes) {
        const oldestKey = cache.keys().next().value;
        if (oldestKey === undefined) break;
        const oldest = cache.get(oldestKey);
        cache.delete(oldestKey);
        cachedBytes -= oldest?.byteLength ?? 0;
      }
    }
    return entry;
  }
  return {
    byteLength: file.byteLength,
    ...(file.etag === undefined ? {} : { etag: file.etag }),
    async slice(start, end = file.byteLength) {
      return sliceEntry(await ensureRange(start, end), start, end);
    },
    prefetch: (ranges) =>
      prefetchRetainableRanges(ensureRange, ranges, file.byteLength, {
        maxBytes,
        maxEntryBytes,
        coalesceBytes,
        maxConcurrentPrefetchReads: options.maxConcurrentPrefetchReads ?? 4,
      }),
  };
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
  const pending = new Map<string, Promise<ArrayBuffer>>();
  async function ensureRange(start: number, end: number): Promise<RangeCacheEntry> {
    const normalizedEnd = end;
    const request = coalescedRange(file.byteLength, start, normalizedEnd, options.coalesceBytes);
    const key = sharedRangeKey(file, cacheKey, request.start, request.end);
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
  }
  return {
    byteLength: file.byteLength,
    ...(file.etag === undefined ? {} : { etag: file.etag }),
    async slice(start, end = file.byteLength) {
      return sliceEntry(await ensureRange(start, end), start, end);
    },
    prefetch: (ranges) => prefetchRetainableRanges(ensureRange, ranges, file.byteLength, options),
  };
}

async function prefetchRetainableRanges(
  ensureRange: (start: number, end: number) => Promise<RangeCacheEntry>,
  ranges: readonly { start: number; end: number }[],
  fileLength: number,
  options: {
    maxBytes: number;
    maxEntryBytes: number;
    coalesceBytes: number;
    maxConcurrentPrefetchReads: number;
  },
): Promise<void> {
  const retained: { start: number; end: number }[] = [];
  const seen = new Set<string>();
  let retainedBytes = 0;
  for (const range of ranges) {
    const request = coalescedRange(fileLength, range.start, range.end, options.coalesceBytes);
    const bytes = request.end - request.start;
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
  }
  await mapConcurrent(retained, options.maxConcurrentPrefetchReads, async (range) => {
    const entry = await ensureRange(range.start, range.end);
    entry.release?.();
  });
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

function sharedRangeKey(
  file: StoreAsyncBuffer,
  cacheKey: string,
  start: number,
  end: number,
): string {
  return ["scan-range", cacheKey, file.byteLength, file.etag ?? "", start, end].join(":");
}

function scanRangePriority(policy: CachePolicy): number {
  if (policy === "io") return 4;
  if (policy === "latency") return 1;
  return 3;
}
