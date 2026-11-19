import { type CachePolicy, type ObjectStoreCacheOptions, SharedMemoryCache } from "lakeql-core";
import type { StoreAsyncBuffer } from "./types.js";

const DEFAULT_MAX_ENTRY_BYTES = 16 * 1024 * 1024;
const DEFAULT_COALESCE_BYTES = 256 * 1024;

export interface RangeCacheOptions {
  maxBytes: number;
  maxEntryBytes?: number;
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
}

export function cachedRangeBuffer(
  file: StoreAsyncBuffer,
  options: RangeCacheOptions,
  cacheKey: string,
): StoreAsyncBuffer {
  const maxBytes = options.maxBytes;
  const maxEntryBytes = options.maxEntryBytes ?? DEFAULT_MAX_ENTRY_BYTES;
  const coalesceBytes = options.coalesceBytes ?? DEFAULT_COALESCE_BYTES;
  if (maxBytes <= 0 || maxEntryBytes <= 0) return file;
  const cache = options.sharedCache ?? new SharedMemoryCache({ maxBytes });
  const identity = JSON.stringify([cacheKey, file.byteLength, file.etag ?? null]);
  const prefix = `scan-range:${identity}:`;
  const pending = new Map<string, { start: number; end: number; read: Promise<RangeCacheEntry> }>();

  async function readRange(start: number, end: number, coalesce: boolean): Promise<ArrayBuffer> {
    if (end === start) return new ArrayBuffer(0);
    const request = coalesce
      ? coalescedRange(file.byteLength, start, end, coalesceBytes)
      : { start, end };
    const key = `${prefix}${request.start}:${request.end}`;
    // Look for the caller's actual slice before applying alignment: a prefetched
    // column chunk may contain the slice without containing an aligned window.
    const cached =
      cache.get<RangeCacheEntry>(key) ??
      cache.find<RangeCacheEntry>(
        (value, candidate) =>
          candidate.startsWith(prefix) &&
          isRangeEntry(value) &&
          value.start <= start &&
          value.end >= end,
      );
    if (cached !== undefined) return sliceEntry(cached.value, start, end);
    for (const inflight of pending.values()) {
      if (inflight.start <= start && inflight.end >= end) {
        return sliceEntry(await inflight.read, start, end);
      }
    }
    const read = file.slice(request.start, request.end).then((bytes) => ({
      bytes,
      byteLength: bytes.byteLength,
      start: request.start,
      end: request.start + bytes.byteLength,
    }));
    pending.set(key, { ...request, read });
    let entry: RangeCacheEntry;
    try {
      entry = await read;
    } finally {
      pending.delete(key);
    }
    if (
      entry.byteLength === request.end - request.start &&
      entry.byteLength <= maxEntryBytes &&
      entry.byteLength <= maxBytes
    ) {
      cache.set(key, entry, entry.byteLength, {
        priority: scanRangePriority(options.cacheOptions?.policy ?? "balanced"),
      });
    }
    return sliceEntry(entry, start, end);
  }

  return {
    byteLength: file.byteLength,
    ...(file.etag === undefined ? {} : { etag: file.etag }),
    slice: (start, end) => readRange(start, end ?? file.byteLength, true),
    async prefetch(ranges) {
      const retained: { start: number; end: number }[] = [];
      let retainedBytes = 0;
      const ordered = ranges
        .map(({ start, end }) => coalescedRange(file.byteLength, start, end, 0))
        .sort((a, b) => a.start - b.start);
      for (const { start, end } of ordered) {
        const bytes = end - start;
        if (bytes <= 0 || bytes > maxEntryBytes) continue;
        const previous = retained.at(-1);
        if (previous !== undefined && previous.end >= start) {
          const extra = Math.max(0, end - previous.end);
          const mergedBytes = Math.max(previous.end, end) - previous.start;
          if (mergedBytes <= maxEntryBytes && retainedBytes + extra <= maxBytes) {
            previous.end = Math.max(previous.end, end);
            retainedBytes += extra;
            continue;
          }
        }
        if (retainedBytes + bytes > maxBytes) continue;
        retained.push({ start, end });
        retainedBytes += bytes;
      }
      // Merge only touching chunks: do not fetch padding or unrelated columns.
      const reads = retained.map(({ start, end }) => readRange(start, end, false));
      await Promise.all(reads);
    },
  };
}

function isRangeEntry(value: unknown): value is RangeCacheEntry {
  return (
    typeof value === "object" &&
    value !== null &&
    "bytes" in value &&
    value.bytes instanceof ArrayBuffer &&
    "start" in value &&
    typeof value.start === "number" &&
    "end" in value &&
    typeof value.end === "number"
  );
}

function coalescedRange(
  fileLength: number,
  start: number,
  end: number,
  coalesceBytes: number,
): { start: number; end: number } {
  const normalizedStart = Math.max(0, start);
  const normalizedEnd = Math.min(fileLength, Math.max(normalizedStart, end));
  if (
    normalizedEnd === fileLength ||
    coalesceBytes <= 0 ||
    normalizedEnd - normalizedStart >= coalesceBytes
  ) {
    return { start: normalizedStart, end: normalizedEnd };
  }
  const coalescedStart = Math.floor(normalizedStart / coalesceBytes) * coalesceBytes;
  const coalescedEnd = Math.min(fileLength, coalescedStart + coalesceBytes);
  if (normalizedEnd <= coalescedEnd) return { start: coalescedStart, end: coalescedEnd };
  return { start: normalizedStart, end: normalizedEnd };
}

function sliceEntry(entry: RangeCacheEntry, start: number, end: number): ArrayBuffer {
  return sliceBuffer(entry.bytes, start - entry.start, end - entry.start);
}

function sliceBuffer(bytes: ArrayBuffer, start: number, end: number): ArrayBuffer {
  return bytes.slice(start, end);
}

function scanRangePriority(policy: CachePolicy): number {
  if (policy === "io") return 4;
  if (policy === "latency") return 1;
  return 3;
}
