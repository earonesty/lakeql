import type { ListOptions, ObjectHead, ObjectInfo, ObjectStore } from "./store.js";

export interface ObjectStoreCacheOptions {
  /** Maximum cached object/range bytes retained in memory. Defaults to 64 MiB. */
  maxBytes?: number;
  /** Optional time-to-live for cache entries. Entries do not expire by default. */
  ttlMs?: number;
  /** How LakeQL should spend the cache budget. Defaults to balanced. */
  policy?: CachePolicy;
}

export type CachePolicy = "balanced" | "io" | "latency";

export interface SharedCacheEntry<T> {
  value: T;
  bytes: number;
}

interface SharedCacheRecord {
  value: unknown;
  bytes: number;
  priority: number;
  expiresAt?: number;
}

export interface SharedCacheSetOptions {
  priority?: number;
}

export class SharedMemoryCache {
  private readonly maxBytes: number;
  private readonly ttlMs: number | undefined;
  private readonly entries = new Map<string, SharedCacheRecord>();
  private cachedBytes = 0;

  constructor(options: ObjectStoreCacheOptions = {}) {
    this.maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES;
    this.ttlMs = options.ttlMs;
  }

  get<T>(key: string): SharedCacheEntry<T> | undefined {
    const entry = this.entries.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt !== undefined && entry.expiresAt <= Date.now()) {
      this.delete(key);
      return undefined;
    }
    this.entries.delete(key);
    this.entries.set(key, entry);
    return { value: entry.value as T, bytes: entry.bytes };
  }

  set<T>(key: string, value: T, bytes: number, options: SharedCacheSetOptions = {}): void {
    if (bytes > this.maxBytes) return;
    this.delete(key);
    const entry: SharedCacheRecord = {
      value,
      bytes,
      priority: options.priority ?? 1,
    };
    const expiry = this.expiresAt();
    if (expiry !== undefined) entry.expiresAt = expiry;
    this.entries.set(key, entry);
    this.cachedBytes += bytes;
    this.evict();
  }

  delete(key: string): void {
    const entry = this.entries.get(key);
    if (!entry) return;
    this.entries.delete(key);
    this.cachedBytes -= entry.bytes;
  }

  deleteWhere(predicate: (value: unknown) => boolean): void {
    for (const [key, entry] of this.entries) {
      if (!predicate(entry.value)) continue;
      this.entries.delete(key);
      this.cachedBytes -= entry.bytes;
    }
  }

  private expiresAt(): number | undefined {
    return this.ttlMs === undefined ? undefined : Date.now() + this.ttlMs;
  }

  private evict(): void {
    while (this.cachedBytes > this.maxBytes) {
      const evictKey = this.evictKey();
      if (evictKey === undefined) return;
      this.delete(evictKey);
    }
  }

  private evictKey(): string | undefined {
    let selected: { key: string; priority: number } | undefined;
    for (const [key, entry] of this.entries) {
      if (selected === undefined || entry.priority < selected.priority) {
        selected = { key, priority: entry.priority };
      }
    }
    return selected?.key;
  }
}

type CacheValue =
  | { kind: "object"; path: string; value: Uint8Array | null }
  | { kind: "range"; path: string; value: Uint8Array }
  | { kind: "head"; path: string; value: ObjectHead | null };

const DEFAULT_MAX_BYTES = 64 * 1024 * 1024;

export function cachedObjectStore(
  inner: ObjectStore,
  options: ObjectStoreCacheOptions = {},
  sharedCache = new SharedMemoryCache(options),
): ObjectStore {
  function get(key: string): CacheValue | undefined {
    const entry = sharedCache.get<CacheValue>(key);
    if (!entry) return undefined;
    return cloneValue(entry.value);
  }

  function set(key: string, value: CacheValue): void {
    const bytes = valueBytes(value);
    sharedCache.set(key, cloneValue(value), bytes, {
      priority: objectCachePriority(value, options),
    });
  }

  function invalidatePath(path: string): void {
    sharedCache.deleteWhere((value) => isCacheValue(value) && value.path === path);
  }

  return {
    async get(path) {
      const key = objectKey(path);
      const cached = get(key);
      if (cached?.kind === "object") return cached.value;
      const value = await inner.get(path);
      set(key, { kind: "object", path, value });
      return value;
    },
    async getRange(path, range) {
      const key = rangeKey(path, range);
      const cached = get(key);
      if (cached?.kind === "range") return cached.value;
      const value = await inner.getRange(path, range);
      set(key, { kind: "range", path, value });
      return value;
    },
    async put(path, body, putOptions) {
      invalidatePath(path);
      await inner.put(path, body, putOptions);
      invalidatePath(path);
    },
    async delete(path) {
      invalidatePath(path);
      await inner.delete(path);
      invalidatePath(path);
    },
    list(prefix: string, listOptions?: ListOptions): AsyncIterable<ObjectInfo> {
      return inner.list(prefix, listOptions);
    },
    async head(path) {
      const key = headKey(path);
      const cached = get(key);
      if (cached?.kind === "head") return cached.value;
      const value = await inner.head(path);
      set(key, { kind: "head", path, value });
      return value;
    },
  };
}

function objectCachePriority(value: CacheValue, options: ObjectStoreCacheOptions): number {
  const policy = options.policy ?? "balanced";
  if (value.kind === "head") return 3;
  if (policy === "io") return 3;
  if (policy === "latency") return 1;
  return 2;
}

function isCacheValue(value: unknown): value is CacheValue {
  return (
    typeof value === "object" &&
    value !== null &&
    "kind" in value &&
    "path" in value &&
    (value.kind === "object" || value.kind === "range" || value.kind === "head") &&
    typeof value.path === "string"
  );
}

function objectKey(path: string): string {
  return `object:${path}`;
}

function rangeKey(path: string, range: { offset: number; length: number }): string {
  return `range:${path}:${range.offset}:${range.length}`;
}

function headKey(path: string): string {
  return `head:${path}`;
}

function valueBytes(value: CacheValue): number {
  if (value.kind === "object") return value.value?.byteLength ?? 0;
  if (value.kind === "range") return value.value.byteLength;
  return 0;
}

function cloneValue(value: CacheValue): CacheValue {
  if (value.kind === "object") {
    return { ...value, value: value.value === null ? null : copyBytes(value.value) };
  }
  if (value.kind === "range") return { ...value, value: copyBytes(value.value) };
  return { ...value, value: cloneHead(value.value) };
}

function copyBytes(bytes: Uint8Array): Uint8Array {
  const out = new Uint8Array(bytes.byteLength);
  out.set(bytes);
  return out;
}

function cloneHead(head: ObjectHead | null): ObjectHead | null {
  if (head === null) return null;
  const copy: ObjectHead = { size: head.size };
  if (head.etag !== undefined) copy.etag = head.etag;
  if (head.lastModified !== undefined) copy.lastModified = new Date(head.lastModified);
  if (head.contentType !== undefined) copy.contentType = head.contentType;
  return copy;
}
