import type { ListOptions, ObjectHead, ObjectInfo, ObjectStore } from "./store.js";

export interface ObjectStoreCacheOptions {
  /** Maximum cached object/range bytes retained in memory. Defaults to 64 MiB. */
  maxBytes?: number;
  /** Optional time-to-live for cache entries. Entries do not expire by default. */
  ttlMs?: number;
}

type CacheValue =
  | { kind: "object"; path: string; value: Uint8Array | null }
  | { kind: "range"; path: string; value: Uint8Array }
  | { kind: "head"; path: string; value: ObjectHead | null };

interface CacheEntry {
  value: CacheValue;
  bytes: number;
  expiresAt?: number;
}

const DEFAULT_MAX_BYTES = 64 * 1024 * 1024;

export function cachedObjectStore(
  inner: ObjectStore,
  options: ObjectStoreCacheOptions = {},
): ObjectStore {
  const maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES;
  const ttlMs = options.ttlMs;
  const entries = new Map<string, CacheEntry>();
  let cachedBytes = 0;

  function expiresAt(): number | undefined {
    return ttlMs === undefined ? undefined : Date.now() + ttlMs;
  }

  function get(key: string): CacheValue | undefined {
    const entry = entries.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt !== undefined && entry.expiresAt <= Date.now()) {
      entries.delete(key);
      cachedBytes -= entry.bytes;
      return undefined;
    }
    entries.delete(key);
    entries.set(key, entry);
    return cloneValue(entry.value);
  }

  function set(key: string, value: CacheValue): void {
    const bytes = valueBytes(value);
    if (bytes > maxBytes) return;
    const current = entries.get(key);
    if (current) {
      entries.delete(key);
      cachedBytes -= current.bytes;
    }
    const entry: CacheEntry = { value: cloneValue(value), bytes };
    const expiry = expiresAt();
    if (expiry !== undefined) entry.expiresAt = expiry;
    entries.set(key, entry);
    cachedBytes += bytes;
    evict();
  }

  function evict(): void {
    while (cachedBytes > maxBytes) {
      const first = entries.keys().next().value;
      if (first === undefined) return;
      const entry = entries.get(first);
      entries.delete(first);
      if (entry) cachedBytes -= entry.bytes;
    }
  }

  function invalidatePath(path: string): void {
    for (const [key, entry] of entries) {
      if (entry.value.path !== path) continue;
      entries.delete(key);
      cachedBytes -= entry.bytes;
    }
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
