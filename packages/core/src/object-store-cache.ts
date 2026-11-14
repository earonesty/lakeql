import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { isLakeqlError, LakeqlError } from "./errors.js";
import type { MemoryBudget, MemoryReservation } from "./memory-budget.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import type { ListOptions, ObjectHead, ObjectInfo, ObjectStore, PutOptions } from "./store.js";

export interface ObjectStoreCacheOptions {
  /** Maximum cached payload and retained entry metadata bytes. Defaults to 64 MiB. */
  maxBytes?: number;
  /** Optional time-to-live for cache entries. Entries do not expire by default. */
  ttlMs?: number;
  /** How LakeQL should spend the cache budget. Defaults to balanced. */
  policy?: CachePolicy;
  /** Shared allocation pool for retained cache payloads. */
  memoryBudget?: MemoryBudget;
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
  reservation?: MemoryReservation;
  owner?: OwnedMemory<unknown>;
}

export interface SharedCacheLease<T> extends SharedCacheEntry<T> {
  /** Release after the last use of the borrowed payload. */
  release(): void;
}

export interface SharedCacheSetOptions {
  priority?: number;
}

export interface SharedOwnedCacheSetOptions extends SharedCacheSetOptions {
  /** Eviction cost including uninstrumented object storage. Must cover owned bytes.
   * This controls cache capacity; it does not claim physical allocation admission.
   */
  cacheBytes?: number;
}

export class SharedMemoryCache {
  private readonly maxBytes: number;
  private readonly ttlMs: number | undefined;
  private readonly entries = new Map<string, SharedCacheRecord>();
  private cachedBytes = 0;
  private readonly memoryBudget: MemoryBudget | undefined;
  private detachReclaimer: (() => void) | undefined;

  constructor(options: ObjectStoreCacheOptions = {}) {
    this.maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES;
    this.ttlMs = options.ttlMs;
    this.memoryBudget = options.memoryBudget;
  }

  /** Advisory capacity for a new priority class after evicting lower/equal entries.
   * This is not a reservation; callers still need allocation and insertion admission.
   * Compute once per speculative window, not once per range.
   */
  availableAdmissionBytes(priority = 1): number {
    const now = Date.now();
    let protectedBytes = 0;
    for (const [key, entry] of this.entries) {
      if (entry.expiresAt !== undefined && entry.expiresAt <= now) {
        this.delete(key);
        continue;
      }
      if (entry.priority > priority) protectedBytes += entry.bytes;
    }
    return Math.max(0, this.maxBytes - protectedBytes);
  }

  /** Borrow for immediate use. Use acquire when retaining across cache mutation. */
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

  /** Charge existing payload retention; use setAllocated to reserve before allocation. */
  set<T>(key: string, value: T, bytes: number, options: SharedCacheSetOptions = {}): void {
    this.setAllocated(key, bytes, () => value, options);
  }

  /** Pin a borrowed payload across eviction or replacement. */
  acquire<T>(key: string): SharedCacheLease<T> | undefined {
    const entry = this.get<T>(key);
    if (entry === undefined) return undefined;
    const record = this.entries.get(key);
    const pin = record?.owner?.retain() ?? record?.reservation?.retain();
    return { ...entry, release: () => pin?.release() };
  }

  /** Whether this cache can retain allocations from the given pool.
   * Capability only: capacity and insertion admission are checked separately.
   */
  supportsOwnedRetention(budget: MemoryBudget): boolean {
    return this.memoryBudget !== undefined && budget.sharesPoolWith(this.memoryBudget);
  }

  /** Retain existing backing with a cache charge in the same allocation pool. */
  setOwned<T>(
    key: string,
    owner: OwnedMemory<T>,
    options: SharedOwnedCacheSetOptions = {},
  ): boolean {
    const cacheBytes = options.cacheBytes ?? owner.bytes;
    if (!Number.isSafeInteger(cacheBytes) || cacheBytes < owner.bytes) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Owned cache cost must cover its backing bytes",
      );
    }
    if (this.memoryBudget === undefined || !owner.budget.sharesPoolWith(this.memoryBudget))
      return false;
    this.delete(key);
    if (cacheBytes > this.maxBytes) return false;
    let retained: OwnedMemory<T>;
    try {
      retained = owner.retainIn(this.memoryBudget);
    } catch (error) {
      if (isLakeqlError(error) && error.code === "LAKEQL_BUDGET_EXCEEDED") return false;
      throw error;
    }
    return this.insert(key, {
      value: retained.value,
      bytes: cacheBytes,
      priority: options.priority ?? 1,
      owner: retained,
    });
  }

  /** Returns independent ownership; ordinary unowned entries cannot satisfy this contract. */
  acquireOwned<T>(key: string, budget?: MemoryBudget): OwnedMemory<T> | undefined {
    if (this.get<T>(key) === undefined) return undefined;
    const owner = this.entries.get(key)?.owner;
    if (owner === undefined) return undefined;
    if (budget !== undefined && !owner.budget.sharesPoolWith(budget)) return undefined;
    return (budget === undefined ? owner.retain() : owner.retainIn(budget)) as OwnedMemory<T>;
  }

  /** Admit and reserve before invoking a synchronous allocation factory. */
  setAllocated<T>(
    key: string,
    bytes: number,
    allocate: () => T,
    options: SharedCacheSetOptions = {},
  ): boolean {
    if (!Number.isSafeInteger(bytes) || bytes < 0) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid cache byte count", { bytes });
    }
    this.delete(key);
    if (bytes > this.maxBytes) return false;
    const reservation = this.memoryBudget?.tryReserve(bytes, "cache payload");
    if (this.memoryBudget !== undefined && reservation === undefined) return false;
    let value: T;
    try {
      value = allocate();
    } catch (error) {
      reservation?.release();
      throw error;
    }
    const entry: SharedCacheRecord = {
      value,
      bytes,
      priority: options.priority ?? 1,
      ...(reservation === undefined ? {} : { reservation }),
    };
    return this.insert(key, entry);
  }

  private insert(key: string, entry: SharedCacheRecord): boolean {
    const expiry = this.expiresAt();
    if (expiry !== undefined) entry.expiresAt = expiry;
    this.entries.set(key, entry);
    this.cachedBytes += entry.bytes;
    if (this.detachReclaimer === undefined && this.memoryBudget !== undefined) {
      const budget = this.memoryBudget;
      this.detachReclaimer = budget.registerReclaimer((needed) => {
        const target = budget.usedBytes - needed;
        while (budget.usedBytes > target) {
          const key = this.evictKey();
          if (key === undefined) break;
          this.delete(key);
        }
      });
    }
    this.evict();
    return this.entries.get(key) === entry;
  }

  delete(key: string): void {
    const entry = this.entries.get(key);
    if (!entry) return;
    this.entries.delete(key);
    this.cachedBytes -= entry.bytes;
    entry.reservation?.release();
    entry.owner?.release();
    if (this.entries.size === 0) {
      this.detachReclaimer?.();
      this.detachReclaimer = undefined;
    }
  }

  deleteWhere(predicate: (value: unknown) => boolean): void {
    for (const [key, entry] of this.entries) {
      if (!predicate(entry.value)) continue;
      this.delete(key);
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
    const bytes = valueBytes(key, value);
    sharedCache.setAllocated(key, bytes, () => cloneValue(value), {
      priority: objectCachePriority(value, options),
    });
  }

  function invalidatePath(path: string): void {
    sharedCache.deleteWhere((value) => isCacheValue(value) && value.path === path);
  }

  const ownedRead = inner.getRangeOwned?.bind(inner);
  const ownedHead = inner.headOwned?.bind(inner);
  const allocation = inner.getRangeOwnedAllocation?.bind(inner);
  return {
    ...(inner.withDefaultMemoryBudget === undefined
      ? {}
      : {
          withDefaultMemoryBudget: (budget: MemoryBudget, maxCacheBytes?: number) =>
            cachedObjectStore(
              inner.withDefaultMemoryBudget?.(budget, maxCacheBytes) ?? inner,
              options,
              sharedCache,
            ),
        }),
    ...(ownedHead === undefined ? {} : { headOwned: ownedHead }),
    ...(ownedRead === undefined || allocation === undefined
      ? {}
      : {
          getRangeOwnedAllocation(path: string, range: { offset: number; length: number }) {
            const forecast = allocation(path, range);
            // Cold reads may retain a cache copy alongside the source output. Hits
            // allocate only the output copy, covered by this conservative forecast.
            return {
              bytes:
                forecast.bytes +
                range.length +
                c.view +
                cacheEntryBytes(rangeKey(path, range), path),
              output: forecast.output,
            };
          },
        }),
    ...(ownedRead === undefined
      ? {}
      : {
          async getRangeOwned(
            path: string,
            range: { offset: number; length: number },
            scope: MemoryScope,
          ) {
            const key = rangeKey(path, range);
            const cached = sharedCache.acquire<CacheValue>(key);
            if (cached !== undefined) {
              try {
                if (cached.value.kind === "range") {
                  const bytes = cached.value.value;
                  return scope.allocate(bytes.byteLength, "cached range output", () =>
                    copyBytes(bytes),
                  );
                }
              } finally {
                cached.release();
              }
            }
            const owner = await ownedRead(path, range, scope);
            try {
              set(key, { kind: "range", path, value: owner.value });
              return owner;
            } catch (error) {
              owner.release();
              throw error;
            }
          },
        }),
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
    ...(inner.putOwned === undefined
      ? {}
      : {
          async putOwned(path: string, body: OwnedMemory<Uint8Array>, options?: PutOptions) {
            invalidatePath(path);
            await (inner.putOwned as NonNullable<ObjectStore["putOwned"]>)(path, body, options);
            invalidatePath(path);
          },
        }),
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
  if (value.kind === "head") return 4;
  if (policy === "io") return 3;
  if (policy === "latency") return 1;
  if (value.kind === "range") return 3;
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

function cacheEntryBytes(key: string, path: string): number {
  return (
    2 * c.object + 8 * c.property + 2 * c.string + (key.length + path.length) * c.stringCodeUnit
  );
}

function valueBytes(key: string, value: CacheValue): number {
  const entry = cacheEntryBytes(key, value.path);
  if (value.kind === "object")
    return entry + (value.value === null ? 0 : c.view + value.value.byteLength);
  if (value.kind === "range") return entry + c.view + value.value.byteLength;
  const head = value.value;
  return (
    entry +
    (head === null
      ? 0
      : c.object +
        5 * c.property +
        (head.etag === undefined ? 0 : c.string + head.etag.length * c.stringCodeUnit) +
        (head.contentType === undefined
          ? 0
          : c.string + head.contentType.length * c.stringCodeUnit) +
        (head.lastModified === undefined ? 0 : c.timestamp))
  );
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
  if (head.rangeReadMode !== undefined) copy.rangeReadMode = head.rangeReadMode;
  return copy;
}
