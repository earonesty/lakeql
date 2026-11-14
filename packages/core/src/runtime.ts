import { MATERIALIZATION_CHARGES } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import type { Bookmark } from "./types.js";

export interface CacheEntry<T> {
  value: T;
  expiresAt?: number;
}

export interface CacheAdapter<T = Uint8Array> {
  get(key: string): Promise<CacheEntry<T> | undefined>;
  set(key: string, entry: CacheEntry<T>): Promise<void>;
  delete(key: string): Promise<void>;
}

export class MemoryCache<T = Uint8Array> implements CacheAdapter<T> {
  private readonly entries = new Map<string, CacheEntry<T>>();

  async get(key: string): Promise<CacheEntry<T> | undefined> {
    const entry = this.entries.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt !== undefined && entry.expiresAt <= Date.now()) {
      this.entries.delete(key);
      return undefined;
    }
    return entry;
  }

  async set(key: string, entry: CacheEntry<T>): Promise<void> {
    this.entries.set(key, entry);
  }

  async delete(key: string): Promise<void> {
    this.entries.delete(key);
  }
}

export function memoryCache<T = Uint8Array>(): CacheAdapter<T> {
  return new MemoryCache<T>();
}

export interface CacheApiCacheOptions {
  namespace?: string;
  now?: () => number;
}

export class CacheApiCache implements CacheAdapter<Uint8Array> {
  private readonly cache: Cache;
  private readonly namespace: string;
  private readonly now: () => number;

  constructor(cache: Cache, options: CacheApiCacheOptions = {}) {
    this.cache = cache;
    this.namespace = options.namespace ?? "lakeql";
    this.now = options.now ?? Date.now;
  }

  async get(key: string): Promise<CacheEntry<Uint8Array> | undefined> {
    const request = this.request(key);
    const response = await this.cache.match(request);
    if (!response) return undefined;
    const expiresAtHeader = response.headers.get("x-lakeql-expires-at");
    const expiresAt = expiresAtHeader === null ? undefined : Number(expiresAtHeader);
    if (expiresAt !== undefined && expiresAt <= this.now()) {
      await this.cache.delete(request);
      return undefined;
    }
    const entry: CacheEntry<Uint8Array> = {
      value: new Uint8Array(await response.arrayBuffer()),
    };
    if (expiresAt !== undefined) entry.expiresAt = expiresAt;
    return entry;
  }

  async set(key: string, entry: CacheEntry<Uint8Array>): Promise<void> {
    const headers = new Headers({ "content-type": "application/octet-stream" });
    if (entry.expiresAt !== undefined) headers.set("x-lakeql-expires-at", String(entry.expiresAt));
    await this.cache.put(this.request(key), new Response(toArrayBuffer(entry.value), { headers }));
  }

  async delete(key: string): Promise<void> {
    await this.cache.delete(this.request(key));
  }

  private request(key: string): Request {
    return new Request(`https://cache.lakeql.invalid/${this.namespace}/${encodeURIComponent(key)}`);
  }
}

export function cacheApiCache(
  cache: Cache,
  options: CacheApiCacheOptions = {},
): CacheAdapter<Uint8Array> {
  return new CacheApiCache(cache, options);
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const buffer = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(buffer).set(bytes);
  return buffer;
}

export interface CheckpointStore {
  get(jobId: string): Promise<Bookmark | undefined>;
  put(jobId: string, bookmark: Bookmark): Promise<void>;
  delete(jobId: string): Promise<void>;
}

export interface SpillRef {
  id: string;
  byteSize: number;
}

export interface SpillUsage {
  entries: number;
  byteSize: number;
}

export interface SpillAdapter {
  write(id: string, data: Uint8Array): Promise<SpillRef>;
  read(ref: SpillRef | string): Promise<Uint8Array>;
  /** Admit the read buffer before allocating it. The returned independent owner
   * remains charged until released by the consumer. Adapter storage is separate.
   */
  readOwned?(ref: SpillRef | string, budget: MemoryBudget): Promise<OwnedMemory<Uint8Array>>;
  delete(ref: SpillRef | string): Promise<void>;
  usage(): Promise<SpillUsage>;
}

export interface MemorySpillAdapterOptions {
  maxBytes?: number;
}

export class MemorySpillAdapter implements SpillAdapter {
  private readonly entries = new Map<string, Uint8Array>();
  private readonly maxBytes: number | undefined;

  constructor(options: MemorySpillAdapterOptions = {}) {
    this.maxBytes = options.maxBytes;
  }

  async write(id: string, data: Uint8Array): Promise<SpillRef> {
    if (!id) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "spill id must be non-empty");
    }
    const nextBytes =
      this.currentBytes() - (this.entries.get(id)?.byteLength ?? 0) + data.byteLength;
    if (this.maxBytes !== undefined && nextBytes > this.maxBytes) {
      throw new LakeqlError(
        "LAKEQL_BUDGET_EXCEEDED",
        `Query exceeded spill bytes budget (${nextBytes} > ${this.maxBytes}). Add a partition filter, date filter, h3 filter, or limit.`,
        { metric: "spill bytes", limit: this.maxBytes, actual: nextBytes },
      );
    }
    const copy = copyBytes(data);
    this.entries.set(id, copy);
    return { id, byteSize: copy.byteLength };
  }

  async read(ref: SpillRef | string): Promise<Uint8Array> {
    const id = spillId(ref);
    const data = this.entries.get(id);
    if (!data) {
      throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No spill entry ${id}`, { id });
    }
    return copyBytes(data);
  }

  async readOwned(ref: SpillRef | string, budget: MemoryBudget): Promise<OwnedMemory<Uint8Array>> {
    const id = spillId(ref);
    const data = this.entries.get(id);
    if (!data) throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No spill entry ${id}`, { id });
    const scope = new MemoryScope(budget);
    try {
      return scope
        .allocate(MATERIALIZATION_CHARGES.view + data.byteLength, "spill read buffer", () =>
          copyBytes(data),
        )
        .retain();
    } finally {
      scope.dispose();
    }
  }

  async delete(ref: SpillRef | string): Promise<void> {
    this.entries.delete(spillId(ref));
  }

  async usage(): Promise<SpillUsage> {
    return { entries: this.entries.size, byteSize: this.currentBytes() };
  }

  private currentBytes(): number {
    let total = 0;
    for (const data of this.entries.values()) total += data.byteLength;
    return total;
  }
}

export function memorySpillAdapter(options: MemorySpillAdapterOptions = {}): SpillAdapter {
  return new MemorySpillAdapter(options);
}

function spillId(ref: SpillRef | string): string {
  return typeof ref === "string" ? ref : ref.id;
}

function copyBytes(data: Uint8Array): Uint8Array {
  const copy = new Uint8Array(data.byteLength);
  copy.set(data);
  return copy;
}

export interface QueueAdapter<T> {
  send(message: T, options?: { delayMs?: number }): Promise<void>;
}

export interface LockAdapter {
  withLock<T>(key: string, fn: () => Promise<T>): Promise<T>;
}

export interface Clock {
  now(): number;
}

export interface IdGenerator {
  id(prefix?: string): string;
}

export interface MetricsHook {
  count(name: string, value?: number, tags?: Record<string, string>): void;
  timing(name: string, ms: number, tags?: Record<string, string>): void;
}

export interface LogHook {
  debug(message: string, fields?: Record<string, unknown>): void;
  info(message: string, fields?: Record<string, unknown>): void;
  warn(message: string, fields?: Record<string, unknown>): void;
  error(message: string, fields?: Record<string, unknown>): void;
}

export interface RuntimeSubstrate {
  checkpointStore?: CheckpointStore;
  spill?: SpillAdapter;
  queue?: QueueAdapter<Bookmark>;
  lock?: LockAdapter;
  clock?: Clock;
  ids?: IdGenerator;
  metrics?: MetricsHook;
  log?: LogHook;
}
