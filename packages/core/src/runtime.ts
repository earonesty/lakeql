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

export interface CheckpointStore {
  get(jobId: string): Promise<Bookmark | undefined>;
  put(jobId: string, bookmark: Bookmark): Promise<void>;
  delete(jobId: string): Promise<void>;
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
  queue?: QueueAdapter<Bookmark>;
  lock?: LockAdapter;
  clock?: Clock;
  ids?: IdGenerator;
  metrics?: MetricsHook;
  log?: LogHook;
}
