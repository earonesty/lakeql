import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { encodeOwnedJsonOutput } from "./json-output-encoding.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import { parseOwnedJson } from "./owned-json-parse.js";
import { readObjectHead } from "./read-object-head.js";
import type { CacheAdapter, CacheEntry } from "./runtime.js";
import type { ObjectStore } from "./store.js";

export interface ObjectStoreJsonCacheOptions {
  store: ObjectStore;
  prefix: string;
  ttlMs?: number;
  now?: () => number;
}

interface StoredJsonCacheEntry<T> {
  value: T;
  expiresAt?: number;
}

export class ObjectStoreJsonCache<T> implements CacheAdapter<T> {
  private readonly store: ObjectStore;
  private readonly prefix: string;
  private readonly ttlMs: number | undefined;
  private readonly now: () => number;

  constructor(options: ObjectStoreJsonCacheOptions) {
    this.store = options.store;
    this.prefix = normalizeCachePrefix(options.prefix);
    this.ttlMs = options.ttlMs;
    this.now = options.now ?? Date.now;
  }

  async get(key: string): Promise<CacheEntry<T> | undefined> {
    const path = this.pathFor(key);
    const bytes = await this.store.get(path);
    if (bytes === null) return undefined;
    let parsed: StoredJsonCacheEntry<T>;
    try {
      parsed = JSON.parse(new TextDecoder().decode(bytes)) as StoredJsonCacheEntry<T>;
    } catch (cause) {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", `Invalid JSON cache entry at ${path}`, {
        path,
        cause,
      });
    }
    if (!isStoredJsonCacheEntry(parsed)) {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", `Invalid JSON cache entry at ${path}`, {
        path,
      });
    }
    if (parsed.expiresAt !== undefined && parsed.expiresAt <= this.now()) {
      await this.store.delete(path);
      return undefined;
    }
    const entry: CacheEntry<T> = { value: parsed.value as T };
    if (parsed.expiresAt !== undefined) entry.expiresAt = parsed.expiresAt;
    return entry;
  }

  async getOwnedEntry(
    key: string,
    budget: MemoryBudget,
  ): Promise<OwnedMemory<CacheEntry<T>> | undefined> {
    const output = new MemoryScope(budget);
    const scratch = output.childScope();
    let parsed: OwnedMemory<unknown> | undefined;
    try {
      const path = scratch.allocate(
        c.string + (this.prefix.length + key.length * 9 + 6) * c.stringCodeUnit,
        "JSON cache path",
        () => this.pathFor(key),
      ).value;
      const head = await readObjectHead(this.store, path, scratch);
      if (head === null) return undefined;
      if (!Number.isSafeInteger(head.size) || head.size < 0) {
        throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Invalid JSON cache object size", { path });
      }
      const range = scratch.allocate(c.object + 2 * c.property, "JSON cache range", () => ({
        offset: 0,
        length: head.size,
      })).value;
      let bytes: Uint8Array;
      if (this.store.getRangeOwned !== undefined) {
        const owner = await this.store.getRangeOwned(path, range, scratch);
        try {
          bytes = scratch.adopt(owner).value;
        } catch (error) {
          owner.release();
          throw error;
        }
      } else {
        // Legacy adapter internals remain external; admit the requested payload
        // before calling it and verify the returned extent before parsing.
        bytes = (
          await scratch.allocateAsync(c.view + head.size, "JSON cache read", () =>
            this.store.getRange(path, range),
          )
        ).value;
      }
      if (bytes.byteLength !== head.size) {
        throw new LakeqlError("LAKEQL_CATALOG_ERROR", "JSON cache read extent changed", { path });
      }
      try {
        parsed = parseOwnedJson(bytes, output);
      } catch (cause) {
        if (!(cause instanceof SyntaxError)) throw cause;
        throw new LakeqlError("LAKEQL_CATALOG_ERROR", `Invalid JSON cache entry at ${path}`, {
          path,
          cause,
        });
      }
      if (!isStoredJsonCacheEntry(parsed.value)) {
        throw new LakeqlError("LAKEQL_CATALOG_ERROR", `Invalid JSON cache entry at ${path}`, {
          path,
        });
      }
      if (parsed.value.expiresAt !== undefined && parsed.value.expiresAt <= this.now()) {
        await this.store.delete(path);
        return undefined;
      }
      const result = parsed as OwnedMemory<CacheEntry<T>>;
      parsed = undefined;
      return result;
    } finally {
      parsed?.release();
      scratch.dispose();
      output.dispose();
    }
  }

  async setOwnedEntry(key: string, entry: OwnedMemory<CacheEntry<T>>): Promise<void> {
    const scope = new MemoryScope(entry.budget);
    try {
      const input = scope.adopt(entry.retain()).value;
      if (input.value === undefined)
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "JSON cache value is not JSON serializable", {
          key,
        });
      const stored = scope.allocate(c.object + 2 * c.property, "JSON cache entry", () => {
        const result: StoredJsonCacheEntry<T> = { value: input.value };
        if (input.expiresAt !== undefined) result.expiresAt = input.expiresAt;
        else if (this.ttlMs !== undefined) result.expiresAt = this.now() + this.ttlMs;
        return result;
      }).value;
      const path = scope.allocate(
        c.string + (this.prefix.length + key.length * 9 + 6) * c.stringCodeUnit,
        "JSON cache path",
        () => this.pathFor(key),
      ).value;
      const options = scope.allocate(c.object + c.property, "JSON cache write options", () => ({
        contentType: "application/json",
      })).value;
      const bytes = scope.adopt(encodeOwnedJsonOutput(stored, scope, {}, "standard"));
      // The serialized backing remains owned through asynchronous transport.
      // Capable adapters admit their own copies; legacy/runtime buffering remains external.
      if (this.store.putOwned !== undefined) await this.store.putOwned(path, bytes, options);
      else await this.store.put(path, bytes.value, options);
    } finally {
      scope.dispose();
    }
  }

  async set(key: string, entry: CacheEntry<T>): Promise<void> {
    if (entry.value === undefined) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "JSON cache value is not JSON serializable", {
        key,
      });
    }
    const stored: StoredJsonCacheEntry<T> = { value: entry.value };
    if (entry.expiresAt !== undefined) {
      stored.expiresAt = entry.expiresAt;
    } else if (this.ttlMs !== undefined) {
      stored.expiresAt = this.now() + this.ttlMs;
    }
    let json: string;
    try {
      json = JSON.stringify(stored);
    } catch (cause) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "JSON cache value is not JSON serializable", {
        key,
        cause,
      });
    }
    await this.store.put(this.pathFor(key), new TextEncoder().encode(json), {
      contentType: "application/json",
    });
  }

  async delete(key: string): Promise<void> {
    await this.store.delete(this.pathFor(key));
  }

  private pathFor(key: string): string {
    return `${this.prefix}/${encodeURIComponent(key)}.json`;
  }
}

export function objectStoreJsonCache<T>(options: ObjectStoreJsonCacheOptions): CacheAdapter<T> {
  return new ObjectStoreJsonCache<T>(options);
}

function normalizeCachePrefix(prefix: string): string {
  const trimmed = prefix.replace(/^\/+|\/+$/g, "");
  if (trimmed === "") {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "JSON cache prefix must be non-empty");
  }
  return trimmed;
}

function isStoredJsonCacheEntry(value: unknown): value is StoredJsonCacheEntry<unknown> {
  return typeof value === "object" && value !== null && "value" in value;
}
