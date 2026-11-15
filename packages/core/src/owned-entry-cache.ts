import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { isLakeqlError } from "./errors.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import { cloneObjectInfos } from "./object-info.js";
import { SharedMemoryCache } from "./object-store-cache.js";
import type { CacheAdapter, CacheEntry } from "./runtime.js";
import type { ObjectInfo } from "./store.js";

export interface OwnedEntryCacheOptions<T> {
  memoryBudget: MemoryBudget;
  maxBytes?: number;
  ttlMs?: number;
  /** Copy value storage into scope before allocating it. Returned data must not
   * retain unowned mutable backing. This defines the supported value domain.
   */
  clone(value: T, scope: MemoryScope): T;
}

/** Shared-pool LRU cache. Owned entries are immutable while retained or pinned.
 * Plain get transfers an independent copy to the caller outside pool accounting.
 */
export class OwnedEntryCache<T> implements CacheAdapter<T> {
  private readonly cache: SharedMemoryCache;
  constructor(private readonly options: OwnedEntryCacheOptions<T>) {
    this.cache = new SharedMemoryCache(options);
  }

  async getOwnedEntry(
    key: string,
    budget: MemoryBudget,
  ): Promise<OwnedMemory<CacheEntry<T>> | undefined> {
    let owner: OwnedMemory<CacheEntry<T>> | undefined;
    try {
      owner = this.cache.acquireOwned<CacheEntry<T>>(key, budget);
    } catch (error) {
      if (isLakeqlError(error) && error.code === "LAKEQL_BUDGET_EXCEEDED") return undefined;
      throw error;
    }
    if (owner === undefined) return undefined;
    if (owner.value.expiresAt !== undefined && owner.value.expiresAt <= Date.now()) {
      owner.release();
      this.cache.delete(key);
      return undefined;
    }
    return owner;
  }

  async setOwnedEntry(key: string, entry: OwnedMemory<CacheEntry<T>>): Promise<void> {
    if (!this.cache.supportsOwnedRetention(entry.budget)) return;
    this.cache.delete(key);
    if (this.options.maxBytes === 0) return;
    if (entry.value.expiresAt !== undefined && entry.value.expiresAt <= Date.now()) {
      this.cache.delete(key);
      return;
    }
    // The shared cache owns its directory metadata independently of reader pins.
    // Retain the supplied payload directly; no wrapper scope or extra owner graph.
    this.cache.setOwned(key, entry);
  }

  async get(key: string): Promise<CacheEntry<T> | undefined> {
    const owner = await this.getOwnedEntry(key, this.options.memoryBudget);
    if (owner === undefined) return undefined;
    const scope = new MemoryScope(this.options.memoryBudget);
    try {
      return this.copyEntry(owner.value, scope);
    } finally {
      scope.dispose();
      owner.release();
    }
  }

  async set(key: string, entry: CacheEntry<T>): Promise<void> {
    const scope = new MemoryScope(this.options.memoryBudget);
    try {
      const value = this.copyEntry(entry, scope);
      const owner = scope.retainAllocations(value);
      try {
        await this.setOwnedEntry(key, owner);
      } finally {
        owner.release();
      }
    } finally {
      scope.dispose();
    }
  }

  async delete(key: string): Promise<void> {
    this.cache.delete(key);
  }
  clear(): void {
    this.cache.deleteWhere(() => true);
  }

  private copyEntry(entry: CacheEntry<T>, scope: MemoryScope): CacheEntry<T> {
    const value = this.options.clone(entry.value, scope);
    return scope.allocate(c.object + 2 * c.property, "owned cache entry", () => ({
      value,
      ...(entry.expiresAt === undefined ? {} : { expiresAt: entry.expiresAt }),
    })).value;
  }
}

/** Bounded reusable discovery metadata, including transport hints and dates. */
export function memoryPlanningCache(
  options: Omit<OwnedEntryCacheOptions<ObjectInfo[]>, "clone">,
): OwnedEntryCache<ObjectInfo[]> {
  return new OwnedEntryCache<ObjectInfo[]>({ ...options, clone: cloneObjectInfos });
}
