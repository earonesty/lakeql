import {
  isLakeqlError,
  LakeqlError,
  type MemoryBudget,
  type MemoryScope,
  mapConcurrent,
  type OwnedMemory,
  SharedMemoryCache,
} from "lakeql-core";
import { coalescedRange, type RangeCacheOptions } from "./range-cache.js";
import type { StoreAsyncBuffer } from "./types.js";

interface PendingRange {
  read: Promise<OwnedMemory<ArrayBuffer>>;
  users: number;
  active: number;
  stage: MemoryScope;
}
interface BudgetCache {
  cache: SharedMemoryCache;
  pending: Map<string, PendingRange>;
}

// Shared cache identity and allocation root isolate unrelated stores/jobs while
// allowing independent scanner wrappers to join the same physical range read.
const sharedPendingPools = new WeakMap<
  SharedMemoryCache,
  WeakMap<MemoryBudget, Map<string, PendingRange>>
>();

/** Ownership-aware requests never fall back to an untracked allocation path. */
export function withOwnedRangeCache(
  file: StoreAsyncBuffer,
  legacy: StoreAsyncBuffer,
  options: RangeCacheOptions,
  cacheKey: string,
): StoreAsyncBuffer {
  const readOwned = file.sliceOwned?.bind(file);
  if (readOwned === undefined || options.maxBytes <= 0 || (options.maxEntryBytes ?? 1) <= 0)
    return legacy;
  const ownedReader = readOwned;
  const budgets = new WeakMap<MemoryBudget, BudgetCache>();
  let pendingPools =
    options.sharedCache === undefined ? undefined : sharedPendingPools.get(options.sharedCache);
  if (pendingPools === undefined) {
    pendingPools = new WeakMap<MemoryBudget, Map<string, PendingRange>>();
    if (options.sharedCache !== undefined)
      sharedPendingPools.set(options.sharedCache, pendingPools);
  }
  const poolPending = pendingPools;
  const maxEntryBytes = options.maxEntryBytes ?? 16 * 1024 * 1024;
  const coalesceBytes = options.coalesceBytes ?? 256 * 1024;
  function state(scope: MemoryScope): BudgetCache {
    if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Range read scope disposed");
    let found = budgets.get(scope.budget);
    if (found === undefined) {
      let pending = poolPending.get(scope.budget.allocationPool);
      if (pending === undefined) {
        pending = new Map();
        poolPending.set(scope.budget.allocationPool, pending);
      }
      found = {
        cache:
          options.sharedCache ??
          new SharedMemoryCache({
            ...options.cacheOptions,
            maxBytes: options.maxBytes,
            memoryBudget: scope.budget,
          }),
        pending,
      };
      budgets.set(scope.budget, found);
    }
    return found;
  }
  function validateRange(start: number, end: number): void {
    if (
      !Number.isSafeInteger(start) ||
      !Number.isSafeInteger(end) ||
      start < 0 ||
      end < start ||
      end > file.byteLength
    ) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid owned range", {
        start,
        end,
        size: file.byteLength,
      });
    }
  }
  function acquire(
    cache: SharedMemoryCache,
    key: string,
    scope: MemoryScope,
  ): OwnedMemory<ArrayBuffer> | undefined {
    try {
      const owner = cache.acquireOwned<ArrayBuffer>(key, scope.budget.allocationPool);
      if (owner === undefined) return undefined;
      try {
        return scope.pinExisting(owner);
      } finally {
        owner.release();
      }
    } catch (error) {
      if (isLakeqlError(error) && error.code === "LAKEQL_BUDGET_EXCEEDED") return undefined;
      throw error;
    }
  }
  async function ensure(start: number, end: number, scope: MemoryScope, demand = false) {
    validateRange(start, end);
    const current = state(scope);
    // Read-ahead only pays when the completed window can be retained. An
    // unbudgeted or unrelated shared cache cannot own this pool's allocations.
    let request = current.cache.supportsOwnedRetention(scope.budget)
      ? coalescedRange(file.byteLength, start, end, coalesceBytes)
      : { start, end };
    const keyFor = (request: { start: number; end: number }) =>
      [
        "owned-scan-range",
        cacheKey,
        file.byteLength,
        file.etag ?? "",
        request.start,
        request.end,
      ].join(":");
    let key = keyFor(request);
    const cached = acquire(current.cache, key, scope);
    if (cached !== undefined) {
      if (cached.budget === scope.budget) return { owner: cached, start: request.start };
      cached.release();
    }
    let pending = current.pending.get(key);
    if (
      pending === undefined &&
      demand &&
      request.end - request.start > scope.availableAllocationBytes
    ) {
      // Output space is already reserved. Keep reusable hits and in-flight windows,
      // but avoid starting optional read-ahead that cannot fit current headroom.
      request = { start, end };
      key = keyFor(request);
      const exact = acquire(current.cache, key, scope);
      if (exact !== undefined) {
        if (exact.budget === scope.budget) return { owner: exact, start };
        exact.release();
      }
      pending = current.pending.get(key);
    }
    if (pending === undefined) {
      // One stage owns in-flight storage. Individual consumers may close independently.
      const stage = scope.detachedScope();
      const read = (async () => {
        try {
          const owner = await ownedReader(request.start, request.end, stage);
          const retained = owner.retain();
          try {
            if (owner.bytes <= maxEntryBytes && owner.bytes <= options.maxBytes)
              current.cache.setOwned(key, owner);
            return retained;
          } catch (error) {
            retained.release();
            throw error;
          }
        } finally {
          stage.dispose();
        }
      })();
      pending = { read, users: 0, active: 0, stage };
      current.pending.set(key, pending);
    }
    pending.users += 1;
    pending.active += 1;
    const joined = pending;
    let active = true;
    const leave = () => {
      if (!active) return;
      active = false;
      joined.active -= 1;
      if (joined.active === 0) {
        if (current.pending.get(key) === joined) current.pending.delete(key);
        joined.stage.dispose();
      }
    };
    const signal = scope.signal;
    signal.addEventListener("abort", leave, { once: true });
    if (signal.aborted) leave();
    let shared: OwnedMemory<ArrayBuffer> | undefined;
    try {
      shared = await pending.read;
      if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Range read scope disposed");
      return { owner: scope.pinExisting(shared), start: request.start };
    } catch (error) {
      if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Range read scope disposed");
      throw error;
    } finally {
      signal.removeEventListener("abort", leave);
      leave();
      pending.users -= 1;
      if (pending.users === 0) {
        if (current.pending.get(key) === pending) current.pending.delete(key);
        shared?.release();
      }
    }
  }
  return {
    ...legacy,
    ...(file.ownedReadAllocationBytes === undefined
      ? {}
      : {
          ownedReadAllocationBytes(start: number, end: number) {
            validateRange(start, end);
            const request = coalescedRange(file.byteLength, start, end, coalesceBytes);
            return end - start + (file.ownedReadAllocationBytes?.(request.start, request.end) ?? 0);
          },
        }),
    async sliceOwned(start, end, scope) {
      validateRange(start, end);
      if (start === end) return scope.allocate(0, "empty range", () => new ArrayBuffer(0));
      // Reserve mandatory output before admitting optional coalesced input.
      return scope.allocateAsync(end - start, "range cache output", async () => {
        const { owner, start: fetchedStart } = await ensure(start, end, scope, true);
        try {
          const from = Math.min(owner.value.byteLength, Math.max(0, start - fetchedStart));
          const until = Math.min(owner.value.byteLength, Math.max(from, end - fetchedStart));
          return owner.value.slice(from, until);
        } finally {
          owner.release();
        }
      });
    },
    async prefetchOwned(ranges, scope) {
      if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Range read scope disposed");
      const cache = state(scope).cache;
      if (!cache.supportsOwnedRetention(scope.budget)) return;
      // Expiry during the cache forecast can release allocation headroom.
      const cacheCapacity = cache.availableAdmissionBytes();
      const admissionBytes = Math.min(
        options.maxBytes,
        scope.availableAllocationBytes,
        cacheCapacity,
      );
      const selected = new Map<string, { start: number; end: number }>();
      let bytes = 0;
      for (const range of ranges) {
        const request = coalescedRange(file.byteLength, range.start, range.end, coalesceBytes);
        const size = request.end - request.start;
        const key = `${request.start}:${request.end}`;
        if (size <= 0 || size > maxEntryBytes || bytes + size > admissionBytes || selected.has(key))
          continue;
        bytes += size;
        selected.set(key, request);
      }
      await mapConcurrent(
        [...selected.values()],
        options.maxConcurrentPrefetchReads ?? 4,
        async (range) => {
          try {
            const { owner } = await ensure(range.start, range.end, scope);
            owner.release();
          } catch (error) {
            // Allocation pressure may decline optional read-ahead. Other errors
            // stop scheduling; active siblings drain before scope cleanup.
            if (
              isLakeqlError(error) &&
              error.code === "LAKEQL_BUDGET_EXCEEDED" &&
              error.details.metric === "allocated memory bytes"
            )
              return;
            throw error;
          }
        },
      );
    },
  };
}
