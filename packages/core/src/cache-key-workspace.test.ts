import { expect, it, vi } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { SharedMemoryCache, sharedCacheMetadataBytes } from "./object-store-cache.js";

it("admits key workspace before construction at an exact boundary without reclamation", () => {
  for (const shortage of [0, 1]) {
    const pool = new MemoryBudget(128 - shortage);
    const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 1024 });
    const reclaim = vi.fn();
    const detach = pool.registerReclaimer(reclaim);
    const create = vi.fn(() => {
      expect(pool.usedBytes).toBe(128);
      return "key";
    });
    const use = vi.fn((key: string) => {
      expect(key).toBe("key");
      expect(pool.usedBytes).toBe(128);
      return true;
    });
    expect(cache.withOptionalKey(128, create, use)).toBe(shortage ? undefined : true);
    expect(create).toHaveBeenCalledTimes(shortage ? 0 : 1);
    expect(use).toHaveBeenCalledTimes(shortage ? 0 : 1);
    expect(reclaim).not.toHaveBeenCalled();
    detach();
    expect(pool.usedBytes).toBe(0);
  }
});

it("respects fallback query headroom and unbudgeted cache capacity before constructing keys", () => {
  const pool = new MemoryBudget(4096);
  const cache = new SharedMemoryCache({ maxBytes: 1024 });
  const create = vi.fn(() => "key");
  expect(cache.withOptionalKey(128, create, () => true, pool.child(127))).toBeUndefined();
  expect(
    new SharedMemoryCache({ maxBytes: 127 }).withOptionalKey(128, create, () => true),
  ).toBeUndefined();
  for (const bytes of [-1, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])
    expect(cache.withOptionalKey(bytes, create, () => true)).toBeUndefined();
  expect(create).not.toHaveBeenCalled();
  expect(pool.usedBytes).toBe(0);
});

it.each(["create", "use"])("releases key scratch when %s throws", (phase) => {
  const pool = new MemoryBudget(128);
  const cache = new SharedMemoryCache({ memoryBudget: pool });
  const failure = new Error("key operation failed");
  expect(() =>
    cache.withOptionalKey(
      128,
      () => {
        if (phase === "create") throw failure;
        return "key";
      },
      () => {
        throw failure;
      },
    ),
  ).toThrow(failure);
  expect(pool.usedBytes).toBe(0);
});

it("pins active key scratch independently of reentrant query scope disposal", () => {
  const pool = new MemoryBudget(128);
  const scope = new MemoryScope(pool);
  const cache = new SharedMemoryCache({ memoryBudget: pool });
  expect(
    cache.withOptionalKey(
      128,
      () => "key",
      () => {
        scope.dispose();
        expect(pool.usedBytes).toBe(128);
        expect(pool.tryReserve(1, "competing allocation")).toBeUndefined();
        return true;
      },
      scope.budget,
    ),
  ).toBe(true);
  expect(pool.usedBytes).toBe(0);
});

it("transfers retained-key responsibility to the cache without pinning temporary workspace", () => {
  const pool = new MemoryBudget(4096);
  const cache = new SharedMemoryCache({ memoryBudget: pool });
  expect(
    cache.withOptionalKey(
      128,
      () => "key",
      (key) => cache.setAllocated(key, 8, () => new Uint8Array(8)),
    ),
  ).toBe(true);
  expect(pool.usedBytes).toBe(8 + sharedCacheMetadataBytes("key"));
  const pin = cache.acquire<Uint8Array>("key");
  cache.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(8);
  pin?.release();
  expect(pool.usedBytes).toBe(0);
});

it("charges configured cache scratch independently of a consumer's payload-only limit", () => {
  const pool = new MemoryBudget(256);
  const consumer = new MemoryScope(pool.child(8));
  const cache = new SharedMemoryCache({ memoryBudget: pool });
  const owner = cache.withOptionalKey(
    128,
    () => "key",
    () => consumer.allocate(8, "payload", () => new Uint8Array(8)),
    consumer.budget,
  );
  expect(owner?.bytes).toBe(8);
  expect(consumer.budget.peakBytes).toBe(8);
  expect(pool.peakBytes).toBe(136);
  expect(pool.usedBytes).toBe(8);
  consumer.dispose();
  expect(pool.usedBytes).toBe(0);
});
