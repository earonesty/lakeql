import { expect, it, vi } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { SharedMemoryCache, sharedCacheMetadataBytes } from "./object-store-cache.js";

it("bounds zero-payload entry count with and without a shared pool", () => {
  const cap = 4096;
  for (const pool of [undefined, new MemoryBudget(cap)]) {
    const cache = new SharedMemoryCache({ maxBytes: cap, ...(pool ? { memoryBudget: pool } : {}) });
    for (let index = 0; index < 1000; index++) cache.set(`key-${index}`, null, 0);
    let count = 0;
    cache.deleteWhere(() => {
      count++;
      return true;
    });
    expect(count).toBeGreaterThan(0);
    expect(count).toBeLessThanOrEqual(Math.floor(cap / sharedCacheMetadataBytes("")));
    expect(pool?.usedBytes ?? 0).toBe(0);
  }
});

it("admits payload and key metadata before the factory at exact capacity", () => {
  const key = "猫\u0000";
  const metadata = sharedCacheMetadataBytes(key);
  for (const shortage of [0, 1]) {
    const pool = new MemoryBudget(metadata + 8 - shortage);
    const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: metadata + 8 });
    const factory = vi.fn(() => {
      expect(pool.usedBytes).toBe(metadata + 8);
      return new Uint8Array(8);
    });
    expect(cache.setAllocated(key, 8, factory)).toBe(shortage === 0);
    expect(factory).toHaveBeenCalledTimes(shortage === 0 ? 1 : 0);
    const pin = cache.acquire<Uint8Array>(key);
    cache.delete(key);
    expect(pool.usedBytes).toBe(shortage === 0 ? 8 : 0);
    if (pin) expect(pin.value).toEqual(new Uint8Array(8));
    pin?.release();
    expect(pool.usedBytes).toBe(0);
  }
});

it("declines a key larger than capacity without allocating payload or evicting other keys", () => {
  const pool = new MemoryBudget(2048);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 2048 });
  cache.set("warm", 1, 8);
  const before = pool.usedBytes;
  const factory = vi.fn();
  expect(cache.setAllocated("x".repeat(2048), 0, factory)).toBe(false);
  expect(factory).not.toHaveBeenCalled();
  expect(cache.get("warm")?.value).toBe(1);
  expect(pool.usedBytes).toBe(before);
  cache.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(0);
});

it("keeps owned payload pins independent of entry metadata and consumer budgets", () => {
  const pool = new MemoryBudget(4096);
  const producer = new MemoryScope(pool.child(8));
  const owner = producer.allocate(8, "payload", () => new Uint8Array(8));
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 4096 });
  expect(cache.setOwned("owned", owner)).toBe(true);
  expect(pool.usedBytes).toBe(8 + sharedCacheMetadataBytes("owned"));
  const consumer = pool.child(8);
  const pin = cache.acquireOwned<Uint8Array>("owned", consumer);
  expect(pin?.bytes).toBe(8);
  expect(consumer.usedBytes).toBe(8);
  producer.dispose();
  cache.delete("owned");
  expect(pool.usedBytes).toBe(8);
  expect(pin?.value).toEqual(new Uint8Array(8));
  pin?.release();
  expect(pool.usedBytes).toBe(0);
});

it("bounds owned zero-byte entries and releases declined retention", () => {
  const pool = new MemoryBudget(2048);
  const scope = new MemoryScope(pool);
  const owner = scope.allocate(0, "empty payload", () => null);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 2048 });
  for (let index = 0; index < 100; index++)
    expect(cache.setOwned(`key-${index}`, owner)).toBe(true);
  expect(pool.usedBytes).toBeGreaterThan(0);
  expect(pool.usedBytes).toBeLessThanOrEqual(2048);
  expect(cache.setOwned("x".repeat(2048), owner)).toBe(false);
  scope.dispose();
  cache.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(0);
});

it("releases entry metadata on expiration and factory failure", () => {
  const pool = new MemoryBudget(4096);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 4096, ttlMs: 0 });
  cache.set("expired", null, 0);
  expect(pool.usedBytes).toBeGreaterThan(0);
  expect(cache.get("expired")).toBeUndefined();
  expect(pool.usedBytes).toBe(0);
  expect(() =>
    cache.setAllocated("failure", 8, () => {
      throw new Error("factory failed");
    }),
  ).toThrow("factory failed");
  expect(pool.usedBytes).toBe(0);
});

it("releases a reentrant same-key insertion when publishing the outer value", () => {
  const pool = new MemoryBudget(4096);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 4096 });
  expect(
    cache.setAllocated("key", 8, () => {
      cache.set("key", "inner", 16);
      return "outer";
    }),
  ).toBe(true);
  expect(cache.get("key")?.value).toBe("outer");
  expect(pool.usedBytes).toBe(8 + sharedCacheMetadataBytes("key"));
  cache.delete("key");
  expect(pool.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("releases temporary owned retention when metadata admission fails (throw=%s)", (throws) => {
  const pool = new MemoryBudget(1024);
  const scope = new MemoryScope(pool.child());
  const owner = scope.allocate(8, "payload", () => new Uint8Array(8));
  const sibling = pool.reserve(1016, "active sibling");
  const detach = pool.registerReclaimer(() => {
    if (throws) throw new Error("reclaimer failed");
  });
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 1024 });
  try {
    if (throws) expect(() => cache.setOwned("key", owner)).toThrow("reclaimer failed");
    else expect(cache.setOwned("key", owner)).toBe(false);
    expect(cache.get("key")).toBeUndefined();
  } finally {
    detach();
    scope.dispose();
    sibling.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("preserves protected entries when metadata admission would reclaim them (owned=%s)", (owned) => {
  const pool = new MemoryBudget(1024);
  const cache = new SharedMemoryCache({
    memoryBudget: pool,
    maxBytes: 8 + sharedCacheMetadataBytes("high"),
  });
  cache.set("high", "warm", 8, { priority: 4 });
  const scope = new MemoryScope(pool);
  const owner = scope.allocate(8, "new payload", () => "new");
  const factory = vi.fn(() => "new");
  expect(
    owned
      ? cache.setOwned("low", owner, { priority: 1 })
      : cache.setAllocated("low", 8, factory, { priority: 1 }),
  ).toBe(false);
  expect(factory).not.toHaveBeenCalled();
  expect(cache.get("high")?.value).toBe("warm");
  scope.dispose();
  cache.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(0);
});

it("rejects invalid cache bounds centrally and allows disabled retention", () => {
  for (const maxBytes of [-1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER + 1])
    expect(() => new SharedMemoryCache({ maxBytes })).toThrow(/maxBytes/);
  for (const ttlMs of [-1, NaN, Infinity])
    expect(() => new SharedMemoryCache({ ttlMs })).toThrow(/ttlMs/);
  const cache = new SharedMemoryCache({ maxBytes: 0 });
  const factory = vi.fn(() => null);
  expect(cache.setAllocated("empty", 0, factory)).toBe(false);
  expect(factory).not.toHaveBeenCalled();
});
