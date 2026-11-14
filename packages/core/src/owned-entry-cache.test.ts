import { expect, it, vi } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { memoryPlanningCache } from "./owned-entry-cache.js";
import { Lake } from "./query.js";

it("bounds LRU entries while preserving pinned metadata through eviction", async () => {
  const pool = new MemoryBudget(65536);
  const cache = memoryPlanningCache({ memoryBudget: pool, maxBytes: 1500 });
  await cache.set("a", {
    value: [{ path: "a", size: 1, rangeReadMode: "native", lastModified: new Date(10) }],
  });
  const pin = await cache.getOwnedEntry("a", pool);
  expect(pin).toBeDefined();
  await cache.set("b", { value: [{ path: "b", size: 2 }] });
  expect(await cache.getOwnedEntry("a", pool)).toBeUndefined();
  expect(pin?.value.value[0]).toMatchObject({
    path: "a",
    rangeReadMode: "native",
    lastModified: new Date(10),
  });
  cache.clear();
  expect(pool.usedBytes).toBe(pin?.bytes);
  pin?.release();
  expect(pool.usedBytes).toBe(0);
});

it("copies legacy reads and writes and expires explicit entries", async () => {
  const pool = new MemoryBudget(65536);
  const cache = memoryPlanningCache({ memoryBudget: pool });
  const date = new Date(10);
  const input = [{ path: "a", size: 1, lastModified: date }];
  await cache.set("a", { value: input, expiresAt: Date.now() + 10000 });
  if (input[0]) input[0].path = "changed";
  date.setTime(20);
  const read = await cache.get("a");
  expect(read?.value[0]).toMatchObject({ path: "a", lastModified: new Date(10) });
  if (read?.value[0]) read.value[0].path = "another change";
  expect((await cache.get("a"))?.value[0]?.path).toBe("a");
  const now = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 20000);
  try {
    expect(await cache.getOwnedEntry("a", pool)).toBeUndefined();
  } finally {
    now.mockRestore();
  }
  expect(pool.usedBytes).toBe(0);
});

it("evicts unpinned cache entries when sibling allocations need the shared pool", async () => {
  const pool = new MemoryBudget(4096);
  const cache = memoryPlanningCache({ memoryBudget: pool });
  await cache.set("a", { value: [{ path: "a", size: 1 }] });
  expect(pool.usedBytes).toBeGreaterThan(0);
  const sibling = pool.reserve(4096, "sibling");
  expect(await cache.getOwnedEntry("a", pool)).toBeUndefined();
  sibling.release();
  expect(pool.usedBytes).toBe(0);
});

it("does not serve a stale entry when replacement exceeds cache capacity", async () => {
  const pool = new MemoryBudget(65536);
  const cache = memoryPlanningCache({ memoryBudget: pool, maxBytes: 1500 });
  await cache.set("a", { value: [{ path: "a", size: 1 }] });
  await cache.set("a", { value: [{ path: "x".repeat(2000), size: 1 }] });
  expect(await cache.get("a")).toBeUndefined();
  expect(pool.usedBytes).toBe(0);
});

it("reuses bounded discovery cache entries across actual query executions", async () => {
  const pool = new MemoryBudget(65536);
  const cache = memoryPlanningCache({ memoryBudget: pool, maxBytes: 4096 });
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    planningCache: cache,
    scanner: {
      async *scan() {
        yield [{ id: 1 }];
      },
    },
  });
  expect(await lake.path("table").toArray()).toEqual([{ id: 1 }]);
  await store.delete("table");
  expect(await lake.path("table").toArray()).toEqual([{ id: 1 }]);
  expect(pool.usedBytes).toBeGreaterThan(0);
  cache.clear();
  expect(pool.usedBytes).toBe(0);
});

it("validates cache limits and supports disabled retention", async () => {
  const pool = new MemoryBudget(65536);
  for (const maxBytes of [-1, NaN, Infinity, 0.5])
    expect(() => memoryPlanningCache({ memoryBudget: pool, maxBytes })).toThrow(/maxBytes/);
  for (const ttlMs of [-1, NaN, Infinity])
    expect(() => memoryPlanningCache({ memoryBudget: pool, ttlMs })).toThrow(/ttlMs/);
  const cache = memoryPlanningCache({ memoryBudget: pool, maxBytes: 0 });
  await cache.set("a", { value: [{ path: "a", size: 1 }] });
  expect(await cache.get("a")).toBeUndefined();
  expect(pool.usedBytes).toBe(0);
});

it("enforces exact cache capacity including keys and directory records", async () => {
  const pool = new MemoryBudget(65536);
  const probe = memoryPlanningCache({ memoryBudget: pool });
  await probe.set("a", { value: [{ path: "a", size: 1 }] });
  const pin = await probe.getOwnedEntry("a", pool);
  expect(pin).toBeDefined();
  const bytes = pin?.bytes ?? 0;
  pin?.release();
  probe.clear();
  for (const maxBytes of [bytes - 1, bytes]) {
    const cache = memoryPlanningCache({ memoryBudget: pool, maxBytes });
    await cache.set("a", { value: [{ path: "a", size: 1 }] });
    const owner = await cache.getOwnedEntry("a", pool);
    expect(owner !== undefined).toBe(maxBytes === bytes);
    owner?.release();
    cache.clear();
    expect(pool.usedBytes).toBe(0);
  }
});

it("treats cache entries too large for a child pin as misses", async () => {
  const pool = new MemoryBudget(65536);
  const cache = memoryPlanningCache({ memoryBudget: pool });
  await cache.set("a", { value: [{ path: "a", size: 1 }] });
  const retained = pool.usedBytes;
  expect(retained).toBeGreaterThan(0);
  const child = pool.child(retained - 1);
  expect(await cache.getOwnedEntry("a", child)).toBeUndefined();
  expect(child.usedBytes).toBe(0);
  cache.clear();
  expect(pool.usedBytes).toBe(0);
});

it("does not invalidate compatible entries when a different pool attempts insertion", async () => {
  const pool = new MemoryBudget(65536);
  const cache = memoryPlanningCache({ memoryBudget: pool });
  await cache.set("a", { value: [{ path: "old", size: 1 }] });
  const retained = pool.usedBytes;
  const foreign = new MemoryScope(new MemoryBudget(65536));
  const owner = foreign.allocate(4096, "foreign cache value", () => ({
    value: [{ path: "new", size: 2 }],
  }));
  await cache.setOwnedEntry("a", owner);
  const result = await cache.getOwnedEntry("a", pool);
  expect(result?.value.value[0]?.path).toBe("old");
  result?.release();
  expect(pool.usedBytes).toBe(retained);
  foreign.dispose();
  cache.clear();
  expect(foreign.budget.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(0);
});
