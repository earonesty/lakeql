import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { ObjectStoreJsonCache } from "./object-store-json-cache.js";
import type { ObjectStore } from "./store.js";

it.each([
  true,
  false,
])("admits JSON cache read/parse overlap with owned transport %s", async (owned) => {
  const backing = memoryStore();
  const store: ObjectStore = owned
    ? backing
    : {
        get: backing.get.bind(backing),
        getRange: backing.getRange.bind(backing),
        head: backing.head.bind(backing),
        list: backing.list.bind(backing),
        put: backing.put.bind(backing),
        delete: backing.delete.bind(backing),
      };
  const cache = new ObjectStoreJsonCache<{ text: string }>({ store, prefix: "cache" });
  await cache.set("snow/雪", { value: { text: "雪😀".repeat(100) } });
  const pool = new MemoryBudget(65536);
  const result = await cache.getOwnedEntry("snow/雪", pool);
  expect(result?.value).toEqual({ value: { text: "雪😀".repeat(100) } });
  const peak = pool.peakBytes;
  expect(pool.usedBytes).toBe(result?.bytes);
  expect(peak).toBeGreaterThan(pool.usedBytes);
  result?.release();
  expect(pool.usedBytes).toBe(0);
  for (const capacity of [peak - 1, peak]) {
    const target = new MemoryBudget(capacity);
    if (capacity < peak)
      await expect(cache.getOwnedEntry("snow/雪", target)).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
      });
    else (await cache.getOwnedEntry("snow/雪", target))?.release();
    expect(target.usedBytes).toBe(0);
  }
});

it("releases cache reads on missing, malformed, invalid and expired entries", async () => {
  const store = memoryStore();
  const cache = new ObjectStoreJsonCache({ store, prefix: "cache", now: () => 100 });
  const pool = new MemoryBudget(65536);
  expect(await cache.getOwnedEntry("missing", pool)).toBeUndefined();
  for (const text of ["{", "null", "[]", "{}"]) {
    await store.put("cache/key.json", new TextEncoder().encode(text));
    await expect(cache.getOwnedEntry("key", pool)).rejects.toMatchObject({
      code: "LAKEQL_CATALOG_ERROR",
    });
    expect(pool.usedBytes).toBe(0);
  }
  await cache.set("key", { value: [1, 2], expiresAt: 99 });
  expect(await cache.getOwnedEntry("key", pool)).toBeUndefined();
  expect(await store.head("cache/key.json")).toBeNull();
  expect(pool.usedBytes).toBe(0);
});

it("rejects changed read extents before parsing and releases transport ownership", async () => {
  const store = memoryStore();
  const read = store.getRangeOwned.bind(store);
  store.getRangeOwned = (path, range, scope) =>
    read(path, { ...range, length: range.length - 1 }, scope);
  const cache = new ObjectStoreJsonCache({ store, prefix: "cache" });
  await cache.set("key", { value: 1 });
  const pool = new MemoryBudget(65536);
  await expect(cache.getOwnedEntry("key", pool)).rejects.toThrow(/extent changed/);
  expect(pool.usedBytes).toBe(0);
});
