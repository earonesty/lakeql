import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import type { CacheAdapter, CacheEntry } from "./runtime.js";
import type { ObjectInfo } from "./store.js";

it("retains owned discovery cache entries across queries and releases on eviction", async () => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  let retained: OwnedMemory<CacheEntry<ObjectInfo[]>> | undefined;
  let hits = 0;
  const cache: CacheAdapter<ObjectInfo[]> = {
    async get() {
      throw Error("legacy get");
    },
    async set() {
      throw Error("legacy set");
    },
    async delete() {
      retained?.release();
      retained = undefined;
    },
    async getOwnedEntry(_key, budget) {
      if (retained === undefined) return undefined;
      hits++;
      return retained.retainIn(budget);
    },
    async setOwnedEntry(_key, owner) {
      retained = owner.retainIn(pool);
    },
  };
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
  const cachedBytes = pool.usedBytes;
  expect(cachedBytes).toBeGreaterThan(0);
  await store.delete("table");
  expect(await lake.path("table").toArray()).toEqual([{ id: 1 }]);
  expect(hits).toBe(1);
  expect(pool.usedBytes).toBe(cachedBytes);
  await cache.delete("object-plan:table");
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "cancel",
  "foreign",
  "limit",
] as const)("releases returned cache ownership on %s", async (outcome) => {
  const pool = new MemoryBudget(65536);
  const foreign = new MemoryBudget(65536);
  const controller = new AbortController();
  const cache: CacheAdapter<ObjectInfo[]> = {
    async get() {
      throw Error("legacy get");
    },
    async set() {},
    async delete() {},
    async getOwnedEntry(_key, budget) {
      const scope = new MemoryScope(outcome === "foreign" ? foreign : budget);
      try {
        const owner = scope
          .allocate(4096, "cache entry", () => ({ value: [{ path: "table", size: 1 }] }))
          .retain();
        if (outcome === "cancel") controller.abort();
        return owner;
      } finally {
        scope.dispose();
      }
    },
  };
  let scanned = false;
  const lake = new Lake({
    store: memoryStore(),
    memoryPool: pool,
    planningCache: cache,
    budget: { signal: controller.signal, ...(outcome === "limit" ? { maxFiles: 0 } : {}) },
    scanner: {
      async *scan() {
        scanned = true;
        yield [];
      },
    },
  });
  await expect(lake.path("table").toArray()).rejects.toMatchObject({
    code:
      outcome === "cancel"
        ? "LAKEQL_ABORTED"
        : outcome === "limit"
          ? "LAKEQL_BUDGET_EXCEEDED"
          : "LAKEQL_VALIDATION_ERROR",
  });
  expect(scanned).toBe(false);
  expect(pool.usedBytes).toBe(0);
  expect(foreign.usedBytes).toBe(0);
});

it.each([
  "success",
  "failure",
  "cancel",
] as const)("holds borrowed cache writes until %s settles", async (outcome) => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const controller = new AbortController();
  let enter!: () => void, finish!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const cache: CacheAdapter<ObjectInfo[]> = {
    async get() {
      return undefined;
    },
    async set() {
      throw Error("legacy set");
    },
    async delete() {},
    async setOwnedEntry(_key, owner) {
      enter();
      await pending;
      expect(owner.value.value[0]?.path).toBe("table");
      expect(pool.usedBytes).toBeGreaterThan(0);
      if (outcome === "failure") throw Error("cache failed");
    },
  };
  const lake = new Lake({
    store,
    memoryPool: pool,
    planningCache: cache,
    budget: { signal: controller.signal },
    scanner: {
      async *scan() {
        yield [{ id: 1 }];
      },
    },
  });
  const work = lake.path("table").toArray();
  const checked =
    outcome === "success"
      ? expect(work).resolves.toEqual([{ id: 1 }])
      : expect(work).rejects.toThrow(outcome === "failure" ? "cache failed" : /abort/i);
  await entered;
  if (outcome === "cancel") controller.abort();
  expect(pool.usedBytes).toBeGreaterThan(0);
  finish();
  await checked;
  expect(pool.usedBytes).toBe(0);
});
