import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

it.each([
  "success",
  "failure",
  "cancel",
] as const)("keeps object-planning arguments admitted until callback %s settles", async (outcome) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(65536);
  const controller = new AbortController();
  let enter!: () => void, finish!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let scanned = false;
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { signal: controller.signal },
    scanner: {
      async planObjects(objects, options) {
        expect(options.columns).toEqual(["id"]);
        expect(pool.usedBytes).toBeGreaterThan(0);
        enter();
        await pending;
        expect(options.columns).toEqual(["id"]);
        expect(pool.usedBytes).toBeGreaterThan(0);
        if (outcome === "failure") throw Error("planner failed");
        return objects;
      },
      async *scan() {
        scanned = true;
        yield [{ id: 1 }];
      },
    },
  });
  const work = lake.path("table").select(["id"]).toArray();
  const checked =
    outcome === "success"
      ? expect(work).resolves.toEqual([{ id: 1 }])
      : outcome === "failure"
        ? expect(work).rejects.toThrow("planner failed")
        : expect(work).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  await entered;
  if (outcome === "cancel") controller.abort();
  expect(pool.usedBytes).toBeGreaterThan(0);
  finish();
  await checked;
  expect(scanned).toBe(outcome === "success");
  expect(pool.usedBytes).toBe(0);
});

it("rejects oversized object-plan columns before invoking the planner", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(1024);
  let planned = false;
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async planObjects(objects) {
        planned = true;
        return objects;
      },
      scan() {
        throw Error("Unexpected scan");
      },
    },
  });
  await expect(
    lake
      .path("table")
      .select(["列".repeat(1000)])
      .toArray(),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(planned).toBe(false);
  expect(pool.usedBytes).toBe(0);
});

it("rejects discovery list growth during iteration and closes the listing", async () => {
  const store = memoryStore();
  const pool = new MemoryBudget(1600);
  let visited = 0,
    closed = false,
    planned = false;
  store.list = async function* () {
    try {
      for (let index = 0; index < 100; index++) {
        if (index > 0) expect(pool.usedBytes).toBeGreaterThan(0);
        visited++;
        yield { path: `data/${index}.parquet`, size: 1 };
      }
    } finally {
      closed = true;
    }
  };
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async planObjects(objects) {
        planned = true;
        return objects;
      },
      scan() {
        throw Error("unexpected scan");
      },
    },
  });
  await expect(lake.path("data/*.parquet").planTasks()).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(visited).toBeLessThan(100);
  expect(closed).toBe(true);
  expect(planned).toBe(false);
  expect(pool.usedBytes).toBe(0);
});

it("keeps discovered metadata charged across delayed row scans", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(65536);
  let observed = 0;
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan(_path, options) {
        observed = pool.usedBytes;
        expect(observed).toBeGreaterThan(0);
        await Promise.resolve();
        expect(options.object?.path).toBe("table");
        expect(pool.usedBytes).toBeGreaterThanOrEqual(observed);
        yield [{ id: 1 }];
      },
    },
  });
  expect(await lake.path("table").toArray()).toEqual([{ id: 1 }]);
  expect(pool.usedBytes).toBe(0);
});
