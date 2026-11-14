import { expect, it } from "vitest";
import { col, eq } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

it("owns detached task metadata, shares common plans, and respects exact admission boundaries", async () => {
  const run = async (capacity: number) => {
    const store = memoryStore();
    await store.put("lake/tenant=a/one", new Uint8Array(1));
    await store.put("lake/tenant=b/two", new Uint8Array(1));
    const pool = new MemoryBudget(capacity);
    const binary = new Uint8Array([7]);
    const ranges = [{ start: 2, end: 5 }];
    const lake = new Lake({
      store,
      memoryPool: pool,
      scanner: {
        async planTask(_path, options) {
          expect(options.columns).toEqual(["id", "payload"]);
          expect(pool.usedBytes).toBeGreaterThan(0);
          return { rowGroupRanges: ranges, rowGroupCount: 1 };
        },
        scan() {
          throw Error("unexpected scan");
        },
      },
    });
    try {
      const owner = await lake
        .hive("lake/**/*")
        .select(["tenant", "id"])
        .where(eq(col("payload"), binary))
        .planOwnedTasks();
      try {
        expect(owner.value).toHaveLength(2);
        const [first, second] = owner.value;
        expect(first?.projectedColumns).toBe(second?.projectedColumns);
        expect(first?.residualPredicate).toBe(second?.residualPredicate);
        const range = ranges[0];
        if (range) range.end = 99;
        binary[0] = 9;
        expect(first?.rowGroupRanges).toEqual([{ start: 2, end: 5 }]);
        expect(first?.residualPredicate).toMatchObject({ right: { value: new Uint8Array([7]) } });
        expect(pool.usedBytes).toBeGreaterThan(0);
        return pool.peakBytes;
      } finally {
        owner.release();
      }
    } finally {
      expect(pool.usedBytes).toBe(0);
    }
  };
  const peak = await run(65536);
  expect(await run(peak)).toBe(peak);
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it.each([
  "success",
  "failure",
  "cancel",
] as const)("keeps task arguments charged until planner %s settles", async (outcome) => {
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
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { signal: controller.signal },
    scanner: {
      async planTask(_path, options) {
        enter();
        await pending;
        expect(options.columns).toEqual(["id"]);
        expect(pool.usedBytes).toBeGreaterThan(0);
        if (outcome === "failure") throw Error("planner failed");
        return { rowGroupRanges: [] };
      },
      scan() {
        throw Error("unexpected scan");
      },
    },
  });
  const work = lake.path("table").select(["id"]).planOwnedTasks();
  const checked =
    outcome === "success"
      ? work.then((owner) => owner.release())
      : expect(work).rejects.toMatchObject(
          outcome === "failure" ? { message: "planner failed" } : { code: "LAKEQL_ABORTED" },
        );
  await entered;
  if (outcome === "cancel") controller.abort();
  expect(pool.usedBytes).toBeGreaterThan(0);
  finish();
  await checked;
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "success",
  "failure",
  "cancel",
] as const)("holds task ownership through window executor %s", async (outcome) => {
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
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { signal: controller.signal },
    scanner: {
      scan() {
        throw Error("unexpected scan");
      },
      async executeWindowTasks(tasks) {
        expect(tasks[0]?.window?.available).toBe(true);
        expect(pool.usedBytes).toBeGreaterThan(0);
        enter();
        await pending;
        expect(tasks[0]?.projectedColumns).toContain("tenant");
        expect(pool.usedBytes).toBeGreaterThan(0);
        if (outcome === "failure") throw Error("executor failed");
        return [];
      },
    },
  });
  const work = lake
    .path("table")
    .select(["tenant", "rn"])
    .window("rn", {
      fn: "row_number",
      args: [],
      over: { partitionBy: [col("tenant")], orderBy: [] },
    })
    .toArray();
  const checked =
    outcome === "success"
      ? expect(work).resolves.toEqual([])
      : expect(work).rejects.toMatchObject(
          outcome === "failure" ? { message: "executor failed" } : { code: "LAKEQL_ABORTED" },
        );
  await entered;
  if (outcome === "cancel") controller.abort();
  expect(pool.usedBytes).toBeGreaterThan(0);
  finish();
  await checked;
  expect(pool.usedBytes).toBe(0);
});
