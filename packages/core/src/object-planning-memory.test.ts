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
