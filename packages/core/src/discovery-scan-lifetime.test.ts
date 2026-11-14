import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { col, gt } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

it.each([
  "ordered",
  "deferred",
  "window",
] as const)("retains %s discovery metadata until cancelled scans settle", async (route) => {
  const path = "x".repeat(4096);
  const store = memoryStore();
  await store.put(path, new Uint8Array(1));
  const pool = new MemoryBudget(1 << 20);
  const controller = new AbortController();
  let enter!: () => void, finish!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let closed = false;
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { signal: controller.signal },
    scanner:
      route !== "deferred"
        ? {
            async *scan(_path, options) {
              try {
                enter();
                await pending;
                expect(options.object?.path).toBe(path);
                expect(pool.usedBytes).toBeGreaterThanOrEqual(path.length * 2);
                yield [{ id: 1, value: 7 }];
              } finally {
                closed = true;
              }
            },
          }
        : {
            scan() {
              throw Error("unexpected row scan");
            },
            async *scanVectorBatches(_path, options) {
              try {
                if (options.columns?.includes("value")) {
                  enter();
                  await pending;
                  expect(options.object?.path).toBe(path);
                  expect(pool.usedBytes).toBeGreaterThanOrEqual(path.length * 2);
                  yield { rowOffset: 0, batch: batchFromColumns({ value: [7] }) };
                } else yield { rowOffset: 0, batch: batchFromColumns({ id: [1] }) };
              } finally {
                closed = true;
              }
            },
          },
  });
  const query =
    route === "ordered"
      ? lake
          .path(path)
          .orderBy([{ column: "id" }])
          .limit(1)
      : route === "window"
        ? lake.path(path).window("rn", {
            fn: "row_number",
            args: [],
            over: { partitionBy: [col("id")], orderBy: [] },
          })
        : lake.path(path).where(gt("id", 0)).select(["id", "value"]).limit(1);
  const work = query.toArray();
  const checked = expect(work).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  await entered;
  controller.abort();
  expect(pool.usedBytes).toBeGreaterThanOrEqual(path.length * 2);
  finish();
  await checked;
  expect(closed).toBe(true);
  expect(pool.usedBytes).toBe(0);
});
