import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { memorySpillAdapter } from "./runtime.js";

it("resumes aggregate checkpoints through owned spill reads and releases checkpoint charges", async () => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        yield [{ value: 2 }, { value: 3 }];
      },
    },
  });
  const spill = memorySpillAdapter();
  const spec = { total: { op: "sum" as const, column: "value" } };
  const first = await lake
    .path("table")
    .run()
    .aggregateWithState([], spec, { spill, spillId: "state" });
  expect(first.rows).toEqual([{ total: 5 }]);
  expect(pool.usedBytes).toBe(0);
  spill.read = async () => {
    throw new Error("unowned read used");
  };
  const resumed = await lake
    .path("table")
    .run()
    .aggregateWithState([], spec, { spill, operatorState: { spillRef: "state" } });
  expect(resumed.rows).toEqual([{ total: 10 }]);
  expect(pool.usedBytes).toBe(0);
});

it("pins aggregate checkpoint bytes until a canceled write settles", async () => {
  const pool = new MemoryBudget(65536);
  const controller = new AbortController();
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { signal: controller.signal },
    scanner: {
      async *scan() {
        yield [{ value: 2 }];
      },
    },
  });
  const spill = memorySpillAdapter();
  const ready = Promise.withResolvers<void>();
  const gate = Promise.withResolvers<void>();
  spill.write = async (id, bytes) => {
    ready.resolve();
    await gate.promise;
    expect(pool.usedBytes).toBeGreaterThanOrEqual(bytes.byteLength);
    return { id, byteSize: bytes.byteLength };
  };
  const pending = lake
    .path("table")
    .run()
    .aggregateWithState([], { total: { op: "sum", column: "value" } }, { spill });
  const rejected = expect(pending).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  await ready.promise;
  controller.abort();
  expect(pool.usedBytes).toBeGreaterThan(0);
  gate.resolve();
  await rejected;
  expect(pool.usedBytes).toBe(0);
});

it("admits aggregate checkpoint overlap at exact capacity and cleans up below it", async () => {
  const run = async (pool: MemoryBudget) => {
    const store = memoryStore();
    await store.put("table", new Uint8Array(1));
    const lake = new Lake({
      store,
      memoryPool: pool,
      scanner: {
        async *scan() {
          yield [{ value: 2 }];
        },
      },
    });
    return lake
      .path("table")
      .run()
      .aggregateWithState([], { total: { op: "sum", column: "value" } });
  };
  const measured = new MemoryBudget(65536);
  await run(measured);
  for (const cap of [measured.peakBytes - 1, measured.peakBytes]) {
    const pool = new MemoryBudget(cap);
    if (cap < measured.peakBytes)
      await expect(run(pool)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    else expect((await run(pool)).rows).toEqual([{ total: 2 }]);
    expect(pool.usedBytes).toBe(0);
  }
});

it.each([
  "median",
  "quantile",
] as const)("%s rejects retained growth during scanning and releases partial state", async (op) => {
  const pool = new MemoryBudget(8192);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  let scanned = 0;
  let closed = false;
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        try {
          for (let value = 0; value < 10000; value++) {
            scanned++;
            yield [{ value }];
          }
        } finally {
          closed = true;
        }
      },
    },
  });
  await expect(
    lake
      .path("table")
      .run()
      .aggregateWithState([], {
        result: op === "median" ? { op, column: "value" } : { op, column: "value", quantile: 0.5 },
      }),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(scanned).toBeGreaterThan(0);
  expect(scanned).toBeLessThan(10000);
  expect(closed).toBe(true);
  expect(pool.usedBytes).toBe(0);
});
