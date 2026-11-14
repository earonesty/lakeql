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
  // This fixture exercises adapters without transactional writes.
  Object.defineProperty(spill, "stageWrite", { value: undefined });
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
  "mode",
  "count_distinct",
  "approx_count_distinct",
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
        result:
          op === "quantile" ? { op, column: "value", quantile: 0.5 } : { op, column: "value" },
      }),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(scanned).toBeGreaterThan(0);
  expect(scanned).toBeLessThan(10000);
  expect(closed).toBe(true);
  expect(pool.usedBytes).toBe(0);
});

it("does not grow retained mode storage for repeated escaped Unicode keys", async () => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const observations: number[] = [];
  const value = '雪\\"\ud800';
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        for (let index = 0; index < 100; index++) {
          yield [{ value }];
          observations.push(pool.usedBytes);
        }
      },
    },
  });
  const result = await lake
    .path("table")
    .run()
    .aggregateWithState([], { result: { op: "mode", column: "value" } });
  expect(result.rows).toEqual([{ result: value }]);
  expect(new Set(observations).size).toBe(1);
  expect(pool.usedBytes).toBe(0);
});

it("bounds high-cardinality group directories before scanning all rows", async () => {
  const pool = new MemoryBudget(16384);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  let scanned = 0;
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        for (let key = 0; key < 1000; key++) {
          scanned++;
          yield [{ key }];
        }
      },
    },
  });
  await expect(
    lake
      .path("table")
      .run()
      .aggregateWithState(["key"], { count: { op: "count" } }),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(scanned).toBeLessThan(1000);
  expect(pool.usedBytes).toBe(0);
});

it("detaches group key bytes before the scanner reuses their backing", async () => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        const key = new Uint8Array(1);
        for (const value of [1, 2, 1]) {
          key[0] = value;
          yield [{ key }];
        }
        key[0] = 99;
      },
    },
  });
  const result = await lake
    .path("table")
    .run()
    .aggregateWithState(["key"], { count: { op: "count" } });
  expect(result.rows).toEqual([
    { key: new Uint8Array([1]), count: 2 },
    { key: new Uint8Array([2]), count: 1 },
  ]);
  expect(pool.usedBytes).toBe(0);
});

it("detaches first, last and any values from reused scanner byte buffers", async () => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        const value = new Uint8Array(1);
        for (const byte of [1, 2, 3]) {
          value[0] = byte;
          yield [{ value }];
        }
        value[0] = 99;
      },
    },
  });
  const result = await lake
    .path("table")
    .run()
    .aggregateWithState([], {
      first: { op: "first", column: "value" },
      last: { op: "last", column: "value" },
      any: { op: "any", column: "value" },
    });
  expect(result.rows).toEqual([
    { first: new Uint8Array([1]), last: new Uint8Array([3]), any: new Uint8Array([1]) },
  ]);
  expect(pool.usedBytes).toBe(0);
});

it("admits aggregate result sorting and slicing with exact end-to-end capacity", async () => {
  const run = async (pool: MemoryBudget) => {
    const store = memoryStore();
    await store.put("table", new Uint8Array(1));
    const lake = new Lake({
      store,
      memoryPool: pool,
      scanner: {
        async *scan() {
          for (let key = 0; key < 4; key++) yield Array.from({ length: key + 1 }, () => ({ key }));
        },
      },
    });
    return lake
      .path("table")
      .run()
      .aggregateWithState(
        ["key"],
        { count: { op: "count" } },
        {
          orderBy: [{ column: "count", direction: "desc" }],
          offset: 1,
          limit: 2,
        },
      );
  };
  const measured = new MemoryBudget(65536);
  expect((await run(measured)).rows).toEqual([
    { key: 2, count: 3 },
    { key: 1, count: 2 },
  ]);
  for (const cap of [measured.peakBytes - 1, measured.peakBytes]) {
    const pool = new MemoryBudget(cap);
    if (cap < measured.peakBytes)
      await expect(run(pool)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    else
      expect((await run(pool)).rows).toEqual([
        { key: 2, count: 3 },
        { key: 1, count: 2 },
      ]);
    expect(pool.usedBytes).toBe(0);
  }
});

it.each([
  1e308,
  Infinity,
  '雪"\\\ud800',
])("measures fixed aggregate state directly with exact serialized bytes for %s", async (value) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const spec = {
    count: { op: "count" as const },
    min: { op: "min" as const, column: "value" },
    max: { op: "max" as const, column: "value" },
    ...(typeof value === "number"
      ? {
          sum: { op: "sum" as const, column: "value" },
          avg: { op: "avg" as const, column: "value" },
          variance: { op: "var_pop" as const, column: "value" },
        }
      : {}),
  };
  const run = (maxMemoryBytes?: number) =>
    new Lake({
      store,
      budget: { maxMemoryBytes, maxAllocatedMemoryBytes: 1024 * 1024 },
      scanner: {
        async *scan() {
          yield [{ value }];
        },
      },
    })
      .path("table")
      .run()
      .aggregateWithState([], spec);
  const baseline = await run();
  expect((await run(baseline.operatorState.byteLength)).operatorState).toEqual(
    baseline.operatorState,
  );
  await expect(run(baseline.operatorState.byteLength - 1)).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
    details: { metric: "operator memory bytes", actual: baseline.operatorState.byteLength },
  });
});

it.each([
  9, 10, 99, 100, 1000,
])("keeps mode serialized limits exact across count %s", async (count) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const run = (maxMemoryBytes?: number) =>
    new Lake({
      store,
      budget: { maxMemoryBytes, maxAllocatedMemoryBytes: 1024 * 1024 },
      scanner: {
        async *scan() {
          for (let i = 0; i < count; i++) yield [{ value: '雪"\\' }];
        },
      },
    })
      .path("table")
      .run()
      .aggregateWithState([], { mode: { op: "mode", column: "value" } });
  const baseline = await run();
  expect((await run(baseline.operatorState.byteLength)).operatorState).toEqual(
    baseline.operatorState,
  );
  await expect(run(baseline.operatorState.byteLength - 1)).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
    details: { metric: "operator memory bytes", actual: baseline.operatorState.byteLength },
  });
});
