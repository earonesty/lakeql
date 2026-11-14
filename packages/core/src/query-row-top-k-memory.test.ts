import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { memorySpillAdapter } from "./runtime.js";

async function fixture(pool: MemoryBudget, cap = pool.maxBytes) {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  let closed = false;
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { maxAllocatedMemoryBytes: cap },
    scanner: {
      async *scan() {
        const row = { score: 0, nested: { text: "" }, bytes: new Uint8Array(1) };
        try {
          for (const score of [3, 1, 2]) {
            row.score = score;
            row.nested.text = `row-${score}`;
            row.bytes[0] = score;
            yield [row];
          }
        } finally {
          row.score = 99;
          row.nested.text = "invalidated";
          row.bytes[0] = 99;
          closed = true;
        }
      },
    },
  });
  return { lake, closed: () => closed };
}

it("detaches retained scanner rows and releases winner ownership after early consumer exit", async () => {
  const pool = new MemoryBudget(65536);
  const { lake, closed } = await fixture(pool);
  const iterator = lake
    .path("table")
    .orderBy([{ column: "score" }])
    .limit(2)
    .batchSize(1)
    .run()
    .batches()
    [Symbol.asyncIterator]();
  const first = await iterator.next();
  expect(first.value).toEqual([
    { score: 1, nested: { text: "row-1" }, bytes: new Uint8Array([1]) },
  ]);
  expect(closed()).toBe(true);
  expect(pool.usedBytes).toBeGreaterThan(0);
  await iterator.return?.();
  expect(pool.usedBytes).toBe(0);
});

it("releases retained candidates when checkpoint serialization rejects nested values", async () => {
  const pool = new MemoryBudget(65536);
  const { lake } = await fixture(pool);
  await expect(
    lake
      .path("table")
      .orderBy([{ column: "score" }])
      .limit(2)
      .topKWithState(),
  ).rejects.toThrow(/JSON scalars/);
  expect(pool.usedBytes).toBe(0);
});

it("detaches reused scalar rows through resumable checkpoint creation", async () => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        const row = { score: 0 };
        for (const score of [3, 1, 2]) {
          row.score = score;
          yield [row];
        }
        row.score = 99;
      },
    },
  });
  const result = await lake
    .path("table")
    .orderBy([{ column: "score" }])
    .limit(2)
    .topKWithState();
  expect(result.rows).toEqual([{ score: 1 }, { score: 2 }]);
  expect(result.operatorState.byteLength).toBeGreaterThan(0);
  expect(pool.usedBytes).toBe(0);
});

it("unwinds scanner and selection ownership on candidate admission failure", async () => {
  const pool = new MemoryBudget(65536);
  const sibling = pool.reserve(37, "sibling");
  const { lake, closed } = await fixture(pool, 1000);
  await expect(
    lake
      .path("table")
      .orderBy([{ column: "score" }])
      .limit(2)
      .toArray(),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(closed()).toBe(true);
  expect(pool.usedBytes).toBe(37);
  sibling.release();
});

it("skips detaching oversized rejected payloads but admits better replacements", async () => {
  const pool = new MemoryBudget(8192);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        yield [{ score: 1, payload: new Uint8Array([1]) }];
        yield [{ score: 1, payload: new Uint8Array(65536) }];
        yield [{ score: 2, payload: new Uint8Array(65536) }];
        yield [{ score: 0, payload: new Uint8Array([0]) }];
      },
    },
  });
  expect(
    await lake
      .path("table")
      .orderBy([{ column: "score" }])
      .limit(1)
      .toArray(),
  ).toEqual([{ score: 0, payload: new Uint8Array([0]) }]);
  expect(pool.usedBytes).toBe(0);
});

it("owns unlimited ordered rows across scanner reuse and releases on completion", async () => {
  const pool = new MemoryBudget(65536);
  const { lake } = await fixture(pool);
  const rows = await lake
    .path("table")
    .orderBy([{ column: "score" }])
    .batchSize(1)
    .toArray();
  expect(rows.map((row) => row.score)).toEqual([1, 2, 3]);
  expect(rows.map((row) => row.nested)).toEqual([
    { text: "row-1" },
    { text: "row-2" },
    { text: "row-3" },
  ]);
  expect(pool.usedBytes).toBe(0);
});

it("enforces the measured unlimited-ordering peak at exact and one-byte-short caps", async () => {
  const measured = new MemoryBudget(65536);
  const baseline = await fixture(measured);
  await baseline.lake
    .path("table")
    .orderBy([{ column: "score" }])
    .toArray();
  const required = measured.peakBytes;
  for (const cap of [required - 1, required]) {
    const pool = new MemoryBudget(cap + 37);
    const sibling = pool.reserve(37, "sibling");
    const candidate = await fixture(pool, cap);
    const run = candidate.lake
      .path("table")
      .orderBy([{ column: "score" }])
      .toArray();
    if (cap < required) await expect(run).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    else expect((await run).map((row) => row.score)).toEqual([1, 2, 3]);
    expect(candidate.closed()).toBe(true);
    expect(pool.usedBytes).toBe(37);
    sibling.release();
  }
});

it("preserves tied encounter order across stateful sort run boundaries", async () => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  await store.put("next", new Uint8Array(1));
  const rows = Array.from({ length: 12 }, (_, id) => ({ id, key: 1 }));
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { maxBufferedRows: 2 },
    scanner: {
      async *scan(path) {
        yield path === "table"
          ? rows
          : [
              { id: 12, key: 1 },
              { id: 13, key: 1 },
            ];
      },
    },
  });
  const result = await lake
    .path("table")
    .orderBy([{ column: "key" }])
    .offset(1)
    .limit(8)
    .sortWithState();
  expect(result.rows.map((row) => row.id)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
  const resumed = await lake
    .path("next")
    .orderBy([{ column: "key" }])
    .sortWithState({ operatorState: result.operatorState });
  expect(resumed.rows.map((row) => row.id)).toEqual(Array.from({ length: 14 }, (_, id) => id));
});

it("detaches reused rows across stateful run flushes and releases on return", async () => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { maxBufferedRows: 1 },
    scanner: {
      async *scan() {
        const row = { score: 0 };
        for (const score of [3, 1, 2]) {
          row.score = score;
          yield [row];
        }
        row.score = 99;
      },
    },
  });
  const result = await lake
    .path("table")
    .orderBy([{ column: "score" }])
    .sortWithState();
  expect(result.rows).toEqual([{ score: 1 }, { score: 2 }, { score: 3 }]);
  expect(pool.peakBytes).toBeGreaterThan(0);
  expect(pool.usedBytes).toBe(0);
});

it("keeps sort rows owned during asynchronous spill and releases them on rejection", async () => {
  const pool = new MemoryBudget(65536);
  const sibling = pool.reserve(37, "sibling");
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { maxBufferedRows: 1 },
    scanner: {
      async *scan() {
        yield [{ id: 2 }, { id: 1 }];
      },
    },
  });
  const spill = memorySpillAdapter();
  let writes = 0;
  spill.write = async () => {
    writes++;
    await Promise.resolve();
    expect(pool.usedBytes).toBeGreaterThan(37);
    throw Error("spill failed");
  };
  await expect(
    lake
      .path("table")
      .orderBy([{ column: "id" }])
      .sortWithState({ spill }),
  ).rejects.toThrow("spill failed");
  expect(writes).toBe(1);
  expect(pool.usedBytes).toBe(37);
  sibling.release();
});
