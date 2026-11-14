import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

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
