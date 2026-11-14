import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { type AggregateOptions, Lake } from "./query.js";
import { memorySpillAdapter } from "./runtime.js";

const rows = Array.from({ length: 200 }, (_, i) => ({ key: i % 100, value: i }));
const spec = { n: { op: "count" as const }, median: { op: "median" as const, column: "value" } };
const expected = Array.from({ length: 100 }, (_, key) => ({ key, n: 2, median: key + 50 }));
async function query(maxBytes: number) {
  const pool = new MemoryBudget(maxBytes);
  const store = memoryStore();
  await store.put("rows", new Uint8Array([1]));
  const lake = new Lake({
    memoryPool: pool,
    store,
    scanner: {
      async *scan() {
        for (let i = 0; i < rows.length; i += 25) yield rows.slice(i, i + 25);
      },
    },
  });
  return { pool, result: lake.path("rows").run() };
}

it("finishes row aggregates within their output budget without a discarded checkpoint", async () => {
  const plain = await query(16 * 1024 * 1024);
  expect(await plain.result.aggregate(["key"], spec)).toEqual(expected);
  const peak = plain.pool.peakBytes;
  expect(plain.pool.usedBytes).toBe(0);
  const exact = await query(peak);
  expect(await exact.result.aggregate(["key"], spec)).toEqual(expected);
  expect(exact.pool.usedBytes).toBe(0);
  const checkpoint = await query(peak);
  await expect(checkpoint.result.aggregateWithState(["key"], spec)).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(checkpoint.pool.usedBytes).toBe(0);
});

it("restores row state without requiring another checkpoint, and preserves explicit spill writes", async () => {
  const initial = await query(16 * 1024 * 1024);
  const state = await initial.result.aggregateWithState(["key"], spec);
  expect(state.rows).toEqual(expected);
  expect(initial.pool.usedBytes).toBe(0);
  const resumed = await query(16 * 1024 * 1024);
  expect(
    await resumed.result.aggregate(["key"], spec, {
      operatorState: state.operatorState,
      orderBy: [{ column: "key" }],
    }),
  ).toEqual(expected.map((row) => ({ ...row, n: 4 })));
  expect(resumed.pool.usedBytes).toBe(0);
  const spill = memorySpillAdapter();
  const explicit = await query(16 * 1024 * 1024);
  const options: AggregateOptions = { spill, spillId: "explicit" };
  expect(await explicit.result.aggregate(["key"], spec, options)).toEqual(expected);
  expect(await spill.read("explicit")).toEqual(state.operatorState);
  expect(explicit.pool.usedBytes).toBe(0);
});
