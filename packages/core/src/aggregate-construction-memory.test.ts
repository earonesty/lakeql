import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import type { AggregateExpr } from "./query.js";
import { createVectorAggregateStates, snapshotVectorAggregateStates } from "./vector-aggregate.js";

const operations: AggregateExpr["op"][] = [
  "count",
  "sum",
  "avg",
  "min",
  "max",
  "first",
  "last",
  "any",
  "var_samp",
  "var_pop",
  "stddev_samp",
  "stddev_pop",
  "median",
  "quantile",
  "mode",
  "count_distinct",
  "approx_count_distinct",
];
it.each(
  operations,
)("admits %s construction exactly and rejects one byte under atomically", (op) => {
  const spec = { result: { op, column: "value", quantile: 0.5 } };
  const reference = snapshotVectorAggregateStates(createVectorAggregateStates(spec));
  const measured = new MemoryScope(new MemoryBudget(65536));
  const output = createVectorAggregateStates(spec, {}, measured);
  expect(snapshotVectorAggregateStates(output)).toEqual(reference);
  const peak = measured.budget.peakBytes;
  expect(peak).toBeGreaterThan(0);
  measured.dispose();
  for (const limit of [peak, peak - 1]) {
    const pool = new MemoryBudget(peak);
    const scope = new MemoryScope(pool.child(limit));
    if (limit === peak) {
      const states = createVectorAggregateStates(spec, {}, scope);
      const retained = scope.retainAllocations(states);
      scope.dispose();
      expect(snapshotVectorAggregateStates(retained.value)).toEqual(reference);
      expect(pool.usedBytes).toBe(peak);
      retained.release();
    } else {
      expect(() => createVectorAggregateStates(spec, {}, scope)).toThrow(/budget/);
      expect(pool.usedBytes).toBe(0);
    }
    scope.dispose();
    expect(pool.usedBytes).toBe(0);
  }
});

it("rolls back a partially constructed directory without releasing caller backing", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  scope.allocate(7, "caller", () => new Uint8Array(7));
  expect(() =>
    createVectorAggregateStates(
      {
        a: { op: "sum", column: "v" },
        b: { op: "quantile", column: "v", quantile: 2 },
      },
      {},
      scope,
    ),
  ).toThrow();
  expect(scope.budget.usedBytes).toBe(7);
  expect(() =>
    createVectorAggregateStates(
      { a: { op: "sum" }, b: { op: "count_distinct" } },
      { budget: { maxMemoryBytes: 1 } },
      scope,
    ),
  ).toThrow(/budget/);
  expect(scope.budget.usedBytes).toBe(7);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
