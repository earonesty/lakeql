import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { col } from "./expr.js";
import { jsonScalarKeyAllocationBytes } from "./json-scalar-key-memory.js";
import { MemoryBudget } from "./memory-budget.js";
import { createWindowAggregateAccumulator } from "./window-aggregate.js";

it.each([
  "mode",
  "count_distinct",
] as const)("admits %s keys before formatting and releases failures", (aggregate) => {
  const value = 'a\u0000"\\\ud800';
  const bytes =
    jsonScalarKeyAllocationBytes(value) +
    c.property +
    c.object +
    (aggregate === "mode" ? c.object + 2 * c.property : 0);
  for (const cap of [bytes - 1, bytes, 2 * bytes]) {
    const pool = new MemoryBudget(cap + 37);
    const sibling = pool.reserve(37, "sibling");
    const budget = pool.child(cap);
    const state = createWindowAggregateAccumulator(
      {
        fn: { aggregate },
        args: [col("value")],
        over: { partitionBy: [], orderBy: [] },
      },
      budget,
    );
    if (cap < bytes) {
      expect(() => state.append([{ value }])).toThrow();
      expect(budget.usedBytes).toBe(0);
    } else {
      expect(state.append([{ value }])).toBe(aggregate === "mode" ? value : 1);
      expect(budget.usedBytes).toBe(bytes);
      if (cap === bytes) {
        expect(() => state.append([{ value }])).toThrow();
        expect(budget.usedBytes).toBe(0);
      } else {
        expect(state.append([{ value }, { value }])).toBe(aggregate === "mode" ? value : 1);
        expect(budget.usedBytes).toBe(bytes);
      }
    }
    state.reset();
    expect(pool.usedBytes).toBe(37);
    sibling.release();
  }
});

it("denies timestamp formatting before invoking toJSON", () => {
  const budget = new MemoryBudget(0);
  let calls = 0;
  const value = {
    epochNanoseconds: 0n,
    unit: "nanos" as const,
    isAdjustedToUTC: true,
    toJSON() {
      calls++;
      return "timestamp";
    },
  };
  const state = createWindowAggregateAccumulator(
    {
      fn: { aggregate: "count_distinct" },
      args: [col("value")],
      over: { partitionBy: [], orderBy: [] },
    },
    budget,
  );
  expect(() => state.append([{ value }])).toThrow();
  expect(calls).toBe(0);
  expect(budget.usedBytes).toBe(0);
});

it("retains only actual key storage after numeric formatting and preserves JSON equality", () => {
  const budget = new MemoryBudget(4096);
  const state = createWindowAggregateAccumulator(
    {
      fn: { aggregate: "count_distinct" },
      args: [col("value")],
      over: { partitionBy: [], orderBy: [] },
    },
    budget,
  );
  expect(state.append([{ value: 123 }])).toBe(1);
  const retained = c.object + c.property + c.string + 3 * c.stringCodeUnit;
  expect(budget.usedBytes).toBe(retained);
  expect(state.append([{ value: 123n }, { value: 123 }])).toBe(1);
  expect(budget.usedBytes).toBe(retained);
  expect(state.append([{ value: "123" }])).toBe(2);
  state.reset();
  expect(budget.usedBytes).toBe(0);
});
