import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as charges } from "./allocation-charges.js";
import { col, lit } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import type { WindowExpr } from "./window.js";
import {
  createWindowAggregateAccumulator,
  createWindowAggregateEvaluator,
} from "./window-aggregate.js";

const expression = (q: number): WindowExpr => ({
  fn: { aggregate: "quantile" },
  args: [col("value"), lit(q)],
  over: { partitionBy: [], orderBy: [] },
});

it("admits numeric state at the exact shared cap and drops it before releasing", () => {
  const values = [8, -3, 2, 2, 100, 0, -7];
  const required = charges.array + values.length * charges.arraySlot;
  for (const q of [0, 0.25, 0.5, 0.9, 1]) {
    for (const cap of [required - 1, required]) {
      const pool = new MemoryBudget(cap + 37);
      const sibling = pool.reserve(37, "sibling");
      const budget = pool.child(cap);
      const evaluate = createWindowAggregateEvaluator(expression(q), budget);
      const sorted = [...values].sort((a, b) => a - b);
      const position = (sorted.length - 1) * q;
      const lower = sorted[Math.floor(position)] ?? 0;
      const expected = lower + ((sorted[Math.ceil(position)] ?? 0) - lower) * (position % 1);
      for (let repeat = 0; repeat < 3; repeat++) {
        const run = () => evaluate(values.map((value) => ({ value })));
        if (cap < required) expect(run).toThrow();
        else expect(run()).toBe(expected);
        expect(budget.usedBytes).toBe(0);
        expect(pool.usedBytes).toBe(37);
        expect(evaluate([])).toBeNull();
      }
      sibling.release();
    }
  }
});

it("preserves append semantics after in-place ordering and releases on input failure", () => {
  const budget = new MemoryBudget(1024);
  const accumulator = createWindowAggregateAccumulator(expression(0.5), budget);
  expect(accumulator.append([{ value: 9 }, { value: 1 }])).toBe(5);
  expect(accumulator.append([{ value: 3 }])).toBe(3);
  expect(budget.usedBytes).toBe(charges.array + 3 * charges.arraySlot);
  function* failing() {
    yield { value: 4 };
    throw new Error("input failed");
  }
  expect(() => accumulator.append(failing())).toThrow("input failed");
  expect(budget.usedBytes).toBe(0);
  expect(accumulator.append([{ value: 8 }])).toBe(8);
  accumulator.reset();
  expect(budget.usedBytes).toBe(0);
});
