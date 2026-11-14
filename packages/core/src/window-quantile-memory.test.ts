import { expect, it, vi } from "vitest";
import { MATERIALIZATION_CHARGES as charges } from "./allocation-charges.js";
import { col, lit } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { MUTABLE_LEDGER_CHUNK_BYTES } from "./mutable-memory-ledger.js";
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
  const required = MUTABLE_LEDGER_CHUNK_BYTES + charges.array + values.length * charges.arraySlot;
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
  const budget = new MemoryBudget(4096);
  const accumulator = createWindowAggregateAccumulator(expression(0.5), budget);
  expect(accumulator.append([{ value: 9 }, { value: 1 }])).toBe(5);
  expect(accumulator.append([{ value: 3 }])).toBe(3);
  expect(budget.usedBytes).toBeGreaterThanOrEqual(
    MUTABLE_LEDGER_CHUNK_BYTES + charges.array + 3 * charges.arraySlot,
  );
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

it("amortizes numeric growth and returns spare capacity to a competing sibling", () => {
  const pool = new MemoryBudget(1024 * 1024);
  const budget = pool.child();
  const reserve = vi.spyOn(budget, "reserve");
  const state = createWindowAggregateAccumulator(expression(0.5), budget);
  expect(state.append(Array.from({ length: 16384 }, (_, value) => ({ value })))).toBe(8191.5);
  expect(reserve.mock.calls.length).toBeLessThan(32);
  const before = pool.usedBytes;
  const sibling = pool.child().reserve(pool.availableBytes + 1, "competing output");
  expect(budget.usedBytes).toBeLessThan(before);
  expect(state.append([])).toBe(8191.5);
  sibling.release();
  state.reset();
  expect(pool.usedBytes).toBe(0);
});

it("isolates quantile cleanup from its parent stage and sibling ownership", () => {
  const budget = new MemoryBudget(16384);
  const scope = new MemoryScope(budget);
  scope.admit(16384, "stage");
  const sibling = scope.reserve(37, "sibling");
  const state = createWindowAggregateAccumulator(expression(0.5), scope);
  expect(state.append([{ value: 1 }, { value: 5 }])).toBe(3);
  state.reset();
  expect(scope.disposed).toBe(false);
  expect(sibling.bytes).toBe(37);
  expect(state.append([{ value: 9 }])).toBe(9);
  state.reset();
  sibling.release();
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("preserves exact admission for reserve-only sources", () => {
  const required = charges.array + 3 * charges.arraySlot;
  const budget = new MemoryBudget(required);
  const source = { reserve: budget.reserve.bind(budget) };
  const state = createWindowAggregateAccumulator(expression(0.5), source);
  expect(state.append([{ value: 3 }, { value: 1 }, { value: 2 }])).toBe(2);
  expect(budget.usedBytes).toBe(required);
  expect(() => state.append([{ value: 4 }])).toThrow();
  expect(budget.usedBytes).toBe(0);
});

it("rejects retained-state reads after its parent scope closes", () => {
  const budget = new MemoryBudget(8192);
  const scope = new MemoryScope(budget);
  const state = createWindowAggregateAccumulator(expression(0.5), scope);
  expect(state.append([{ value: 2 }, { value: 4 }])).toBe(3);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
  expect(() => state.append([])).toThrow(/disposed/);
  state.reset();
  expect(budget.usedBytes).toBe(0);
});
