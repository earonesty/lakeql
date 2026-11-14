import { expect, it, vi } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { col } from "./expr.js";
import { jsonScalarKeyAllocationBytes } from "./json-scalar-key-memory.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { MUTABLE_LEDGER_CHUNK_BYTES } from "./mutable-memory-ledger.js";
import { createWindowAggregateAccumulator } from "./window-aggregate.js";

it.each([
  "mode",
  "count_distinct",
] as const)("amortizes duplicate %s keys and preserves them during sibling reclamation", (aggregate) => {
  const pool = new MemoryBudget(1024 * 1024);
  const budget = pool.child();
  const reserve = vi.spyOn(budget, "reserve");
  const state = createWindowAggregateAccumulator(
    { fn: { aggregate }, args: [col("value")], over: { partitionBy: [], orderBy: [] } },
    budget,
  );
  const expected = aggregate === "mode" ? 0 : 8;
  expect(state.append(Array.from({ length: 16384 }, (_, id) => ({ value: id % 8 })))).toBe(
    expected,
  );
  expect(reserve.mock.calls.length).toBeLessThan(32);
  const before = pool.usedBytes;
  const sibling = pool.child().reserve(pool.availableBytes + 1, "sibling");
  expect(budget.usedBytes).toBeLessThan(before);
  expect(state.append([])).toBe(expected);
  sibling.release();
  state.reset();
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "mode",
  "count_distinct",
] as const)("admits %s first-key metadata exactly and rejects one byte below", (aggregate) => {
  const value = "escaped\u0000\ud800";
  const bytes =
    MUTABLE_LEDGER_CHUNK_BYTES +
    jsonScalarKeyAllocationBytes(value) +
    c.property +
    c.object +
    (aggregate === "mode" ? c.object + 2 * c.property : 0);
  for (const cap of [bytes - 1, bytes]) {
    const budget = new MemoryBudget(cap);
    const scope = new MemoryScope(budget);
    const state = createWindowAggregateAccumulator(
      { fn: { aggregate }, args: [col("value")], over: { partitionBy: [], orderBy: [] } },
      scope,
    );
    if (cap < bytes) {
      expect(() => state.append([{ value }])).toThrow();
      expect(budget.usedBytes).toBe(0);
    } else expect(state.append([{ value }])).toBe(aggregate === "mode" ? value : 1);
    scope.dispose();
    expect(budget.usedBytes).toBe(0);
    expect(() => state.append([])).toThrow(/disposed/);
    state.reset();
  }
});
