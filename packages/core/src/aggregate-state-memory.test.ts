import { expect, it, vi } from "vitest";
import { AggregateStateMemory } from "./aggregate-state-memory.js";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { finalizeVectorAggregateStates } from "./vector-aggregate.js";

it("shares a ledger across many independent accumulator directories", () => {
  const scope = new MemoryScope(new MemoryBudget(16 * 1024 * 1024));
  const arena = AggregateStateMemory.allocate(scope);
  const reserve = vi.spyOn(scope.budget, "reserve");
  const states = [];
  for (let i = 0; i < 8192; i++) {
    const state = arena.create({ n: { op: "count" } });
    if (!state.n) throw Error("Missing count");
    arena.updateValue(state.n, i);
    states.push(state);
  }
  expect(reserve.mock.calls.length).toBeLessThan(128);
  for (const state of states) expect(finalizeVectorAggregateStates(state)).toEqual({ n: 1 });
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  expect(arena.usedBytes).toBe(0);
  const state = states[0]?.n;
  if (!state) throw Error("Missing count");
  expect(() => arena.updateValue(state, 1)).toThrow(/disposed/);
});

it("tracks replacement payload by accumulator identity across shared aliases", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const arena = AggregateStateMemory.allocate(scope);
  const left = arena.create({ value: { op: "last" } });
  const right = arena.create({ value: { op: "last" } });
  if (!left.value || !right.value) throw Error("Missing values");
  const initial = arena.usedBytes;
  arena.updateValue(left.value, NaN);
  expect(arena.usedBytes).toBe(initial + c.number);
  arena.updateValue(right.value, "a".repeat(100));
  const before = arena.usedBytes;
  for (let i = 0; i < 1000; i++) {
    arena.updateValue(left.value, NaN);
    arena.updateValue(right.value, String(i).padStart(100, "0"));
  }
  expect(arena.usedBytes).toBe(before);
  expect(finalizeVectorAggregateStates(left).value).toBeNaN();
  expect(finalizeVectorAggregateStates(right).value).toBe("999".padStart(100, "0"));
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects foreign state and rolls back failed construction", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const arena = AggregateStateMemory.allocate(scope);
  const other = AggregateStateMemory.allocate(scope);
  const foreign = other.create({ n: { op: "count" } });
  if (!foreign.n) throw Error("Missing count");
  expect(() => arena.updateValue(foreign.n, 1)).toThrow(/another arena/);
  const before = arena.usedBytes;
  expect(() => arena.create({ a: { op: "count" }, b: { op: "quantile", quantile: 2 } })).toThrow();
  expect(arena.usedBytes).toBe(before);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it.each([
  "count_distinct",
  "approx_count_distinct",
  "mode",
] as const)("normalizes owned %s keys once without changing scalar or limit semantics", async (op) => {
  const { timestampFromEpoch } = await import("./timestamp.js");
  const {
    createVectorAggregateStates,
    updateVectorAggregateStateValue,
    snapshotVectorAggregateStates,
  } = await import("./vector-aggregate.js");
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  try {
    const arena = AggregateStateMemory.allocate(scope);
    const actual = arena.create({ value: { op } });
    const expected = createVectorAggregateStates({ value: { op } });
    const timestamp = timestampFromEpoch(123456789n, "micros");
    const values = [null, "", "雪😀", "1", 1, 1n, true, NaN, -0, 0, timestamp, timestamp, "雪😀"];
    const state = actual.value;
    const reference = expected.value;
    if (!state || !reference) throw new Error("Missing aggregate state");
    const options = { budget: { maxMemoryBytes: 65536, maxBufferedRows: 32 } };
    for (const value of values) updateVectorAggregateStateValue(reference, value, options);
    const stringify = vi.spyOn(timestamp, "toString");
    for (const value of values) arena.updateValue(state, value, options);
    expect(stringify).toHaveBeenCalledTimes(2);
    stringify.mockRestore();
    expect(snapshotVectorAggregateStates(actual)).toEqual(snapshotVectorAggregateStates(expected));
    const limited = arena.create({ value: { op } }).value;
    if (
      !limited ||
      (limited.op !== "mode" &&
        limited.op !== "count_distinct" &&
        limited.op !== "approx_count_distinct")
    )
      throw new Error("Missing limited state");
    const limit = { budget: { maxBufferedRows: 1 } };
    arena.updateValue(limited, "first", limit);
    arena.updateValue(limited, "first", limit);
    const before = arena.usedBytes;
    expect(() => arena.updateValue(limited, "second", limit)).toThrow(
      expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
    );
    expect(arena.usedBytes).toBe(before);
    expect(limited.values.size).toBe(1);
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});
