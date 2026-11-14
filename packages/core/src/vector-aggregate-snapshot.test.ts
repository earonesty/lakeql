import { expect, it } from "vitest";
import { unmeasuredMemoryBytes } from "./aggregate-memory.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import {
  restoreVectorAggregateStates,
  type VectorAggregateStateSnapshot,
} from "./vector-aggregate.js";
import {
  snapshotVectorAggregateState,
  vectorAggregateStateFromSnapshot,
} from "./vector-aggregate-snapshot.js";

it("restores mode entries with last-value semantics and stable insertion order", () => {
  const state = vectorAggregateStateFromSnapshot({
    op: "mode",
    values: [
      { key: "first", value: "é😀", count: 2 },
      { key: "second", value: { type: "bigint", value: "9007199254740993" }, count: 3 },
      { key: "first", value: "replacement", count: 4 },
    ],
  });
  expect(state.op).toBe("mode");
  if (state.op !== "mode") throw Error("Expected mode state");
  expect(state.memoryBytes).toBe(unmeasuredMemoryBytes);
  expect([...state.values.keys()]).toEqual(["first", "second"]);
  const snapshot = snapshotVectorAggregateState(state);
  expect(snapshot).toEqual({
    op: "mode",
    values: [
      { key: "first", value: "replacement", count: 4 },
      { key: "second", value: { type: "bigint", value: "9007199254740993" }, count: 3 },
    ],
  });
  const first = state.values.get("first");
  if (!first) throw Error("Missing first entry");
  first.count = 99;
  expect(snapshot).toEqual(
    snapshotVectorAggregateState(vectorAggregateStateFromSnapshot(snapshot)),
  );
  if (snapshot.op !== "mode") throw Error("Expected mode snapshot");
  expect(snapshot.values[0]?.count).toBe(4);
});

it("preserves empty mode accounting and snapshot shape", () => {
  const state = vectorAggregateStateFromSnapshot({ op: "mode", values: [] });
  expect(state).toEqual({ op: "mode", values: new Map(), memoryBytes: 2 });
  expect(snapshotVectorAggregateState(state)).toEqual({ op: "mode", values: [] });
});

it.each([
  "count",
  "avg",
  "var_samp",
  "var_pop",
  "stddev_samp",
  "stddev_pop",
  "mode",
] as const)("validates %s counters in direct and scoped restore", (op) => {
  const snapshot = (count: number): VectorAggregateStateSnapshot => {
    if (op === "count") return { op, count };
    if (op === "avg") return { op, count, sum: 0 };
    if (op === "mode") return { op, values: [{ key: "x", value: "x", count }] };
    return { op, count, mean: 0, m2: 0 };
  };
  for (const count of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1, NaN, Infinity]) {
    const value = snapshot(count);
    expect(() => vectorAggregateStateFromSnapshot(value)).toThrow(/checkpoint count/);
    const budget = new MemoryBudget(65536);
    const scope = new MemoryScope(budget);
    expect(() => restoreVectorAggregateStates({ x: value }, {}, scope)).toThrow(/checkpoint count/);
    scope.dispose();
    expect(budget.usedBytes).toBe(0);
  }
  for (const count of [0, 1, Number.MAX_SAFE_INTEGER]) {
    expect(snapshotVectorAggregateState(vectorAggregateStateFromSnapshot(snapshot(count)))).toEqual(
      snapshot(count),
    );
  }
});
