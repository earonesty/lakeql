import { expect, it } from "vitest";
import { aggregateFinalizeAllocation } from "./aggregate-finalize-allocation.js";
import { aggregateRestoreAllocationBytes } from "./aggregate-restore-allocation.js";
import { aggregateSnapshotAllocationBytes } from "./aggregate-snapshot-allocation.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import type { AggregateExpr } from "./query.js";
import { TimestampValue } from "./timestamp.js";
import {
  createVectorAggregateStates,
  finalizeVectorAggregateStates,
  restoreVectorAggregateStates,
  snapshotVectorAggregateStates,
  updateVectorAggregateStateValue,
  type VectorAggregateStates,
} from "./vector-aggregate.js";

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
it.each(operations)("admits %s checkpoints with live input and retained output", (op) => {
  const states = createVectorAggregateStates({ result: { op, quantile: 0.5 } });
  const state = states.result;
  if (!state) throw Error("Missing state");
  for (const value of [1, 4, 9, 4]) updateVectorAggregateStateValue(state, value);
  const expected = snapshotVectorAggregateStates(states);
  const bytes = aggregateSnapshotAllocationBytes(states);
  for (const limit of [bytes + 7, bytes + 6]) {
    const pool = new MemoryBudget(limit);
    const scope = new MemoryScope(pool);
    scope.allocate(7, "live source", () => new Uint8Array(7));
    if (limit === bytes + 7) {
      const snapshot = snapshotVectorAggregateStates(states, scope);
      expect(snapshot).toEqual(expected);
      expect(pool.usedBytes).toBe(bytes + 7);
      const retained = scope.retainAllocations(snapshot);
      scope.dispose();
      expect(retained.value).toEqual(expected);
      expect(pool.usedBytes).toBe(bytes + 7);
      retained.release();
    } else {
      expect(() => snapshotVectorAggregateStates(states, scope)).toThrow(/budget/i);
      expect(pool.usedBytes).toBe(7);
      expect(snapshotVectorAggregateStates(states)).toEqual(expected);
    }
    scope.dispose();
    expect(pool.usedBytes).toBe(0);
  }
});

it("forecasts Unicode and arbitrary-size signed bigint serialization before copying", () => {
  for (const value of [
    0n,
    -(10n ** 1000n),
    "é😀",
    new TimestampValue(-123456789n, "nanos", true),
  ]) {
    const states: VectorAggregateStates = { value: { op: "min", value } };
    const bytes = aggregateSnapshotAllocationBytes(states);
    const scope = new MemoryScope(new MemoryBudget(bytes));
    expect(snapshotVectorAggregateStates(states, scope)).toEqual(
      snapshotVectorAggregateStates(states),
    );
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("copies sorted distinct backing and merges duplicate runs without changing the source", () => {
  for (const extra of [
    { sortedValues: ["a", "b"] },
    {
      sortedRuns: [
        ["a", "b"],
        ["b", "c"],
      ],
    },
  ]) {
    const states: VectorAggregateStates = {
      value: { op: "count_distinct", values: new Set(), memoryBytes: 0, ...extra },
    };
    const scope = new MemoryScope(new MemoryBudget(aggregateSnapshotAllocationBytes(states)));
    const snapshot = snapshotVectorAggregateStates(states, scope);
    expect(snapshot.value).toEqual({
      op: "count_distinct",
      values: extra.sortedValues ? ["a", "b"] : ["a", "b", "c"],
    });
    if (snapshot.value?.op !== "count_distinct") throw Error("Missing snapshot");
    snapshot.value.values.push("changed");
    expect(extra.sortedValues ?? extra.sortedRuns?.flat()).not.toContain("changed");
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("releases checkpoint admission when copying throws without releasing caller backing", () => {
  const states: VectorAggregateStates = {
    count: {
      op: "count",
      get count(): number {
        throw Error("copy failed");
      },
    },
  };
  const scope = new MemoryScope(new MemoryBudget(4096));
  scope.allocate(7, "caller", () => new Uint8Array(7));
  expect(() => snapshotVectorAggregateStates(states, scope)).toThrow("copy failed");
  expect(scope.budget.usedBytes).toBe(7);
  scope.dispose();
});

it("counts decimal checkpoint storage at chunk boundaries", () => {
  const baseline = aggregateSnapshotAllocationBytes({ value: { op: "min", value: 0n } });
  for (const digits of [1, 18, 19, 36, 37, 1001]) {
    for (const sign of [1n, -1n]) {
      const value = sign * 10n ** BigInt(digits - 1);
      expect(aggregateSnapshotAllocationBytes({ value: { op: "min", value } }) - baseline).toBe(
        2 * (value.toString().length - 1),
      );
    }
  }
});

it.each(operations)("admits %s restore with a live checkpoint and independent result", (op) => {
  const states = createVectorAggregateStates({ result: { op, quantile: 0.5 } });
  const state = states.result;
  if (!state) throw Error("Missing state");
  for (const value of [1, 4, 9, 4]) updateVectorAggregateStateValue(state, value);
  const reference = snapshotVectorAggregateStates(states);
  const checkpointBytes = aggregateSnapshotAllocationBytes(states);
  const restoreBytes = aggregateRestoreAllocationBytes(reference);
  for (const limit of [checkpointBytes + restoreBytes, checkpointBytes + restoreBytes - 1]) {
    const pool = new MemoryBudget(limit);
    const source = new MemoryScope(pool);
    const target = new MemoryScope(pool);
    const checkpoint = snapshotVectorAggregateStates(states, source);
    if (limit === checkpointBytes + restoreBytes) {
      const restored = restoreVectorAggregateStates(checkpoint, {}, target);
      expect(snapshotVectorAggregateStates(restored)).toEqual(reference);
      const retained = target.retainAllocations(restored);
      source.dispose();
      target.dispose();
      expect(pool.usedBytes).toBe(restoreBytes);
      expect(snapshotVectorAggregateStates(retained.value)).toEqual(reference);
      retained.release();
    } else {
      expect(() => restoreVectorAggregateStates(checkpoint, {}, target)).toThrow(/budget/i);
      expect(pool.usedBytes).toBe(checkpointBytes);
      expect(checkpoint).toEqual(reference);
    }
    source.dispose();
    target.dispose();
    expect(pool.usedBytes).toBe(0);
  }
});

it("rolls back restore on invalid scalar decoding or serialized budget rejection", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  scope.allocate(7, "caller", () => new Uint8Array(7));
  expect(() =>
    restoreVectorAggregateStates(
      {
        first: { op: "count", count: 1 },
        bad: { op: "min", value: { type: "bigint", value: "invalid integer" } },
      },
      {},
      scope,
    ),
  ).toThrow();
  expect(scope.budget.usedBytes).toBe(7);
  expect(() =>
    restoreVectorAggregateStates(
      {
        first: { op: "count", count: 1 },
        bad: { op: "median", values: [1, 2, 3] },
      },
      { budget: { maxBufferedRows: 2 } },
      scope,
    ),
  ).toThrow(/budget/);
  expect(scope.budget.usedBytes).toBe(7);
  scope.dispose();
});

it("restores serialized bigint and timestamp payloads within the forecast", () => {
  for (const value of [-(10n ** 1000n), new TimestampValue(-123456789n, "nanos", false), "é😀"]) {
    const source = { value: { op: "min" as const, value } };
    const snapshot = snapshotVectorAggregateStates(source);
    const scope = new MemoryScope(new MemoryBudget(aggregateRestoreAllocationBytes(snapshot)));
    expect(restoreVectorAggregateStates(snapshot, {}, scope)).toEqual(source);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it.each(operations)("admits %s finalization and releases scratch before retaining output", (op) => {
  const states = createVectorAggregateStates({ result: { op, quantile: 0.5 } });
  const state = states.result;
  if (!state) throw Error("Missing state");
  for (const value of [1, 4, 9, 4]) updateVectorAggregateStateValue(state, value);
  const expected = finalizeVectorAggregateStates(states);
  const plan = aggregateFinalizeAllocation(states);
  const peak = plan.outputBytes + plan.scratchBytes + 7;
  for (const limit of [peak, peak - 1]) {
    const pool = new MemoryBudget(limit);
    const source = new MemoryScope(pool);
    source.allocate(7, "source", () => new Uint8Array(7));
    const target = new MemoryScope(pool);
    if (limit === peak) {
      const row = finalizeVectorAggregateStates(states, target);
      expect(row).toEqual(expected);
      expect(pool.peakBytes).toBe(peak);
      expect(pool.usedBytes).toBe(plan.outputBytes + 7);
      const retained = target.retainAllocations(row);
      target.dispose();
      source.dispose();
      expect(pool.usedBytes).toBe(plan.outputBytes);
      expect(retained.value).toEqual(expected);
      retained.release();
    } else {
      expect(() => finalizeVectorAggregateStates(states, target)).toThrow(/budget/i);
      expect(pool.usedBytes).toBe(7);
      expect(finalizeVectorAggregateStates(states)).toEqual(expected);
    }
    source.dispose();
    target.dispose();
    expect(pool.usedBytes).toBe(0);
  }
});

it("rolls back result and scratch when a finalizer throws", () => {
  const states: VectorAggregateStates = {
    result: { op: "median", values: [1, "two"], memoryBytes: 0 },
  };
  const plan = aggregateFinalizeAllocation(states);
  const scope = new MemoryScope(new MemoryBudget(plan.outputBytes + plan.scratchBytes + 7));
  scope.allocate(7, "caller", () => new Uint8Array(7));
  expect(() => finalizeVectorAggregateStates(states, scope)).toThrow();
  expect(scope.budget.usedBytes).toBe(7);
  scope.dispose();
});

it("bounds distinct finalization scratch by run count rather than total entries", () => {
  let previousScratch: number | undefined;
  for (const count of [16, 1024, 8192]) {
    const run = Array.from({ length: count }, (_, index) => String(index).padStart(5, "0"));
    const states: VectorAggregateStates = {
      result: {
        op: "count_distinct",
        values: new Set(),
        sortedRuns: [run, [], [...run]],
        memoryBytes: 0,
      },
    };
    const plan = aggregateFinalizeAllocation(states);
    if (previousScratch !== undefined) expect(plan.scratchBytes).toBe(previousScratch);
    previousScratch = plan.scratchBytes;
    const scope = new MemoryScope(new MemoryBudget(plan.outputBytes + plan.scratchBytes));
    expect(finalizeVectorAggregateStates(states, scope)).toEqual({ result: count });
    expect(scope.budget.usedBytes).toBe(plan.outputBytes);
    expect(snapshotVectorAggregateStates(states).result).toEqual({
      op: "count_distinct",
      values: run,
    });
    scope.dispose();
  }
});

it("shares peak scratch across finalizers and retains only the result payloads", () => {
  const text = "é😀".repeat(1024);
  const states: VectorAggregateStates = {
    median: { op: "median", values: [text, text, text], memoryBytes: 0 },
    quantile: { op: "quantile", quantile: 0.5, values: [1, 2, 3, 4], memoryBytes: 0 },
    mode: { op: "mode", values: new Map([["key", { value: text, count: 2 }]]), memoryBytes: 0 },
    timestamp: { op: "min", value: new TimestampValue(-123n, "nanos", false) },
  };
  const medianPlan = aggregateFinalizeAllocation({
    median: states.median as VectorAggregateStates[string],
  });
  const quantilePlan = aggregateFinalizeAllocation({
    quantile: states.quantile as VectorAggregateStates[string],
  });
  const plan = aggregateFinalizeAllocation(states);
  expect(plan.scratchBytes).toBe(Math.max(medianPlan.scratchBytes, quantilePlan.scratchBytes));
  const scope = new MemoryScope(new MemoryBudget(plan.outputBytes + plan.scratchBytes));
  const row = finalizeVectorAggregateStates(states, scope);
  expect(row).toEqual({
    median: text,
    quantile: 2.5,
    mode: text,
    timestamp: states.timestamp?.op === "min" ? states.timestamp.value : null,
  });
  expect(scope.budget.usedBytes).toBe(plan.outputBytes);
  const owner = scope.retainAllocations(row);
  scope.dispose();
  expect(owner.value.mode).toBe(text);
  owner.release();
  expect(scope.budget.usedBytes).toBe(0);
});

it.each([
  {},
  { type: "bigint" },
  { type: "bigint", value: 1 },
  { type: "bigint", value: "1.5" },
  { type: "bigint", value: "" },
  { type: "timestamp", epochNanoseconds: "1", unit: "seconds", isAdjustedToUTC: true },
  { type: "timestamp", epochNanoseconds: "1", unit: "nanos" },
  { type: "timestamp", epochNanoseconds: [], unit: "nanos", isAdjustedToUTC: true },
])("rejects malformed checkpoint scalars before admission: %j", (value) => {
  const snapshot = { result: { op: "min", value } } as unknown as Parameters<
    typeof aggregateRestoreAllocationBytes
  >[0];
  const scope = new MemoryScope(new MemoryBudget(4096));
  scope.allocate(7, "sibling", () => 1);
  expect(() => aggregateRestoreAllocationBytes(snapshot)).toThrow(
    /Invalid aggregate checkpoint scalar/,
  );
  for (const allocationScope of [undefined, scope]) {
    try {
      restoreVectorAggregateStates(snapshot, {}, allocationScope);
      throw Error("Expected rejection");
    } catch (error) {
      expect(error).toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    }
    expect(scope.budget.usedBytes).toBe(7);
  }
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
