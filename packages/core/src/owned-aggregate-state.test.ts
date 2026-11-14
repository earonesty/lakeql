import { expect, it, vi } from "vitest";
import { type Batch, batchFromColumns } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { OwnedAggregateState } from "./owned-aggregate-state.js";
import type { AggregateExpr } from "./query.js";
import { TimestampValue } from "./timestamp.js";
import {
  createVectorAggregateStates,
  finalizeVectorAggregateStates,
  mergeVectorAggregateStates,
  snapshotVectorAggregateStates,
  updateVectorAggregateStates,
  updateVectorAggregateStateValue,
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
it.each(operations)("owns %s updates, independent checkpoints and final result", (op) => {
  const pool = new MemoryBudget(1_000_000);
  const parent = new MemoryScope(pool);
  const spec = { value: { op, quantile: 0.5 } };
  const owner = new OwnedAggregateState(parent, spec);
  const reference = createVectorAggregateStates(spec);
  const state = reference.value;
  if (!state) throw Error("Missing state");
  owner.updateValue("value", 1);
  updateVectorAggregateStateValue(state, 1);
  const checkpoint = owner.checkpoint();
  const before = JSON.stringify(checkpoint.value);
  for (const value of [4, 9, 4, null]) {
    owner.updateValue("value", value);
    updateVectorAggregateStateValue(state, value);
  }
  expect(JSON.stringify(checkpoint.value)).toBe(before);
  const result = owner.finish();
  expect(result.value).toEqual(finalizeVectorAggregateStates(reference));
  expect(() => owner.updateValue("value", 10)).toThrow(/disposed/);
  parent.dispose();
  expect(result.value).toEqual(finalizeVectorAggregateStates(reference));
  checkpoint.release();
  expect(pool.usedBytes).toBe(result.bytes);
  result.release();
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "median",
  "quantile",
  "mode",
  "count_distinct",
  "approx_count_distinct",
] as const)("rejects %s growth before retaining it and can continue after capacity returns", (op) => {
  const pool = new MemoryBudget(65536);
  const parent = new MemoryScope(pool);
  const owner = new OwnedAggregateState(parent, { value: { op, quantile: 0.5 } });
  owner.updateValue("value", 1);
  const before = owner.checkpoint();
  const occupied = pool.reserve(pool.availableBytes + 1, "other job");
  const remainder = pool.reserve(pool.availableBytes, "other job remainder");
  expect(() => owner.updateValue("value", 2)).toThrow(/budget/i);
  occupied.release();
  remainder.release();
  const after = owner.checkpoint();
  expect(after.value).toEqual(before.value);
  before.release();
  after.release();
  owner.updateValue("value", 2);
  const result = owner.finish();
  result.release();
  parent.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("releases replaced scalar backing instead of accumulating every last value", () => {
  const pool = new MemoryBudget(65536);
  const parent = new MemoryScope(pool);
  const owner = new OwnedAggregateState(parent, { value: { op: "last" } });
  owner.updateValue("value", "a".repeat(100));
  owner.updateValue("value", "b".repeat(100));
  const used = pool.usedBytes;
  for (let i = 0; i < 1000; i++) owner.updateValue("value", String(i).padStart(100, "0"));
  expect(pool.usedBytes).toBe(used);
  const result = owner.finish();
  expect(result.value).toEqual({ value: "999".padStart(100, "0") });
  result.release();
  parent.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("does not retain duplicate collection entries and closes on parent cancellation", () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const owner = new OwnedAggregateState(parent, { value: { op: "count_distinct" } });
  owner.updateValue("value", "é😀");
  const used = parent.budget.usedBytes;
  for (let i = 0; i < 1000; i++) owner.updateValue("value", "é😀");
  expect(parent.budget.usedBytes).toBe(used);
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
  expect(() => owner.checkpoint()).toThrow(/disposed/);
  owner.dispose();
});

it("cleans construction and finalization failures", () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  expect(() => new OwnedAggregateState(parent, { bad: { op: "quantile", quantile: 2 } })).toThrow();
  expect(parent.budget.usedBytes).toBe(0);
  const owner = new OwnedAggregateState(parent, { value: { op: "median" } });
  owner.updateValue("value", 1);
  owner.updateValue("value", "two");
  expect(() => owner.finish()).toThrow();
  expect(parent.budget.usedBytes).toBe(0);
  expect(() => owner.updateValue("value", 1)).toThrow(/disposed/);
  parent.dispose();
});

it.each(operations)("merges %s checkpoints with independently admitted target growth", (op) => {
  const parent = new MemoryScope(new MemoryBudget(1_000_000));
  const spec = { value: { op, quantile: 0.5 } };
  const owner = new OwnedAggregateState(parent, spec);
  const left = createVectorAggregateStates(spec);
  const right = createVectorAggregateStates(spec);
  if (!left.value || !right.value) throw Error("Missing state");
  for (const value of [1, 4, null]) {
    owner.updateValue("value", value);
    updateVectorAggregateStateValue(left.value, value);
  }
  for (const value of [4, 9, 9, null]) updateVectorAggregateStateValue(right.value, value);
  const checkpoint = snapshotVectorAggregateStates(right);
  const sourceBefore = JSON.stringify(checkpoint);
  owner.merge(checkpoint);
  mergeVectorAggregateStates(left, right);
  const merged = owner.checkpoint();
  expect(merged.value).toEqual(snapshotVectorAggregateStates(left));
  expect(JSON.stringify(checkpoint)).toBe(sourceBefore);
  merged.release();
  const result = owner.finish();
  expect(result.value).toEqual(finalizeVectorAggregateStates(left));
  result.release();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});

it("merges weighted mode counts without replaying each observation", () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const owner = new OwnedAggregateState(parent, { value: { op: "mode" } });
  owner.updateValue("value", "a");
  owner.merge({
    value: {
      op: "mode",
      values: [
        { key: "string:a", value: "a", count: 1_000_000_000 },
        { key: "string:b", value: "b", count: 2 },
      ],
    },
  });
  const checkpoint = owner.checkpoint();
  expect(checkpoint.value.value).toEqual({
    op: "mode",
    values: [
      { key: "string:a", value: "a", count: 1_000_000_001 },
      { key: "string:b", value: "b", count: 2 },
    ],
  });
  checkpoint.release();
  owner.dispose();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});

it("does not retain duplicate distinct checkpoint backing", () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const owner = new OwnedAggregateState(parent, { value: { op: "count_distinct" } });
  owner.updateValue("value", "é😀");
  const used = parent.budget.usedBytes;
  for (let i = 0; i < 100; i++)
    owner.merge({ value: { op: "count_distinct", values: ["string:é😀", "string:é😀"] } });
  expect(parent.budget.usedBytes).toBe(used);
  owner.dispose();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});

it("validates all merge aliases before mutation and releases rejected restore storage", () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const owner = new OwnedAggregateState(
    parent,
    { n: { op: "count" }, median: { op: "median" } },
    { budget: { maxBufferedRows: 2 } },
  );
  owner.updateValue("n", 1);
  const used = parent.budget.usedBytes;
  expect(() =>
    owner.merge({ n: { op: "count", count: 100 }, unknown: { op: "sum", sum: 2 } }),
  ).toThrow(/mismatch/);
  expect(() =>
    owner.merge({ n: { op: "count", count: 100 }, median: { op: "median", values: [1, 2, 3] } }),
  ).toThrow(/budget/);
  expect(parent.budget.usedBytes).toBe(used);
  const result = owner.finish();
  expect(result.value).toEqual({ n: 1, median: null });
  result.release();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});

it("keeps only admitted merge entries when combined state exceeds its limit", () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const owner = new OwnedAggregateState(
    parent,
    { value: { op: "median" } },
    { budget: { maxBufferedRows: 2 } },
  );
  owner.updateValue("value", 1);
  const source = { value: { op: "median" as const, values: [2, 3] } };
  expect(() => owner.merge(source)).toThrow(/budget/);
  const checkpoint = owner.checkpoint();
  expect(checkpoint.value).toEqual({ value: { op: "median", values: [1, 2] } });
  expect(source.value.values).toEqual([2, 3]);
  checkpoint.release();
  const result = owner.finish();
  expect(result.value).toEqual({ value: 1.5 });
  result.release();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});

it.each(operations)("updates %s across selected batches without checkpoint continuation", (op) => {
  const parent = new MemoryScope(new MemoryBudget(1_000_000));
  const spec = { value: { op, column: "value", quantile: 0.5 } };
  const owner = new OwnedAggregateState(parent, spec);
  const reference = createVectorAggregateStates(spec);
  for (let start = 0; start < 32; start += 4) {
    const batch = batchFromColumns({ value: [start, null, start + 2, start + 3] });
    const selection = new Uint8Array([1, 1, 0, 1]);
    owner.updateBatch(batch, selection);
    updateVectorAggregateStates(reference, spec, batch, selection);
  }
  const result = owner.finish();
  expect(result.value).toEqual(finalizeVectorAggregateStates(reference));
  result.release();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});

it("updates packed expression inputs with reusable batch state and bounded scratch", () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const owner = new OwnedAggregateState(parent, {
    n: { op: "count" },
    unique: {
      op: "count_distinct",
      expr: { kind: "call", fn: "upper", args: [{ kind: "column", name: "text" }] },
    },
  });
  const batch: Batch = {
    rowCount: 2,
    columns: {
      text: { type: "utf8-view", data: new TextEncoder().encode("aa"), fixedLength: 1, length: 2 },
    },
  };
  owner.updateBatch(batch);
  const used = parent.budget.usedBytes;
  for (let i = 0; i < 128; i++) owner.updateBatch(batch, new Uint8Array([1, 0]));
  expect(parent.budget.usedBytes).toBe(used);
  const result = owner.finish();
  expect(result.value).toEqual({ n: 130, unique: 1 });
  result.release();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});

it("rejects unsupported allocation shapes before updating another accumulator", () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const owner = new OwnedAggregateState(parent, {
    n: { op: "count" },
    value: {
      op: "count_distinct",
      expr: {
        kind: "call",
        fn: "regexp_replace",
        args: [
          { kind: "literal", value: "a" },
          { kind: "literal", value: "a" },
          { kind: "literal", value: "b" },
        ],
      },
    },
  });
  expect(() => owner.updateBatch({ rowCount: 3, columns: {} })).toThrow(/allocation contract/);
  const result = owner.finish();
  expect(result.value).toEqual({ n: 0, value: 0 });
  result.release();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});

it.each([
  "min",
  "max",
] as const)("does not reserve for losing %s candidates under a full shared pool", (op) => {
  const pool = new MemoryBudget(65536);
  const parent = new MemoryScope(pool);
  const owner = new OwnedAggregateState(parent, { value: { op } });
  owner.updateValue("value", 0);
  const sibling = pool.reserve(pool.availableBytes + 1, "other job");
  const remainder = pool.reserve(pool.availableBytes, "remaining capacity");
  const reserve = vi.spyOn(pool, "reserve");
  for (let i = 1; i <= 10000; i++) owner.updateValue("value", op === "min" ? i : -i);
  for (const value of [null, NaN, -0, 0]) owner.updateValue("value", value);
  expect(() => owner.updateValue("value", "invalid type")).toThrow(
    /same type|numeric|aggregate|expects/i,
  );
  expect(reserve).not.toHaveBeenCalled();
  expect(() => owner.updateValue("value", op === "min" ? -1 : 1)).toThrow(/budget/i);
  sibling.release();
  remainder.release();
  const result = owner.finish();
  expect(result.value).toEqual({ value: 0 });
  result.release();
  parent.dispose();
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "min",
  "max",
  "first",
  "last",
  "any",
] as const)("keeps retained NaN payload charged for %s", (op) => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const owner = new OwnedAggregateState(parent, { value: { op } });
  const before = parent.budget.usedBytes;
  owner.updateValue("value", NaN);
  const retained = parent.budget.usedBytes;
  expect(retained).toBeGreaterThanOrEqual(before);
  for (let i = 0; i < 100; i++) owner.updateValue("value", NaN);
  expect(parent.budget.usedBytes).toBe(retained);
  const result = owner.finish();
  expect(result.value.value).toBeNaN();
  result.release();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});

it("preserves timestamp comparison and signed-zero replacement semantics", () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const owner = new OwnedAggregateState(parent, { min: { op: "min" }, last: { op: "last" } });
  const first = new TimestampValue(123n, "nanos", true);
  owner.updateValue("min", first);
  const used = parent.budget.usedBytes;
  owner.updateValue("min", new TimestampValue(124n, "micros", true));
  expect(parent.budget.usedBytes).toBe(used);
  owner.updateValue("last", -0);
  owner.updateValue("last", 0);
  const result = owner.finish();
  expect(result.value.min).toEqual(first);
  expect(Object.is(result.value.last, 0)).toBe(true);
  result.release();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});
