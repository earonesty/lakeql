import { expect, it, vi } from "vitest";
import { type Batch, batchFromColumns } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { OwnedGroupDirectory } from "./owned-group-directory.js";
import type { AggregateExpr } from "./query.js";
import { TimestampValue } from "./timestamp.js";
import { finalizeVectorAggregateStates } from "./vector-aggregate.js";
import {
  createVectorGroupByState,
  finalizeVectorGroupByRows,
  restoreVectorGroupByState,
  updateVectorGroupByState,
  type VectorGroup,
} from "./vector-group-by.js";

it("owns group keys and shared accumulators without scanning prior groups", () => {
  const scope = new MemoryScope(new MemoryBudget(16 * 1024 * 1024));
  const directory = OwnedGroupDirectory.allocate(scope, ["key"], { n: { op: "count" } });
  const map = (directory as unknown as { groups: Map<string, VectorGroup> }).groups;
  const values = vi.spyOn(map, "values");
  const iterator = vi.spyOn(map, Symbol.iterator);
  for (let i = 0; i < 8192; i++) {
    const group = directory.getOrCreate([i]);
    directory.update(group, "n", 1);
    expect(directory.getOrCreate([i])).toBe(group);
    directory.update(group, "n", 1);
    expect(finalizeVectorAggregateStates(group.states)).toEqual({ n: 2 });
  }
  expect(directory.size).toBe(8192);
  expect(values).not.toHaveBeenCalled();
  expect(iterator).not.toHaveBeenCalled();
  directory.dispose();
  expect(directory.size).toBe(0);
  expect(() => directory.getOrCreate([0])).toThrow(/disposed/);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("preserves typed/composite key identity and enforces group limits only for new groups", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const directory = OwnedGroupDirectory.allocate(
    scope,
    ["a", "b"],
    { n: { op: "count" } },
    { maxGroups: 2 },
  );
  const input = ["é😀", "a:b"];
  const first = directory.getOrCreate(input);
  input[0] = "changed";
  expect(first.keyValues).toEqual(["é😀", "a:b"]);
  const second = directory.getOrCreate([1, "a:b"]);
  expect(second).not.toBe(first);
  expect(directory.getOrCreate(["é😀", "a:b"])).toBe(first);
  expect(() => directory.getOrCreate([1n, "a:b"])).toThrow(/group budget/);
  expect(directory.size).toBe(2);
  expect(() => directory.getOrCreate(["short"])).toThrow(/arity/);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rolls back nested directory and accumulator admission on exhaustion", () => {
  const scope = new MemoryScope(new MemoryBudget(8192));
  const directory = OwnedGroupDirectory.allocate(scope, ["key"], { n: { op: "count" } });
  const before = scope.budget.usedBytes;
  for (let i = 0; i < 3; i++) {
    expect(() => directory.getOrCreate(["x"])).toThrow(/budget/i);
    expect(directory.size).toBe(0);
    expect(scope.budget.usedBytes).toBe(before);
  }
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("keeps the legacy operator metric incremental and separates it from allocation bytes", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const directory = OwnedGroupDirectory.allocate(
    scope,
    ["key"],
    { n: { op: "count" }, median: { op: "median" } },
    { budget: { maxMemoryBytes: 1000 } },
  );
  const group = directory.getOrCreate(["a"]);
  expect(directory.memoryBytes).toBe(162);
  directory.update(group, "n", 1);
  directory.update(group, "median", 2);
  expect(directory.memoryBytes).toBe(163);
  expect(scope.budget.usedBytes).toBeGreaterThan(directory.memoryBytes);
  const other = OwnedGroupDirectory.allocate(scope, ["key"], { n: { op: "count" } });
  expect(() => other.update(group, "n", 1)).toThrow(/another directory/);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("retains independent checkpoints and final rows across mutation and parent closure", () => {
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  const directory = OwnedGroupDirectory.allocate(scope, ["key"], {
    median: { op: "median" },
    n: { op: "count" },
  });
  const first = directory.getOrCreate([12345678901234567890n]);
  directory.update(first, "median", 2);
  directory.update(first, "n", 1);
  const checkpoint = directory.checkpoint();
  directory.update(first, "median", 8);
  directory.update(first, "n", 1);
  directory.getOrCreate(["é😀"]);
  const rows = directory.finish();
  expect(rows.value).toEqual([
    { key: 12345678901234567890n, median: 5, n: 2 },
    { key: "é😀", median: null, n: 0 },
  ]);
  expect(checkpoint.value.groups).toHaveLength(1);
  expect(checkpoint.value.groups[0]?.keyValues).toEqual([
    { type: "bigint", value: "12345678901234567890" },
  ]);
  expect(checkpoint.value.groups[0]?.states.n).toMatchObject({ count: 1 });
  expect(() => directory.checkpoint()).toThrow(/disposed/);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(rows.bytes + checkpoint.bytes);
  checkpoint.release();
  expect(scope.budget.usedBytes).toBe(rows.bytes);
  rows.release();
  expect(scope.budget.usedBytes).toBe(0);
});

it("preserves key property order and aggregate alias precedence", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const directory = OwnedGroupDirectory.allocate(scope, ["key", "n"], { n: { op: "count" } });
  const group = directory.getOrCreate(["a", "overwritten"]);
  directory.update(group, "n", 1);
  const result = directory.finish();
  expect(result.value).toEqual([{ key: "a", n: 1 }]);
  expect(Object.keys(result.value[0] ?? {})).toEqual(["key", "n"]);
  result.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rolls back failed checkpoints and closes a failed finalization", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const directory = OwnedGroupDirectory.allocate(scope, ["key"], { n: { op: "count" } });
  directory.getOrCreate(["x"]);
  // Fill the pool after reclaiming reusable arena capacity.
  expect(scope.budget.tryReserve(scope.budget.maxBytes, "force reclamation")).toBeUndefined();
  const remaining = scope.budget.availableBytes;
  const filler = scope.budget.reserve(remaining, "test pressure");
  const before = scope.budget.usedBytes;
  expect(() => directory.checkpoint()).toThrow(/budget/i);
  expect(scope.budget.usedBytes).toBe(before);
  expect(directory.size).toBe(1);
  expect(() => directory.finish()).toThrow(/budget/i);
  expect(directory.size).toBe(0);
  filler.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("enforces grouped output row limits and owns empty results", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const directory = OwnedGroupDirectory.allocate(
    scope,
    ["key"],
    {},
    { budget: { maxOutputRows: 0 } },
  );
  directory.getOrCreate(["x"]);
  expect(() => directory.finish()).toThrow(/output rows/);
  expect(directory.size).toBe(0);
  const empty = OwnedGroupDirectory.allocate(scope, ["key"], {}).finish();
  expect(empty.value).toEqual([]);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(empty.bytes);
  empty.release();
  expect(scope.budget.usedBytes).toBe(0);
});

it("round-trips mixed timestamp and bigint keys through the existing checkpoint format", () => {
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  const spec = { n: { op: "count" as const } };
  const keys = ["time", "id"];
  const directory = OwnedGroupDirectory.allocate(scope, keys, spec);
  const timestamp = new TimestampValue(-123456789n, "micros", false);
  const group = directory.getOrCreate([timestamp, -(10n ** 200n)]);
  directory.update(group, "n", 1);
  const snapshot = directory.checkpoint();
  const restored = restoreVectorGroupByState(keys, spec, snapshot.value);
  const result = directory.finish();
  expect(finalizeVectorGroupByRows(restored)).toEqual(result.value);
  expect(snapshot.value.groups[0]?.keyValues[0]).toEqual({
    type: "timestamp",
    epochNanoseconds: "-123456789",
    unit: "micros",
    isAdjustedToUTC: false,
  });
  snapshot.release();
  result.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

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

it.each(operations)("updates grouped %s across selected batches without snapshots", (op) => {
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  const spec = { value: { op, column: "value", quantile: 0.5 }, n: { op: "count" as const } };
  const owner = OwnedGroupDirectory.allocate(scope, ["key"], spec);
  const checkpoint = vi.spyOn(owner, "checkpoint");
  const reference = createVectorGroupByState(["key"], spec);
  for (let start = 0; start < 64; start += 4) {
    const batch = batchFromColumns({
      key: ["a", "b", "ignored", "a"],
      value: [start, null, start + 2, start + 3],
    });
    const selection = new Uint8Array([1, 1, 0, 1]);
    owner.updateBatch(batch, selection);
    updateVectorGroupByState(reference, batch, selection);
  }
  expect(checkpoint).not.toHaveBeenCalled();
  const result = owner.finish();
  expect(result.value).toEqual(finalizeVectorGroupByRows(reference));
  result.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("reuses grouped packed-key and expression inputs without retained growth", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const owner = OwnedGroupDirectory.allocate(scope, ["text"], {
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
  const used = scope.budget.usedBytes;
  for (let i = 0; i < 128; i++) owner.updateBatch(batch, new Uint8Array([1, 0]));
  expect(scope.budget.usedBytes).toBe(used);
  const result = owner.finish();
  expect(result.value).toEqual([{ text: "a", n: 130, unique: 1 }]);
  result.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("preflights grouped input shape and selection before inserting groups", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const owner = OwnedGroupDirectory.allocate(scope, ["key"], {
    n: { op: "count" },
    s: { op: "sum", column: "missing" },
  });
  const batch = batchFromColumns({ key: ["a"] });
  expect(() => owner.updateBatch(batch)).toThrow(/Unknown column/);
  expect(() => owner.updateBatch(batch, new Uint8Array(0))).toThrow(/batch shape/);
  expect(owner.size).toBe(0);
  owner.dispose();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it.each(operations)("restores and merges grouped %s checkpoints into shared ownership", (op) => {
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  const spec = { value: { op, column: "value", quantile: 0.5 } };
  const first = OwnedGroupDirectory.allocate(scope, ["key"], spec);
  const batch = batchFromColumns({ key: ["a", "b", "a", "b"], value: [1, null, 3, 7] });
  first.updateBatch(batch);
  const checkpoint = first.checkpoint();
  first.dispose();
  const resumed = OwnedGroupDirectory.allocate(scope, ["key"], spec);
  resumed.merge(checkpoint.value);
  resumed.merge(checkpoint.value);
  resumed.updateBatch(batch);
  const reference = createVectorGroupByState(["key"], spec);
  for (let i = 0; i < 3; i++) updateVectorGroupByState(reference, batch);
  const result = resumed.finish();
  expect(result.value).toEqual(finalizeVectorGroupByRows(reference));
  checkpoint.release();
  result.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("preflights every checkpoint group before applying mismatched state", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const owner = OwnedGroupDirectory.allocate(scope, ["key"], { n: { op: "count" } });
  expect(() =>
    owner.merge({
      groups: [
        { keyValues: ["a"], states: { n: { op: "count", count: 1 } } },
        { keyValues: ["b"], states: { other: { op: "count", count: 1 } } },
      ],
    }),
  ).toThrow(/specification mismatch/);
  expect(owner.size).toBe(0);
  expect(() => owner.merge({ groups: [{ keyValues: [], states: {} }] })).toThrow(/arity/);
  expect(owner.size).toBe(0);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("retains only an admitted merge prefix when buffered state exceeds its limit", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const owner = OwnedGroupDirectory.allocate(
    scope,
    ["key"],
    { value: { op: "median" } },
    { budget: { maxBufferedRows: 2 } },
  );
  const group = owner.getOrCreate(["a"]);
  owner.update(group, "value", 1);
  expect(() =>
    owner.merge({
      groups: [{ keyValues: ["a"], states: { value: { op: "median", values: [2, 3] } } }],
    }),
  ).toThrow(/budget/);
  const result = owner.finish();
  expect(result.value).toEqual([{ key: "a", value: 1.5 }]);
  result.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
