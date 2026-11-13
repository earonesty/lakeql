import { expect, it, vi } from "vitest";
import { type Batch, batchFromColumns, batchFromVectors, materializeBatchRows } from "./batch.js";
import { materializeOwnedBatchRows, planBatchMaterialization } from "./materialization-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { timestampFromEpoch } from "./timestamp.js";

it("preflights packed text without decoding and rejects before creating output", () => {
  const data = new TextEncoder().encode("猫abc");
  const batch = batchFromVectors({
    text: { type: "utf8-view", data, length: 2, ranges: Uint32Array.of(0, 3, 3, 3) },
  });
  const decoder = vi.spyOn(TextDecoder.prototype, "decode");
  try {
    const plan = planBatchMaterialization(batch);
    expect(plan.rows).toBe(2);
    expect(decoder).not.toHaveBeenCalled();
    const tight = new MemoryScope(new MemoryBudget(plan.allocationBytes - 1));
    expect(() => plan.execute(tight)).toThrow(/budget/);
    expect(tight.budget.peakBytes).toBe(0);
    expect(decoder).not.toHaveBeenCalled();
    tight.dispose();
    const exact = new MemoryScope(new MemoryBudget(plan.allocationBytes));
    const rows = plan.execute(exact);
    expect(rows.value).toEqual([{ text: "猫" }, { text: "abc" }]);
    expect(decoder).toHaveBeenCalledTimes(2);
    expect(exact.budget.peakBytes).toBe(plan.allocationBytes);
    expect(plan.allocationBytes - plan.retainedBytes).toBe(432);
    expect(exact.budget.usedBytes).toBe(plan.retainedBytes);
    exact.dispose();
    expect(exact.budget.usedBytes).toBe(0);
    expect(() => rows.value).toThrow(/released/);
  } finally {
    decoder.mockRestore();
  }
});

it("materializes nested, nullable, dictionary and wide timestamp shapes under one output charge", () => {
  const batch = batchFromColumns({
    list: [[1, null, 3], null],
    struct: [
      { field: "cat", nested: [true, false] },
      { field: "", nested: [] },
    ],
    map: [new Map([["key", 7]]), new Map()],
    at: [timestampFromEpoch(1n << 64n, "nanos"), timestampFromEpoch(-1n, "nanos")],
  });
  batch.columns.dictionary = {
    type: "dict",
    indices: Uint32Array.of(1, 0),
    dictionary: { type: "utf8", values: ["a", "b"] },
  };
  const expected = materializeBatchRows(batch);
  const plan = planBatchMaterialization(batch);
  const scope = new MemoryScope(new MemoryBudget(plan.allocationBytes));
  expect(plan.execute(scope).value).toEqual(expected);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const selection = Uint8Array.of(0, 1);
  const selected = planBatchMaterialization(batch, selection);
  expect(selected.rows).toBe(1);
  expect(selected.allocationBytes).toBeLessThan(plan.allocationBytes);
  const other = new MemoryScope(new MemoryBudget(selected.allocationBytes));
  expect(selected.execute(other).value).toEqual(expected.slice(1));
  other.dispose();
});

it("keeps borrowed binary backing and output charged until the final output owner releases", () => {
  const batch: Batch = {
    rowCount: 1,
    columns: {
      bytes: { type: "binary-view", data: Uint8Array.of(1, 2, 3), fixedLength: 3, length: 1 },
    },
  };
  const plan = planBatchMaterialization(batch);
  const pool = new MemoryBudget(plan.allocationBytes + 3);
  const source = new MemoryScope(pool);
  source.allocate(3, "source backing", () => batch.columns.bytes);
  const input = source.retainAllocations(batch);
  const output = materializeOwnedBatchRows(input);
  const retained = output.retain();
  source.dispose();
  input.release();
  output.release();
  expect(pool.usedBytes).toBe(plan.retainedBytes + 3);
  expect(retained.value).toEqual([{ bytes: Uint8Array.of(1, 2, 3) }]);
  retained.release();
  expect(pool.usedBytes).toBe(0);
});

it("rejects malformed physical shapes before materializing", () => {
  const mismatched: Batch = {
    rowCount: 2,
    columns: { x: { type: "i32", values: Int32Array.of(1) } },
  };
  expect(() => planBatchMaterialization(mismatched)).toThrow(/length/);
  expect(() => planBatchMaterialization({ rowCount: 0, columns: {} }, Uint8Array.of(1))).toThrow(
    /extent/,
  );
  expect(() =>
    planBatchMaterialization(
      batchFromVectors({
        x: { type: "utf8-view", data: Uint8Array.of(1), length: 1, ranges: Uint32Array.of(0, 2) },
      }),
    ),
  ).toThrow(/range/);
});

it("detaches copied output from source ownership and unwinds failed decoding", () => {
  const bytes = new TextEncoder().encode("hello");
  const batch = batchFromVectors({
    text: { type: "utf8-view", data: bytes, length: 1, fixedLength: 5 },
  });
  const plan = planBatchMaterialization(batch);
  expect(plan.retainsInput).toBe(false);
  const pool = new MemoryBudget(plan.allocationBytes + bytes.length);
  const source = new MemoryScope(pool);
  source.allocate(bytes.length, "input", () => bytes);
  const input = source.retainAllocations(batch);
  const decoder = vi.spyOn(TextDecoder.prototype, "decode").mockImplementation(() => {
    throw new Error("decode failure");
  });
  try {
    expect(() => materializeOwnedBatchRows(input)).toThrow(/decode failure/);
  } finally {
    decoder.mockRestore();
  }
  expect(pool.usedBytes).toBe(bytes.length);
  const output = materializeOwnedBatchRows(input);
  input.release();
  source.dispose();
  expect(pool.usedBytes).toBe(plan.retainedBytes);
  expect(output.value).toEqual([{ text: "hello" }]);
  output.release();
  expect(pool.usedBytes).toBe(0);
});

it("plans only own enumerable fields, including nested structs", () => {
  const child = { type: "i32", values: Int32Array.of(7) } as const;
  const nested = Object.assign(
    Object.create({ inherited: { type: "i32", values: new Int32Array(0) } }),
    { value: child },
  );
  const columns = Object.assign(
    Object.create({ inherited: { type: "i32", values: new Int32Array(0) } }),
    {
      record: { type: "struct", fields: nested, length: 1 },
    },
  );
  const batch: Batch = { rowCount: 1, columns };
  const plan = planBatchMaterialization(batch);
  const ownPlan = planBatchMaterialization({
    rowCount: 1,
    columns: {
      record: { type: "struct", fields: { value: child }, length: 1 },
    },
  });
  expect(plan.allocationBytes).toBe(ownPlan.allocationBytes);
  const scope = new MemoryScope(new MemoryBudget(plan.allocationBytes));
  try {
    expect(plan.execute(scope).value).toEqual([{ record: { value: 7 } }]);
  } finally {
    scope.dispose();
  }
});
