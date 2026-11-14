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

it("plans one packed row independently of batch size and validates its index", async () => {
  const { planBatchRowMaterialization } = await import("./materialization-plan.js");
  const data = new TextEncoder().encode("界".repeat(4096));
  const ranges = new Uint32Array(512 * 2);
  for (let i = 0; i < 512; i++) ranges[i * 2 + 1] = data.length;
  const batch = batchFromVectors({ text: { type: "utf8-view", data, length: 512, ranges } });
  const plan = planBatchRowMaterialization(batch, 511);
  expect(plan.rows).toBe(1);
  expect(planBatchMaterialization(batch).allocationBytes).toBeGreaterThan(
    plan.allocationBytes * 100,
  );
  const budget = new MemoryBudget(plan.allocationBytes);
  const scope = new MemoryScope(budget);
  try {
    const rows = plan.execute(scope);
    expect(rows.value).toEqual([{ text: "界".repeat(4096) }]);
    rows.release();
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
  for (const index of [-1, 512, 0.5, NaN])
    expect(() => planBatchRowMaterialization(batch, index)).toThrow();
  const tight = new MemoryScope(new MemoryBudget(plan.allocationBytes - 1));
  try {
    expect(() => plan.execute(tight)).toThrow(/budget/);
  } finally {
    tight.dispose();
  }
  expect(tight.budget.usedBytes).toBe(0);
});

it("admits planner traversal frames and releases them on success and failure", () => {
  const batch = batchFromColumns({ value: [1, 2] });
  const pool = new MemoryBudget(4096);
  const scope = new MemoryScope(pool);
  const plan = planBatchMaterialization(batch, undefined, scope);
  expect(plan.rows).toBe(2);
  expect(pool.peakBytes).toBeGreaterThan(0);
  expect(pool.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(pool.peakBytes - 1));
  expect(() => planBatchMaterialization(batch, undefined, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  const vector = {
    type: "dict" as const,
    indices: Uint32Array.of(0),
    dictionary: batch.columns.value ?? { type: "null" as const, length: 1 },
  };
  vector.dictionary = vector;
  expect(() =>
    planBatchMaterialization({ rowCount: 1, columns: { cycle: vector } }, undefined, scope),
  ).toThrow(/Cyclic/);
  expect(pool.usedBytes).toBe(0);
  tight.dispose();
  scope.dispose();
});

it.each([
  { name: "empty", rowCount: 0, selection: undefined, expectedRows: 0 },
  { name: "all", rowCount: 3, selection: undefined, expectedRows: 3 },
  { name: "selected", rowCount: 3, selection: Uint8Array.of(0, 2, 1), expectedRows: 1 },
  { name: "excluded", rowCount: 3, selection: new Uint8Array(3), expectedRows: 0 },
])("preserves $name rows in a zero-column materialization", ({
  rowCount,
  selection,
  expectedRows,
}) => {
  const plan = planBatchMaterialization({ rowCount, columns: {} }, selection);
  expect(plan.rows).toBe(expectedRows);
  const scope = new MemoryScope(new MemoryBudget(plan.allocationBytes));
  try {
    expect(plan.execute(scope).value).toEqual(Array.from({ length: expectedRows }, () => ({})));
    expect(scope.budget.usedBytes).toBe(plan.retainedBytes);
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(plan.allocationBytes - 1));
  try {
    expect(() => plan.execute(tight)).toThrow(/budget/);
  } finally {
    tight.dispose();
  }
  expect(tight.budget.usedBytes).toBe(0);
});

it("reuses immutable dictionary text only when optional workspace fits", () => {
  const batch = batchFromVectors({
    text: {
      type: "dict",
      indices: Uint32Array.from({ length: 16 }, (_, i) => i % 2),
      dictionary: {
        type: "utf8-view",
        data: Uint8Array.of(239, 187, 191, 255, 65, 66, 67, 68),
        fixedLength: 4,
        length: 2,
      },
    },
  });
  const expected = materializeBatchRows(batch);
  const plan = planBatchMaterialization(batch);
  for (const extra of [0, 175, 176]) {
    const scope = new MemoryScope(new MemoryBudget(plan.allocationBytes + extra));
    const decode = vi.spyOn(TextDecoder.prototype, "decode");
    try {
      const result = plan.execute(scope);
      expect(result.value).toEqual(expected);
      expect(decode).toHaveBeenCalledTimes(extra === 176 ? 2 : 16);
      expect(scope.budget.peakBytes).toBe(plan.allocationBytes + (extra === 176 ? 176 : 0));
      expect(scope.budget.usedBytes).toBe(plan.retainedBytes);
    } finally {
      decode.mockRestore();
      scope.dispose();
    }
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("keeps sparse unique dictionaries direct using the selected row count", () => {
  const dictionary = {
    type: "utf8-view" as const,
    data: new TextEncoder().encode("abcdefgh"),
    fixedLength: 1,
    length: 8,
  };
  const batch = batchFromVectors({
    text: { type: "dict", indices: Uint32Array.from({ length: 16 }, (_, i) => i % 8), dictionary },
  });
  const selection = Uint8Array.from({ length: 16 }, (_, i) => Number(i === 1 || i === 7));
  const plan = planBatchMaterialization(batch, selection);
  const scope = new MemoryScope(new MemoryBudget(plan.allocationBytes + 4096));
  const decode = vi.spyOn(TextDecoder.prototype, "decode");
  try {
    expect(plan.execute(scope).value).toEqual([{ text: "b" }, { text: "h" }]);
    expect(decode).toHaveBeenCalledTimes(2);
    expect(scope.budget.peakBytes).toBe(plan.allocationBytes);
  } finally {
    decode.mockRestore();
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("releases optional dictionary workspace and output admission when decoding throws", () => {
  const batch = batchFromVectors({
    text: {
      type: "dict",
      indices: new Uint32Array(16),
      dictionary: { type: "utf8-view", data: Uint8Array.of(65), fixedLength: 1, length: 1 },
    },
  });
  const plan = planBatchMaterialization(batch);
  const scope = new MemoryScope(new MemoryBudget(plan.allocationBytes + 4096));
  const error = new Error("decoder failed");
  const decode = vi.spyOn(TextDecoder.prototype, "decode").mockImplementation(() => {
    throw error;
  });
  try {
    expect(() => plan.execute(scope)).toThrow(error);
    expect(scope.budget.usedBytes).toBe(0);
  } finally {
    decode.mockRestore();
    scope.dispose();
  }
});

it("caches empty and null dictionary values without bypassing outer validity", () => {
  const batch = batchFromVectors({
    text: {
      type: "dict",
      indices: Uint32Array.from({ length: 12 }, (_, i) => i % 3),
      valid: Uint8Array.from({ length: 12 }, (_, i) => Number(i !== 2)),
      dictionary: {
        type: "utf8-view",
        data: Uint8Array.of(65),
        ranges: Uint32Array.of(0, 0, 0, 1, 0, 1),
        valid: Uint8Array.of(1, 0, 1),
        length: 3,
      },
    },
  });
  const expected = materializeBatchRows(batch);
  const plan = planBatchMaterialization(batch);
  const scope = new MemoryScope(new MemoryBudget(plan.allocationBytes + 4096));
  const decode = vi.spyOn(TextDecoder.prototype, "decode");
  try {
    expect(plan.execute(scope).value).toEqual(expected);
    expect(decode).toHaveBeenCalledTimes(2);
  } finally {
    decode.mockRestore();
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("does not bypass a parent stage allowance to acquire optional dictionary storage", () => {
  const batch = batchFromVectors({
    text: {
      type: "dict",
      indices: new Uint32Array(16),
      dictionary: { type: "utf8-view", data: Uint8Array.of(65), fixedLength: 1, length: 1 },
    },
  });
  const plan = planBatchMaterialization(batch);
  const root = new MemoryScope(new MemoryBudget(plan.allocationBytes + 4096));
  const stage = root.childStage(plan.allocationBytes, "caller stage");
  const decode = vi.spyOn(TextDecoder.prototype, "decode");
  try {
    expect(plan.execute(stage).value).toHaveLength(16);
    expect(decode).toHaveBeenCalledTimes(16);
    expect(root.budget.peakBytes).toBe(plan.allocationBytes);
  } finally {
    decode.mockRestore();
    root.dispose();
  }
  expect(root.budget.usedBytes).toBe(0);
});
