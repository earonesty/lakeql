import { MATERIALIZATION_CHARGES as c, MemoryBudget, MemoryScope, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { allocateDecodedArray, decodedBinaryValue, decodedStringValue } from "./decoded-array.js";
import { createParquetLake, writeParquet } from "./index.js";

it("admits decoded arrays before construction at the exact slot extent", () => {
  const bytes = c.array + 3 * c.arraySlot;
  const scope = new MemoryScope(new MemoryBudget(bytes));
  expect(allocateDecodedArray(3, scope)).toHaveLength(3);
  expect(scope.budget.usedBytes).toBe(bytes);
  scope.dispose();
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => allocateDecodedArray(3, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});
it("preserves byte aliases and admits only newly created view headers", () => {
  const input = new Uint8Array([1, 2, 3]);
  const scope = new MemoryScope(new MemoryBudget(c.view * 2));
  expect(decodedBinaryValue(input, scope)).toBe(input);
  expect(scope.budget.usedBytes).toBe(0);
  const view = decodedBinaryValue(new DataView(input.buffer, 1, 2), scope);
  expect(view.buffer).toBe(input.buffer);
  expect([...view]).toEqual([2, 3]);
  expect(decodedBinaryValue(null, scope)).toHaveLength(0);
  expect(scope.budget.usedBytes).toBe(2 * c.view);
  expect(() => decodedBinaryValue(input.buffer, scope)).toThrow(/budget/);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
it("rejects heterogeneous decoder values without arbitrary string coercion", () => {
  expect(decodedStringValue("é😀")).toBe("é😀");
  expect(decodedStringValue(null)).toBe("");
  expect(() =>
    decodedStringValue({
      toString() {
        throw new Error("must not execute");
      },
    }),
  ).toThrow(/Mixed decoded/);
  const scope = new MemoryScope(new MemoryBudget(c.view));
  expect(() => decodedBinaryValue(42, scope)).toThrow(/Mixed decoded/);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
it.each([
  { nullable: false, binary: false },
  { nullable: true, binary: false },
  { nullable: false, binary: true },
  { nullable: true, binary: true },
])("preserves actual delta byte-array output, nullable=$nullable binary=$binary", async ({
  nullable,
  binary,
}) => {
  const store = memoryStore();
  const text = ["prefix-é", nullable ? null : "prefix-a", "prefix-😀", "", "tail"];
  const values = binary
    ? text.map((value) => (value === null ? null : new TextEncoder().encode(value)))
    : text;
  await writeParquet(store, "delta.parquet", {
    schema: [
      { name: "root", num_children: 1 },
      {
        name: "value",
        type: "BYTE_ARRAY",
        repetition_type: nullable ? "OPTIONAL" : "REQUIRED",
        ...(binary ? {} : { converted_type: "UTF8" as const }),
      },
    ],
    rowGroupSize: [5],
    columnData: [
      {
        name: "value",
        encoding: "DELTA_BYTE_ARRAY",
        data: values,
      },
    ],
  });
  const pool = new MemoryBudget(1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const owner = await lake.path("delta.parquet").batchSize(2).run().toOwnedArray();
  try {
    expect(owner.value).toEqual(values.map((value) => ({ value })));
  } finally {
    owner.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("keeps sliced delta outputs valid after iterator return, binary=%s", async (binary) => {
  const { materializeBatchRows } = await import("lakeql-core");
  const { readOwnedParquetVectorBatches } = await import("./owned-vector-batches.js");
  const { readParquetMetadataFromFile } = await import("./metadata-cache.js");
  const { asyncBufferFromStore } = await import("./store-buffer.js");
  const store = memoryStore();
  const text = ["head", null, null, "é😀", "tail"];
  const values = binary
    ? text.map((value) => (value === null ? null : new TextEncoder().encode(value)))
    : text;
  await writeParquet(store, "windows.parquet", {
    columnData: [
      {
        name: "value",
        type: binary ? "BYTE_ARRAY" : "STRING",
        encoding: "DELTA_BYTE_ARRAY",
        data: values,
      },
    ],
  });
  const file = await asyncBufferFromStore(store, "windows.parquet");
  const metadata = await readParquetMetadataFromFile(file);
  for (const [start, end] of [
    [1, 3],
    [2, 5],
    [3, 5],
  ] as const) {
    const budget = new MemoryBudget(65536);
    const iterator = readOwnedParquetVectorBatches(file, metadata, budget, {
      columns: ["value"],
      rowStart: start,
      rowEnd: end,
      batchSize: end - start,
    })[Symbol.asyncIterator]();
    const result = await iterator.next();
    if (result.done) throw new Error("Missing sliced output");
    const retained = budget.usedBytes;
    await iterator.return?.();
    try {
      expect(budget.usedBytes).toBe(retained);
      expect(materializeBatchRows(result.value.value.batch)).toEqual(
        values.slice(start, end).map((value) => ({ value })),
      );
    } finally {
      result.value.release();
    }
    expect(budget.usedBytes).toBe(0);
  }
});

it.each([
  "DELTA_BYTE_ARRAY",
  "DELTA_LENGTH_BYTE_ARRAY",
] as const)("reads all-null encoded pages under ownership, encoding=%s", async (encoding) => {
  const store = memoryStore();
  await writeParquet(store, "nulls.parquet", {
    columnData: [{ name: "value", type: "STRING", encoding, data: [null, null, null] }],
  });
  const pool = new MemoryBudget(65536);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const owner = await lake.path("nulls.parquet").batchSize(1).run().toOwnedArray();
  try {
    expect(owner.value).toEqual([{ value: null }, { value: null }, { value: null }]);
  } finally {
    owner.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it("compacts sparse decoded arrays only when needed and admits the exact result", async () => {
  const { compactDecodedValues } = await import("./decoded-array.js");
  const typed = new Int32Array([1, 2]);
  const dense = ["a", "b"];
  const emptyBudget = new MemoryScope(new MemoryBudget(0));
  expect(compactDecodedValues(typed, emptyBudget)).toBe(typed);
  expect(compactDecodedValues(dense, emptyBudget)).toBe(dense);
  emptyBudget.dispose();
  const payload = new Uint8Array([1, 2]);
  const sparse = [null, payload, undefined, payload];
  const bytes = c.array + 2 * c.arraySlot;
  const scope = new MemoryScope(new MemoryBudget(bytes));
  const result = compactDecodedValues(sparse, scope);
  expect(result).toEqual([payload, payload]);
  expect(result[0]).toBe(payload);
  expect(sparse).toEqual([null, payload, undefined, payload]);
  expect(scope.budget.usedBytes).toBe(bytes);
  scope.dispose();
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => compactDecodedValues(sparse, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  const allNull = new MemoryScope(new MemoryBudget(c.array));
  expect(compactDecodedValues([null, undefined], allNull)).toEqual([]);
  expect(allNull.budget.usedBytes).toBe(c.array);
  allNull.dispose();
});
