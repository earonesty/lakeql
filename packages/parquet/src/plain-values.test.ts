import type { ParquetType } from "hyparquet/src/types.js";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { readByteStreamSplitValues, readPlainValues } from "./plain-values.js";

const cases = [
  { type: "INT32", input: Int32Array.of(-7, 42) },
  { type: "INT64", input: BigInt64Array.of(-7n, 9007199254740993n) },
  { type: "FLOAT", input: Float32Array.of(-1.5, 42.25) },
  { type: "DOUBLE", input: Float64Array.of(-1.5, Number.MAX_VALUE) },
] satisfies {
  type: ParquetType;
  input: Int32Array | BigInt64Array | Float32Array | Float64Array;
}[];

it.each(cases)("admits misaligned $type copies exactly and preserves aligned aliases", ({
  type,
  input,
}) => {
  const raw = new Uint8Array(input.byteLength + 3);
  raw.set(new Uint8Array(input.buffer), 3);
  const scope = new MemoryScope(new MemoryBudget(input.byteLength));
  // Both the DataView offset and reader offset participate in alignment.
  const reader = { view: new DataView(raw.buffer, 1), offset: 2 };
  const output = readPlainValues(reader, type, input.length, undefined, scope);
  expect(Array.from(output)).toEqual(Array.from(input));
  expect(reader.offset).toBe(2 + input.byteLength);
  expect(scope.budget.usedBytes).toBe(input.byteLength);
  const tight = new MemoryScope(new MemoryBudget(input.byteLength - 1));
  const rejectedReader = { view: new DataView(raw.buffer), offset: 3 };
  expect(() => readPlainValues(rejectedReader, type, input.length, undefined, tight)).toThrow(
    /budget/,
  );
  expect(rejectedReader.offset).toBe(3);
  expect(tight.budget.peakBytes).toBe(0);
  const aligned = readPlainValues(
    { view: new DataView(input.buffer), offset: 0 },
    type,
    input.length,
    undefined,
    tight,
  );
  expect(ArrayBuffer.isView(aligned) && aligned.buffer).toBe(input.buffer);
  expect(tight.budget.usedBytes).toBe(0);
  scope.dispose();
  tight.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects malformed numeric lengths before allocating or advancing the reader", () => {
  const scope = new MemoryScope(new MemoryBudget(64));
  for (const count of [-1, 1.5, Infinity, Number.MAX_SAFE_INTEGER, 2]) {
    const reader = { view: new DataView(new ArrayBuffer(5)), offset: 1 };
    expect(() => readPlainValues(reader, "INT32", count, undefined, scope)).toThrow(
      /Invalid PLAIN/,
    );
    expect(reader.offset).toBe(1);
  }
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});

it.each(cases)("admits byte-stream-split $type output once", ({ type, input }) => {
  const bytes = new Uint8Array(input.buffer);
  const split = new Uint8Array(bytes.length + 3);
  const width = input.BYTES_PER_ELEMENT;
  for (let lane = 0; lane < width; lane += 1) {
    for (let index = 0; index < input.length; index += 1)
      split[3 + lane * input.length + index] = bytes[index * width + lane] ?? 0;
  }
  const scope = new MemoryScope(new MemoryBudget(bytes.length));
  const reader = { view: new DataView(split.buffer, 1), offset: 2 };
  const output = readByteStreamSplitValues(reader, type, input.length, scope);
  expect([...output]).toEqual([...input]);
  expect(scope.budget.usedBytes).toBe(bytes.length);
  expect(reader.offset).toBe(2 + bytes.length);
  const retained = scope.retainAllocation(output);
  expect(scope.tryReleaseAllocation(output)).toBe(true);
  expect(scope.budget.usedBytes).toBe(bytes.length);
  expect([...retained.value]).toEqual([...input]);
  retained.release();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(bytes.length - 1));
  const rejected = { view: new DataView(split.buffer), offset: 3 };
  expect(() => readByteStreamSplitValues(rejected, type, input.length, tight)).toThrow(/budget/);
  expect(rejected.offset).toBe(3);
  expect(tight.budget.peakBytes).toBe(0);
  const truncated = { view: new DataView(split.buffer, 0, split.length - 1), offset: 3 };
  expect(() => readByteStreamSplitValues(truncated, type, input.length, scope)).toThrow(/range/);
  scope.dispose();
  tight.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("admits signed INT96 carriers without losing high words to low-word sign extension", async () => {
  const { MATERIALIZATION_CHARGES: c } = await import("lakeql-core");
  const values = [0n, (3n << 64n) | (1n << 63n), -1n];
  const bytes = new Uint8Array(1 + 12 * values.length);
  const view = new DataView(bytes.buffer);
  for (let i = 0; i < values.length; i++) {
    view.setBigUint64(1 + i * 12, BigInt.asUintN(64, values[i] ?? 0n), true);
    view.setInt32(9 + i * 12, Number((values[i] ?? 0n) >> 64n), true);
  }
  const borrowedReader = { view, offset: 1 };
  expect(readPlainValues(borrowedReader, "INT96", values.length, undefined, undefined)).toEqual(
    values,
  );
  expect(borrowedReader.offset).toBe(bytes.length);
  const invalidReader = { view, offset: 2 };
  expect(() =>
    readPlainValues(invalidReader, "INT96", values.length, undefined, undefined),
  ).toThrow(/extent/);
  expect(invalidReader.offset).toBe(2);
  const retained = c.array + values.length * (c.arraySlot + c.bigint);
  const peak = retained + 3 * c.bigint;
  const scope = new MemoryScope(new MemoryBudget(peak));
  const reader = { view, offset: 1 };
  expect(readPlainValues(reader, "INT96", values.length, undefined, scope)).toEqual(values);
  expect(reader.offset).toBe(bytes.length);
  expect(scope.budget.usedBytes).toBe(retained);
  scope.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() =>
    readPlainValues({ view, offset: 1 }, "INT96", values.length, undefined, tight),
  ).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  const truncated = new MemoryScope(new MemoryBudget(peak));
  expect(() =>
    readPlainValues(
      { view: new DataView(bytes.buffer, 0, bytes.length - 1), offset: 1 },
      "INT96",
      values.length,
      undefined,
      truncated,
    ),
  ).toThrow(/extent/);
  expect(truncated.budget.peakBytes).toBe(0);
  truncated.dispose();
});

it.each([
  undefined,
  2,
])("admits borrowed PLAIN binary views and releases range scratch, width=%s", async (width) => {
  const { MATERIALIZATION_CHARGES: c } = await import("lakeql-core");
  const bytes =
    width === undefined
      ? new Uint8Array([2, 0, 0, 0, 7, 8, 2, 0, 0, 0, 9, 10])
      : new Uint8Array([7, 8, 9, 10]);
  const type = width === undefined ? "BYTE_ARRAY" : "FIXED_LEN_BYTE_ARRAY";
  const scope = new MemoryScope(new MemoryBudget(65536));
  const reader = { view: new DataView(bytes.buffer), offset: 0 };
  const output = readPlainValues(reader, type, 2, width, scope) as Uint8Array[];
  expect(output.map((value) => [...value])).toEqual([
    [7, 8],
    [9, 10],
  ]);
  expect(output[0]?.buffer).toBe(bytes.buffer);
  expect(scope.budget.usedBytes).toBe(c.array + 2 * (c.arraySlot + c.view));
  expect(reader.offset).toBe(bytes.length);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  const rejected = { view: new DataView(bytes.buffer), offset: 0 };
  expect(() => readPlainValues(rejected, type, 2, width, tight)).toThrow(/budget/);
  expect(rejected.offset).toBe(0);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it.each([
  undefined,
  0,
  -1,
  1.5,
  Number.MAX_SAFE_INTEGER,
])("rejects invalid fixed binary widths before allocation: %s", (width) => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const reader = { view: new DataView(new ArrayBuffer(8)), offset: 0 };
  expect(() => readPlainValues(reader, "FIXED_LEN_BYTE_ARRAY", 2, width, scope)).toThrow();
  expect(reader.offset).toBe(0);
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});

it("rejects every truncated variable binary extent without committing or retaining scratch", () => {
  const bytes = new Uint8Array([2, 0, 0, 0, 7, 8, 2, 0, 0, 0, 9, 10]);
  for (let length = 0; length < bytes.length; length++) {
    const scope = new MemoryScope(new MemoryBudget(65536));
    const reader = { view: new DataView(bytes.buffer, 0, length), offset: 0 };
    expect(() => readPlainValues(reader, "BYTE_ARRAY", 2, undefined, scope)).toThrow();
    expect(reader.offset).toBe(0);
    expect(scope.budget.usedBytes).toBe(0);
    expect(scope.budget.peakBytes).toBe(0);
    scope.dispose();
  }
});
