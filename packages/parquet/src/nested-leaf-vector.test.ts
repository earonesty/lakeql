import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import { MemoryBudget, MemoryScope, type Vector, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { nestedLeafVector } from "./nested-leaf-vector.js";
import { lakeqlParquetParsers } from "./parsers.js";

const decoder = {
  type: "INT64",
  element: { name: "value", type: "INT64", converted_type: "UINT_64" },
  parsers: DEFAULT_PARSERS,
  pathInSchema: ["items", "value"],
  schemaPath: [],
} as ColumnDecoder;
const physical = (values: BigInt64Array | Uint32Array) => ({
  values,
  packedBinary: undefined,
  dictionaryEncoded: false,
  levels: {
    presentCount: values.length,
    rowsStarted: 1,
    valueOffset: 0,
    repetition: Uint8Array.of(0, 1, 1),
    definition: Uint8Array.of(1, 0, 1),
  },
});
it("keeps compact unsigned values and physical levels with exact-budget admission", () => {
  const input = physical(BigInt64Array.of(-1n, 7n));
  const scope = new MemoryScope(new MemoryBudget(4096));
  const vector = nestedLeafVector(input, decoder, scope);
  expect(vector?.type).toBe("u64");
  if (vector?.type !== "u64") throw Error("Expected unsigned vector");
  expect(vector.values.buffer).toBe(input.values.buffer);
  expect(vector.values.length).toBe(2);
  expect(vectorValue(vector, 0)).toBe(18446744073709551615n);
  expect(Array.from(input.levels.definition)).toEqual([1, 0, 1]);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(nestedLeafVector(input, decoder, exact)).toBeDefined();
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => nestedLeafVector(input, decoder, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});
it("borrows logical dictionaries and rolls back invalid indices without releasing input", () => {
  const input = { ...physical(Uint32Array.of(1, 0)), dictionaryEncoded: true };
  const dictionary: Vector = { type: "utf8", values: ["first", "second"] };
  const scope = new MemoryScope(new MemoryBudget(4096));
  const vector = nestedLeafVector(input, decoder, scope, dictionary);
  expect(vector?.type).toBe("dict");
  if (vector?.type !== "dict") throw Error("Expected dictionary vector");
  expect(vector.indices).toBe(input.values);
  expect(vector.dictionary).toBe(dictionary);
  expect(vectorValue(vector, 0)).toBe("second");
  scope.dispose();
  const invalid = new MemoryScope(new MemoryBudget(4096));
  expect(() => nestedLeafVector(input, decoder, invalid)).toThrow(/Invalid/);
  input.values[1] = 2;
  expect(() => nestedLeafVector(input, decoder, invalid, dictionary)).toThrow(/Invalid/);
  expect(invalid.budget.usedBytes).toBe(0);
  expect(vectorValue(dictionary, 0)).toBe("first");
  invalid.dispose();
});

it("shares packed UTF8 backing and declines unsupported logical annotations before admission", () => {
  const payload = new TextEncoder().encode("雪");
  const ranges = Uint32Array.of(0, 3, 3, 0);
  const input = {
    ...physical(Uint32Array.of(0, 0)),
    values: undefined,
    packedBinary: { count: 2, payload, ranges, value: () => payload },
  };
  const textDecoder = {
    ...decoder,
    type: "BYTE_ARRAY",
    element: {
      name: "value",
      type: "BYTE_ARRAY",
      converted_type: "UTF8",
    },
  } as ColumnDecoder;
  const scope = new MemoryScope(new MemoryBudget(4096));
  const vector = nestedLeafVector(input, textDecoder, scope);
  if (vector?.type !== "utf8-view") throw Error("Expected packed text vector");
  expect(vector.data).toBe(payload);
  expect(vector.ranges).toBe(ranges);
  expect(vectorValue(vector, 0)).toBe("雪");
  expect(vectorValue(vector, 1)).toBe("");
  scope.dispose();
  const unsupported = new MemoryScope(new MemoryBudget(0));
  expect(
    nestedLeafVector(
      input,
      {
        ...textDecoder,
        element: { ...textDecoder.element, converted_type: "INTERVAL" },
      },
      unsupported,
    ),
  ).toBeUndefined();
  expect(unsupported.budget.peakBytes).toBe(0);
  unsupported.dispose();
});

it.each([
  {
    name: "TIMESTAMP",
    element: { logical_type: { type: "TIMESTAMP", unit: "NANOS", isAdjustedToUTC: true } },
    bytes: [new Uint8Array(12).fill(255), new Uint8Array(12)],
    expected: [
      lakeqlParquetParsers.timestampFromNanoseconds(-1n),
      lakeqlParquetParsers.timestampFromNanoseconds(0n),
    ],
  },
  {
    name: "BSON",
    element: { converted_type: "BSON" },
    bytes: [
      Uint8Array.from([12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0]),
      Uint8Array.from([15, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 10, 98, 0, 0]),
    ],
    expected: [{ a: 1 }, { a: 1, b: null }],
  },
  {
    name: "JSON",
    element: { converted_type: "JSON" },
    bytes: [new TextEncoder().encode('{"a":[1,null]}'), new TextEncoder().encode("null")],
    expected: [{ a: [1, null] }, null],
  },
  {
    name: "FLOAT16",
    element: { logical_type: { type: "FLOAT16" } },
    bytes: [Uint8Array.of(0, 0x80), Uint8Array.of(0, 0x7c)],
    expected: [-0, Infinity],
  },
  {
    name: "DECIMAL",
    element: { converted_type: "DECIMAL", scale: 2, precision: 4 },
    bytes: [Uint8Array.of(123), Uint8Array.of(255)],
    expected: [1.23, -0.01],
  },
])("converts packed $name leaves with admitted temporary views", ({ element, bytes, expected }) => {
  const packedBinary = {
    count: bytes.length,
    payload: new Uint8Array(),
    value: (index: number) => bytes[index] ?? new Uint8Array(),
  };
  const page = { ...physical(Uint32Array.of(0, 0)), values: undefined, packedBinary };
  const logicalDecoder = {
    ...decoder,
    parsers: lakeqlParquetParsers,
    type: "BYTE_ARRAY",
    element: {
      name: "value",
      type: "BYTE_ARRAY",
      ...element,
    },
  } as ColumnDecoder;
  const scope = new MemoryScope(new MemoryBudget(65536));
  const vector = nestedLeafVector(page, logicalDecoder, scope);
  if (!vector) throw Error("Missing logical vector");
  expect(expected.map((_, index) => vectorValue(vector, index))).toEqual(expected);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(nestedLeafVector(page, logicalDecoder, exact)).toBeDefined();
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => nestedLeafVector(page, logicalDecoder, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("rolls back prior packed JSON values after malformed input", () => {
  const bytes = [new TextEncoder().encode('{"a":[1,2]}'), new TextEncoder().encode("{")];
  const page = {
    ...physical(Uint32Array.of(0, 0)),
    values: undefined,
    packedBinary: {
      count: 2,
      payload: new Uint8Array(),
      value: (i: number) => bytes[i] ?? new Uint8Array(),
    },
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(() =>
    nestedLeafVector(
      page,
      {
        ...decoder,
        type: "BYTE_ARRAY",
        element: { name: "value", type: "BYTE_ARRAY", converted_type: "JSON" },
      },
      scope,
    ),
  ).toThrow();
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
