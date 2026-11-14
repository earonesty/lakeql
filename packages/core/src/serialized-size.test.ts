import { expect, it } from "vitest";
import {
  type Batch,
  batchFromColumns,
  materializeBatchRows,
  type Vector,
  vectorValue,
} from "./batch.js";
import { jsonSafeValue } from "./evaluator.js";
import { intervalValue } from "./interval.js";
import { stableStringify } from "./manifest.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import {
  serializedBatchBytes,
  serializedValueBytes,
  serializedVectorBytes,
} from "./serialized-size.js";
import { timestampFromEpoch } from "./timestamp.js";

const reference = (value: unknown) =>
  new TextEncoder().encode(stableStringify(jsonSafeValue(value))).length;

it("counts nested maps with duplicate keys, null keys, empty rows and dictionary selection", () => {
  const list: Vector = {
    type: "list",
    offsets: Int32Array.of(0, 2, 3, 3, 4, 6),
    child: { type: "i64", values: BigInt64Array.of(1n, 9007199254740993n, 2n, 3n, 4n, 5n) },
  };
  const map: Vector = {
    type: "map",
    offsets: Int32Array.of(0, 5, 5),
    keys: {
      type: "utf8",
      values: ["same", "__proto__", "same", "ignored", "猫"],
      valid: Uint8Array.of(1, 1, 1, 0, 1),
    },
    values: list,
  };
  const vector: Vector = {
    type: "dict",
    indices: Uint32Array.of(1, 0, 0, 0),
    valid: Uint8Array.of(1, 1, 0, 1),
    dictionary: {
      type: "struct",
      length: 2,
      fields: { map, flag: { type: "bool", values: Uint8Array.of(1, 0) } },
    },
  };
  for (let row = 0; row < 4; row++)
    expect(serializedVectorBytes(vector, row)).toBe(reference(vectorValue(vector, row)));
});

it("preserves semantic interval normalization in physical structs", () => {
  const vector: Vector = {
    type: "struct",
    length: 1,
    fields: {
      kind: { type: "utf8", values: ["interval"] },
      months: { type: "i32", values: Int32Array.of(2) },
      days: { type: "i32", values: Int32Array.of(3) },
      nanoseconds: { type: "utf8", values: ["123"] },
    },
  };
  expect(serializedVectorBytes(vector, 0)).toBe(reference(vectorValue(vector, 0)));
});

it("does not serialize overwritten map values", () => {
  const vector: Vector = {
    type: "map",
    offsets: Int32Array.of(0, 2),
    keys: { type: "utf8", values: ["k", "k"] },
    values: {
      type: "timestamp",
      values: BigInt64Array.of(9223372036854775807n, 0n),
      unit: "ms",
      isAdjustedToUTC: true,
    },
  };
  // The overwritten timestamp cannot be converted to an ISO Date, but does not
  // participate in the public last-key-wins map value or its serialization.
  expect(serializedVectorBytes(vector, 0)).toBe(reference(vectorValue(vector, 0)));
});

it("does not inspect unused child payload and rejects invalid dictionary indices", () => {
  const empty: Vector = {
    type: "list",
    offsets: Int32Array.of(100000, 100000),
    get child(): Vector {
      throw new Error("Unused child inspected");
    },
  };
  expect(serializedVectorBytes(empty, 0)).toBe(2);
  const hidden: Vector = {
    type: "list",
    offsets: Int32Array.of(0, 100000),
    valid: Uint8Array.of(0),
    get child(): Vector {
      throw new Error("Null child inspected");
    },
  };
  expect(serializedVectorBytes(hidden, 0)).toBe(4);
  expect(() =>
    serializedVectorBytes({ type: "dict", indices: Uint32Array.of(10), dictionary: empty }, 0),
  ).toThrow(/bounds/);
});
const sparse = new Array<unknown>(3);
sparse[0] = undefined;
sparse[2] = null;
const values: unknown[] = [
  null,
  undefined,
  true,
  false,
  0,
  -0,
  1e21,
  1e-7,
  Number.NaN,
  Infinity,
  -Infinity,
  9007199254740991n,
  9007199254740993n,
  -9007199254740993n,
  "",
  "é猫😀\ud800\n\u0000",
  new Uint8Array(1024),
  Uint8Array.of(0, 255, 127),
  timestampFromEpoch(123456789n, "nanos"),
  intervalValue("2 days 3 seconds"),
  new Date(0),
  new Map([["x", 1]]),
  Uint16Array.of(1, 65535),
  Symbol("x"),
  [],
  sparse,
  { omitted: undefined, included: null },
  Object.fromEntries([
    ["__proto__", { x: 1 }],
    ["constructor", 2],
  ]),
];

it("matches the existing serialized metric for primitive and structured edge cases", () => {
  for (const value of values) {
    expect(serializedValueBytes(value)).toBe(reference(value));
    expect(serializedValueBytes({ nested: [value, { inner: value }] })).toBe(
      reference({ nested: [value, { inner: value }] }),
    );
  }
});

it("matches nested combinations without depending on field order", () => {
  for (let index = 0; index < 500; index++) {
    const value = {
      "猫\n": values[index % values.length],
      z: [values[(index * 7) % values.length], values[(index * 11) % values.length]],
      a: { b: values[(index * 13) % values.length], a: values[(index * 17) % values.length] },
    };
    expect(serializedValueBytes(value)).toBe(reference(value));
  }
});

it("counts batches with scalar, nested and dictionary shapes without changing the metric", () => {
  const rows = Array.from({ length: 31 }, (_, index) => ({
    id: index,
    text: index % 3 ? "猫😀\ud800" : null,
    binary: Uint8Array.of(index, 255),
    wide: 9007199254740993n + BigInt(index),
    nested: { values: [index, null, index + 1] },
  }));
  const batch = batchFromColumns({
    id: rows.map((row) => row.id),
    text: rows.map((row) => row.text),
    binary: rows.map((row) => row.binary),
    wide: rows.map((row) => row.wide),
    nested: rows.map((row) => row.nested),
  });
  const wrapped: Batch = {
    rowCount: 4,
    columns: Object.fromEntries(
      Object.entries(batch.columns).map(([name, dictionary]) => [
        name,
        {
          type: "dict",
          dictionary,
          indices: Uint32Array.of(30, 0, 5, 5),
          valid: Uint8Array.of(1, 1, 0, 1),
        },
      ]),
    ),
  };
  for (const candidate of [
    batch,
    wrapped,
    { rowCount: 0, columns: {} },
    { rowCount: 3, columns: {} },
  ])
    expect(serializedBatchBytes(candidate)).toBe(reference(materializeBatchRows(candidate)));
});

it("counts large binary payloads without constructing their hex representation", () => {
  const bytes = new Uint8Array(1024 * 1024);
  // Iteration is required by the previous hex encoder, but not by byte sizing.
  bytes[Symbol.iterator] = () => {
    throw new Error("Hex encoding attempted");
  };
  expect(serializedValueBytes(bytes)).toBe(bytes.byteLength * 2 + 2);
});

it("preserves the exact serialized budget boundary in columnar top-k queries", async () => {
  const batch = batchFromColumns({ id: [2, 1], text: ["猫", "x"] });
  const expected = [
    { id: 1, text: "x" },
    { id: 2, text: "猫" },
  ];
  const size = reference(expected);
  const store = memoryStore();
  await store.put("fixture", Uint8Array.of(0));
  for (const maxMemoryBytes of [size, size - 1]) {
    const lake = new Lake({
      store,
      budget: { maxMemoryBytes, maxAllocatedMemoryBytes: 65536 },
      scanner: {
        scan() {
          throw new Error("Unexpected row scan");
        },
        async *scanColumns() {
          yield batch;
        },
      },
    });
    const query = lake
      .path("fixture")
      .orderBy([{ column: "id" }])
      .limit(2);
    if (maxMemoryBytes === size) await expect(query.toArray()).resolves.toEqual(expected);
    else await expect(query.toArray()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  }
});

it("preserves null masks and selected-index validation through dictionary map keys", () => {
  const dictionary: Vector = {
    type: "utf8",
    values: ["first", "hidden", "last"],
    valid: Uint8Array.of(1, 0, 1),
  };
  const keys: Vector = {
    type: "dict",
    indices: Uint32Array.of(0, 1, 99, 2),
    valid: Uint8Array.of(1, 1, 0, 1),
    dictionary,
  };
  const outerKeys: Vector = {
    type: "dict",
    indices: Uint32Array.of(0, 1, 2, 3),
    dictionary: keys,
  };
  const map: Vector = {
    type: "map",
    offsets: Int32Array.of(0, 4),
    keys: outerKeys,
    values: { type: "i32", values: Int32Array.of(10, 20, 30, 40) },
  };
  expect(serializedVectorBytes(map, 0)).toBe(reference(vectorValue(map, 0)));
  keys.valid = Uint8Array.of(1, 1, 1, 1);
  expect(() => serializedVectorBytes(map, 0)).toThrow(/out of bounds/);
});
