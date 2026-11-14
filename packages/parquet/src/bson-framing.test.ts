import { readFileSync } from "node:fs";
import { parquetMetadata } from "hyparquet";
import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { readDataPage } from "hyparquet/src/datapage.js";
import { getSchemaPath } from "hyparquet/src/schema.js";
import { deserializeTCompactProtocol } from "hyparquet/src/thrift.js";
import type { ColumnDecoder, DataPageHeader } from "hyparquet/src/types.js";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { validateBsonFraming } from "./bson-framing.js";

const i32 = (value: number) => {
  const out = Buffer.alloc(4);
  out.writeInt32LE(value);
  return out;
};
const element = (type: number, payload: Uint8Array, key = "a") =>
  Buffer.concat([Buffer.from([type]), Buffer.from(`${key}\0`), payload]);
const document = (...elements: Uint8Array[]) => {
  const body = Buffer.concat(elements);
  return Buffer.concat([i32(body.length + 5), body, Buffer.from([0])]);
};
const string = (value: string) => {
  const text = Buffer.from(value);
  return Buffer.concat([i32(text.length + 1), text, Buffer.from([0])]);
};

it("validates actual Parquet BSON payloads without constructing decoded documents", () => {
  const bytes = readFileSync(
    new URL("../../../fixtures/external/parquet-testing/data/bson.parquet", import.meta.url),
  );
  const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  const metadata = parquetMetadata(buffer);
  const column = metadata.row_groups[0]?.columns[0]?.meta_data;
  if (!column) throw Error("Missing fixture column");
  const reader = { view: new DataView(buffer), offset: Number(column.data_page_offset) };
  const raw = deserializeTCompactProtocol(reader);
  const path = getSchemaPath(metadata.schema, column.path_in_schema);
  const decoder = {
    ...column,
    schemaPath: path,
    element: path.at(-1)?.element,
    parsers: DEFAULT_PARSERS,
  } as ColumnDecoder;
  const header = {
    num_values: raw.field_5.field_1,
    encoding: "PLAIN",
    definition_level_encoding: "RLE",
    repetition_level_encoding: "RLE",
  } as DataPageHeader;
  const values = readDataPage(
    new Uint8Array(buffer, reader.offset, raw.field_3),
    header,
    decoder,
  ).dataPage;
  expect(values).toHaveLength(2);
  const scope = new MemoryScope(new MemoryBudget(4096));
  for (const value of values) {
    expect(value).toBeInstanceOf(Uint8Array);
    validateBsonFraming(value as Uint8Array, scope);
    expect(scope.budget.usedBytes).toBe(0);
  }
  scope.dispose();
});

it("validates framing for every BSON wire type, including deprecated types", () => {
  const empty = document();
  const scoped = Buffer.concat([i32(14), string(""), empty]);
  const payloads: [number, Uint8Array][] = [
    [1, Buffer.alloc(8)],
    [2, string("é😀")],
    [3, empty],
    [4, document(element(10, Buffer.alloc(0), "0"), element(8, Buffer.from([1]), "1"))],
    [5, Buffer.concat([i32(7), Buffer.from([2]), i32(3), Buffer.from([1, 2, 3])])],
    [6, Buffer.alloc(0)],
    [7, Buffer.alloc(12)],
    [8, Buffer.from([0])],
    [9, Buffer.alloc(8)],
    [10, Buffer.alloc(0)],
    [11, Buffer.from("pattern\0im\0")],
    [12, Buffer.concat([string("namespace"), Buffer.alloc(12)])],
    [13, string("code")],
    [14, string("symbol")],
    [15, scoped],
    [16, i32(123)],
    [17, Buffer.alloc(8)],
    [18, Buffer.alloc(8)],
    [19, Buffer.alloc(16)],
    [127, Buffer.alloc(0)],
    [255, Buffer.alloc(0)],
  ];
  const scope = new MemoryScope(new MemoryBudget(4096));
  for (const [type, payload] of payloads) {
    const valid = document(element(type, payload));
    validateBsonFraming(valid, scope);
    for (let end = 0; end < valid.length; end++)
      expect(() => validateBsonFraming(valid.subarray(0, end), scope)).toThrow(/Invalid BSON/);
    expect(scope.budget.usedBytes).toBe(0);
  }
  scope.dispose();
});

it("bounds deep traversal by admitted frames without recursion and releases on denial", () => {
  const depth = 5000;
  const bytes = new Uint8Array(5 + depth * 8);
  const view = new DataView(bytes.buffer);
  for (let level = 0; level <= depth; level++) {
    const offset = level * 7;
    view.setInt32(offset, bytes.length - level * 8, true);
    if (level < depth) {
      bytes[offset + 4] = 3;
      bytes[offset + 5] = 97;
    }
  }
  const scope = new MemoryScope(new MemoryBudget(4 * 1024 * 1024));
  validateBsonFraming(bytes, scope);
  const peak = scope.budget.peakBytes;
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
  for (const cap of [peak, peak - 1, 1024]) {
    const bounded = new MemoryScope(new MemoryBudget(cap));
    if (cap === peak) validateBsonFraming(bytes, bounded);
    else expect(() => validateBsonFraming(bytes, bounded)).toThrow(/budget/i);
    expect(bounded.budget.usedBytes).toBe(0);
    bounded.dispose();
  }
});

it("rejects invalid array indices, booleans, binary lengths and code scopes", () => {
  const cases = [
    document(element(4, document(element(10, Buffer.alloc(0), "1")))),
    document(element(4, document(element(10, Buffer.alloc(0), "00")))),
    document(element(8, Buffer.from([2]))),
    document(element(5, Buffer.concat([i32(4), Buffer.from([2]), i32(1)]))),
    document(element(15, Buffer.concat([i32(15), string(""), document(), Buffer.from([0])]))),
    document(element(0, Buffer.alloc(0))),
    Buffer.concat([document(), Buffer.from([0])]),
  ];
  const scope = new MemoryScope(new MemoryBudget(4096));
  for (const bytes of cases) {
    expect(() => validateBsonFraming(bytes, scope)).toThrow(/Invalid BSON/);
    expect(scope.budget.usedBytes).toBe(0);
  }
  scope.dispose();
});
