import { readFileSync } from "node:fs";
import { parquetMetadata } from "hyparquet";
import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { decompressPage, readDataPage } from "hyparquet/src/datapage.js";
import { getSchemaPath } from "hyparquet/src/schema.js";
import { deserializeTCompactProtocol } from "hyparquet/src/thrift.js";
import type { ColumnDecoder, DataPageHeader } from "hyparquet/src/types.js";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { readAlpValues } from "./alp-values.js";
import { lakeqlParquetCompressors } from "./compressors.js";

it("decodes every ALP fixture vector bit-for-bit against its independent PLAIN column", () => {
  const bytes = readFileSync(
    new URL(
      "../../../fixtures/external/parquet-testing/data/alp_extended.zstd.parquet",
      import.meta.url,
    ),
  );
  const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  const metadata = parquetMetadata(buffer);
  let checked = 0;
  for (const group of metadata.row_groups) {
    const references = new Map<string, Uint8Array>();
    for (const chunk of group.columns) {
      const column = chunk.meta_data;
      if (!column) throw Error("Missing column");
      const reader = { view: new DataView(buffer), offset: Number(column.data_page_offset) };
      const raw = deserializeTCompactProtocol(reader);
      const payload = new Uint8Array(buffer, reader.offset, raw.field_3);
      const page = decompressPage(payload, raw.field_2, column.codec, lakeqlParquetCompressors);
      const name = column.path_in_schema[0] ?? "";
      if (name.endsWith("_plain")) {
        const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
        const decoder = {
          ...column,
          schemaPath,
          element: schemaPath.at(-1)?.element,
          parsers: DEFAULT_PARSERS,
        } as ColumnDecoder;
        const header = {
          num_values: raw.field_5.field_1,
          encoding: "PLAIN",
          definition_level_encoding: "RLE",
          repetition_level_encoding: "RLE",
        } as DataPageHeader;
        const result = readDataPage(page, header, decoder).dataPage as Float32Array | Float64Array;
        references.set(
          column.type,
          new Uint8Array(result.buffer, result.byteOffset, result.byteLength),
        );
      } else {
        expect(raw.field_5.field_2).toBe(10);
        const view = new DataView(page.buffer, page.byteOffset, page.byteLength);
        // All fixture leaves are optional and flat: V1 definition levels precede ALP.
        const start = 4 + view.getUint32(0, true);
        const expected = references.get(column.type);
        if (!expected) throw Error("Missing PLAIN reference");
        const count = expected.byteLength / (column.type === "FLOAT" ? 4 : 8);
        const pool = new MemoryBudget(expected.byteLength + 256);
        const scope = new MemoryScope(pool);
        const result = readAlpValues({ view, offset: start }, column.type, count, scope);
        expect(new Uint8Array(result.buffer), name).toEqual(expected);
        expect(pool.peakBytes).toBe(expected.byteLength + 256);
        scope.dispose();
        expect(pool.usedBytes).toBe(0);
        const short = new MemoryScope(new MemoryBudget(expected.byteLength + 255));
        expect(() => readAlpValues({ view, offset: start }, column.type, count, short)).toThrow(
          /budget/i,
        );
        expect(short.budget.usedBytes).toBe(0);
        short.dispose();
        checked++;
      }
    }
  }
  expect(checked).toBe(30);
});

function constantPage(): Uint8Array {
  const page = new Uint8Array(24);
  const view = new DataView(page.buffer);
  page[2] = 3;
  view.setInt32(3, 1, true);
  view.setUint32(7, 4, true);
  view.setBigInt64(15, 1n, true);
  return page;
}

it.each([
  [
    "mode",
    (bytes: Uint8Array) => {
      bytes[0] = 1;
    },
  ],
  [
    "integer encoding",
    (bytes: Uint8Array) => {
      bytes[1] = 1;
    },
  ],
  [
    "vector log",
    (bytes: Uint8Array) => {
      bytes[2] = 16;
    },
  ],
  [
    "count",
    (bytes: Uint8Array) => {
      bytes[3] = 2;
    },
  ],
  [
    "offset",
    (bytes: Uint8Array) => {
      bytes[7] = 3;
    },
  ],
  [
    "exponent",
    (bytes: Uint8Array) => {
      bytes[11] = 19;
    },
  ],
  [
    "factor",
    (bytes: Uint8Array) => {
      bytes[12] = 1;
    },
  ],
  [
    "exception count",
    (bytes: Uint8Array) => {
      bytes[13] = 2;
    },
  ],
  [
    "bit width",
    (bytes: Uint8Array) => {
      bytes[23] = 65;
    },
  ],
  [
    "packed extent",
    (bytes: Uint8Array) => {
      bytes[23] = 1;
    },
  ],
] as const)("rejects malformed ALP %s before output allocation", (_name, corrupt) => {
  const page = constantPage();
  corrupt(page);
  const scope = new MemoryScope(new MemoryBudget(65536));
  const reader = { view: new DataView(page.buffer), offset: 0 };
  expect(() => readAlpValues(reader, "DOUBLE", 1, scope)).toThrow(/Invalid ALP/);
  expect(reader.offset).toBe(0);
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});

it("rejects every truncated prefix, trailing bytes and invalid exception positions", () => {
  const original = constantPage();
  const variants = Array.from({ length: original.length }, (_, length) =>
    original.slice(0, length),
  );
  const trailing = new Uint8Array(25);
  trailing.set(original);
  variants.push(trailing);
  const exception = new Uint8Array(34);
  exception.set(original);
  exception[13] = 1;
  exception[24] = 1; // Position 1 is outside the single-value vector.
  variants.push(exception);
  for (const bytes of variants) {
    const scope = new MemoryScope(new MemoryBudget(65536));
    expect(() =>
      readAlpValues({ view: new DataView(bytes.buffer), offset: 0 }, "DOUBLE", 1, scope),
    ).toThrow(/Invalid ALP/);
    expect(scope.budget.peakBytes).toBe(0);
    scope.dispose();
  }
});

it("accepts an empty page and a zero-bit constant without extra vector storage", () => {
  const empty = new Uint8Array(7);
  empty[2] = 3;
  expect(readAlpValues({ view: new DataView(empty.buffer), offset: 0 }, "DOUBLE", 0)).toHaveLength(
    0,
  );
  const bytes = constantPage();
  const reader = { view: new DataView(bytes.buffer), offset: 0 };
  expect(Array.from(readAlpValues(reader, "DOUBLE", 1))).toEqual([1]);
  expect(reader.offset).toBe(bytes.length);
});
