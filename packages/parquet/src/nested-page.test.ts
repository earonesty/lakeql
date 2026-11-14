import { gzipSync } from "node:zlib";
import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { getSchemaPath } from "hyparquet/src/schema.js";
import type { ColumnDecoder, PageHeader } from "hyparquet/src/types.js";
import { ByteWriter } from "hyparquet-writer/src/bytewriter.js";
import {
  deltaBinaryPack,
  deltaByteArray,
  deltaLengthByteArray,
} from "hyparquet-writer/src/delta.js";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { readNestedPage } from "./nested-page.js";

const schema = [
  { name: "root", num_children: 1 },
  { name: "items", repetition_type: "REPEATED" as const, num_children: 1 },
  { name: "value", repetition_type: "OPTIONAL" as const, type: "INT32" as const },
];
const decoder = (gzip: boolean): ColumnDecoder => ({
  type: "INT32",
  codec: gzip ? "GZIP" : "UNCOMPRESSED",
  element: { name: "value", repetition_type: "OPTIONAL", type: "INT32" },
  schemaPath: getSchemaPath(schema, ["items", "value"]),
  pathInSchema: ["items", "value"],
  parsers: DEFAULT_PARSERS,
});
for (const v2 of [false, true])
  for (const gzip of [false, true])
    it(`decodes admitted nested PLAIN values v2=${v2}, gzip=${gzip}`, () => {
      const rep = [2, 0, 2, 1, 2, 0],
        def = [2, 2, 2, 1, 2, 2];
      const values = Uint8Array.of(7, 0, 0, 0, 9, 0, 0, 0);
      const raw = Uint8Array.from(
        v2 ? [...rep, ...def, ...values] : [6, 0, 0, 0, ...rep, 6, 0, 0, 0, ...def, ...values],
      );
      const compressed = gzip
        ? v2
          ? Uint8Array.from([...rep, ...def, ...gzipSync(values)])
          : gzipSync(raw)
        : raw;
      const header = {
        type: v2 ? "DATA_PAGE_V2" : "DATA_PAGE",
        compressed_page_size: compressed.length,
        uncompressed_page_size: raw.length,
        ...(v2
          ? {
              data_page_header_v2: {
                num_values: 3,
                num_rows: 2,
                num_nulls: 1,
                encoding: "PLAIN",
                repetition_levels_byte_length: 6,
                definition_levels_byte_length: 6,
                is_compressed: gzip,
              },
            }
          : {
              data_page_header: {
                num_values: 3,
                encoding: "PLAIN",
                repetition_level_encoding: "RLE",
                definition_level_encoding: "RLE",
              },
            }),
      } as PageHeader;
      const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
      const output = readNestedPage(compressed, header, decoder(gzip), true, scope);
      expect(Array.from(output?.values ?? [])).toEqual([7, 9]);
      expect(output?.levels).toMatchObject({ rowsStarted: 2, presentCount: 2 });
      const peak = scope.budget.peakBytes;
      scope.dispose();
      const exact = new MemoryScope(new MemoryBudget(peak));
      readNestedPage(compressed, header, decoder(gzip), true, exact);
      exact.dispose();
      const tight = new MemoryScope(new MemoryBudget(peak - 1));
      expect(() => readNestedPage(compressed, header, decoder(gzip), true, tight)).toThrow(
        /budget/,
      );
      expect(tight.budget.usedBytes).toBe(0);
      tight.dispose();
      const invalid = new MemoryScope(new MemoryBudget(1024 * 1024));
      expect(() =>
        readNestedPage(compressed.subarray(1), header, decoder(gzip), true, invalid),
      ).toThrow(/Malformed/);
      expect(invalid.budget.usedBytes).toBe(0);
      invalid.dispose();
    });

it("decodes boolean physical bits and declines unsupported level encodings without allocation", () => {
  const bytes = Uint8Array.of(2, 0, 0, 0, 4, 0, 2, 0, 0, 0, 4, 2, 1);
  const header: PageHeader = {
    type: "DATA_PAGE",
    compressed_page_size: bytes.length,
    uncompressed_page_size: bytes.length,
    data_page_header: {
      num_values: 2,
      encoding: "PLAIN",
      repetition_level_encoding: "RLE",
      definition_level_encoding: "RLE",
    },
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const physical = { ...decoder(false), type: "BOOLEAN" as const };
  const result = readNestedPage(bytes, header, physical, true, scope);
  expect(Array.from(result?.values ?? [])).toEqual([1, 0]);
  scope.dispose();
  const declined = new MemoryScope(new MemoryBudget(0));
  if (!header.data_page_header) throw Error("Missing header");
  header.data_page_header.repetition_level_encoding = "BIT_PACKED";
  expect(readNestedPage(bytes, header, physical, true, declined)).toBeUndefined();
  expect(declined.budget.peakBytes).toBe(0);
  declined.dispose();
});

it.each([
  "RLE_DICTIONARY",
  "PLAIN_DICTIONARY",
] as const)("admits nested %s indices and rejects invalid references", (encoding) => {
  const levels = [2, 0, 2, 1, 2, 0, 2, 2, 2, 1, 2, 2];
  const bytes = Uint8Array.from([...levels, 1, 2, 1, 2, 0]);
  const header: PageHeader = {
    type: "DATA_PAGE_V2",
    compressed_page_size: bytes.length,
    uncompressed_page_size: bytes.length,
    data_page_header_v2: {
      num_values: 3,
      num_rows: 2,
      num_nulls: 1,
      encoding,
      repetition_levels_byte_length: 6,
      definition_levels_byte_length: 6,
      is_compressed: false,
    },
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const result = readNestedPage(bytes, header, decoder(false), true, scope, 2);
  expect(result?.dictionaryEncoded).toBe(true);
  expect(Array.from(result?.values ?? [])).toEqual([1, 0]);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  readNestedPage(bytes, header, decoder(false), true, exact, 2);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => readNestedPage(bytes, header, decoder(false), true, tight, 2)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  const invalid = new MemoryScope(new MemoryBudget(65536));
  expect(() => readNestedPage(bytes, header, decoder(false), true, invalid, 1)).toThrow(
    /out of bounds/,
  );
  expect(invalid.budget.usedBytes).toBe(0);
  invalid.dispose();
  const missing = new MemoryScope(new MemoryBudget(0));
  expect(readNestedPage(bytes, header, decoder(false), true, missing)).toBeUndefined();
  expect(missing.budget.usedBytes).toBe(0);
  missing.dispose();
});

for (const encoding of ["DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT"] as const)
  for (const type of ["INT32", "INT64", "FLOAT", "DOUBLE"] as const) {
    if (encoding === "DELTA_BINARY_PACKED" && type !== "INT32" && type !== "INT64") continue;
    it(`decodes nested ${encoding} ${type} with admitted values`, () => {
      const expected = type === "INT64" ? [9007199254740993n, -9007199254740995n] : [7, -9];
      let payload: Uint8Array;
      if (encoding === "DELTA_BINARY_PACKED") {
        const writer = new ByteWriter();
        deltaBinaryPack(writer, expected);
        payload = new Uint8Array(writer.getBuffer());
      } else {
        const width = type === "INT64" || type === "DOUBLE" ? 8 : 4;
        const raw = new Uint8Array(width * 2);
        const view = new DataView(raw.buffer);
        for (let i = 0; i < 2; i++) {
          if (type === "INT64") view.setBigInt64(i * width, BigInt(expected[i] ?? 0), true);
          else if (type === "DOUBLE") view.setFloat64(i * width, Number(expected[i]), true);
          else if (type === "FLOAT") view.setFloat32(i * width, Number(expected[i]), true);
          else view.setInt32(i * width, Number(expected[i]), true);
        }
        payload = new Uint8Array(raw.length);
        for (let lane = 0; lane < width; lane++)
          for (let i = 0; i < 2; i++) payload[lane * 2 + i] = raw[i * width + lane] ?? 0;
      }
      const bytes = Uint8Array.from([2, 0, 2, 1, 2, 0, 2, 2, 2, 1, 2, 2, ...payload]);
      const header: PageHeader = {
        type: "DATA_PAGE_V2",
        compressed_page_size: bytes.length,
        uncompressed_page_size: bytes.length,
        data_page_header_v2: {
          num_values: 3,
          num_rows: 2,
          num_nulls: 1,
          encoding,
          repetition_levels_byte_length: 6,
          definition_levels_byte_length: 6,
          is_compressed: false,
        },
      };
      const physical = { ...decoder(false), type, element: { name: "value", type } };
      const scope = new MemoryScope(new MemoryBudget(65536));
      expect(
        Array.from(readNestedPage(bytes, header, physical, true, scope)?.values ?? []),
      ).toEqual(expected);
      const peak = scope.budget.peakBytes;
      scope.dispose();
      const exact = new MemoryScope(new MemoryBudget(peak));
      readNestedPage(bytes, header, physical, true, exact);
      exact.dispose();
      const tight = new MemoryScope(new MemoryBudget(peak - 1));
      expect(() => readNestedPage(bytes, header, physical, true, tight)).toThrow(/budget/);
      expect(tight.budget.usedBytes).toBe(0);
      tight.dispose();
    });
  }

it.each([
  "PLAIN",
  "DELTA_LENGTH_BYTE_ARRAY",
  "DELTA_BYTE_ARRAY",
] as const)("keeps nested %s binary values packed", (encoding) => {
  const expected = ["prefix-é", "prefix-😀", ""].map((value) => new TextEncoder().encode(value));
  let payload: Uint8Array;
  if (encoding === "PLAIN") {
    payload = Uint8Array.from(expected.flatMap((value) => [value.length, 0, 0, 0, ...value]));
  } else {
    const writer = new ByteWriter();
    (encoding === "DELTA_BYTE_ARRAY" ? deltaByteArray : deltaLengthByteArray)(writer, expected);
    payload = new Uint8Array(writer.getBuffer());
  }
  const bytes = Uint8Array.from([2, 0, 2, 1, 2, 0, 6, 2, ...payload]);
  const header: PageHeader = {
    type: "DATA_PAGE_V2",
    compressed_page_size: bytes.length,
    uncompressed_page_size: bytes.length,
    data_page_header_v2: {
      num_values: 3,
      num_rows: 2,
      num_nulls: 0,
      encoding,
      repetition_levels_byte_length: 6,
      definition_levels_byte_length: 2,
      is_compressed: false,
    },
  };
  const physical = {
    ...decoder(false),
    type: "BYTE_ARRAY" as const,
    element: { name: "value", type: "BYTE_ARRAY" as const },
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const result = readNestedPage(bytes, header, physical, true, scope);
  expect(result?.values).toBeUndefined();
  expect(expected.map((_, i) => result?.packedBinary?.value(i))).toEqual(expected);
  expect(result?.packedBinary?.payload.buffer === bytes.buffer).toBe(
    encoding !== "DELTA_BYTE_ARRAY",
  );
  const retainedBytes = scope.budget.usedBytes;
  if (!header.data_page_header_v2) throw Error("Missing V2 header");
  const compressed = Uint8Array.from([...bytes.subarray(0, 8), ...gzipSync(payload)]);
  const compressedScope = new MemoryScope(new MemoryBudget(1024 * 1024));
  try {
    const compressedResult = readNestedPage(
      compressed,
      {
        ...header,
        compressed_page_size: compressed.length,
        data_page_header_v2: { ...header.data_page_header_v2, is_compressed: true },
      },
      { ...physical, codec: "GZIP" },
      true,
      compressedScope,
    );
    expect(expected.map((_, i) => compressedResult?.packedBinary?.value(i))).toEqual(expected);
    expect(compressedScope.budget.usedBytes - retainedBytes).toBe(
      encoding === "DELTA_BYTE_ARRAY" ? 0 : payload.length,
    );
  } finally {
    compressedScope.dispose();
  }
  expect(compressedScope.budget.usedBytes).toBe(0);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  readNestedPage(bytes, header, physical, true, exact);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => readNestedPage(bytes, header, physical, true, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

for (const v2 of [false, true])
  it.each([
    "PLAIN",
    "DELTA_BINARY_PACKED",
  ] as const)(`retains decompressed backing only for aliasing outputs, v2=${v2}, encoding=%s`, (encoding) => {
    const writer = new ByteWriter();
    if (encoding === "PLAIN") {
      writer.appendInt32(7);
      writer.appendInt32(-9);
    } else deltaBinaryPack(writer, [7, -9]);
    const payload = new Uint8Array(writer.getBuffer());
    const rep = [2, 0, 2, 1, 2, 0],
      def = [2, 2, 2, 1, 2, 2];
    const prefix = v2 ? [...rep, ...def] : [6, 0, 0, 0, ...rep, 6, 0, 0, 0, ...def];
    const raw = Uint8Array.from([...prefix, ...payload]);
    const retained: number[] = [];
    for (const compressed of [false, true]) {
      const bytes = compressed
        ? v2
          ? Uint8Array.from([...prefix, ...gzipSync(payload)])
          : gzipSync(raw)
        : raw;
      const header = {
        type: v2 ? "DATA_PAGE_V2" : "DATA_PAGE",
        compressed_page_size: bytes.length,
        uncompressed_page_size: raw.length,
        ...(v2
          ? {
              data_page_header_v2: {
                num_values: 3,
                num_rows: 2,
                num_nulls: 1,
                encoding,
                repetition_levels_byte_length: 6,
                definition_levels_byte_length: 6,
                is_compressed: compressed,
              },
            }
          : {
              data_page_header: {
                num_values: 3,
                encoding,
                repetition_level_encoding: "RLE",
                definition_level_encoding: "RLE",
              },
            }),
      } as PageHeader;
      const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
      const output = readNestedPage(bytes, header, decoder(compressed), true, scope);
      expect(Array.from(output?.values ?? [])).toEqual([7, -9]);
      retained.push(scope.budget.usedBytes);
      const owner = scope.retainAllocations(output);
      scope.dispose();
      try {
        expect(Array.from(owner.value?.values ?? [])).toEqual([7, -9]);
        expect(owner.value?.levels.rowsStarted).toBe(2);
      } finally {
        owner.release();
      }
      expect(scope.budget.usedBytes).toBe(0);
    }
    expect((retained[1] ?? 0) - (retained[0] ?? 0)).toBe(
      encoding === "PLAIN" ? (v2 ? payload.length : raw.length) : 0,
    );
  });
