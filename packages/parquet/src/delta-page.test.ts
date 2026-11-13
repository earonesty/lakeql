import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { getSchemaPath } from "hyparquet/src/schema.js";
import type { ColumnDecoder, PageHeader } from "hyparquet/src/types.js";
import { ByteWriter } from "hyparquet-writer/src/bytewriter.js";
import { deltaByteArray, deltaLengthByteArray } from "hyparquet-writer/src/delta.js";
import { batchFromVectors, MemoryBudget, MemoryScope, materializeBatchRows } from "lakeql-core";
import { expect, it } from "vitest";
import { readNumericPage } from "./numeric-page.js";

for (const v2 of [false, true])
  for (const prefixes of [false, true]) {
    it(`admits packed delta page v2=${v2} prefixes=${prefixes}`, () => {
      const schema = [
        { name: "root", num_children: 1 },
        {
          name: "value",
          type: "BYTE_ARRAY" as const,
          repetition_type: "OPTIONAL" as const,
          converted_type: "UTF8" as const,
        },
      ];
      const schemaPath = getSchemaPath(schema, ["value"]);
      const decoder = {
        type: "BYTE_ARRAY",
        codec: "UNCOMPRESSED",
        element: schema[1],
        schemaPath,
        parsers: DEFAULT_PARSERS,
        pathInSchema: ["value"],
      } as ColumnDecoder;
      const writer = new ByteWriter();
      (prefixes ? deltaByteArray : deltaLengthByteArray)(writer, [
        new TextEncoder().encode("prefix-é"),
        new TextEncoder().encode("prefix-😀"),
      ]);
      const encoded = new Uint8Array(writer.getBuffer());
      const levelBytes = v2 ? 2 : 6;
      const bytes = new Uint8Array(levelBytes + encoded.length);
      if (!v2) new DataView(bytes.buffer).setUint32(0, 2, true);
      bytes.set([3, 5], v2 ? 0 : 4);
      bytes.set(encoded, levelBytes);
      const encoding = prefixes ? "DELTA_BYTE_ARRAY" : "DELTA_LENGTH_BYTE_ARRAY";
      const header: PageHeader = {
        type: v2 ? "DATA_PAGE_V2" : "DATA_PAGE",
        compressed_page_size: bytes.length,
        uncompressed_page_size: bytes.length,
        ...(v2
          ? {
              data_page_header_v2: {
                num_values: 4,
                num_rows: 4,
                num_nulls: 2,
                encoding,
                definition_levels_byte_length: 2,
                repetition_levels_byte_length: 0,
                is_compressed: false,
              },
            }
          : {
              data_page_header: {
                num_values: 4,
                encoding,
                definition_level_encoding: "RLE",
                repetition_level_encoding: "RLE",
              },
            }),
      };
      const scope = new MemoryScope(new MemoryBudget(65536));
      const page = readNumericPage(bytes, header, decoder, scope);
      if (!page?.packedVector) throw new Error("Did not use packed scoped decoder");
      expect(materializeBatchRows(batchFromVectors({ value: page.packedVector }))).toEqual([
        { value: "prefix-é" },
        { value: null },
        { value: "prefix-😀" },
        { value: null },
      ]);
      const peak = scope.budget.peakBytes;
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
      const tight = new MemoryScope(new MemoryBudget(peak - 1));
      try {
        expect(() => readNumericPage(bytes, header, decoder, tight)).toThrow(/budget/);
      } finally {
        tight.dispose();
      }
      expect(tight.budget.usedBytes).toBe(0);
    });
  }

it.each([
  false,
  true,
])("releases GZIP scratch only when delta payload is independent, prefixes=%s", async (prefixes) => {
  const { gzipSync } = await import("node:zlib");
  const { scopedParquetCompressors } = await import("./compressors.js");
  const schema = [
    { name: "root", num_children: 1 },
    {
      name: "value",
      type: "BYTE_ARRAY" as const,
      repetition_type: "REQUIRED" as const,
      converted_type: "UTF8" as const,
    },
  ];
  const writer = new ByteWriter();
  const text = ["prefix-é", "prefix-😀"];
  (prefixes ? deltaByteArray : deltaLengthByteArray)(
    writer,
    text.map((value) => new TextEncoder().encode(value)),
  );
  const raw = new Uint8Array(writer.getBuffer());
  const compressed = gzipSync(raw);
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  let decompressed: Uint8Array | undefined;
  const compressors = scopedParquetCompressors(scope);
  const decoder = {
    type: "BYTE_ARRAY",
    codec: "GZIP",
    element: schema[1],
    schemaPath: getSchemaPath(schema, ["value"]),
    parsers: DEFAULT_PARSERS,
    pathInSchema: ["value"],
    compressors: {
      ...compressors,
      GZIP(input: Uint8Array, length: number) {
        if (!compressors.GZIP) throw new Error("Missing GZIP compressor");
        const result = compressors.GZIP(input, length);
        decompressed = result;
        return result;
      },
    },
  } as ColumnDecoder;
  const header: PageHeader = {
    type: "DATA_PAGE",
    compressed_page_size: compressed.length,
    uncompressed_page_size: raw.length,
    data_page_header: {
      num_values: 2,
      encoding: prefixes ? "DELTA_BYTE_ARRAY" : "DELTA_LENGTH_BYTE_ARRAY",
      definition_level_encoding: "RLE",
      repetition_level_encoding: "RLE",
    },
  };
  try {
    const page = readNumericPage(compressed, header, decoder, scope);
    if (!page?.packedVector || !decompressed) throw new Error("Missing decoded output");
    expect(materializeBatchRows(batchFromVectors({ value: page.packedVector }))).toEqual(
      text.map((value) => ({ value })),
    );
    expect(scope.tryReleaseAllocation(decompressed)).toBe(!prefixes);
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});
