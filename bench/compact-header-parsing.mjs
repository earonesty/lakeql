import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import {
  PAGE_HEADER_PROJECTION,
  readCompactHeader,
} from "../packages/parquet/dist/compact-header.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { deserializeTCompactProtocol } from "../packages/parquet/node_modules/hyparquet/src/thrift.js";

const require = createRequire(new URL("../packages/parquet/package.json", import.meta.url));
const { parquetWriteBuffer } = await import(pathToFileURL(require.resolve("hyparquet-writer")));
for (const encoding of [
  "PLAIN",
  "RLE_DICTIONARY",
  "nullable.impala.parquet",
  "alltypes_plain.parquet",
]) {
  let file;
  if (encoding.endsWith(".parquet")) {
    const buffer = await readFile(
      new URL(`../fixtures/external/parquet-testing/data/${encoding}`, import.meta.url),
    );
    file = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
  } else
    file = parquetWriteBuffer({
      codec: "SNAPPY",
      pageSize: 128,
      columnData: [
        {
          name: "x",
          type: "INT32",
          encoding,
          data: Array.from({ length: 4096 }, (_, i) => i % 31),
        },
      ],
    });
  const metadata = await readParquetMetadataFromFile({
    byteLength: file.byteLength,
    async slice(a, b) {
      return file.slice(a, b);
    },
  });
  const headers = [];
  let statisticsHeaders = 0;
  for (const group of metadata.row_groups)
    for (const entry of group.columns) {
      const column = entry.meta_data;
      let offset = Number(column.dictionary_page_offset ?? column.data_page_offset);
      const end = offset + Number(column.total_compressed_size);
      while (offset < end) {
        const reader = { view: new DataView(file), offset };
        const header = deserializeTCompactProtocol(reader);
        if (header.field_5?.field_5 || header.field_8?.field_8) statisticsHeaders++;
        headers.push(new DataView(file, offset, reader.offset - offset));
        offset = reader.offset + header.field_3;
      }
    }
  for (const view of headers) {
    const actual = { view, offset: 0 };
    const expected = { view, offset: 0 };
    assert.deepEqual(readCompactHeader(actual), deserializeTCompactProtocol(expected));
    assert.equal(actual.offset, expected.offset);
  }
  const projected = (reader) => readCompactHeader(reader, PAGE_HEADER_PROJECTION);
  const project = (value, selection) =>
    Object.fromEntries(
      Object.entries(selection)
        .filter(([field]) => value[`field_${field}`] !== undefined)
        .map(([field, selected]) => [
          `field_${field}`,
          typeof selected !== "object"
            ? value[`field_${field}`]
            : project(value[`field_${field}`], selected),
        ]),
    );
  for (const view of headers) {
    const input = { view, offset: 0 };
    assert.deepEqual(
      projected(input),
      project(deserializeTCompactProtocol({ view, offset: 0 }), PAGE_HEADER_PROJECTION),
    );
    assert.equal(input.offset, view.byteLength);
  }
  for (const [mode, parse] of [
    ["upstream", deserializeTCompactProtocol],
    ["bounded", readCompactHeader],
    ["projected", projected],
  ]) {
    const samples = [];
    let checksum = 0;
    for (let repeat = 0; repeat < 10; repeat++) {
      const start = performance.now();
      for (let index = 0; index < 20000; index++) {
        const view = headers[index % headers.length];
        checksum += parse({ view, offset: 0 }).field_3;
      }
      if (repeat >= 3) samples.push(performance.now() - start);
    }
    samples.sort((a, b) => a - b);
    assert.ok(checksum > 0);
    console.log(
      JSON.stringify({
        encoding,
        mode,
        distinctHeaders: headers.length,
        statisticsHeaders,
        parses: 20000,
        medianMs: samples[3],
        samples: samples.length,
        scope:
          "Generated/external local Parquet page/dictionary headers; exact parser output parity; no I/O, heap measurement or whole-scan claim",
      }),
    );
  }
}
