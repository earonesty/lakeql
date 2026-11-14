import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import {
  inspectCompactStructure,
  readCompactHeader,
} from "../packages/parquet/dist/compact-header.js";
import { incrementalParquetMetadata } from "../packages/parquet/dist/incremental-metadata.js";
import { lakeqlParquetParsers } from "../packages/parquet/dist/parsers.js";
import { convertRowGroupMetadata } from "../packages/parquet/dist/row-group-metadata.js";
import { parquetMetadata } from "../packages/parquet/node_modules/hyparquet/src/metadata.js";
import { deserializeTCompactProtocol } from "../packages/parquet/node_modules/hyparquet/src/thrift.js";

const root = new URL("../fixtures/external/parquet-testing/", import.meta.url);
const results = [];
for (const path of (await readdir(root, { recursive: true }))
  .filter((path) => path.endsWith(".parquet"))
  .sort()) {
  const bytes = await readFile(new URL(path, root));
  const entry = { path, sha256: createHash("sha256").update(bytes).digest("hex") };
  try {
    const file = new DataView(bytes.buffer, bytes.byteOffset, bytes.length);
    const length = file.getUint32(bytes.length - 8, true);
    const reader = {
      view: new DataView(bytes.buffer, bytes.byteOffset + bytes.length - 8 - length, length),
      offset: 0,
    };
    const counts = inspectCompactStructure(reader);
    if (reader.offset !== length) throw new Error("Trailing footer bytes");
    const oracle = parquetMetadata(
      bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length),
      { parsers: lakeqlParquetParsers },
    );
    assert.deepEqual(
      incrementalParquetMetadata(
        bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length),
      ),
      oracle,
    );
    const leaves = oracle.schema.filter((element) => element.type);
    let rowGroups = 0;
    const locationReader = { view: reader.view, offset: 0 };
    readCompactHeader(locationReader, {
      4: {
        list: false,
        visitRange: (start, end, index) => {
          assert.equal(index, rowGroups++);
          const view = new DataView(
            reader.view.buffer,
            reader.view.byteOffset + start,
            end - start,
          );
          const actual = { view, offset: 0 },
            expected = { view, offset: 0 };
          const rawGroup = readCompactHeader(actual);
          assert.deepEqual(rawGroup, deserializeTCompactProtocol(expected));
          assert.deepEqual(convertRowGroupMetadata(rawGroup, leaves), oracle.row_groups[index]);
          assert.equal(actual.offset, view.byteLength);
          assert.equal(expected.offset, view.byteLength);
        },
      },
    });
    assert.equal(locationReader.offset, length);
    results.push({
      ...entry,
      status: "validated",
      footerBytes: length,
      counts,
      rowGroupsCompared: rowGroups,
    });
  } catch (error) {
    results.push({ ...entry, status: "rejected", error: String(error.message) });
  }
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local external-file compact footer structural preflight; no decoded metadata tree retained. Counts are not measured heap bytes or full file conformance.",
      results,
    },
    null,
    2,
  ),
);
