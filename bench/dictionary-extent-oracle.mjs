import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { DuckDBInstance } from "@duckdb/node-api";
import { lakeqlParquetCompressors } from "../packages/parquet/dist/compressors.js";
import { parquetMetadata } from "../packages/parquet/node_modules/hyparquet/src/metadata.js";
import { parquetReadObjects } from "../packages/parquet/node_modules/hyparquet/src/read.js";
import { deserializeTCompactProtocol } from "../packages/parquet/node_modules/hyparquet/src/thrift.js";

const path = "fixtures/external/parquet-testing/data/nation.dict-malformed.parquet";
const bytes = readFileSync(path);
const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
const metadata = parquetMetadata(buffer);
const file = { byteLength: buffer.byteLength, slice: (start, end) => buffer.slice(start, end) };
await assert.rejects(parquetReadObjects({ file, metadata, compressors: lakeqlParquetCompressors }));
const columns = [];
for (const group of metadata.row_groups) {
  for (const column of group.columns) {
    const meta = column.meta_data;
    const start = Number(meta.dictionary_page_offset ?? meta.data_page_offset);
    const declaredEnd = start + Number(meta.total_compressed_size);
    const reader = { view: new DataView(buffer), offset: start };
    const pages = [];
    while (reader.offset < declaredEnd) {
      const at = reader.offset;
      const header = deserializeTCompactProtocol(reader);
      const bodyEnd = reader.offset + header.field_3;
      assert.ok(Number.isSafeInteger(bodyEnd) && bodyEnd > at && bodyEnd <= buffer.byteLength);
      pages.push({ at, headerBytes: reader.offset - at, type: header.field_1, bodyEnd });
      reader.offset = bodyEnd;
    }
    const shortfall = reader.offset - declaredEnd;
    if (shortfall) {
      assert.equal(pages[0].type, 2, "first page must be a dictionary");
      assert.equal(shortfall, pages[0].headerBytes, "omitted dictionary header extent");
      // Diagnostic correction only. Production must validate physical boundaries
      // and admit incremental remote reads before attempting any recovery.
      meta.total_compressed_size += BigInt(shortfall);
    }
    columns.push({ path: meta.path_in_schema, start, declaredEnd, shortfall, pages });
  }
}
const recovered = await parquetReadObjects({
  file,
  metadata,
  compressors: lakeqlParquetCompressors,
});
const db = await DuckDBInstance.create();
const connection = await db.connect();
let expected, version;
try {
  expected = (
    await connection.runAndReadAll(`select * from read_parquet('${path}')`)
  ).getRowObjectsJS();
  version = (await connection.runAndReadAll("select version() as version")).getRowObjectsJS()[0]
    .version;
} finally {
  connection.closeSync();
  db.closeSync();
}
const normalize = (rows) =>
  rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        value instanceof Uint8Array || typeof value === "string"
          ? Buffer.from(value).toString("hex")
          : value,
      ]),
    ),
  );
assert.equal(recovered.length, 25);
assert.deepEqual(normalize(recovered), normalize(expected));
const report = {
  fixture: path,
  fixtureSha256: createHash("sha256").update(bytes).digest("hex"),
  node: process.version,
  duckdb: version,
  columns,
  correctedMetadataRows: recovered.length,
  allValuesMatchDuckdb: true,
  conclusion:
    "Two column compressed extents omit exactly their 15-byte dictionary page header. Adjusting only those metadata extents restores all 25 rows, byte-for-byte equal to DuckDB. This establishes a recoverable metadata inconsistency, not grounds to waive the conformance failure. No production code or fixture bytes were changed. Physical boundary validation and admitted bounded remote recovery remain required.",
};
writeFileSync(
  "docs/benchmarks/dictionary-extent-oracle-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(
  JSON.stringify({
    rows: recovered.length,
    allValuesMatchDuckdb: true,
    shortfalls: columns.map((column) => column.shortfall),
  }),
);
