// Diagnostic only: bypass the public schema gate to compare existing decoder behavior.
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { DuckDBConnection } from "@duckdb/node-api";
import { materializeBatchRows } from "../packages/core/dist/index.js";
import { readParquetColumnBatch } from "../packages/parquet/dist/column-batches.js";
import { lakeqlParquetCompressors } from "../packages/parquet/dist/compressors.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { lakeqlParquetParsers } from "../packages/parquet/dist/parsers.js";
import { parquetReadObjects } from "../packages/parquet/node_modules/hyparquet/src/read.js";
import { nestedDifference } from "./nested-value-comparison.mjs";

const files = [
  "data/nested_structs.rust.parquet",
  "data/nonnullable.impala.parquet",
  "data/nullable.impala.parquet",
  "shredded_variant/case-001.parquet",
  "shredded_variant/case-002.parquet",
];
const connection = await DuckDBConnection.create();
try {
  for (const file of files) {
    const path = resolve("fixtures/external/parquet-testing", file);
    const report = { file, publicSchemaGateBypassed: true };
    try {
      const bytes = await readFile(path);
      const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
      const source = {
        byteLength: buffer.byteLength,
        async slice(start, end) {
          return buffer.slice(start, end);
        },
      };
      const metadata = await readParquetMetadataFromFile(source);
      const columns = [
        ...new Set(
          metadata.row_groups.flatMap((group) =>
            group.columns.map((column) => column.meta_data?.path_in_schema[0]).filter(Boolean),
          ),
        ),
      ];
      const batch = await readParquetColumnBatch(
        source,
        metadata,
        columns,
        0,
        Number(metadata.num_rows),
      );
      const actual = materializeBatchRows(batch);
      const reference = await connection.runAndReadAll(
        `SELECT * FROM read_parquet('${path.replaceAll("'", "''")}')`,
      );
      const expected = reference.getRowObjects();
      report.actualRows = actual.length;
      report.expectedRows = expected.length;
      report.firstDifference = nestedDifference(actual, expected) ?? null;
      report.logicalValuesEqual = report.firstDifference === null;
      const upstream = await parquetReadObjects({
        file: source,
        metadata,
        parsers: lakeqlParquetParsers,
        compressors: lakeqlParquetCompressors,
        utf8: false,
      });
      report.upstreamFirstDifference = nestedDifference(upstream, expected) ?? null;
      report.upstreamLogicalValuesEqual = report.upstreamFirstDifference === null;
    } catch (error) {
      report.error = error.message;
    }
    console.log(JSON.stringify(report));
  }
} finally {
  connection.closeSync();
}
