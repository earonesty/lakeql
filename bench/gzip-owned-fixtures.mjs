import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { DuckDBConnection } from "@duckdb/node-api";
import { MemoryBudget, materializeBatchRows, memoryStore } from "../packages/core/dist/index.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { readOwnedParquetVectorBatches } from "../packages/parquet/dist/owned-vector-batches.js";
import { nestedDifference } from "./nested-value-comparison.mjs";

const connection = await DuckDBConnection.create();
const reports = [];
try {
  for (const name of [
    "byte_stream_split_extended.gzip.parquet",
    "concatenated_gzip_members.parquet",
    "data_index_bloom_encoding_stats.parquet",
    "incorrect_map_schema.parquet",
    "rle_boolean_encoding.parquet",
  ]) {
    const path = resolve("fixtures/external/parquet-testing/data", name);
    let expected, referenceError, fullProjectionError;
    let referenceMode = "full-projection";
    try {
      expected = (
        await connection.runAndReadAll(
          `SELECT * FROM read_parquet('${path.replaceAll("'", "''")}')`,
        )
      ).getRowObjects();
    } catch (error) {
      referenceError = error.message;
    }
    const store = memoryStore();
    await store.put(name, await readFile(path));
    const file = await asyncBufferFromStore(store, name);
    const metadata = await readParquetMetadataFromFile(file);
    const columns = [
      ...new Set(
        metadata.row_groups.flatMap((group) =>
          group.columns.map((entry) => entry.meta_data.path_in_schema[0]),
        ),
      ),
    ];
    if (name === "byte_stream_split_extended.gzip.parquet" && referenceError !== undefined) {
      // The Apache fixture README documents seven PLAIN/BSS pairs with identical
      // values. Projecting only the PLAIN twins avoids DuckDB's BSS type limit.
      const plain = columns.filter((column) => column.endsWith("_plain"));
      assert.equal(plain.length * 2, columns.length);
      const projection = plain
        .map((column) => {
          const quoted = `"${column.replaceAll('"', '""')}"`;
          return column === "decimal_plain" ? `CAST(${quoted} AS DOUBLE) AS ${quoted}` : quoted;
        })
        .join(", ");
      try {
        const rows = (
          await connection.runAndReadAll(
            `SELECT ${projection} FROM read_parquet('${path.replaceAll("'", "''")}')`,
          )
        ).getRowObjects();
        expected = rows.map((row) =>
          Object.fromEntries(
            plain.flatMap((column) => [
              [column, row[column]],
              [column.replace(/_plain$/, "_byte_stream_split"), row[column]],
            ]),
          ),
        );
        fullProjectionError = referenceError;
        referenceError = undefined;
        referenceMode = "documented-plain-twins-with-decimal-cast-to-double";
      } catch (error) {
        referenceError = error.message;
      }
    }
    for (const batchSize of [1, 127]) {
      const budget = new MemoryBudget(8 * 1024 * 1024);
      const actual = [];
      let rejection;
      try {
        for await (const owner of readOwnedParquetVectorBatches(file, metadata, budget, {
          columns,
          batchSize,
          canStopEarly: true,
        })) {
          actual.push(...materializeBatchRows(owner.value.batch));
          owner.release();
        }
      } catch (error) {
        rejection = { code: error.code, message: error.message };
      }
      const difference =
        rejection === undefined && expected !== undefined
          ? (nestedDifference(actual, expected) ?? null)
          : null;
      assert.equal(budget.usedBytes, 0);
      reports.push({
        name,
        batchSize,
        status: rejection
          ? "rejected"
          : referenceError
            ? "reference-error"
            : difference
              ? "value-mismatch"
              : "matches-reference",
        rejection,
        referenceError,
        fullProjectionError,
        referenceMode,
        difference,
        referenceRows: expected?.length,
        actualRows: actual.length,
        peakBytes: budget.peakBytes,
        releasedBytes: budget.usedBytes,
      });
    }
  }
} finally {
  connection.closeSync();
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local external GZIP files through owned reader with independent DuckDB values; diagnostic failures are unresolved, not expected passes; tracks engine backing but excludes uninstrumented nested/JS allocations and oracle storage",
      reports,
    },
    null,
    2,
  ),
);
