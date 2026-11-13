import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { DuckDBConnection } from "@duckdb/node-api";
import { MemoryBudget, materializeBatchRows, memoryStore } from "../packages/core/dist/index.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { readOwnedParquetVectorBatches } from "../packages/parquet/dist/owned-vector-batches.js";

const path = resolve("fixtures/external/parquet-testing/data/rle_boolean_encoding.parquet");
const connection = await DuckDBConnection.create();
try {
  const expected = (
    await connection.runAndReadAll(`SELECT * FROM read_parquet('${path.replaceAll("'", "''")}')`)
  ).getRowObjects();
  const store = memoryStore();
  await store.put("boolean.parquet", await readFile(path));
  const file = await asyncBufferFromStore(store, "boolean.parquet");
  const metadata = await readParquetMetadataFromFile(file);
  const columns = metadata.row_groups[0].columns.map((entry) =>
    entry.meta_data.path_in_schema.join("."),
  );
  const reports = [];
  for (const batchSize of [1, 7, 1024]) {
    const budget = new MemoryBudget(65536);
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
    if (rejection === undefined) assert.deepEqual(actual, expected);
    assert.equal(budget.usedBytes, 0);
    reports.push({
      batchSize,
      status: rejection === undefined ? "matches-reference" : "rejected",
      rejection,
      referenceRows: expected.length,
      rows: actual.length,
      peakBytes: budget.peakBytes,
      releasedBytes: budget.usedBytes,
    });
  }
  console.log(
    JSON.stringify(
      {
        fixture: path,
        scope:
          "Diagnostic of local external Apache RLE boolean fixture through owned reader; rejection is an unresolved failure, not an expected pass; independent DuckDB reference; not remote or whole-worker coverage",
        reports,
      },
      null,
      2,
    ),
  );
} finally {
  connection.closeSync();
}
