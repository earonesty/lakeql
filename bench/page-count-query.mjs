import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { MemoryBudget, memoryStore } from "../packages/core/dist/index.js";
import { createParquetLake } from "../packages/parquet/dist/index.js";

const store = memoryStore();
await store.put(
  "fixture.parquet",
  await readFile(
    new URL(
      "../fixtures/external/parquet-testing/data/overflow_i16_page_cnt.parquet",
      import.meta.url,
    ),
  ),
);
const pool = new MemoryBudget(64 * 1024 * 1024);
const lake = createParquetLake({
  store,
  memoryPool: pool,
  scanRangeCache: { maxBytes: 0 },
  budget: { maxAllocatedMemoryBytes: 64 * 1024 * 1024 },
});
const outputMode = process.env.LAKEQL_PAGE_COUNT_OUTPUT ?? "stream";
assert.ok(["stream", "owned"].includes(outputMode));
let rows = 0;
const start = performance.now();
const query = lake.path("fixture.parquet").batchSize(128).run();
if (outputMode === "owned") {
  const output = await query.toOwnedArray();
  try {
    rows = output.value.length;
  } finally {
    output.release();
  }
} else {
  for await (const _row of query.rows()) rows++;
}
assert.equal(rows, 40000);
assert.equal(pool.usedBytes, 0);
console.log(
  JSON.stringify({
    scope: "Local memory-store query; fixture storage and runtime heap excluded; no cloud claim",
    node: process.version,
    outputMode,
    rows,
    elapsedMs: performance.now() - start,
    peakBytes: pool.peakBytes,
    afterReleaseBytes: pool.usedBytes,
  }),
);
