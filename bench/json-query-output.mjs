import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { MemoryBudget, memoryStore } from "../packages/core/dist/index.js";
import { createParquetLake } from "../packages/parquet/dist/index.js";

const fixture = await readFile(
  new URL(
    "../fixtures/external/parquet-testing/data/overflow_i16_page_cnt.parquet",
    import.meta.url,
  ),
);
const store = memoryStore();
await store.put("fixture.parquet", fixture);
const oracle = createParquetLake({ store });
const expected = await oracle.path("fixture.parquet").limit(512).run().toArray();
const runs = [];
for (const format of ["json", "ndjson"]) {
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const lake = createParquetLake({
    store,
    memoryPool: pool,
    scanRangeCache: { maxBytes: 0 },
    budget: { maxAllocatedMemoryBytes: pool.maxBytes },
  });
  const query = lake.path("fixture.parquet").batchSize(128).limit(512).run();
  const start = performance.now();
  const text = await new Response(
    format === "json" ? query.streamJson() : query.streamNdjson(),
  ).text();
  const elapsedMs = performance.now() - start;
  const actual =
    format === "json"
      ? JSON.parse(text)
      : text
          .trimEnd()
          .split("\n")
          .map((line) => JSON.parse(line));
  assert.deepEqual(actual, expected);
  assert.equal(actual.length, 512);
  assert.equal(pool.usedBytes, 0);
  runs.push({ format, elapsedMs, peakBytes: pool.peakBytes, afterReleaseBytes: pool.usedBytes });
}
console.log(
  JSON.stringify({
    fixtureSha256: createHash("sha256").update(fixture).digest("hex"),
    rows: 512,
    runs,
    elapsedMs: runs.reduce((sum, run) => sum + run.elapsedMs, 0),
    scope:
      "Actual local Parquet queries and JSON/NDJSON output streams. Oracle decoding and consumer response accumulation are outside timing/pool accounting. No cloud-throughput or total-heap claim.",
  }),
);
