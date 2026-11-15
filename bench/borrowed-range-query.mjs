import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import {
  MemoryBudget,
  materializeBatchRows,
  memoryStore,
  SharedMemoryCache,
} from "../packages/core/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { cachedRangeBuffer } from "../packages/parquet/dist/range-cache.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/store-buffer.js";
import { readParquetVectorBatchesFromFile } from "../packages/parquet/dist/vector-batches.js";

const fixture = await readFile(
  new URL(
    "../fixtures/external/parquet-testing/data/overflow_i16_page_cnt.parquet",
    import.meta.url,
  ),
);
const source = memoryStore();
await source.put("fixture.parquet", fixture);
const cases = [];
for (const shared of [false, true]) {
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 16 * 1024 * 1024 });
  let requests = 0;
  const store = {
    ...Object.fromEntries(
      [
        "get",
        "put",
        "delete",
        "head",
        "headOwned",
        "list",
        "getRangeOwnedAllocation",
        "getRangeOwned",
      ]
        .filter((name) => typeof source[name] === "function")
        .map((name) => [name, source[name].bind(source)]),
    ),
    async getRange(...args) {
      requests++;
      return source.getRange(...args);
    },
  };
  const file = cachedRangeBuffer(
    await asyncBufferFromStore(store, "fixture.parquet"),
    {
      maxBytes: 16 * 1024 * 1024,
      coalesceBytes: 65536,
      cacheOptions: { memoryBudget: pool },
      ...(shared ? { sharedCache: cache } : {}),
    },
    "fixture.parquet",
  );
  const runs = [];
  let expected;
  try {
    for (const temperature of ["cold", "warm"]) {
      const before = requests;
      const started = performance.now();
      const metadata = await readParquetMetadataFromFile(file);
      const columns = metadata.schema
        .filter((field) => field.type !== undefined)
        .map((field) => field.name);
      const rows = [];
      for await (const batch of readParquetVectorBatchesFromFile(file, metadata, {
        columns,
        rowStart: 0,
        rowEnd: 512,
        batchSize: 128,
      }))
        rows.push(...materializeBatchRows(batch.batch));
      const elapsedMs = performance.now() - started;
      assert.equal(rows.length, 512);
      if (temperature === "cold") expected = rows;
      else {
        assert.deepEqual(rows, expected);
        assert.equal(requests - before, 0);
      }
      runs.push({
        temperature,
        elapsedMs,
        requests: requests - before,
        retainedBytes: pool.usedBytes,
      });
    }
  } finally {
    cache.deleteWhere(() => true);
    const reclaim = pool.reserve(pool.maxBytes, "reclaim private benchmark cache");
    reclaim.release();
  }
  assert.equal(pool.usedBytes, 0);
  cases.push({ shared, runs, afterReleaseBytes: pool.usedBytes });
}
console.log(
  JSON.stringify({
    fixtureSha256: createHash("sha256").update(fixture).digest("hex"),
    rowsPerQuery: 512,
    poolBytes: 64 * 1024 * 1024,
    cacheCapacityBytes: 16 * 1024 * 1024,
    cases,
    elapsedMs: cases.flatMap((value) => value.runs).reduce((sum, run) => sum + run.elapsedMs, 0),
    scope:
      "Borrowed reads over a real external Parquet fixture, private and shared caches; all cold/warm values and zero warm requests checked. Source/returned rows and transport buffers are caller-managed. Pool statistics cover cache/key accounting, not process memory or cloud throughput. Private cache cleanup uses root reclamation; its forced full-pool reservation is excluded from timing and no peak metric is reported.",
  }),
);
