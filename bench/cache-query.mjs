import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { MemoryBudget, memoryStore, SharedMemoryCache } from "../packages/core/dist/index.js";
import { createParquetLake } from "../packages/parquet/dist/index.js";

const fixture = await readFile(
  new URL(
    "../fixtures/external/parquet-testing/data/overflow_i16_page_cnt.parquet",
    import.meta.url,
  ),
);
const source = memoryStore();
await source.put("fixture.parquet", fixture);
let requests = 0;
const store = {
  ...Object.fromEntries(
    ["get", "put", "delete", "head", "headOwned", "list", "getRangeOwnedAllocation"]
      .filter((name) => typeof source[name] === "function")
      .map((name) => [name, source[name].bind(source)]),
  ),
  async getRange(...args) {
    requests++;
    return source.getRange(...args);
  },
  async getRangeOwned(...args) {
    requests++;
    return source.getRangeOwned(...args);
  },
};
const pool = new MemoryBudget(64 * 1024 * 1024);
const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 4 * 1024 * 1024 });
const lake = createParquetLake({
  store,
  memoryPool: pool,
  scanRangeCache: { maxBytes: 4 * 1024 * 1024, coalesceBytes: 65536, sharedCache: cache },
});
const runs = [];
let expected;
try {
  for (const temperature of ["cold", "warm"]) {
    const before = requests;
    const start = performance.now();
    const output = await lake
      .path("fixture.parquet")
      .batchSize(128)
      .limit(512)
      .run()
      .toOwnedArray();
    const elapsedMs = performance.now() - start;
    try {
      assert.equal(output.value.length, 512);
      if (temperature === "cold") expected = structuredClone(output.value);
      else {
        assert.deepEqual(output.value, expected);
        assert.equal(requests - before, 0);
      }
    } finally {
      output.release();
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
}
assert.equal(pool.usedBytes, 0);
console.log(
  JSON.stringify({
    fixtureSha256: createHash("sha256").update(fixture).digest("hex"),
    rowsPerQuery: 512,
    poolBytes: pool.maxBytes,
    cacheCapacityBytes: 4 * 1024 * 1024,
    runs,
    elapsedMs: runs.reduce((sum, run) => sum + run.elapsedMs, 0),
    peakBytes: pool.peakBytes,
    afterReleaseBytes: pool.usedBytes,
    scope:
      "Cold/warm owned queries over a real external many-page Parquet fixture in memoryStore; source storage, oracle copy and runtime heap excluded. No cloud-throughput claim.",
  }),
);
