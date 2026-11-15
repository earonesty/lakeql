import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { readParquetFooterEnvelope } from "../packages/parquet/dist/incremental-metadata.js";
import { parquetWriteBuffer } from "../packages/parquet/node_modules/hyparquet-writer/src/index.js";

const cases = [];
for (const width of [1, 8, 32, 128]) {
  const fixture = parquetWriteBuffer({
    columnData: Array.from({ length: width }, (_, i) => ({
      name: `c${String(i).padStart(3, "0")}`,
      type: "INT32",
      data: [i, i + 1],
    })),
    kvMetadata: Array.from({ length: width }, (_, i) => ({
      key: `k${String(i).padStart(3, "0")}`,
      value: "value ✓",
    })),
  });
  const expected = readParquetFooterEnvelope(fixture).metadata;
  const times = [];
  let peakBytes = 0;
  let retainedBytes = 0;
  for (let round = 0; round < 7; round++) {
    const start = performance.now();
    for (let i = 0; i < 20; i++) {
      const pool = new MemoryBudget(16 * 1024 * 1024);
      const scope = new MemoryScope(pool);
      try {
        const result = readParquetFooterEnvelope(fixture, undefined, scope);
        if (i === 0) assert.deepEqual(result.metadata, expected);
        peakBytes = Math.max(peakBytes, pool.peakBytes);
        retainedBytes = Math.max(retainedBytes, pool.usedBytes);
      } finally {
        scope.dispose();
      }
      assert.equal(pool.usedBytes, 0);
    }
    if (round >= 2) times.push((performance.now() - start) / 20);
  }
  times.sort((a, b) => a - b);
  cases.push({
    width,
    fixtureSha256: createHash("sha256").update(new Uint8Array(fixture)).digest("hex"),
    elapsedMs: times[2],
    peakBytes,
    retainedBytes,
    afterReleaseBytes: 0,
  });
}
console.log(
  JSON.stringify({
    cases,
    elapsedMs: cases.reduce((sum, result) => sum + result.elapsedMs, 0),
    scope:
      "Generated valid Parquet footer envelope decoding, matching all schema/key-value metadata. Fixed 16 MiB pool; fixture backing/runtime excluded. Local CPU and modeled admission, not cloud throughput or total heap.",
  }),
);
