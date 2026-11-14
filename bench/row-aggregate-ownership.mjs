import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { memoryStore } from "../packages/core/dist/memory-store.js";
import { Lake } from "../packages/core/dist/query.js";

const spec = {
  count: { op: "count" },
  sum: { op: "sum", column: "amount" },
  median: { op: "median", column: "amount" },
  quantile: { op: "quantile", column: "amount", quantile: 0.5 },
  distinct: { op: "count_distinct", column: "label" },
  mode: { op: "mode", column: "label" },
  last: { op: "last", column: "amount" },
};
async function run(rows, pool) {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    ...(process.env.LAKEQL_ROW_AGGREGATE_SERIALIZED === "1"
      ? { budget: { maxMemoryBytes: 64 * 1024 * 1024 } }
      : {}),
    scanner: {
      async *scan() {
        const batch = Array.from({ length: 128 }, () => ({ bucket: 0, amount: 0, label: "" }));
        for (let base = 0; base < rows; base += batch.length) {
          for (let i = 0; i < batch.length; i++) {
            const id = base + i;
            Object.assign(batch[i], { bucket: id % 8, amount: id, label: `label-${id % 16}` });
          }
          yield batch;
        }
      },
    },
  });
  const result = await lake.path("table").run().aggregateWithState(["bucket"], spec);
  assert.equal(result.rows.length, 8);
  for (const row of result.rows) {
    const first = row.bucket;
    const last = rows - 8 + first;
    assert.equal(row.count, rows / 8);
    assert.equal(row.sum, ((first + last) * (rows / 8)) / 2);
    assert.equal(row.median, (first + last) / 2);
    assert.equal(row.quantile, row.median);
    assert.equal(row.distinct, 2);
    assert.equal(row.mode, `label-${first}`);
    assert.equal(row.last, last);
  }
  assert.ok(result.operatorState.byteLength > 0);
}
const results = [];
for (const rows of [1024, 4096, 16384]) {
  const samples = [];
  let peakBytes = 0;
  for (let round = 0; round < 5; round++) {
    const pool = new MemoryBudget(64 * 1024 * 1024);
    const start = performance.now();
    await run(rows, pool);
    const ms = performance.now() - start;
    assert.equal(pool.usedBytes, 0);
    if (round >= 2) samples.push(ms);
    peakBytes = pool.peakBytes;
  }
  results.push({
    rows,
    samples,
    medianMilliseconds: [...samples].sort((a, b) => a - b)[1],
    peakBytes,
  });
}
const concurrent = [];
for (const capMiB of [64, 128]) {
  const pool = new MemoryBudget(capMiB * 1024 * 1024);
  const start = performance.now();
  await Promise.all(Array.from({ length: 4 }, () => run(16384, pool)));
  assert.equal(pool.usedBytes, 0);
  concurrent.push({
    capMiB,
    jobs: 4,
    rowsPerJob: 16384,
    elapsedMilliseconds: performance.now() - start,
    peakBytes: pool.peakBytes,
  });
}
const report = {
  description:
    "Local reused row-batch scanner; eight groups and seven aggregate states, including growing median/quantile and distinct/mode. Two warmups and three timed samples per size. Independent arithmetic assertions and final zero pool usage. Input scanner buffers and runtime heap are outside reservation accounting; this is not cloud transport or total Worker heap certification.",
  serializedSizeAccounting: process.env.LAKEQL_ROW_AGGREGATE_SERIALIZED === "1",
  querySourceSha256: createHash("sha256")
    .update(readFileSync("packages/core/src/query.ts"))
    .digest("hex"),
  results,
  concurrent,
};
writeFileSync(
  process.env.LAKEQL_ROW_AGGREGATE_REPORT ??
    "docs/benchmarks/row-aggregate-ownership-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(report, null, 2));
