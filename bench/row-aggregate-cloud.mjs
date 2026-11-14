import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { Lake, MemoryBudget, SharedMemoryCache } from "../packages/core/dist/index.js";
import { httpStore } from "../packages/http/dist/index.js";
import { parquetScanner } from "../packages/parquet/dist/index.js";

const baseUrl = "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/bench/";
const values = [];
for (let region = 0; region < 3; region++)
  for (let account = 0; account < 4; account++)
    for (let i = 0; i < 12; i++) if (i % 5 !== 4) values.push(10 + account * 4 + i * 1.5);
const ordered = [...values].sort((a, b) => a - b);
const median = (ordered[59] + ordered[60]) / 2;
const reports = [];
for (const capMiB of [64, 128]) {
  const pool = new MemoryBudget(capMiB * 1024 * 1024);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 65536 });
  let reads = 0;
  let bytes = 0;
  const store = httpStore({
    baseUrl,
    fetch: async (input, init) => {
      const response = await fetch(input, {
        ...init,
        signal: AbortSignal.any([
          AbortSignal.timeout(30000),
          ...(init?.signal ? [init.signal] : []),
        ]),
      });
      if (init?.method !== "HEAD") {
        assert.equal(response.status, 206);
        assert.ok(new Headers(init?.headers).has("Range"));
        reads++;
        bytes += Number(response.headers.get("content-length") ?? 0);
      }
      return response;
    },
  });
  const scanner = parquetScanner(store, {
    scanRangeCache: { maxBytes: 65536, coalesceBytes: 0, sharedCache: cache },
  });
  for (const temperature of ["cold", "warm"]) {
    reads = 0;
    bytes = 0;
    const start = performance.now();
    await Promise.all(
      Array.from({ length: 4 }, async () => {
        const lake = new Lake({
          store,
          memoryPool: pool,
          budget: { maxAllocatedMemoryBytes: 8 * 1024 * 1024 },
          scanner: { scan: scanner.scan.bind(scanner) },
        });
        const spec = {
          count: { op: "count" },
          expressionCount: {
            op: "count",
            expr: {
              kind: "call",
              fn: "replace",
              args: [
                {
                  kind: "call",
                  fn: "cast",
                  args: [
                    { kind: "column", name: "amount" },
                    { kind: "literal", value: "string" },
                  ],
                },
                { kind: "literal", value: "1" },
                { kind: "literal", value: "$`é😀$'" },
              ],
            },
          },
          sum: { op: "sum", column: "amount" },
          median: { op: "median", column: "amount" },
          quantile: { op: "quantile", column: "amount", quantile: 0.5 },
          distinct: { op: "count_distinct", column: "amount" },
        };
        const result = await lake.path("window-events.parquet").run().aggregateWithState([], spec);
        assert.deepEqual(result.rows, [
          {
            count: 144,
            expressionCount: values.length,
            sum: values.reduce((a, b) => a + b, 0),
            median,
            quantile: median,
            distinct: new Set(values).size,
          },
        ]);
        assert.ok(result.operatorState.byteLength > 0);
        if (process.env.LAKEQL_ROW_AGGREGATE_CLOUD_RESTORE === "1") {
          const resumed = await lake
            .path("window-events.parquet")
            .run()
            .aggregateWithState([], spec, {
              operatorState: result.operatorState,
            });
          assert.deepEqual(resumed.rows, [
            {
              count: 288,
              expressionCount: 2 * values.length,
              sum: 2 * values.reduce((a, b) => a + b, 0),
              median,
              quantile: median,
              distinct: new Set(values).size,
            },
          ]);
        }
      }),
    );
    assert.ok(pool.usedBytes > 0);
    assert.ok(pool.usedBytes <= 65536);
    reports.push({
      capMiB,
      jobs: 4,
      temperature,
      reads,
      transferredBytes: bytes,
      peakBytes: pool.peakBytes,
      retainedCacheBytes: pool.usedBytes,
      elapsedMilliseconds: performance.now() - start,
    });
  }
  cache.deleteWhere(() => true);
  assert.equal(pool.usedBytes, 0);
}
const report = {
  checkpointResume: process.env.LAKEQL_ROW_AGGREGATE_CLOUD_RESTORE === "1",
  source: `${baseUrl}window-events.parquet`,
  scope:
    "Actual public R2 HTTP 206; forced row scanner; four concurrent stateful aggregate queries; 64/128 MiB shared pools with 8 MiB per-query caps and shared range cache. Independent fixture arithmetic oracle. Small fixture operational evidence, not throughput scaling or total Worker heap certification.",
  reports,
};
writeFileSync(
  process.env.LAKEQL_ROW_AGGREGATE_CLOUD_REPORT ??
    "docs/benchmarks/row-aggregate-cloud-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(report, null, 2));
