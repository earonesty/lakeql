import assert from "node:assert/strict";
import {
  MemoryBudget,
  SharedMemoryCache,
  withObjectStoreReadControls,
} from "../packages/core/dist/index.js";
import { httpStore } from "../packages/http/dist/index.js";
import { createParquetLake } from "../packages/parquet/dist/index.js";

const baseUrl = "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/bench/";
const path = "window-events.parquet";
const reports = [];
const oracle = [];
for (const _region of ["north", "south", "west"])
  for (let account = 0; account < 4; account++)
    for (let i = 0; i < 12; i++)
      oracle.push({
        event_id: oracle.length + 1,
        amount: i % 5 === 4 ? null : 10 + account * 4 + i * 1.5,
      });
for (const memoryMiB of [64, 128])
  for (const jobs of [1, 4])
    for (const mode of ["auto", "cached"]) {
      const requests = [];
      const store = withObjectStoreReadControls(
        httpStore({
          baseUrl,
          fetch: async (input, init) => {
            const signals = [AbortSignal.timeout(30000)];
            if (init?.signal) signals.push(init.signal);
            const response = await fetch(input, { ...init, signal: AbortSignal.any(signals) });
            if (init?.method !== "HEAD") {
              const range = new Headers(init?.headers).get("Range");
              assert.ok(range);
              assert.equal(response.status, 206);
              requests.push({
                kind: range === "bytes=0-1" ? "discovery-probe" : "scan-range",
                range,
                bytes: Number(response.headers.get("Content-Length")),
              });
            }
            return response;
          },
        }),
        { maxConcurrentReads: 2 },
      );
      const pool = new MemoryBudget(memoryMiB * 1024 * 1024);
      const cache = new SharedMemoryCache({ maxBytes: 1024 * 1024, memoryBudget: pool });
      const lake = createParquetLake({
        store,
        memoryPool: pool,
        scanMetadataMode: mode,
        scanRangeCache: { maxBytes: 1024 * 1024, coalesceBytes: 0, sharedCache: cache },
        budget: { maxAllocatedMemoryBytes: pool.maxBytes, maxConcurrentReads: 2 },
        queryId: () => "cloud-planning-parity",
      });
      const rounds = [];
      for (const temperature of ["cold", "warm"]) {
        requests.length = 0;
        const start = performance.now();
        const results = await Promise.all(
          Array.from({ length: jobs }, async () => {
            const result = lake.path(path).select(["event_id", "amount"]).batchSize(7).run();
            const rows = await result.toArray();
            return { rows, stats: result.stats };
          }),
        );
        const milliseconds = performance.now() - start;
        for (const result of results) assert.deepEqual(result.rows, oracle);
        if (temperature === "warm")
          assert.equal(
            requests.filter((request) => request.kind === "scan-range").length,
            0,
            JSON.stringify({ mode, jobs, memoryMiB, requests }),
          );
        assert.ok(pool.peakBytes <= pool.maxBytes);
        rounds.push({
          temperature,
          milliseconds,
          jobStats: results.map((result) => ({
            cacheHits: result.stats.cacheHits,
            cacheMisses: result.stats.cacheMisses,
            rowsReturned: result.stats.rowsReturned,
          })),
          requests: [...requests],
          trackedPeakBytes: pool.peakBytes,
          retainedTrackedBytes: pool.usedBytes,
        });
      }
      cache.deleteWhere(() => true);
      assert.equal(pool.usedBytes, 0);
      reports.push({ memoryMiB, jobs, mode, rounds });
    }
console.log(
  JSON.stringify(
    {
      source: baseUrl + path,
      scope:
        "Public numeric scans over actual R2 HTTP 206; shared max-two read control, cold/warm rounds and independent fixture row parity. Single round per case, not stable latency estimates. Tracked allocations exclude decoded JS and runtime transport heap; 64/128 MiB are configured pools, not observed whole-Worker heap caps.",
      reports,
    },
    null,
    2,
  ),
);
