import assert from "node:assert/strict";
import {
  MemoryBudget,
  SharedMemoryCache,
  withObjectStoreReadControls,
} from "../packages/core/dist/index.js";
import { httpStore } from "../packages/http/dist/index.js";
import { asyncBufferFromStore, createParquetLake } from "../packages/parquet/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { planRowGroupsFromMetadata } from "../packages/parquet/dist/row-group-plan.js";
import { fetchWithTimeout } from "./bench-utils.mjs";

const baseUrl = "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/bench/";
const path = "window-events.parquet";
const reports = [];
// Independent ordinary-reader oracle; discovery is outside measured scenarios.
const oracleStore = httpStore({ baseUrl, fetch: fetchWithTimeout });
const metadata = await readParquetMetadataFromFile(await asyncBufferFromStore(oracleStore, path));
const expectedPlan = planRowGroupsFromMetadata(metadata);
const oracle = [
  { rowGroupCount: metadata.row_groups.length, rowGroupRanges: expectedPlan.rowGroupRanges },
];
for (const memoryMiB of [64, 128])
  for (const jobs of [1, 4])
    for (const mode of ["auto", "cached"]) {
      const requests = [];
      const store = withObjectStoreReadControls(
        httpStore({
          baseUrl,
          fetch: async (input, init) => {
            const response = await fetchWithTimeout(input, init);
            if (init?.method !== "HEAD") {
              const range = new Headers(init?.headers).get("Range");
              assert.ok(range);
              assert.equal(response.status, 206);
              const length = response.headers.get("Content-Length");
              assert.ok(length !== null && /^[0-9]+$/.test(length));
              const bytes = Number(length);
              assert.ok(Number.isSafeInteger(bytes));
              requests.push({
                kind: range === "bytes=0-1" ? "discovery-probe" : "footer",
                range,
                bytes,
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
        taskMetadataMode: mode,
        scanRangeCache: { maxBytes: 1024 * 1024, coalesceBytes: 0, sharedCache: cache },
        budget: { maxAllocatedMemoryBytes: pool.maxBytes, maxConcurrentReads: 2 },
        queryId: () => "cloud-planning-parity",
      });
      const rounds = [];
      for (const temperature of ["cold", "warm"]) {
        requests.length = 0;
        const start = performance.now();
        const results = await Promise.all(
          Array.from({ length: jobs }, () => lake.path(path).planTasks()),
        );
        const milliseconds = performance.now() - start;
        for (const tasks of results) {
          const shape = tasks.map(({ rowGroupCount, rowGroupRanges }) => ({
            rowGroupCount,
            rowGroupRanges,
          }));
          assert.deepEqual(shape, oracle);
        }
        if (temperature === "warm")
          assert.equal(
            requests.filter((request) => request.kind === "footer").length,
            0,
            JSON.stringify({ mode, jobs, memoryMiB, requests }),
          );
        assert.ok(pool.peakBytes <= pool.maxBytes);
        rounds.push({
          temperature,
          milliseconds,
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
        "Public task planning over actual R2 HTTP 206; shared max-two read control, cold/warm rounds and plan parity. Single round per case, not stable latency estimates. Tracked allocations include admitted metadata containers but exclude remaining runtime overhead; 64/128 MiB are configured pools, not observed whole-Worker heap caps.",
      oracle,
      reports,
    },
    null,
    2,
  ),
);
