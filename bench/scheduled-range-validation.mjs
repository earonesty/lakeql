// Internal stage primitive over real HTTP ranges; not automatic reader scheduling.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryStageScheduler } from "../packages/core/dist/memory-stage-scheduler.js";
import { ReadSemaphore } from "../packages/core/dist/read-semaphore.js";
import { withObjectStoreReadControls } from "../packages/core/dist/store.js";
import { httpStore } from "../packages/http/dist/index.js";

const baseUrl = "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/bench/";
const reports = [];
for (const capacity of [64, 128]) {
  const pool = new MemoryBudget(capacity);
  const scheduler = new MemoryStageScheduler(pool);
  const semaphore = new ReadSemaphore(2);
  let requests = 0;
  let active = 0;
  let peakReads = 0;
  const jobs = Array.from({ length: 4 }, () =>
    scheduler.run({
      budget: pool.child(64),
      bytes: 64,
      purpose: "complete 64-byte range stage",
      async execute(scope, signal) {
        // The memory reservation already exists before any scarce I/O slot is acquired.
        assert.equal(scope.availableAllocationBytes, 64);
        const release = await semaphore.acquire(signal);
        try {
          const store = httpStore({
            baseUrl,
            fetch: async (input, init) => {
              requests++;
              const response = await fetch(input, {
                ...init,
                signal: AbortSignal.any([signal, AbortSignal.timeout(30_000)]),
              });
              assert.equal(response.status, 206);
              assert.equal(response.headers.get("content-range")?.startsWith("bytes 0-63/"), true);
              assert.equal(response.headers.get("content-length"), "64");
              return response;
            },
          });
          active++;
          peakReads = Math.max(peakReads, active);
          try {
            const controlled = withObjectStoreReadControls(store, {
              maxConcurrentReads: 2,
              signal,
            });
            const owner = await controlled.getRangeOwned(
              "window-events.parquet",
              { offset: 0, length: 64 },
              scope,
            );
            assert.equal(owner.value.length, 64);
            assert.equal(new TextDecoder().decode(owner.value.subarray(0, 4)), "PAR1");
            const digest = createHash("sha256").update(owner.value).digest("hex");
            owner.release();
            return digest;
          } finally {
            active--;
          }
        } finally {
          release();
        }
      },
    }),
  );
  assert.equal(scheduler.activeStages, capacity / 64);
  assert.equal(scheduler.queuedStages, 4 - capacity / 64);
  const results = await Promise.all(jobs);
  assert.equal(new Set(results.map((result) => result.value)).size, 1);
  for (const result of results) result.release();
  assert.equal(pool.usedBytes, 0);
  assert.equal(pool.peakBytes, capacity);
  assert.equal(peakReads, capacity / 64);
  assert.equal(requests, 4);
  reports.push({
    capacity,
    requests,
    peakReads,
    peakChargedBytes: pool.peakBytes,
    finalChargedBytes: pool.usedBytes,
    sha256: results[0].value,
  });
}
console.log(
  JSON.stringify(
    {
      scope:
        "Real public R2 HTTP 206 ranges through the internal stage primitive; engine range buffers only. No automatic scan integration, whole-heap bound, or decode working-set claim.",
      reports,
    },
    null,
    2,
  ),
);
