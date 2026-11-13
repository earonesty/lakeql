import assert from "node:assert/strict";
import {
  MemoryBudget,
  SharedMemoryCache,
  withObjectStoreReadControls,
} from "../packages/core/dist/index.js";
import { MemoryStageScheduler } from "../packages/core/dist/memory-stage-scheduler.js";
import { httpStore } from "../packages/http/dist/index.js";
import { cachedRangeBuffer } from "../packages/parquet/dist/range-cache.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/store-buffer.js";

let requests = 0;
let wireBytes = 0;
let measuringStage = false;
let metadataProbeRequests = 0;
let metadataProbeBytes = 0;
const store = httpStore({
  baseUrl: "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/bench/",
  fetch: async (input, init) => {
    const response = await fetch(input, {
      ...init,
      signal: AbortSignal.any([
        AbortSignal.timeout(30_000),
        ...(init?.signal ? [init.signal] : []),
      ]),
    });
    if (init?.method !== "HEAD") {
      assert.equal(response.status, 206);
      if (!measuringStage) {
        metadataProbeRequests++;
        metadataProbeBytes += Number(response.headers.get("content-length"));
        return response;
      }
      assert.equal(response.headers.get("content-range")?.startsWith("bytes 0-4095/"), true);
      requests++;
      wireBytes += Number(response.headers.get("content-length"));
    }
    return response;
  },
});
const source = await asyncBufferFromStore(
  withObjectStoreReadControls(store, { maxConcurrentReads: 2 }),
  "window-events.parquet",
);
measuringStage = true;
const extent = 4096 + 3 * 16;
const pool = new MemoryBudget(extent);
const cache = new SharedMemoryCache({ maxBytes: 4096, memoryBudget: pool });
const file = cachedRangeBuffer(
  source,
  { maxBytes: 4096, coalesceBytes: 4096, sharedCache: cache },
  "scheduled-public-fixture",
);
const scheduler = new MemoryStageScheduler(pool);
const output = await scheduler.run({
  budget: pool.child(),
  bytes: extent,
  purpose: "coalesced input and three copied outputs",
  async execute(scope) {
    const [first, second] = await Promise.all([
      file.sliceOwned(0, 16, scope),
      file.sliceOwned(16, 32, scope),
    ]);
    assert.equal(new TextDecoder().decode(new Uint8Array(first.value, 0, 4)), "PAR1");
    assert.equal(first.value.byteLength, 16);
    assert.equal(second.value.byteLength, 16);
    assert.equal(scope.availableAllocationBytes, 16);
    const expected = Array.from(new Uint8Array(first.value));
    first.release();
    second.release();
    const warm = await file.sliceOwned(0, 16, scope);
    assert.deepEqual(Array.from(new Uint8Array(warm.value)), expected);
    warm.release();
    assert.equal(scope.availableAllocationBytes, 0);
    return "validated";
  },
});
output.release();
assert.equal(requests, 1);
assert.equal(wireBytes, 4096);
assert.equal(pool.peakBytes, extent);
assert.equal(pool.usedBytes, 4096);
cache.deleteWhere(() => true);
assert.equal(pool.usedBytes, 0);
console.log(
  JSON.stringify(
    {
      scope:
        "Public HTTP 206 through scheduled stage, controlled store and owned range cache; exact engine buffer accounting, not full heap or automatic query scheduling",
      requests,
      metadataProbeRequests,
      metadataProbeBytes,
      wireBytes,
      copies: 3,
      extent,
      peakChargedBytes: pool.peakBytes,
      cachedBytesAfterStage: 4096,
      finalChargedBytes: pool.usedBytes,
    },
    null,
    2,
  ),
);
