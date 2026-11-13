// Read-only R2 footer validation. Build TypeScript before running.
import assert from "node:assert/strict";
import { MemoryBudget } from "../packages/core/dist/index.js";
import { httpStore } from "../packages/http/dist/index.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/index.js";
import {
  readOwnedParquetMetadata,
  readParquetMetadataFromFile,
} from "../packages/parquet/dist/metadata-cache.js";

const baseUrl = "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/bench/";
const path = "window-events.parquet";
const requests = [];
const store = httpStore({
  baseUrl,
  fetch: async (input, init) => {
    const response = await fetch(input, { ...init, signal: AbortSignal.timeout(30000) });
    if (init?.method !== "HEAD") {
      const range = new Headers(init?.headers).get("Range");
      assert.ok(range, "Full-object requests are forbidden");
      assert.equal(response.status, 206);
      requests.push({ range, bytes: Number(response.headers.get("Content-Length")) });
    }
    return response;
  },
});
const file = await asyncBufferFromStore(store, path);
// Discovery/oracle requests are intentionally outside measured ownership cases.
const trailer = await file.slice(file.byteLength - 8, file.byteLength);
const footerBytes = new DataView(trailer).getUint32(0, true) + 8;
const oracle = await readParquetMetadataFromFile(file);
const reports = [];
for (const cap of [footerBytes - 1, footerBytes, footerBytes * 2]) {
  requests.length = 0;
  const budget = new MemoryBudget(cap);
  if (cap < footerBytes) {
    await assert.rejects(readOwnedParquetMetadata(path, file, budget), {
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
  } else {
    const { owner } = await readOwnedParquetMetadata(path, file, budget);
    try {
      assert.deepEqual(owner.value, oracle);
    } finally {
      owner.release();
    }
  }
  assert.equal(budget.usedBytes, 0);
  assert.ok(budget.peakBytes <= cap);
  reports.push({ cap, footerBytes, peakBytes: budget.peakBytes, requests: [...requests] });
}
console.log(
  JSON.stringify(
    {
      source: baseUrl + path,
      scope:
        "Actual public R2 HTTP 206 responses; encoded allocation bounds only, excluding parsed JS objects and whole-worker heap",
      reports,
    },
    null,
    2,
  ),
);
