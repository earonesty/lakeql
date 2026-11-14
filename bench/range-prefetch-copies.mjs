import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { writeFile } from "node:fs/promises";
import { mapConcurrent, SharedMemoryCache } from "../packages/core/dist/index.js";
import { cachedRangeBuffer } from "../packages/parquet/dist/range-cache.js";

// Isolate the removed copy: the control performs discarded demand reads of the
// same four coalesced windows. This is not a historical full-revision comparison.
async function run(width, shared, variant) {
  const metrics = {
    requests: 0,
    fetchedBytes: 0,
    copiedBytes: 0,
    copies: 0,
    active: 0,
    peakReads: 0,
  };
  const windows = Array.from({ length: 4 }, (_, i) => ({ start: i * width, end: (i + 1) * width }));
  const file = {
    byteLength: 4 * width,
    async slice(start, end = 4 * width) {
      metrics.requests++;
      metrics.fetchedBytes += end - start;
      metrics.peakReads = Math.max(metrics.peakReads, ++metrics.active);
      try {
        await new Promise((resolve) => setTimeout(resolve, 1));
        const bytes = new Uint8Array(end - start).fill(start / width + 1).buffer;
        const copy = bytes.slice.bind(bytes);
        bytes.slice = (a, b) => {
          const result = copy(a, b);
          metrics.copies++;
          metrics.copiedBytes += result.byteLength;
          return result;
        };
        return bytes;
      } finally {
        metrics.active--;
      }
    },
  };
  const sharedCache = shared ? new SharedMemoryCache({ maxBytes: 4 * width }) : undefined;
  const cache = cachedRangeBuffer(
    file,
    {
      maxBytes: 4 * width,
      coalesceBytes: width,
      maxConcurrentPrefetchReads: 2,
      ...(sharedCache ? { sharedCache } : {}),
    },
    "prefetch-copy-benchmark",
  );
  try {
    const started = performance.now();
    if (variant === "ensure-cache") {
      await cache.prefetch(
        windows.flatMap(({ start }) => [
          { start: start + 1, end: start + 17 },
          { start: start + 20, end: start + 36 },
        ]),
      );
    } else
      await mapConcurrent(windows, 2, async ({ start, end }) => {
        await cache.slice(start, end);
      });
    const elapsedMs = performance.now() - started;
    const measured = { ...metrics, elapsedMs };
    assert.equal(metrics.requests, 4);
    assert.equal(metrics.fetchedBytes, 4 * width);
    assert.equal(metrics.peakReads, 2);
    assert.equal(metrics.active, 0);
    assert.equal(metrics.copiedBytes, variant === "ensure-cache" ? 0 : 4 * width);
    assert.equal(metrics.copies, variant === "ensure-cache" ? 0 : 4);
    const output = new Uint8Array(await cache.slice(width + 1, width + 17));
    assert.deepEqual(output, new Uint8Array(16).fill(2));
    output.fill(99);
    assert.deepEqual(
      new Uint8Array(await cache.slice(width + 1, width + 17)),
      new Uint8Array(16).fill(2),
    );
    assert.equal(metrics.requests, 4);
    return measured;
  } finally {
    sharedCache?.deleteWhere(() => true);
  }
}

const results = [];
for (const width of [256 * 1024, 1024 * 1024]) {
  for (const shared of [false, true]) {
    const variants = { "ensure-cache": [], "discarded-demand-control": [] };
    for (let round = 0; round < 12; round++) {
      const order = Object.keys(variants);
      if (round % 2) order.reverse();
      for (const variant of order) {
        const sample = await run(width, shared, variant);
        if (round >= 4) variants[variant].push(sample);
      }
    }
    results.push({ windowBytes: width, shared, variants });
  }
}
const report = {
  runtimeSource: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  node: process.version,
  conditions: {
    windows: 4,
    concurrency: 2,
    simulatedReadDelayMs: 1,
    warmupRounds: 4,
    measuredRounds: 8,
    alternatingOrder: true,
  },
  results,
  limitations: [
    "Controlled in-memory transport; not cloud latency or throughput.",
    "Counts ArrayBuffer slice output bytes, not heap or GC residency.",
    "Control models discarded demand-copy behavior after identical range coalescing; not a full historical implementation.",
    "Independent ownership, cache identity and cancellation contracts also require the focused test suite.",
  ],
};
const json = `${JSON.stringify(report, null, 2)}\n`;
if (process.env.LAKEQL_PREFETCH_COPY_REPORT)
  await writeFile(process.env.LAKEQL_PREFETCH_COPY_REPORT, json);
else process.stdout.write(json);
