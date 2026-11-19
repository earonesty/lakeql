// Build first. Workload: https://github.com/C-Loftus/cloud-native-geo-lance
// Run on both revisions; remote timings are observations, not CI thresholds.
import { createHash } from "node:crypto";
import { cachedObjectStore } from "../packages/core/dist/index.js";
import { httpStore } from "../packages/http/dist/index.js";

const { openLanceDataset } = await import(
  process.env.LANCE_MODULE ?? new URL("../packages/lance/dist/index.js", import.meta.url).href
);

const url = new URL(
  process.argv[2] ??
    "https://storage.googleapis.com/lance-test-bucket/usgs_monitoring_locations.lance",
);
const cut = url.pathname.lastIndexOf("/");
const path = decodeURIComponent(url.pathname.slice(cut + 1));
const baseUrl = `${url.origin}${url.pathname.slice(0, cut)}`;
const select = ["site_no", "feature_name", "mainstem_id", "lon", "lat"];
const box = (w, s, e, n) => ({
  predicate: "intersects",
  geometry: {
    type: "Polygon",
    coordinates: [
      [
        [w, s],
        [e, s],
        [e, n],
        [w, n],
        [w, s],
      ],
    ],
  },
});

for (const concurrency of [8, 32])
  for (const byteCache of [false, true]) {
    let trace;
    const reset = () => {
      trace = { requests: 0, bytes: 0, active: 0, peak: 0, indexRequests: 0, durations: [] };
    };
    reset();
    const transport = httpStore({
      baseUrl,
      fetch: async (input, init) => {
        trace.requests++;
        const response = await fetch(input, { ...init, signal: AbortSignal.timeout(30_000) });
        if (new Headers(init?.headers).has("range") && response.status !== 206)
          throw new Error(`Range request returned ${response.status}: ${input}`);
        return response;
      },
    });
    const measured = {
      head: transport.head.bind(transport),
      getRange: async (path, range) => {
        const current = trace;
        const start = performance.now();
        current.peak = Math.max(current.peak, ++current.active);
        if (path.includes("/_indices/")) current.indexRequests++;
        try {
          const bytes = await transport.getRange(path, range);
          current.bytes += bytes.byteLength;
          return bytes;
        } finally {
          current.active--;
          current.durations.push(performance.now() - start);
        }
      },
      get: async () => {
        throw new Error("Whole-object reads forbidden");
      },
      list: () => {
        throw new Error("Dataset scans forbidden");
      },
      put: async () => {
        throw new Error("Read-only benchmark");
      },
      delete: async () => {
        throw new Error("Read-only benchmark");
      },
    };
    const store = byteCache
      ? cachedObjectStore(measured, { maxBytes: 256 << 20, policy: "latency" })
      : measured;
    const dataset = await openLanceDataset({
      store,
      path,
      indexCacheBytes: 64 << 20,
      coalesceGapBytes: 0,
      budget: {
        maxBytes: 48 << 20,
        maxRangeRequests: 2000,
        maxMemoryBytes: 256 << 20,
        maxOutputRows: 10000,
        maxConcurrentReads: concurrency,
        maxElapsedMs: 120_000,
      },
    });
    console.log(
      JSON.stringify({
        phase: "open",
        concurrency,
        byteCache,
        requests: trace.requests,
        snapshotId: dataset.snapshotId,
      }),
    );
    for (const [phase, query, expectedRows] of [
      ["cold", box(-77.29, 38.76, -76.81, 39.04), 1117],
      ["repeat", box(-77.29, 38.76, -76.81, 39.04), 1117],
      ["nearby", box(-77.19, 38.76, -76.71, 39.04), 693],
    ]) {
      reset();
      global.gc?.();
      const before = process.memoryUsage();
      const start = performance.now();
      const result = await dataset.spatialRows({
        snapshotId: dataset.snapshotId,
        index: "geometry_idx",
        select,
        query,
      });
      const elapsedMs = performance.now() - start;
      const after = process.memoryUsage();
      if (result.rows.length !== expectedRows)
        throw new Error(`Unexpected result count: ${result.rows.length}`);
      const rowsHash = createHash("sha256")
        .update(
          JSON.stringify(
            result.rows
              .map((row, i) => [result.rowIds[i], row])
              .sort((a, b) => String(a[0]).localeCompare(String(b[0]))),
          ),
        )
        .digest("hex");
      const ordered = trace.durations.sort((a, b) => a - b);
      console.log(
        JSON.stringify({
          phase,
          concurrency,
          byteCache,
          rows: result.rows.length,
          rowsHash,
          elapsedMs: +elapsedMs.toFixed(2),
          requests: trace.requests,
          indexRequests: trace.indexRequests,
          bytes: trace.bytes,
          peakConcurrentRanges: trace.peak,
          medianRangeMs: ordered[ordered.length >> 1] ?? 0,
          pagesVisited: result.pagesVisited,
          candidatesExamined: result.candidatesExamined,
          peakAccountedMemoryBytes: result.stats.peakMemoryBytes,
          heapDeltaBytes: after.heapUsed - before.heapUsed,
          rssDeltaBytes: after.rss - before.rss,
          stats: result.stats,
        }),
      );
    }
  }
