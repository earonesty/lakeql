import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { createServer } from "node:http";
import {
  lt,
  MemoryBudget,
  memoryStore,
  SharedMemoryCache,
  withObjectStoreReadControls,
} from "../packages/core/dist/index.js";
import { HttpObjectStore } from "../packages/http/dist/index.js";
import { createParquetLake } from "../packages/parquet/dist/index.js";
import { parquetWriteBuffer } from "../packages/parquet/node_modules/hyparquet-writer/src/index.js";

const outputPath =
  process.env.LAKEQL_MIXED_MEMORY_OUTPUT ??
  `docs/benchmarks/mixed-query-memory-${new Date().toISOString().slice(0, 10)}.json`;
const count = 2048;
const rows = Array.from({ length: count }, (_, id) => ({ id, text: `${id}:`.padEnd(4096, "x") }));
const bytes = new Uint8Array(
  parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    rowGroupSize: Array(16).fill(128),
    columnData: [
      { name: "id", type: "INT32", data: rows.map((r) => r.id) },
      { name: "text", type: "STRING", data: rows.map((r) => r.text) },
    ],
  }),
);
const local = memoryStore();
await local.put("rows.parquet", bytes);
let requests = 0,
  transferred = 0;
let activeReads = 0,
  peakActiveReads = 0;
const server = createServer((request, response) => {
  if (request.method === "HEAD") {
    response.writeHead(200, { "Content-Length": bytes.length }).end();
    return;
  }
  const range = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
  if (!range) {
    response.writeHead(400).end();
    return;
  }
  const start = Number(range[1]),
    end = Math.min(bytes.length, Number(range[2]) + 1);
  requests++;
  activeReads++;
  peakActiveReads = Math.max(peakActiveReads, activeReads);
  let timer;
  response.once("close", () => {
    clearTimeout(timer);
    activeReads--;
  });
  transferred += end - start;
  timer = setTimeout(
    () =>
      response
        .writeHead(206, {
          "Content-Length": end - start,
          "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}`,
          ETag: '"mixed-fixture"',
        })
        .end(bytes.subarray(start, end)),
    2,
  );
});
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const reports = [];
try {
  const remote = new HttpObjectStore({ baseUrl: `http://127.0.0.1:${server.address().port}/` });
  for (const [transport, rawStore] of [
    ["memory", local],
    ["http-206", remote],
  ])
    for (const mib of [64, 128])
      for (const cacheMiB of [4, 16]) {
        const pool = new MemoryBudget(mib * 1024 * 1024);
        const cache = new SharedMemoryCache({
          maxBytes: cacheMiB * 1024 * 1024,
          memoryBudget: pool,
        });
        const store = withObjectStoreReadControls(rawStore, { maxConcurrentReads: 2 });
        const lake = createParquetLake({
          store,
          memoryPool: pool,
          scanRangeCache: {
            maxBytes: cacheMiB * 1024 * 1024,
            coalesceBytes: 0,
            sharedCache: cache,
          },
          budget: { maxAllocatedMemoryBytes: 32 * 1024 * 1024, maxConcurrentReads: 2 },
        });
        const pinned = await lake.path("rows.parquet").batchSize(64).run().toOwnedArray();
        assert.deepEqual(pinned.value, rows);
        cache.deleteWhere(() => true);
        const pinnedBytes = pool.usedBytes;
        assert.ok(pinnedBytes > 0);
        const rounds = [];
        try {
          for (const temperature of ["cold", "warm"]) {
            assert.equal(activeReads, 0);
            requests = transferred = peakActiveReads = 0;
            const started = performance.now();
            const runtimePeak = process.memoryUsage();
            const sampleMemory = () => {
              const sample = process.memoryUsage();
              for (const key of Object.keys(sample))
                runtimePeak[key] = Math.max(runtimePeak[key], sample[key]);
            };
            const sampler = setInterval(sampleMemory, 10);
            const outputs = [];
            let active = 0,
              peakActive = 0;
            const jobs = [
              [
                "collect",
                async () => {
                  const out = await lake.path("rows.parquet").batchSize(64).run().toOwnedArray();
                  outputs.push(out);
                  assert.deepEqual(out.value, rows);
                },
              ],
              [
                "select",
                async () => {
                  const out = await lake
                    .path("rows.parquet")
                    .where(lt("id", count / 2))
                    .batchSize(64)
                    .run()
                    .toOwnedArray();
                  outputs.push(out);
                  assert.deepEqual(out.value, rows.slice(0, count / 2));
                },
              ],
              [
                "top-k",
                async () => {
                  const out = await lake
                    .path("rows.parquet")
                    .orderBy([{ column: "id", direction: "desc" }])
                    .limit(64)
                    .batchSize(64)
                    .run()
                    .toOwnedArray();
                  outputs.push(out);
                  assert.deepEqual(out.value, rows.slice(-64).reverse());
                },
              ],
              [
                "aggregate",
                async () => {
                  const out = await lake
                    .path("rows.parquet")
                    .batchSize(64)
                    .groupBy([])
                    .aggregate({ total: { op: "sum", column: "id" } });
                  assert.deepEqual(out, [{ total: (count * (count - 1)) / 2 }]);
                },
              ],
            ];
            let outcomes;
            try {
              outcomes = await Promise.all(
                jobs.map(async ([kind, run]) => {
                  const jobStarted = performance.now();
                  active++;
                  peakActive = Math.max(active, peakActive);
                  try {
                    await run();
                    return { kind, status: "completed", elapsedMs: performance.now() - jobStarted };
                  } catch (error) {
                    return error.code === "LAKEQL_BUDGET_EXCEEDED"
                      ? { kind, status: "admission-rejected" }
                      : {
                          kind,
                          status: "failed",
                          error: {
                            code: error.code,
                            message: error.message,
                            details: error.details,
                          },
                        };
                  } finally {
                    active--;
                  }
                }),
              );
            } finally {
              sampleMemory();
              clearInterval(sampler);
            }
            assert.equal(peakActive, 4);
            assert.ok(peakActiveReads <= 2, `HTTP concurrency exceeded limit: ${peakActiveReads}`);
            assert.ok(outcomes.some((o) => o.status === "completed"));
            assert.deepEqual(pinned.value, rows);
            if (
              transport === "http-206" &&
              cacheMiB === 16 &&
              temperature === "warm" &&
              outcomes.every((outcome) => outcome.status === "completed")
            ) {
              assert.equal(requests, 4, "Warm fitting cache should need discovery probes only");
              assert.equal(transferred, 8);
            }
            const retainedBeforeRelease = pool.usedBytes;
            for (const output of outputs) output.release();
            assert.ok(
              outcomes.every((outcome) => outcome.status !== "failed"),
              JSON.stringify(outcomes),
            );
            assert.ok(pool.usedBytes >= pinnedBytes);
            assert.ok(pool.peakBytes <= pool.maxBytes);
            rounds.push({
              temperature,
              elapsedMs: performance.now() - started,
              sampledRuntimePeak: runtimePeak,
              runtimeSampleIntervalMs: 10,
              peakActiveHttpReads: peakActiveReads,
              outcomes,
              requests,
              transferredBytes: transferred,
              peakActiveJobs: peakActive,
              peakAdmittedBytes: pool.peakBytes,
              retainedBeforeRelease,
              retainedAfterJobRelease: pool.usedBytes,
              runtimeMemorySample: process.memoryUsage(),
            });
          }
          cache.deleteWhere(() => true);
          assert.equal(pool.usedBytes, pinnedBytes);
          assert.deepEqual(pinned.value, rows);
        } finally {
          pinned.release();
          cache.deleteWhere(() => true);
        }
        assert.equal(pool.usedBytes, 0);
        reports.push({
          transport,
          poolMiB: mib,
          cacheMiB,
          queryLimitMiB: 32,
          pinnedBytes,
          rounds,
          remainingBytes: pool.usedBytes,
        });
      }
} finally {
  await new Promise((resolve) => server.close(resolve));
}
writeFileSync(
  outputPath,
  `${JSON.stringify(
    {
      description:
        "Four overlapping public queries over a real written Parquet fixture, shared raw-range cache, retained prior output, cold/warm rounds and two-read transport control. Node memory/local HTTP only; configured pools are not total runtime heap caps. One run per scenario, not stable latency or fairness evidence.",
      sourceCommit: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
      runtime: process.version,
      platform: `${process.platform}/${process.arch}`,
      fixtureSha256: createHash("sha256").update(bytes).digest("hex"),
      httpResponseDelayMs: 2,
      runtimeMemoryBoundary:
        "Node process includes the HTTP server and generated fixture. Samples may miss transient peaks and are not Worker memory measurements.",
      rows: count,
      textBytesPerRow: 4096,
      fixtureBytes: bytes.length,
      reports,
    },
    null,
    2,
  )}\n`,
);
console.log(JSON.stringify(reports, null, 2));
