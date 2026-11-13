import assert from "node:assert/strict";
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
  transferred += end - start;
  response
    .writeHead(206, {
      "Content-Length": end - start,
      "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}`,
      ETag: '"mixed-fixture"',
    })
    .end(bytes.subarray(start, end));
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
            requests = transferred = 0;
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
            const outcomes = await Promise.all(
              jobs.map(async ([kind, run]) => {
                active++;
                peakActive = Math.max(active, peakActive);
                try {
                  await run();
                  return { kind, status: "completed" };
                } catch (error) {
                  return error.code === "LAKEQL_BUDGET_EXCEEDED"
                    ? { kind, status: "admission-rejected" }
                    : {
                        kind,
                        status: "failed",
                        error: { code: error.code, message: error.message, details: error.details },
                      };
                } finally {
                  active--;
                }
              }),
            );
            assert.equal(peakActive, 4);
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
  "docs/benchmarks/mixed-query-memory-2026-11-13.json",
  `${JSON.stringify(
    {
      description:
        "Four overlapping public queries over a real written Parquet fixture, shared raw-range cache, retained prior output, cold/warm rounds and two-read transport control. Node memory/local HTTP only; configured pools are not total runtime heap caps. One run per scenario, not stable latency or fairness evidence.",
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
