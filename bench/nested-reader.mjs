import { deepStrictEqual } from "node:assert";
import { writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { performance } from "node:perf_hooks";
import { MemoryBudget, memoryStore, vectorValue } from "../packages/core/dist/index.js";
import { HttpObjectStore } from "../packages/http/dist/index.js";
import { readOwnedParquetColumnBatches } from "../packages/parquet/dist/column-batches.js";
import { readOwnedParquetVectorBatches } from "../packages/parquet/dist/owned-vector-batches.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/store-buffer.js";
import { parquetMetadata } from "../packages/parquet/node_modules/hyparquet/src/metadata.js";
import { parquetWriteBuffer } from "../packages/parquet/node_modules/hyparquet-writer/src/index.js";

// The budget sweep uses the same serialized fixtures and value oracle as timing runs.
const sweep = process.argv.includes("--budget-sweep");
const results = [];
for (const rowCount of sweep ? [8192] : [128, 1024]) {
  for (const payloadBytes of sweep ? [16] : [16, 4096]) {
    const records = Array.from({ length: rowCount }, (_, i) =>
      i % 11 === 0
        ? null
        : {
            score: i % 7 === 0 ? null : i,
            payload: `${i}:`.padEnd(payloadBytes, "x"),
          },
    );
    const bytes = new Uint8Array(
      parquetWriteBuffer({
        schema: [
          { name: "root", num_children: 1 },
          { name: "record", num_children: 2, repetition_type: "OPTIONAL" },
          { name: "score", type: "INT32", repetition_type: "OPTIONAL" },
          {
            name: "payload",
            type: "BYTE_ARRAY",
            converted_type: "UTF8",
            repetition_type: "REQUIRED",
          },
        ],
        columnData: [{ name: "record", data: records }],
      }),
    );
    const metadata = parquetMetadata(bytes.buffer);
    const local = memoryStore();
    await local.put("fixture", bytes);
    let requests = 0,
      transferred = 0,
      active = 0,
      peakActive = 0;
    const server = createServer((request, response) => {
      if (request.method === "HEAD") {
        response.writeHead(200, { "Content-Length": bytes.length }).end();
        return;
      }
      const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
      if (!match) {
        response.writeHead(400).end();
        return;
      }
      const start = Number(match[1]),
        end = Math.min(bytes.length, Number(match[2]) + 1);
      requests++;
      transferred += end - start;
      peakActive = Math.max(peakActive, ++active);
      setTimeout(() => {
        response.writeHead(206, {
          "Content-Length": end - start,
          "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}`,
        });
        response.end(bytes.subarray(start, end));
        active--;
      }, 2);
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    try {
      const remote = new HttpObjectStore({ baseUrl: `http://127.0.0.1:${server.address().port}/` });
      for (const [transport, store] of [
        ["memory", local],
        ["http-2ms", remote],
      ]) {
        const source = await asyncBufferFromStore(store, "fixture");
        for (const batchSize of sweep ? [128, 1024, 8192] : [128]) {
          for (const budgetBytes of sweep
            ? [128, 256, 512, 1024].map((kib) => kib * 1024)
            : [64 * 1024 * 1024]) {
            const measurements = sweep ? { nested: [] } : { nested: [], legacy: [] };
            for (let round = 0; round < (sweep ? 1 : 6); round++) {
              for (const kind of sweep
                ? ["nested"]
                : round % 2
                  ? ["legacy", "nested"]
                  : ["nested", "legacy"]) {
                requests = transferred = peakActive = 0;
                const budget = new MemoryBudget(budgetBytes);
                try {
                  const read =
                    kind === "nested"
                      ? readOwnedParquetVectorBatches
                      : readOwnedParquetColumnBatches;
                  const iterator = read(source, metadata, budget, {
                    columns: ["record"],
                    batchSize,
                    maxConcurrentReads: 2,
                  })[Symbol.asyncIterator]();
                  let rows = 0,
                    batches = 0,
                    largestBatch = 0,
                    elapsed = 0;
                  try {
                    for (;;) {
                      let started = performance.now();
                      const next = await iterator.next();
                      elapsed += performance.now() - started;
                      if (next.done) break;
                      const owner = next.value;
                      batches++;
                      largestBatch = Math.max(largestBatch, owner.value.batch.rowCount);
                      try {
                        if (owner.value.rowOffset !== rows) throw Error("offset mismatch");
                        if (
                          owner.value.batch.rowCount < 1 ||
                          owner.value.batch.rowCount > batchSize
                        )
                          throw Error("batch size bound violated");
                        for (let i = 0; i < owner.value.batch.rowCount; i++) {
                          const actual = vectorValue(owner.value.batch.columns.record, i);
                          const expected = records[rows++];
                          deepStrictEqual(actual, expected);
                        }
                      } finally {
                        started = performance.now();
                        owner.release();
                        elapsed += performance.now() - started;
                      }
                    }
                  } finally {
                    await iterator.return?.();
                  }
                  if (rows !== rowCount || budget.usedBytes !== 0)
                    throw Error("row count or cleanup mismatch");
                  if (sweep || round >= 2)
                    measurements[kind].push({
                      ms: elapsed,
                      batches,
                      largestBatch,
                      peakBytes: budget.peakBytes,
                      requests,
                      transferred,
                      peakActive,
                    });
                } catch (error) {
                  if (sweep && error.code !== "LAKEQL_BUDGET_EXCEEDED") throw error;
                  if (budget.peakBytes > budgetBytes) throw Error("peak exceeds requested budget");
                  if (budget.usedBytes !== 0)
                    throw Error(`Leaked after failure: ${budget.usedBytes}`, { cause: error });
                  if (sweep || round >= 2)
                    measurements[kind].push({
                      error: String(error),
                      code: error.code,
                      details: error.details,
                      peakBytes: budget.peakBytes,
                      requests,
                      transferred,
                      peakActive,
                    });
                }
              }
            }
            results.push({
              rowCount,
              payloadBytes,
              fileBytes: bytes.length,
              batchSize,
              transport,
              budgetBytes,
              measurements,
            });
            console.log(
              rowCount,
              payloadBytes,
              batchSize,
              budgetBytes,
              transport,
              Object.fromEntries(
                Object.entries(measurements).map(([kind, samples]) => [
                  kind,
                  samples.map((x) => x.ms?.toFixed(2) ?? x.error),
                ]),
              ),
            );
          }
        }
      }
    } finally {
      await new Promise((resolve) => server.close(resolve));
    }
  }
}
writeFileSync(
  sweep
    ? "docs/benchmarks/nested-budget-sweep-2026-11-13.json"
    : "docs/benchmarks/nested-reader-2026-11-13.json",
  `${JSON.stringify(
    {
      description: sweep
        ? "Budget feasibility sweep, one run per configuration; timings are diagnostic, not comparative benchmarks. Same serialized fixtures and every-value oracle as normal runs. No cache. HTTP range response delay 2 ms. Tracked allocations exclude inputs, metadata, fixture writer, server and untracked runtime heap."
        : "Serialized optional struct fixtures; two warmups and four alternating samples. Time includes iterator advances and owner release, excludes value verification. No cache; HTTP server delays each range response by 2 ms. Tracked allocation coverage differs from legacy decoding; neither is a whole-heap measurement. Inputs, metadata, fixture writer, and HTTP server heap excluded.",
      results,
    },
    null,
    2,
  )}\n`,
);
