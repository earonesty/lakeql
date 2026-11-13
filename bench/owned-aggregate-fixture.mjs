import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import {
  CpuPhysicalBackend,
  eq,
  Lake,
  MemoryBudget,
  MemoryScope,
  memoryStore,
  SharedMemoryCache,
} from "../packages/core/dist/index.js";
import { HttpObjectStore } from "../packages/http/dist/index.js";
import { DecodedColumnCache } from "../packages/parquet/dist/decoded-column-cache.js";
import { readOwnedParquetVectorBatches } from "../packages/parquet/dist/owned-vector-batches.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/store-buffer.js";
import { parquetMetadata } from "../packages/parquet/node_modules/hyparquet/src/metadata.js";

const bytes = new Uint8Array(readFileSync("fixtures/data/sales.parquet"));
const metadata = parquetMetadata(bytes.buffer);
const amounts = Array.from({ length: 100 }, (_, i) => ((i * 37) % 1000) + i / 100);
const sorted = [...amounts].sort((a, b) => a - b);
const expected = {
  rows: 100,
  sum: amounts.reduce((a, b) => a + b, 0),
  median: (sorted[49] + sorted[50]) / 2,
  regions: 4,
};
const grouped = process.argv.includes("--grouped");
const publicQuery = process.argv.includes("--public");
const selective = process.argv.includes("--selective");
const cached = process.argv.includes("--cache");
const caseExpression = process.argv.includes("--case");
assert.ok(!cached || publicQuery, "Cache comparison requires public query mode");
assert.ok(!selective || publicQuery, "Selective benchmark requires the public query route");
let expectedRows = grouped
  ? ["west", "east", "north", "south"].map((region, group) => {
      const values = amounts.filter((_, index) => index % 4 === group);
      const ordered = [...values].sort((a, b) => a - b);
      return {
        region,
        rows: 25,
        sum: values.reduce((a, b) => a + b, 0),
        median: ordered[12],
        regions: 1,
      };
    })
  : [expected];
if (selective) {
  const values = amounts.filter((_, index) => index % 4 === 0);
  const ordered = [...values].sort((a, b) => a - b);
  expectedRows = [
    {
      ...(grouped ? { region: "west" } : {}),
      rows: 25,
      sum: values.reduce((a, b) => a + b, 0),
      median: ordered[12],
      regions: 1,
    },
  ];
}
if (caseExpression) {
  expectedRows = expectedRows.map((row) => ({
    ...row,
    conditionalSum: amounts.reduce((total, amount, index) => {
      const region = ["west", "east", "north", "south"][index % 4];
      const included = (!selective || region === "west") && (!grouped || region === row.region);
      return total + (included && amount > 500 ? amount : 0);
    }, 0),
  }));
}
const local = memoryStore();
await local.put("fixture", bytes);
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
    })
    .end(bytes.subarray(start, end));
});
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const results = [];
try {
  const remote = new HttpObjectStore({ baseUrl: `http://127.0.0.1:${server.address().port}/` });
  for (const [transport, store] of [
    ["memory", local],
    ["http-206", remote],
  ]) {
    for (const batchSize of [1, 7, 64]) {
      requests = transferred = 0;
      const pool = new MemoryBudget(64 * 1024 * 1024);
      const parent = new MemoryScope(pool);
      const cacheOptions = { maxBytes: 16 * 1024 * 1024, memoryBudget: pool, policy: "balanced" };
      const cache = cached ? new SharedMemoryCache(cacheOptions) : undefined;
      const decodedCache = cache ? new DecodedColumnCache(cache, cacheOptions) : undefined;
      let continuationOpens = 0;
      class MeasuredCpu extends CpuPhysicalBackend {
        openReduction(...args) {
          continuationOpens++;
          return super.openReduction(...args);
        }
      }
      const operator = {
        kind: grouped ? "grouped-reduce" : "reduce",
        ...(grouped ? { keys: ["region"] } : {}),
        aggregates: {
          ...(caseExpression
            ? {
                conditionalSum: {
                  op: "sum",
                  expr: {
                    kind: "case",
                    whens: [
                      {
                        when: {
                          kind: "compare",
                          op: "gt",
                          left: { kind: "column", name: "amount" },
                          right: { kind: "literal", value: 500 },
                        },
                        value: { kind: "column", name: "amount" },
                      },
                    ],
                    else: { kind: "literal", value: 0 },
                  },
                },
              }
            : {}),
          rows: { op: "count" },
          sum: { op: "sum", column: "amount" },
          median: { op: "median", column: "amount" },
          regions: {
            op: "count_distinct",
            expr: { kind: "call", fn: "upper", args: [{ kind: "column", name: "region" }] },
          },
        },
      };
      const reducer = publicQuery
        ? undefined
        : new MeasuredCpu().openReduction(operator, { allocationScope: parent });
      let batches = 0;
      let queryStats;
      try {
        if (publicQuery) {
          const scanner = {
            scan() {
              throw Error("Unexpected row fallback");
            },
            async *scanOwnedVectorBatches(path, options) {
              queryStats = options.stats;
              const source = await asyncBufferFromStore(store, path);
              for await (const input of readOwnedParquetVectorBatches(
                source,
                metadata,
                options.memoryBudget,
                {
                  columns: options.columns ?? ["amount", "region"],
                  stats: options.stats,
                  batchSize: options.batchSize,
                  rowStart: options.rowStart,
                  rowEnd: options.rowEnd,
                  maxConcurrentReads: 2,
                  ...(decodedCache
                    ? { decodedColumnCache: decodedCache, decodedColumnCacheKey: "sales-fixture" }
                    : {}),
                },
              )) {
                batches++;
                yield input;
              }
            },
          };
          const lake = new Lake({
            store,
            scanner,
            memoryPool: pool,
            physicalExecution: { backends: [new MeasuredCpu("aaa-fixture-cpu")] },
          });
          let query = lake.path("fixture").batchSize(batchSize);
          if (selective) query = query.where(eq("region", "west"));
          const rows = await query
            .groupBy(grouped ? ["region"] : [])
            .aggregate(operator.aggregates);
          assert.deepEqual(rows, expectedRows);
        } else {
          const source = await asyncBufferFromStore(store, "fixture");
          for await (const input of readOwnedParquetVectorBatches(source, metadata, pool, {
            columns: ["amount", "region"],
            batchSize,
            maxConcurrentReads: 2,
          })) {
            try {
              await reducer.append(input.value.batch);
              batches++;
            } finally {
              input.release();
            }
          }
          const output = await reducer.finish();
          try {
            parent.dispose();
            assert.deepEqual(output.value, expectedRows);
            assert.equal(pool.usedBytes, output.bytes);
          } finally {
            output.release();
          }
        }
      } finally {
        reducer?.dispose();
        parent.dispose();
      }
      if (!selective)
        assert.equal(continuationOpens, 1, "Expected one persistent reduction continuation");
      const cachedBytesAfterQuery = pool.usedBytes;
      cache?.deleteWhere(() => true);
      assert.equal(pool.usedBytes, 0);
      assert.ok(pool.peakBytes <= pool.maxBytes);
      results.push({
        transport,
        batchSize,
        batches,
        continuationOpens,
        requests,
        transferredBytes: transferred,
        peakAdmittedBytes: pool.peakBytes,
        remainingBytes: pool.usedBytes,
        cachedBytesAfterQuery,
        cacheHits: queryStats?.cacheHits,
        cacheMisses: queryStats?.cacheMisses,
      });
    }
  }
} finally {
  await new Promise((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
}
writeFileSync(
  `docs/benchmarks/${caseExpression ? "case-" : ""}${cached ? "cached-" : ""}${selective ? "selective-" : ""}${publicQuery ? "public" : "owned"}-${grouped ? "grouped-" : ""}aggregate-fixture-2026-11-13.json`,
  `${JSON.stringify({ description: "Real sales Parquet fixture through owned decoding and the CPU physical reduction continuation. HTTP is a local range-server simulation; publicQuery selects the public columnar query route with an owned-only Parquet scanner. This is not a Worker heap measurement.", grouped, publicQuery, selective, cached, caseExpression, expected: expectedRows, results }, null, 2)}\n`,
);
console.log(results);
