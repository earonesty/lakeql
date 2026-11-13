import assert from "node:assert/strict";
import { resolve } from "node:path";
import { DuckDBConnection } from "@duckdb/node-api";
import { MemoryBudget } from "../packages/core/dist/index.js";
import { httpStore } from "../packages/http/dist/index.js";
import { createParquetLake } from "../packages/parquet/dist/index.js";

const baseUrl = "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/plotly/";
const path = "2015_flights.parquet";
const columns = ["DEPARTURE_DELAY", "ARRIVAL_DELAY", "DISTANCE"];
const connection = await DuckDBConnection.create();
try {
  const local = resolve("fixtures/external/plotly", path).replaceAll("'", "''");
  const expected = (
    await connection.runAndReadAll(
      `SELECT ${columns.map((c) => `"${c}"`).join(",")} FROM read_parquet('${local}') LIMIT 2048`,
    )
  ).getRowObjects();
  const reports = [];
  for (const memoryMiB of [64, 128]) {
    for (const concurrency of [1, 2]) {
      const pool = new MemoryBudget(memoryMiB * 1024 * 1024);
      const ranges = [];
      let requests = 0,
        wireBytes = 0,
        active = 0,
        peakReads = 0;
      const store = httpStore({
        baseUrl,
        fetch: async (input, init) => {
          assert.ok(new Headers(init?.headers).has("Range"), "Expected only range requests");
          active++;
          peakReads = Math.max(peakReads, active);
          try {
            const response = await fetch(input, {
              ...init,
              signal: AbortSignal.any([
                AbortSignal.timeout(30000),
                ...(init?.signal ? [init.signal] : []),
              ]),
            });
            assert.equal(response.status, 206);
            requests++;
            wireBytes += Number(response.headers.get("Content-Length"));
            ranges.push({
              requested: new Headers(init?.headers).get("Range"),
              returned: response.headers.get("Content-Range"),
              bytes: Number(response.headers.get("Content-Length")),
            });
            return response;
          } finally {
            active--;
          }
        },
      });
      const lake = createParquetLake({
        store,
        memoryPool: pool,
        budget: { maxAllocatedMemoryBytes: pool.maxBytes, maxConcurrentReads: 4 },
      });
      const started = performance.now();
      await Promise.all(
        Array.from({ length: concurrency }, async () => {
          const rows = await lake.path(path).select(columns).batchSize(128).limit(2048).toArray();
          assert.deepEqual(rows, expected);
        }),
      );
      assert.equal(pool.usedBytes, 0);
      assert.ok(pool.peakBytes <= pool.maxBytes);
      reports.push({
        memoryMiB,
        concurrency,
        rowsVerified: 2048 * concurrency,
        requests,
        wireBytes,
        ranges,
        peakRequestsAwaitingHeaders: peakReads,
        peakTrackedBytes: pool.peakBytes,
        elapsedMs: performance.now() - started,
      });
    }
  }
  console.log(
    JSON.stringify(
      {
        source: baseUrl + path,
        fixtureRows: 5819079,
        fixtureBytes: 25238218,
        operation:
          "first 2048 rows, three numeric columns; local DuckDB reference versus remote HTTP ranges",
        scope:
          "large-object bounded projection, not full scan or total Worker heap proof; active count measures requests until response headers",
        reports,
      },
      null,
      2,
    ),
  );
} finally {
  connection.disconnectSync();
}
