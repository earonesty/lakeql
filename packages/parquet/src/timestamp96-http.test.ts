import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import {
  col,
  lit,
  MemoryBudget,
  SharedMemoryCache,
  timestampFromEpoch,
  withObjectStoreReadControls,
} from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake } from "./index.js";

it.each([
  false,
  true,
])("shares bounded HTTP wide timestamp scans when ranges are ignored=%s", async (ignoreRange) => {
  const seconds = [0n, 1n, -1n, 9223372036n, 253402300799n, -62135596800n];
  const expected = seconds.map((value) => ({
    timestamp_millis: timestampFromEpoch(value * 1000n, "millis"),
    timestamp_micros: timestampFromEpoch(value * 1000000n, "micros"),
    timestamp_nanos: timestampFromEpoch(value * 1000000000n, "nanos"),
  }));
  const bytes = readFileSync(
    new URL(
      "../../../fixtures/external/parquet-testing/data/flba12_timestamp.parquet",
      import.meta.url,
    ),
  );
  const ranges: string[] = [];
  let active = 0;
  let peak = 0;
  const server = createServer((request, response) => {
    const range = request.headers.range ?? "";
    const match = /^bytes=(\d+)-(\d+)$/.exec(range);
    if (!match) {
      response.writeHead(400).end();
      return;
    }
    ranges.push(range);
    const start = ignoreRange ? 0 : Number(match[1]);
    const end = ignoreRange ? bytes.length : Math.min(bytes.length, Number(match[2]) + 1);
    peak = Math.max(peak, ++active);
    setTimeout(() => {
      response.writeHead(ignoreRange ? 200 : 206, {
        "Content-Length": end - start,
        ...(ignoreRange ? {} : { "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}` }),
        ETag: '"wide-timestamp-fixture-v1"',
      });
      response.end(bytes.subarray(start, end));
      active--;
    }, 2);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const pool = new MemoryBudget(1024 * 1024);
  const cache = new SharedMemoryCache({ maxBytes: 256 * 1024, memoryBudget: pool });
  try {
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Missing listener");
    const store = withObjectStoreReadControls(
      new HttpObjectStore({ baseUrl: `http://127.0.0.1:${address.port}/` }),
      { maxConcurrentReads: 2 },
    );
    const lake = createParquetLake({
      store,
      memoryPool: pool,
      scanRangeCache: { maxBytes: 256 * 1024, coalesceBytes: 0, sharedCache: cache },
      budget: { maxAllocatedMemoryBytes: 512 * 1024 },
    });
    for (const round of ["cold", "warm"]) {
      ranges.length = 0;
      const results = await Promise.all(
        Array.from({ length: 4 }, () =>
          lake.path("timestamps.parquet").batchSize(3).run().toArray(),
        ),
      );
      for (const result of results) expect(result).toEqual(expected);
      const dataReads = ranges.filter((range) => range !== "bytes=0-1");
      if (round === "warm") expect(dataReads).toHaveLength(0);
      else expect(dataReads.length).toBeGreaterThan(0);
      expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    }
    const ordered = await Promise.all(
      Array.from({ length: 4 }, () =>
        lake
          .path("timestamps.parquet")
          .batchSize(3)
          .orderBy([{ column: "timestamp_nanos" }])
          .limit(3)
          .run()
          .toOwnedArray(),
      ),
    );
    try {
      for (const output of ordered)
        expect(output.value).toEqual([expected[5], expected[2], expected[0]]);
    } finally {
      for (const output of ordered) output.release();
    }
    const ranked = await Promise.all(
      Array.from({ length: 4 }, () => {
        let query = lake.path("timestamps.parquet").batchSize(2);
        for (const fn of ["rank", "dense_rank", "percent_rank", "cume_dist"] as const)
          query = query.window(fn, {
            fn,
            args: [],
            over: {
              partitionBy: [],
              orderBy: [{ expr: col("timestamp_nanos") }],
            },
          });
        for (const fn of ["first_value", "last_value", "nth_value"] as const)
          query = query.window(fn, {
            fn,
            args: fn === "nth_value" ? [col("timestamp_nanos"), lit(3)] : [col("timestamp_nanos")],
            over: {
              partitionBy: [],
              orderBy: [{ expr: col("timestamp_nanos") }],
              frame: {
                mode: "rows",
                start: { kind: "unbounded-preceding" },
                end: { kind: "unbounded-following" },
              },
            },
          });
        for (const [fn, offset] of [
          ["lag", 3],
          ["lead", 2],
        ] as const)
          query = query.window(fn, {
            fn,
            args: [col("timestamp_nanos"), lit(offset)],
            ignoreNulls: true,
            over: { partitionBy: [], orderBy: [{ expr: col("timestamp_nanos") }] },
          });
        return query
          .window("distinct_timestamps", {
            fn: { aggregate: "count_distinct" },
            args: [col("timestamp_nanos")],
            over: { partitionBy: [], orderBy: [{ expr: col("timestamp_nanos") }] },
          })
          .window("running_median", {
            fn: { aggregate: "median" },
            args: [lit(7)],
            over: { partitionBy: [], orderBy: [{ expr: col("timestamp_nanos") }] },
          })
          .window("running_count", {
            fn: { aggregate: "count" },
            args: [],
            over: { partitionBy: [], orderBy: [{ expr: col("timestamp_nanos") }] },
          })
          .window("all_tied", {
            fn: "cume_dist",
            args: [],
            over: {
              partitionBy: [],
              orderBy: [{ expr: lit(1) }],
            },
          })
          .run()
          .toArray();
      }),
    );
    const ranks = [3, 4, 2, 5, 6, 1];
    const sorted = [expected[5], expected[2], expected[0], expected[1], expected[3], expected[4]];
    for (const output of ranked)
      expect(output).toEqual(
        expected.map((row, index) => {
          const rank = ranks[index] ?? 0;
          return {
            ...row,
            rank,
            dense_rank: rank,
            percent_rank: (rank - 1) / 5,
            cume_dist: rank / 6,
            running_count: rank,
            running_median: 7,
            distinct_timestamps: rank,
            lag: sorted[rank - 4]?.timestamp_nanos ?? null,
            lead: sorted[rank + 1]?.timestamp_nanos ?? null,
            first_value: expected[5]?.timestamp_nanos,
            last_value: expected[4]?.timestamp_nanos,
            nth_value: expected[0]?.timestamp_nanos,
            all_tied: 1,
          };
        }),
      );
    expect(peak).toBeLessThanOrEqual(2);
  } finally {
    cache.deleteWhere(() => true);
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
  expect(pool.usedBytes).toBe(0);
});
