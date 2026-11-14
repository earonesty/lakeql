import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import {
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
