import { createServer } from "node:http";
import { parquetWriteBuffer } from "hyparquet-writer";
import { col, MemoryBudget, SharedMemoryCache, withObjectStoreReadControls } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake } from "./index.js";

it.each([
  { ignoreRange: false, mib: 64 },
  { ignoreRange: true, mib: 64 },
  { ignoreRange: false, mib: 128 },
  { ignoreRange: true, mib: 128 },
])("owns concurrent window tasks over HTTP: %j", async ({ ignoreRange, mib }) => {
  const records = Array.from({ length: 256 }, (_, id) => ({
    id,
    region: id % 4,
    payload: `${id}:${"é😀".repeat(128)}`,
    rn: Math.floor(id / 4) + 1,
  }));
  const bytes = new Uint8Array(
    parquetWriteBuffer({
      rowGroupSize: [64, 64, 64, 64],
      columnData: [
        { name: "id", type: "INT32", data: records.map((row) => row.id) },
        { name: "region", type: "INT32", data: records.map((row) => row.region) },
        { name: "payload", type: "STRING", data: records.map((row) => row.payload) },
      ],
    }),
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
        ETag: '"owned-window-v1"',
      });
      response.end(bytes.subarray(start, end));
      active--;
    }, 2);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const pool = new MemoryBudget(mib * 1024 * 1024);
  const cache = new SharedMemoryCache({ maxBytes: 4 * 1024 * 1024, memoryBudget: pool });
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
      scanRangeCache: { maxBytes: 4 * 1024 * 1024, coalesceBytes: 0, sharedCache: cache },
      budget: { maxAllocatedMemoryBytes: 16 * 1024 * 1024 },
    });
    const query = () =>
      lake
        .path("windows.parquet")
        .batchSize(16)
        .window("rn", {
          fn: "row_number",
          args: [],
          over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
        });
    await expect(query().explain()).resolves.toMatchObject({
      json: { windowPlan: { execution: "parquet-work-unit-fanout" } },
    });
    cache.deleteWhere(() => true);
    for (const round of ["cold", "warm"]) {
      ranges.length = 0;
      const results = await Promise.all(
        Array.from({ length: 4 }, () => query().run().toOwnedArray()),
      );
      try {
        for (const result of results) expect(result.value).toEqual(records);
        expect(pool.usedBytes).toBeGreaterThan(0);
        if (round === "warm")
          expect(() => pool.reserve(pool.maxBytes, "competing job")).toThrow(/budget/i);
      } finally {
        for (const result of results) result.release();
      }
      const dataReads = ranges.filter((range) => range !== "bytes=0-1");
      if (round === "warm") expect(dataReads).toHaveLength(0);
      else expect(dataReads.length).toBeGreaterThan(0);
      expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    }
    expect(peak).toBeGreaterThan(1);
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
