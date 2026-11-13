import { createServer } from "node:http";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, SharedMemoryCache, withObjectStoreReadControls } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake } from "./index.js";

it.each([
  false,
  true,
])("shares bounded HTTP struct scans when ranges are ignored=%s", async (ignoreRange) => {
  const records = Array.from({ length: 32 }, (_, id) =>
    id % 7 === 0
      ? null
      : {
          id,
          text: id % 5 === 0 ? null : `${id}:${"αz".repeat(1500)}`,
        },
  );
  const bytes = new Uint8Array(
    parquetWriteBuffer({
      schema: [
        { name: "root", num_children: 1 },
        { name: "record", num_children: 2, repetition_type: "OPTIONAL" },
        { name: "id", type: "INT32", repetition_type: "REQUIRED" },
        { name: "text", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
      ],
      rowGroupSize: [8, 8, 8, 8],
      columnData: [{ name: "record", data: records }],
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
        ETag: '"nested-fixture-v1"',
      });
      response.end(bytes.subarray(start, end));
      active--;
    }, 2);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  // Four retained materialized results now participate in output admission.
  const pool = new MemoryBudget(2 * 1024 * 1024);
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
        Array.from({ length: 4 }, () => lake.path("struct.parquet").batchSize(3).run().toArray()),
      );
      for (const result of results) expect(result).toEqual(records.map((record) => ({ record })));
      const dataReads = ranges.filter((range) => range !== "bytes=0-1");
      if (round === "warm") expect(dataReads).toHaveLength(0);
      else expect(dataReads.length).toBeGreaterThan(0);
      expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
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
