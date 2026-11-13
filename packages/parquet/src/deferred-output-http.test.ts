import { createServer } from "node:http";
import { parquetWriteBuffer } from "hyparquet-writer";
import {
  col,
  eq,
  MemoryBudget,
  mod,
  SharedMemoryCache,
  withObjectStoreReadControls,
} from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake } from "./index.js";

it.each([
  false,
  true,
])("defers sparse output columns over HTTP with ignored ranges=%s", async (ignoreRange) => {
  let seed = 12345;
  const rows = Array.from({ length: 512 }, (_, id) => ({
    id,
    text: Array.from({ length: 512 }, () => {
      seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
      return String.fromCharCode(33 + ((seed >>> 16) % 90));
    }).join(""),
  }));
  const bytes = new Uint8Array(
    parquetWriteBuffer({
      rowGroupSize: Array(16).fill(32),
      columnData: [
        { name: "id", type: "INT32", data: rows.map((row) => row.id) },
        { name: "text", type: "STRING", data: rows.map((row) => row.text) },
      ],
    }),
  );
  let transferred = 0;
  let active = 0;
  let peak = 0;
  const ranges: string[] = [];
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
    transferred += end - start;
    peak = Math.max(peak, ++active);
    setTimeout(() => {
      response.writeHead(ignoreRange ? 200 : 206, {
        "Content-Length": end - start,
        ...(ignoreRange ? {} : { "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}` }),
        ETag: '"deferred-output-fixture"',
      });
      response.end(bytes.subarray(start, end));
      active--;
    }, 2);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const cache = new SharedMemoryCache({ maxBytes: 8 * 1024 * 1024, memoryBudget: pool });
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
      scanRangeCache: { maxBytes: 8 * 1024 * 1024, coalesceBytes: 0, sharedCache: cache },
    });
    const query = (select: string[]) =>
      lake
        .path("rows.parquet")
        .select(select)
        .where(eq(mod(col("id"), 257), 256))
        .limit(1)
        .batchSize(16)
        .run();
    const baseline = await query(["*"]).toOwnedArray();
    expect(baseline.value).toEqual([rows[256]]);
    baseline.release();
    const eagerBytes = transferred;
    const eagerRanges = [...ranges];
    cache.deleteWhere(() => true);
    transferred = 0;
    ranges.length = 0;
    const deferred = await query(["id", "text"]).toOwnedArray();
    expect(deferred.value).toEqual([rows[256]]);
    deferred.release();
    if (!ignoreRange) expect(transferred).toBeLessThan(eagerBytes / 2);
    else
      expect(transferred, JSON.stringify({ eagerRanges, ranges })).toBeLessThanOrEqual(eagerBytes);
    const deferredBytes = transferred;
    cache.deleteWhere(() => true);
    const cold = await Promise.all(
      Array.from({ length: 4 }, () => query(["id", "text"]).toOwnedArray()),
    );
    for (const owner of cold) {
      expect(owner.value).toEqual([rows[256]]);
      owner.release();
    }
    ranges.length = 0;
    const concurrent = await Promise.all(
      Array.from({ length: 4 }, () => query(["id", "text"]).toOwnedArray()),
    );
    for (const owner of concurrent) {
      expect(owner.value).toEqual([rows[256]]);
      owner.release();
    }
    expect(ranges.filter((range) => range !== "bytes=0-1")).toHaveLength(0);
    expect(peak).toBeLessThanOrEqual(2);
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    if (process.env.LAKEQL_DEFERRED_METRICS === "1")
      process.stdout.write(
        `${JSON.stringify({
          ignoreRange,
          fixtureBytes: bytes.length,
          eagerBytes,
          deferredBytes,
          peakReads: peak,
          peakAllocatedBytes: pool.peakBytes,
        })}\n`,
      );
  } finally {
    cache.deleteWhere(() => true);
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
  expect(pool.usedBytes).toBe(0);
});
