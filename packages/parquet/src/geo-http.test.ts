import { createServer } from "node:http";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, SharedMemoryCache, withObjectStoreReadControls } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake } from "./index.js";
import { readOwnedParquetMetadata, sharedParquetMetadataCache } from "./metadata-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it.each([
  false,
  true,
])("shares bounded HTTP GeoParquet scans when ranges are ignored=%s", async (ignoreRange) => {
  const records = Array.from({ length: 32 }, (_, id) => {
    const value = new Uint8Array(21);
    const view = new DataView(value.buffer);
    view.setUint8(0, 1);
    view.setUint32(1, 1, true);
    view.setFloat64(5, id, true);
    view.setFloat64(13, -id, true);
    return value;
  });
  const bytes = new Uint8Array(
    parquetWriteBuffer({
      rowGroupSize: [8, 8, 8, 8],
      columnData: [{ name: "geometry", type: "BYTE_ARRAY", data: records }],
      kvMetadata: [
        {
          key: "geo",
          value: JSON.stringify({
            columns: {
              geometry: { encoding: "WKB", crs: { id: { authority: "EPSG", code: 4326 } } },
            },
            unused: Array(100).fill("metadata scratch"),
          }),
        },
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
        ETag: '"geo-fixture-v1"',
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
        Array.from({ length: 4 }, () => lake.path("geo.parquet").batchSize(3).run().toArray()),
      );
      for (const result of results)
        expect(result).toEqual(records.map((geometry) => ({ geometry })));
      const dataReads = ranges.filter((range) => range !== "bytes=0-1");
      if (round === "warm") expect(dataReads).toHaveLength(0);
      else expect(dataReads.length).toBeGreaterThan(0);
      expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    }
    // Exercise the full metadata fallback directly; ordinary scans may use
    // the indexed source and therefore cannot prove this ownership path.
    cache.deleteWhere(() => true);
    const file = await asyncBufferFromStore(store, "geo.parquet");
    const metadataCache = sharedParquetMetadataCache(cache);
    for (const round of ["cold", "warm"]) {
      ranges.length = 0;
      const budgets = Array.from({ length: 4 }, () => pool.child());
      const results = await Promise.all(
        budgets.map((budget) =>
          readOwnedParquetMetadata("geo.parquet", file, budget, metadataCache),
        ),
      );
      try {
        if (round === "warm") {
          expect(results.every((result) => result.cached)).toBe(true);
          expect(ranges).toHaveLength(0);
          cache.deleteWhere(() => true);
          expect(pool.usedBytes).toBeGreaterThan(0);
        } else expect(ranges.length).toBeGreaterThan(0);
        for (const result of results) {
          expect(result.owner.value.num_rows).toBe(32n);
          expect(result.owner.value.row_groups).toHaveLength(4);
          expect(result.owner.value.schema[1]?.logical_type).toEqual({
            type: "GEOMETRY",
            crs: "EPSG:4326",
          });
          expect(result.owner.bytes).toBeGreaterThan(bytes.byteLength);
        }
      } finally {
        for (const result of results) result.owner.release();
      }
      for (const budget of budgets) expect(budget.usedBytes).toBe(0);
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
