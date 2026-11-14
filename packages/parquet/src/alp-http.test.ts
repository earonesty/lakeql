import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import { MemoryBudget, SharedMemoryCache, withObjectStoreReadControls } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake } from "./index.js";

it.each([
  false,
  true,
])("streams real ALP fixture across concurrent HTTP reads, ignored=%s", async (ignoreRange) => {
  const bytes = readFileSync(
    new URL(
      "../../../fixtures/external/parquet-testing/data/alp_extended.zstd.parquet",
      import.meta.url,
    ),
  );
  let active = 0;
  let peakReads = 0;
  let reads = 0;
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
    reads++;
    peakReads = Math.max(peakReads, ++active);
    const start = ignoreRange ? 0 : Number(match[1]);
    const end = ignoreRange ? bytes.length : Math.min(bytes.length, Number(match[2]) + 1);
    setTimeout(() => {
      response.writeHead(ignoreRange ? 200 : 206, {
        "Content-Length": end - start,
        ...(ignoreRange ? {} : { "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}` }),
        ETag: '"alp-fixture"',
      });
      response.end(bytes.subarray(start, end));
      active--;
    }, 1);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    if (!address || typeof address === "string") throw Error("Missing listener");
    const store = withObjectStoreReadControls(
      new HttpObjectStore({ baseUrl: `http://127.0.0.1:${address.port}/` }),
      { maxConcurrentReads: 2 },
    );
    for (const cap of [64, 128]) {
      const pool = new MemoryBudget(cap * 1024 * 1024);
      const cache = new SharedMemoryCache({ maxBytes: 512 * 1024, memoryBudget: pool });
      const lake = createParquetLake({
        store,
        memoryPool: pool,
        budget: { maxAllocatedMemoryBytes: 8 * 1024 * 1024 },
        scanRangeCache: { maxBytes: 512 * 1024, coalesceBytes: 0, sharedCache: cache },
      });
      for (let round = 0; round < 2; round++) {
        await Promise.all(
          Array.from({ length: 4 }, async () => {
            let rows = 0;
            for await (const row of lake.path("alp.parquet").batchSize(128).run().rows()) {
              for (const type of ["float", "double"]) {
                for (const size of [32, 1024, 4096])
                  expect(Object.is(row[`${type}_alp_${size}`], row[`${type}_plain`])).toBe(true);
              }
              rows++;
            }
            expect(rows).toBe(9032);
          }),
        );
      }
      expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
      cache.deleteWhere(() => true);
      expect(pool.usedBytes).toBe(0);
    }
    expect(reads).toBeGreaterThan(0);
    expect(peakReads).toBeLessThanOrEqual(2);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
}, 30000);
