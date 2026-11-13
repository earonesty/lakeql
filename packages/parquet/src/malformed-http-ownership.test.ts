import { existsSync, readFileSync } from "node:fs";
import { createServer } from "node:http";
import { MemoryBudget, withObjectStoreReadControls } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake } from "./index.js";

for (const [name, cause] of [
  ["ARROW-GH-41317.parquet", "parquet unsupported page type: INDEX_PAGE"],
  ["ARROW-GH-41321.parquet", "parquet bitpack offset 9 out of range"],
] as const) {
  const fixture = new URL(
    `../../../fixtures/external/parquet-testing/bad_data/${name}`,
    import.meta.url,
  );
  for (const ignoreRange of [false, true]) {
    it.skipIf(!existsSync(fixture))(
      `releases corrupt HTTP fixture ${name}, ignored ranges=${ignoreRange}`,
      async () => {
        const bytes = readFileSync(fixture);
        let requests = 0;
        const server = createServer((request, response) => {
          const range = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
          if (!range) {
            response.writeHead(400).end();
            return;
          }
          requests++;
          const start = ignoreRange ? 0 : Number(range[1]);
          const end = ignoreRange ? bytes.length : Math.min(bytes.length, Number(range[2]) + 1);
          response.writeHead(ignoreRange ? 200 : 206, {
            "Content-Length": end - start,
            ...(ignoreRange
              ? {}
              : { "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}` }),
            ETag: '"corrupt-fixture"',
          });
          response.end(bytes.subarray(start, end));
        });
        await new Promise<void>((resolve, reject) => {
          server.once("error", reject);
          server.listen(0, "127.0.0.1", resolve);
        });
        try {
          const address = server.address();
          if (!address || typeof address === "string") throw new Error("Missing listener");
          const pool = new MemoryBudget(64 * 1024 * 1024);
          const store = withObjectStoreReadControls(
            new HttpObjectStore({ baseUrl: `http://127.0.0.1:${address.port}/` }),
            { maxConcurrentReads: 2 },
          );
          const lake = createParquetLake({
            store,
            memoryPool: pool,
            scanRangeCache: { maxBytes: 0 },
          });
          for (let attempt = 0; attempt < 2; attempt++) {
            const consume = async () => {
              for await (const _row of lake.path(name).batchSize(3).run().rows()) {
                /* consume */
              }
            };
            await expect(consume()).rejects.toMatchObject({
              code: "LAKEQL_PARQUET_READ_ERROR",
              details: { cause: expect.objectContaining({ message: cause }) },
            });
            expect(pool.usedBytes).toBe(0);
            expect(pool.peakBytes).toBeGreaterThan(0);
            expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
          }
          expect(requests).toBeGreaterThan(0);
        } finally {
          await new Promise<void>((resolve, reject) =>
            server.close((error) => (error ? reject(error) : resolve())),
          );
        }
      },
    );
  }
}
