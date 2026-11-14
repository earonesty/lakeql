import { existsSync, readFileSync } from "node:fs";
import { createServer } from "node:http";
import { MemoryBudget } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake, readParquetObjects } from "./index.js";

const fixture = new URL(
  "../../../fixtures/external/parquet-testing/data/nation.dict-malformed.parquet",
  import.meta.url,
);
const names = [
  "ALGERIA",
  "ARGENTINA",
  "BRAZIL",
  "CANADA",
  "EGYPT",
  "ETHIOPIA",
  "FRANCE",
  "GERMANY",
  "INDIA",
  "INDONESIA",
  "IRAN",
  "IRAQ",
  "JAPAN",
  "JORDAN",
  "KENYA",
  "MOROCCO",
  "MOZAMBIQUE",
  "PERU",
  "CHINA",
  "ROMANIA",
  "SAUDI ARABIA",
  "VIETNAM",
  "RUSSIA",
  "UNITED KINGDOM",
  "UNITED STATES",
];
it.skipIf(!existsSync(fixture)).each([false, true])(
  "recovers proven dictionary extents over HTTP (ignored ranges=%s)",
  async (ignoreRange) => {
    const bytes = readFileSync(fixture);
    const ranges: [number, number][] = [];
    const server = createServer((request, response) => {
      const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
      if (!match) {
        response.writeHead(400).end();
        return;
      }
      const start = Number(match[1]),
        end = Number(match[2]) + 1;
      ranges.push([start, end]);
      response.writeHead(ignoreRange ? 200 : 206, {
        "Content-Length": ignoreRange ? bytes.length : end - start,
        ...(ignoreRange ? {} : { "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}` }),
        ETag: '"dictionary-extent-v1"',
      });
      response.end(ignoreRange ? bytes : bytes.subarray(start, end));
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    try {
      const address = server.address();
      if (!address || typeof address === "string") throw Error("Missing listener");
      const store = new HttpObjectStore({ baseUrl: `http://127.0.0.1:${address.port}/` });
      const expected = await readParquetObjects(store, "nation.parquet");
      expect(expected).toHaveLength(25);
      expect(expected.map((row) => row.nation_key)).toEqual(names.map((_, i) => i));
      expect(expected.map((row) => new TextDecoder().decode(row.name as Uint8Array))).toEqual(
        names,
      );
      for (const metadataMode of ["auto", "cached"] as const) {
        const pool = new MemoryBudget(64 * 1024 * 1024);
        const lake = createParquetLake({
          store,
          memoryPool: pool,
          scanRangeCache: { maxBytes: 0 },
          scanMetadataMode: metadataMode,
        });
        await Promise.all(
          [1, 3, 32].map(async (batchSize) => {
            const query = lake.path("nation.parquet").batchSize(batchSize).run();
            const owner = await query.toOwnedArray();
            try {
              expect(owner.value).toEqual(expected);
            } finally {
              owner.release();
            }
            expect(query.memoryBudget.usedBytes).toBe(0);
          }),
        );
        expect(pool.usedBytes).toBe(0);
        expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
        const query = lake.path("nation.parquet").batchSize(1).run();
        const iterator = query.rows()[Symbol.asyncIterator]();
        expect((await iterator.next()).value).toEqual(expected[0]);
        await iterator.return?.();
        expect(pool.usedBytes).toBe(0);
      }
      expect(ranges.length).toBeGreaterThan(0);
      for (const [start, end] of ranges) {
        expect(start).toBeGreaterThanOrEqual(0);
        expect(end).toBeLessThanOrEqual(bytes.length);
        expect(end).toBeGreaterThan(start);
      }
    } finally {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    }
  },
);
