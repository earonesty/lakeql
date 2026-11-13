import { existsSync, readFileSync } from "node:fs";
import { createServer } from "node:http";
import {
  concatOwnedBatches,
  gatherOwnedBatch,
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  SharedMemoryCache,
  withObjectStoreReadControls,
} from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake } from "./index.js";
import { ParquetScanAdapter } from "./scan-adapter.js";
import { testQueryStats } from "./test-helpers.js";

const fixture = new URL(
  "../../../fixtures/external/parquet-testing/data/json.parquet",
  import.meta.url,
);
it.skipIf(!existsSync(fixture)).each([false, true])(
  "preserves mixed JSON over HTTP, ignored=%s",
  async (ignoreRange) => {
    const bytes = new Uint8Array(readFileSync(fixture));
    const records = [{ a: 1 }, { a: 1, b: null }, [1, null, 3], null];
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
          ETag: '"json-fixture-v1"',
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
          Array.from({ length: 4 }, () =>
            lake.path("json.parquet").batchSize(3).run().toOwnedArray(),
          ),
        );
        try {
          for (const result of results)
            expect(result.value).toEqual(records.map((json_field) => ({ json_field })));
        } finally {
          for (const result of results) result.release();
        }
        const dataReads = ranges.filter((range) => range !== "bytes=0-1");
        if (round === "warm") expect(dataReads).toHaveLength(0);
        else expect(dataReads.length).toBeGreaterThan(0);
        expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
      }
      const producer = pool.child(512 * 1024);
      const consumer = pool.child(512 * 1024);
      const adapter = new ParquetScanAdapter(store, {
        scanRangeCache: { maxBytes: 256 * 1024, coalesceBytes: 0, sharedCache: cache },
      });
      if (!adapter.scanOwnedVectorBatches) throw Error("Missing owned scanner");
      const iterator = adapter
        .scanOwnedVectorBatches("json.parquet", {
          memoryBudget: producer,
          batchSize: 4,
          budget: {},
          stats: testQueryStats(),
          now: () => 0,
          startedAt: 0,
        })
        [Symbol.asyncIterator]();
      const scanned = await iterator.next();
      if (scanned.done) throw Error("Missing fixture batch");
      const bridge = new MemoryScope(producer);
      const source = bridge.retainBacking(scanned.value.value.batch, [scanned.value]).retain();
      bridge.dispose();
      scanned.value.release();
      await iterator.return?.();
      cache.deleteWhere(() => true);
      let selected = gatherOwnedBatch(source, [0, 2], consumer);
      try {
        for (let i = 0; i < 128; i++) {
          const joined = concatOwnedBatches([selected, source], consumer);
          try {
            const next = gatherOwnedBatch(joined, [4, 0], consumer);
            selected.release();
            selected = next;
          } finally {
            joined.release();
          }
          expect(materializeBatchRows(selected.value)).toEqual([
            { json_field: records[2] },
            { json_field: i === 0 ? records[0] : records[2] },
          ]);
        }
        source.release();
        expect(producer.usedBytes).toBe(0);
        expect(materializeBatchRows(selected.value)).toEqual([
          { json_field: records[2] },
          { json_field: records[2] },
        ]);
      } finally {
        selected.release();
        source.release();
      }
      expect(consumer.usedBytes).toBe(0);
      expect(peak).toBeLessThanOrEqual(2);
    } finally {
      cache.deleteWhere(() => true);
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    }
    expect(pool.usedBytes).toBe(0);
  },
);
