import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import {
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  memoryStore,
  SharedMemoryCache,
  withObjectStoreReadControls,
} from "lakeql-core";
import { fixturePath, SALES } from "lakeql-fixtures";
import { describe, expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readParquetObjectBatchesFromFile } from "./object-batches.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { cachedRangeBuffer } from "./range-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import type { StoreAsyncBuffer } from "./types.js";

async function decode(file: StoreAsyncBuffer) {
  const metadata = await readParquetMetadataFromFile(file);
  const rows = [];
  for await (const batch of readParquetObjectBatchesFromFile(file, metadata, { batchSize: 13 })) {
    rows.push(...batch.rows);
  }
  return rows;
}

describe("range cache over HTTP", () => {
  it.each([
    false,
    true,
  ])("decodes a real Parquet fixture with bounded parallel ranges (shared=%s)", async (shared) => {
    const fixture = await readFile(fixturePath(SALES.file));
    let active = 0;
    let peak = 0;
    let fetched = 0;
    let fullGets = 0;
    const ranges: string[] = [];
    const server = createServer((request, response) => {
      if (request.method === "HEAD") {
        response.writeHead(200, { "Content-Length": fixture.length, ETag: '"fixture-v1"' });
        response.end();
        return;
      }
      const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
      if (match === null) {
        fullGets += 1;
        response.writeHead(400).end();
        return;
      }
      const start = Number(match[1]);
      const end = Number(match[2]) + 1;
      ranges.push(`${start}:${end}`);
      active += 1;
      peak = Math.max(peak, active);
      setTimeout(() => {
        fetched += end - start;
        response.writeHead(206, {
          "Content-Length": end - start,
          "Content-Range": `bytes ${start}-${end - 1}/${fixture.length}`,
          ETag: '"fixture-v1"',
        });
        response.end(fixture.subarray(start, end));
        active -= 1;
      }, 5);
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    try {
      const address = server.address();
      if (address === null || typeof address === "string") throw new Error("Missing HTTP listener");
      const url = `http://127.0.0.1:${address.port}/sales.parquet`;
      const store = withObjectStoreReadControls(
        {
          ...memoryStore(),
          async head() {
            const response = await fetch(url, { method: "HEAD" });
            return {
              size: Number(response.headers.get("Content-Length")),
              etag: response.headers.get("ETag") ?? "",
            };
          },
          async getRange(_path, range) {
            const response = await fetch(url, {
              headers: { Range: `bytes=${range.offset}-${range.offset + range.length - 1}` },
            });
            expect(response.status).toBe(206);
            return new Uint8Array(await response.arrayBuffer());
          },
        },
        { maxConcurrentReads: 2 },
      );
      const source = await asyncBufferFromStore(store, "sales.parquet");
      const cached = cachedRangeBuffer(
        source,
        {
          maxBytes: 1024,
          coalesceBytes: 128,
          ...(shared ? { sharedCache: new SharedMemoryCache({ maxBytes: 1024 }) } : {}),
        },
        url,
      );
      await Promise.all([
        cached.prefetch?.([
          { start: 0, end: 8 },
          { start: 128, end: 136 },
          { start: 256, end: 264 },
          { start: 2, end: 4 },
        ]),
        cached.slice(2, 4),
      ]);
      expect(ranges.sort()).toEqual(["0:128", "128:256", "256:384"]);
      expect(fetched).toBe(384);
      expect(peak).toBe(2);
      const localBytes = Uint8Array.from(fixture).buffer;
      const local = {
        byteLength: localBytes.byteLength,
        async slice(start: number, end?: number) {
          return localBytes.slice(start, end);
        },
      };
      const actual = await decode(cached);
      expect(actual).toHaveLength(SALES.rows);
      expect(actual).toEqual(await decode(local));
      expect(fullGets).toBe(0);
      expect(peak).toBe(2);
    } finally {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    }
  });
});

it.each([64, 128])("runs owned HTTP fixture reads under a %i MiB shared pool", async (mib) => {
  const fixture = await readFile(fixturePath(SALES.file));
  const ranges: string[] = [];
  let active = 0;
  let peak = 0;
  let fullGets = 0;
  const server = createServer((request, response) => {
    const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
    if (match === null) {
      fullGets += 1;
      response.writeHead(400).end();
      return;
    }
    const start = Number(match[1]);
    const end = Number(match[2]) + 1;
    ranges.push(`${start}:${end}`);
    active += 1;
    peak = Math.max(peak, active);
    setTimeout(() => {
      response.writeHead(206, {
        "Content-Length": end - start,
        "Content-Range": `bytes ${start}-${end - 1}/${fixture.length}`,
      });
      response.end(fixture.subarray(start, end));
      active -= 1;
    }, 5);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const budget = new MemoryBudget(mib * 1024 * 1024);
  const first = new MemoryScope(budget);
  const second = new MemoryScope(budget);
  const cache = new SharedMemoryCache({ maxBytes: 64 * 1024, memoryBudget: budget });
  try {
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("Missing listener");
    const store = withObjectStoreReadControls(
      new HttpObjectStore({ baseUrl: `http://127.0.0.1:${address.port}/` }),
      { maxConcurrentReads: 2 },
    );
    const source = await asyncBufferFromStore(store, "sales.parquet");
    ranges.length = 0; // Exclude HTTP adapter's metadata probe.
    const file = cachedRangeBuffer(
      source,
      { maxBytes: 64 * 1024, coalesceBytes: 128, sharedCache: cache },
      "owned-sales",
    );
    const [, output] = await Promise.all([
      file.prefetchOwned?.(
        [
          { start: 0, end: 8 },
          { start: 128, end: 136 },
          { start: 256, end: 264 },
        ],
        first,
      ),
      file.sliceOwned?.(2, 4, second),
    ]);
    expect(ranges.toSorted()).toEqual(["0:128", "128:256", "256:384"]);
    expect(peak).toBe(2);
    expect(new Uint8Array(output?.value ?? new ArrayBuffer(0))).toEqual(
      Uint8Array.from(fixture.subarray(2, 4)),
    );
    output?.release();
    const beforeWarm = ranges.length;
    await file.prefetchOwned?.([{ start: 0, end: 8 }], second);
    expect(ranges.length).toBe(beforeWarm);
    // Retain every source/output allocation for this decode stage. Decoded JS
    // rows are outside this read-allocation assertion and are checked for parity.
    const ownedFile: StoreAsyncBuffer = {
      byteLength: file.byteLength,
      async slice(start, end = file.byteLength) {
        const owner = await file.sliceOwned?.(start, end, second);
        if (owner === undefined) throw new Error("Missing owned slice");
        return owner.value;
      },
    };
    const rows = await decode(ownedFile);
    const local: StoreAsyncBuffer = {
      byteLength: fixture.length,
      async slice(start, end) {
        return Uint8Array.from(fixture.subarray(start, end)).buffer;
      },
    };
    expect(rows).toEqual(await decode(local));
    expect(rows).toHaveLength(SALES.rows);
    const metadata = await readParquetMetadataFromFile(ownedFile);
    // Transport buffers, cache pins, and admitted decoder storage share the root.
    // Output row objects and parsed metadata are not covered by this assertion.
    const columns = ["store_id", "amount"];
    first.dispose();
    second.dispose();
    cache.deleteWhere(() => true);
    expect(budget.usedBytes).toBe(0);
    const coldReadStart = ranges.length;
    const decodedCache = new DecodedColumnCache(cache, { maxBytes: 64 * 1024, policy: "latency" });
    const children = [budget.child(), budget.child()];
    const run = async (child: MemoryBudget) => {
      const decoded = [];
      for await (const owner of readOwnedParquetVectorBatches(file, metadata, child, {
        columns,
        batchSize: 7,
        decodedColumnCache: decodedCache,
        decodedColumnCacheKey: "http-sales",
      })) {
        try {
          expect(owner.value.batch.rowCount).toBeLessThanOrEqual(7);
          decoded.push(...materializeBatchRows(owner.value.batch));
        } finally {
          owner.release();
        }
      }
      expect(child.usedBytes).toBe(0);
      return decoded;
    };
    const expected = rows.map((row) => ({ store_id: row.store_id, amount: row.amount }));
    const results = await Promise.all(children.map(run));
    expect(results).toEqual([expected, expected]);
    const coldReads = ranges.slice(coldReadStart);
    expect(coldReads.length).toBeGreaterThan(0);
    expect(new Set(coldReads).size).toBe(coldReads.length);
    const warmReads = ranges.length;
    expect(await run(budget.child())).toEqual(expected);
    expect(ranges.length).toBe(warmReads);
    const retainedBudget = budget.child();
    const iterator = readOwnedParquetVectorBatches(file, metadata, retainedBudget, {
      columns,
      batchSize: 1,
    })[Symbol.asyncIterator]();
    const outputBatch = await iterator.next();
    if (outputBatch.done) throw new Error("Missing owned HTTP output");
    await iterator.return?.();
    expect(retainedBudget.usedBytes).toBeGreaterThan(0);
    expect(materializeBatchRows(outputBatch.value.value.batch)).toEqual(expected.slice(0, 1));
    outputBatch.value.release();
    expect(retainedBudget.usedBytes).toBe(0);
    expect(peak).toBeLessThanOrEqual(2);

    expect(fullGets).toBe(0);
    expect(budget.usedBytes).toBeGreaterThan(0);
    expect(budget.peakBytes).toBeLessThanOrEqual(budget.maxBytes);
  } finally {
    first.dispose();
    second.dispose();
    cache.deleteWhere(() => true);
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
  expect(budget.usedBytes).toBe(0);
});
