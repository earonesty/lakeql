import { createServer } from "node:http";
import { parquetWriteBuffer } from "hyparquet-writer";
import {
  col,
  eq,
  isNotNull,
  isNull,
  Lake,
  MemoryBudget,
  mod,
  SharedMemoryCache,
  withObjectStoreReadControls,
} from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake, ParquetScanAdapter } from "./index.js";

it.each(
  [
    { ignoreRange: false, ranked: false },
    { ignoreRange: true, ranked: false },
    { ignoreRange: false, ranked: true },
    { ignoreRange: true, ranked: true },
  ].flatMap((config) => [
    { ...config, borrowed: false, poolMiB: 64 },
    ...[64, 128].map((poolMiB) => ({ ...config, borrowed: true, poolMiB })),
  ]),
)("defers sparse output columns over HTTP: %j", async ({
  ignoreRange,
  ranked,
  borrowed,
  poolMiB,
}) => {
  let seed = 12345;
  const rows = Array.from({ length: 512 }, (_, id) => ({
    id,
    text:
      id % 7 === 0
        ? null
        : Array.from({ length: 512 }, () => {
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
  const pool = new MemoryBudget(poolMiB * 1024 * 1024);
  const cache = new SharedMemoryCache({ maxBytes: 8 * 1024 * 1024, memoryBudget: pool });
  try {
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Missing listener");
    const store = withObjectStoreReadControls(
      new HttpObjectStore({ baseUrl: `http://127.0.0.1:${address.port}/` }),
      { maxConcurrentReads: 2 },
    );
    const scanRangeCache = { maxBytes: 8 * 1024 * 1024, coalesceBytes: 0, sharedCache: cache };
    const adapter = new ParquetScanAdapter(store, { scanRangeCache });
    const borrowedReads: { columns: string[] | undefined; rowStart: number | undefined }[] = [];
    let borrowedColumnReads = 0;
    const lake = borrowed
      ? new Lake({
          store,
          memoryPool: pool,
          scanner: {
            scan: adapter.scan.bind(adapter),
            async *scanColumns(path, options) {
              borrowedColumnReads++;
              yield* adapter.scanColumns(path, options);
            },
            async *scanVectorBatches(path, options) {
              borrowedReads.push({ columns: options.columns, rowStart: options.rowStart });
              yield* adapter.scanVectorBatches(path, options);
            },
          },
        })
      : createParquetLake({ store, memoryPool: pool, scanRangeCache });
    if (!ranked) {
      // A scanner's batch size is a hint, while the query output boundary must
      // still hold if a remote row group is returned in a larger vector batch.
      let largestInput = 0;
      const oversizedLake = new Lake({
        store,
        memoryPool: pool,
        scanner: {
          scan: adapter.scan.bind(adapter),
          ...(borrowed
            ? {
                async *scanVectorBatches(path, options) {
                  for await (const input of adapter.scanVectorBatches(path, {
                    ...options,
                    batchSize: 32,
                  })) {
                    largestInput = Math.max(largestInput, input.batch.rowCount);
                    yield input;
                  }
                },
              }
            : {
                async *scanOwnedVectorBatches(path, options) {
                  for await (const owner of adapter.scanOwnedVectorBatches(path, {
                    ...options,
                    batchSize: 32,
                  })) {
                    largestInput = Math.max(largestInput, owner.value.batch.rowCount);
                    yield owner;
                  }
                },
              }),
        },
      });
      const actual = [];
      for await (const batch of oversizedLake
        .path("rows.parquet")
        .where(eq(mod(col("id"), 2), 0))
        .project({ copy: col("text") })
        .offset(5)
        .limit(17)
        .batchSize(3)
        .run()
        .batches()) {
        expect(batch.length).toBeLessThanOrEqual(3);
        actual.push(...batch);
      }
      expect(largestInput).toBeGreaterThan(3);
      expect(actual).toEqual(
        rows
          .filter((row) => row.id % 2 === 0)
          .slice(5, 22)
          .map((row) => ({ ...row, copy: row.text })),
      );
      cache.deleteWhere(() => true);
      expect(pool.usedBytes).toBe(0);
      transferred = 0;
      ranges.length = 0;
    }
    const query = (select: string[]) => {
      const base = lake
        .path("rows.parquet")
        .select(select)
        .where(eq(mod(col("id"), 257), 256));
      return (ranked ? base.orderBy([{ column: "id", direction: "desc" }]) : base)
        .limit(1)
        .batchSize(16)
        .run();
    };
    const baseline = await query(["*"]).toOwnedArray();
    expect(baseline.value).toEqual([rows[256]]);
    if (borrowed && ranked) expect(borrowedColumnReads).toBeGreaterThan(0);
    baseline.release();
    const eagerBytes = transferred;
    const eagerRanges = [...ranges];
    cache.deleteWhere(() => true);
    transferred = 0;
    ranges.length = 0;
    borrowedReads.length = 0;
    const deferred = await query(["id", "text"]).toOwnedArray();
    expect(deferred.value).toEqual([rows[256]]);
    deferred.release();
    if (borrowed && !ignoreRange) {
      expect(borrowedReads.some((read) => read.columns?.join() === "id")).toBe(true);
      expect(
        borrowedReads.some((read) => read.columns?.join() === "text" && read.rowStart === 256),
      ).toBe(true);
    }
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
    cache.deleteWhere(() => true);
    borrowedReads.length = 0;
    const aggregate = await lake
      .path("rows.parquet")
      .where(eq(mod(col("id"), 257), 256))
      .batchSize(16)
      .groupBy(["id"])
      .aggregate({ first: { op: "min", column: "text" }, n: { op: "count" } });
    expect(aggregate).toEqual([{ id: 256, first: rows[256]?.text, n: 1 }]);
    if (borrowed && !ignoreRange) {
      expect(borrowedReads.some((read) => read.columns?.join() === "id")).toBe(true);
      expect(
        borrowedReads.some((read) => read.columns?.join() === "text" && read.rowStart === 256),
      ).toBe(true);
    }

    if (ranked) {
      const stream = query(["id", "text"]).batches()[Symbol.asyncIterator]();
      try {
        const first = await stream.next();
        expect(first.value).toEqual([rows[256]]);
        cache.deleteWhere(() => true);
        expect(pool.usedBytes).toBeGreaterThan(0);
        expect(first.value).toEqual([rows[256]]);
      } finally {
        await stream.return?.();
      }
      expect(pool.usedBytes).toBe(0);
    }

    // Exercise the same retained slice contract through real HTTP 206 and 200
    // fallback responses, including borrowed scanner adapters and cache eviction.
    const sliceQuery = lake
      .path("rows.parquet")
      .where(eq(mod(col("id"), 17), 0))
      .limit(20)
      .batchSize(7)
      .run();
    const expectedSlice = rows.filter((row) => row.id % 17 === 0).slice(0, 20);
    const firstSlice = await sliceQuery.slice({ maxRows: 9 });
    expect(firstSlice.rows).toEqual(expectedSlice.slice(0, 9));
    if (!firstSlice.bookmark) throw new Error("Expected slice continuation");
    cache.deleteWhere(() => true);
    const secondSlice = await sliceQuery.slice({ maxRows: 11, bookmark: firstSlice.bookmark });
    expect(secondSlice.rows).toEqual(expectedSlice.slice(9));
    expect(secondSlice.bookmark).toBeUndefined();
    cache.deleteWhere(() => true);
    expect(firstSlice.rows).toEqual(expectedSlice.slice(0, 9));
    expect(pool.usedBytes).toBe(0);

    for (const nullRows of [true, false]) {
      const owner = await lake
        .path("rows.parquet")
        .select(["id"])
        .where(nullRows ? isNull("text") : isNotNull("text"))
        .offset(1)
        .limit(5)
        .batchSize(3)
        .run()
        .toOwnedArray();
      try {
        expect(owner.value).toEqual(
          rows
            .filter((row) => (row.text === null) === nullRows)
            .slice(1, 6)
            .map(({ id }) => ({ id })),
        );
        cache.deleteWhere(() => true);
        expect(pool.usedBytes).toBeGreaterThan(0);
      } finally {
        owner.release();
      }
      expect(pool.usedBytes).toBe(0);
    }

    expect(peak).toBeLessThanOrEqual(2);
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    if (process.env.LAKEQL_DEFERRED_METRICS === "1")
      process.stdout.write(
        `${JSON.stringify({
          ignoreRange,
          ranked,
          borrowed,
          poolMiB,
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
