import { env } from "cloudflare:test";
import {
  eq,
  gte,
  Lake,
  lt,
  memoryCache,
  memoryStore,
  SharedMemoryCache,
  withObjectStoreReadControls,
} from "lakeql-core";
import { describe, expect, it } from "vitest";
import { createLake } from "../../lakeql/src/sql.js";
import { type R2BucketLike, r2Store } from "../../r2/src/index.js";
import { createParquetLake, type ParquetMetadata, writeParquet } from "./index.js";
import { ParquetScanAdapter } from "./scan-adapter.js";

describe("parquet workerd runtime", () => {
  it("writes and scans Parquet through the real adapter in the Workers runtime", async () => {
    expect((globalThis as Record<string, unknown>).WebSocketPair).toBeTypeOf("function");

    const store = memoryStore();
    await writeParquet(store, "events.parquet", {
      rowGroupSize: [2],
      columnData: [
        { name: "id", data: [1, 2, 3], type: "INT32" },
        { name: "region", data: ["west", "east", "west"], type: "STRING" },
        { name: "amount", data: [10, 20, 30], type: "INT32" },
      ],
    });

    const metadataCache = memoryCache<ParquetMetadata>();
    const lake = createParquetLake({
      store,
      metadataCache,
      queryId: () => "q_parquet_workerd",
    });
    const result = lake
      .path("events.parquet")
      .select(["id", "amount"])
      .where(eq("region", "west"))
      .run();

    await expect(result.toArray()).resolves.toEqual([
      { id: 1, amount: 10 },
      { id: 3, amount: 30 },
    ]);
    expect(result.stats).toMatchObject({
      queryId: "q_parquet_workerd",
      filesPlanned: 1,
      filesRead: 1,
      rowsDecoded: 3,
      rowsMatched: 2,
      rowsReturned: 2,
    });
    expect(result.stats.rangeRequests).toBeGreaterThan(0);
    expect(result.stats.cacheMisses).toBe(1);

    const cached = lake.path("events.parquet").where(lt("amount", 0)).run();
    await expect(cached.count()).resolves.toBe(0);
    expect(cached.stats.cacheHits).toBe(1);
    expect(cached.stats.cacheMisses).toBe(0);
    expect(cached.stats.rangeRequests).toBe(0);
  });
});

it("orders concurrent real Parquet jobs through native R2 with bounded candidates and reusable ranges", async () => {
  const bucket = (env as unknown as { ORDERING_FIXTURES: R2BucketLike }).ORDERING_FIXTURES;
  const store = r2Store(bucket);
  const rows = Array.from({ length: 256 }, (_, id) => ({
    id,
    score: id % 17,
    payload: `${id}:`.padEnd(128, "x"),
  }));
  await writeParquet(store, "ordering.parquet", {
    rowGroupSize: [64, 64, 64, 64],
    columnData: [
      { name: "id", data: rows.map((r) => r.id) },
      { name: "score", data: rows.map((r) => r.score) },
      { name: "payload", data: rows.map((r) => r.payload) },
    ],
  });
  const scanner = new ParquetScanAdapter(store, {
    scanRangeCache: {
      maxBytes: 64 * 1024,
      sharedCache: new SharedMemoryCache({ maxBytes: 64 * 1024 }),
    },
  });
  const expected = [...rows]
    .sort((a, b) => b.score - a.score)
    .slice(3, 13)
    .map(({ score, payload }) => ({ score, payload }));
  for (const mib of [64, 128]) {
    const deferred = new Lake({
      store,
      scanner,
      budget: { maxMemoryBytes: mib * 1024 * 1024, maxBufferedRows: 13 },
    });
    const ordinary = new Lake({
      store,
      scanner: { scan: scanner.scan.bind(scanner) },
      budget: { maxMemoryBytes: mib * 1024 * 1024, maxBufferedRows: 13 },
    });
    const run = (lake: Lake) =>
      lake
        .path("ordering.parquet")
        .batchSize(31)
        .select(["score", "payload"])
        .where(gte("id", 0))
        .orderBy([{ column: "score", direction: "desc" }])
        .offset(3)
        .limit(10)
        .run();
    const result = await Promise.all([
      run(deferred).toArray(),
      run(ordinary).toArray(),
      run(deferred).toArray(),
      run(ordinary).toArray(),
    ]);
    expect(result).toEqual([expected, expected, expected, expected]);
    const warm = run(deferred);
    expect(await warm.toArray()).toEqual(expected);
    expect(warm.stats.rangeRequests).toBe(0);
  }
  let reads = 0;
  const getRange = store.getRange.bind(store);
  store.getRange = async (path, range) => {
    reads++;
    return getRange(path, range);
  };
  const scalar = "(SELECT MAX(score) FROM input)";
  const sql = async (repeat: boolean, memoMaxBytes?: number) => {
    const lake = createLake({ store, scanRangeCache: { maxBytes: 0 } });
    return lake
      .sql(`SELECT id, ${scalar} AS a${repeat ? `, ${scalar} AS b` : ""} FROM input LIMIT 2`, {
        tables: { input: "ordering.parquet" },
        ...(memoMaxBytes === undefined ? {} : { memoMaxBytes }),
      })
      .toArray();
  };
  expect(await sql(false)).toEqual([
    { id: 0, a: 16 },
    { id: 1, a: 16 },
  ]);
  const singleReads = reads;
  reads = 0;
  expect(await sql(true)).toEqual([
    { id: 0, a: 16, b: 16 },
    { id: 1, a: 16, b: 16 },
  ]);
  expect(reads).toBe(singleReads);
  reads = 0;
  await sql(true, 0);
  expect(reads).toBeGreaterThan(singleReads);
  await store.delete("ordering.parquet");
});

it("bounds wide deferred payloads across concurrent native R2 jobs", async () => {
  const bucket = (env as unknown as { ORDERING_FIXTURES: R2BucketLike }).ORDERING_FIXTURES;
  const store = withObjectStoreReadControls(r2Store(bucket), { maxConcurrentReads: 2 });
  const rows = Array.from({ length: 12 }, (_, score) => ({ score, payload: "雪🙂".repeat(1024) }));
  await writeParquet(store, "payload-budget.parquet", {
    rowGroupSize: Array(12).fill(1),
    columnData: [
      { name: "score", data: rows.map((r) => r.score) },
      { name: "payload", data: rows.map((r) => r.payload) },
    ],
  });
  const scanner = new ParquetScanAdapter(store, { scanRangeCache: { maxBytes: 0 } });
  const run = (maxMemoryBytes: number, ordered: boolean) => {
    const lake = new Lake({ store, scanner, budget: { maxMemoryBytes, maxConcurrentReads: 2 } });
    let query = lake
      .path("payload-budget.parquet")
      .select(["score", "payload"])
      .where(gte("score", 0))
      .offset(1)
      .limit(2);
    if (ordered) query = query.orderBy([{ column: "score" }]);
    return query.toArray();
  };
  await Promise.all(
    [false, true].flatMap((ordered) => [
      expect(run(4096, ordered)).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: { metric: "operator memory bytes" },
      }),
      expect(run(32 * 1024, ordered)).resolves.toEqual(rows.slice(1, 3)),
    ]),
  );
  await Promise.all(
    [false, true].map(async (ordered) => {
      const lake = new Lake({
        store,
        scanner,
        budget: { maxMemoryBytes: 32 * 1024, maxConcurrentReads: 2 },
      });
      let query = lake
        .path("payload-budget.parquet")
        .select(["score", "payload"])
        .where(gte("score", 0))
        .batchSize(3)
        .limit(rows.length);
      if (ordered) query = query.orderBy([{ column: "score", direction: "desc" }]);
      const output = await query.toArray();
      expect(output).toEqual(ordered ? [...rows].reverse() : rows);
      expect(new TextEncoder().encode(JSON.stringify(output)).length).toBeGreaterThan(32 * 1024);
    }),
  );
  expect(await run(32 * 1024, true)).toEqual(rows.slice(1, 3));
});
