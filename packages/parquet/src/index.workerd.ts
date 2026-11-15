import { env } from "cloudflare:test";
import { eq, gte, Lake, lt, memoryCache, memoryStore, SharedMemoryCache } from "lakeql-core";
import { describe, expect, it } from "vitest";
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
  await store.delete("ordering.parquet");
});
