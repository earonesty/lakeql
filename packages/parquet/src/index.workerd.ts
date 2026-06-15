import { eq, lt, memoryCache, memoryStore } from "@laql/core";
import { describe, expect, it } from "vitest";
import { createParquetLake, type ParquetMetadata, writeParquet } from "./index.js";

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
