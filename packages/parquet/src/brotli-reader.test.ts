import { readFileSync } from "node:fs";
import { MemoryBudget, MemoryScope, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { brotliFixtureRows, createBrotliFixture } from "../../../fixtures/src/brotli.js";
import { createParquetLake, readParquetObjectBatches, readParquetObjects } from "./index.js";

it("rejects the actual multi-gigabyte Brotli fixture within the requested memory bound", async () => {
  const store = memoryStore();
  const path = "large_string_map.brotli.parquet";
  await store.put(
    path,
    readFileSync(
      new URL(`../../../fixtures/external/parquet-testing/data/${path}`, import.meta.url),
    ),
  );
  for (const cap of [64, 128]) {
    const pool = new MemoryBudget(cap * 1024 * 1024);
    const scope = new MemoryScope(pool);
    try {
      await expect(async () => {
        for await (const batch of readParquetObjectBatches(store, path, {
          decompressionScope: scope,
          batchSize: 1,
        })) {
          void batch;
        }
      }).rejects.toThrow(/memory budget/);
    } finally {
      scope.dispose();
    }
    expect(pool.usedBytes).toBe(0);
    const lake = createParquetLake({
      store,
      memoryPool: pool,
      scanRangeCache: { maxBytes: 0 },
      budget: { maxAllocatedMemoryBytes: pool.maxBytes },
    });
    await expect(lake.path(path).batchSize(1).run().toOwnedArray()).rejects.toThrow(
      /memory budget/,
    );
    expect(pool.usedBytes).toBe(0);
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
  }
});

it("reads actual Brotli Parquet pages through public and owned readers", async () => {
  const store = memoryStore();
  const rows = brotliFixtureRows();
  await store.put("brotli.parquet", createBrotliFixture());
  expect(await readParquetObjects(store, "brotli.parquet")).toEqual(rows);
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  for (const batchSize of [1, 64, 256]) {
    const owner = await lake.path("brotli.parquet").batchSize(batchSize).run().toOwnedArray();
    try {
      expect(owner.value).toEqual(rows);
    } finally {
      owner.release();
    }
    expect(pool.usedBytes).toBe(0);
  }
});
