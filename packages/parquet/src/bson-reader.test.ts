import { readFileSync } from "node:fs";
import { MemoryBudget, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { createParquetLake, readParquetObjects } from "./index.js";

it("decodes the actual BSON fixture through public and owned readers without dropping null fields", async () => {
  const store = memoryStore();
  const path = "bson.parquet";
  await store.put(
    path,
    readFileSync(
      new URL("../../../fixtures/external/parquet-testing/data/bson.parquet", import.meta.url),
    ),
  );
  const expected = [
    { bson_field: { a: 1 } },
    { bson_field: { a: 1, b: null } },
    { bson_field: null },
  ];
  expect(await readParquetObjects(store, path)).toEqual(expected);
  const pool = new MemoryBudget(1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const result = await lake.path(path).batchSize(1).run().toOwnedArray();
  try {
    expect(result.value).toEqual(expected);
    expect(pool.usedBytes).toBeGreaterThan(0);
  } finally {
    result.release();
  }
  expect(pool.usedBytes).toBe(0);
});
