import { readFileSync } from "node:fs";
import { MemoryBudget, memoryStore } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { createParquetLake, readParquetMetadata, readParquetObjects } from "./index.js";

it("rejects the actual BSON fixture with typed schema diagnostics before page reads", async () => {
  const store = memoryStore();
  const path = "bson.parquet";
  await store.put(
    path,
    readFileSync(
      new URL("../../../fixtures/external/parquet-testing/data/bson.parquet", import.meta.url),
    ),
  );
  const reads = vi.spyOn(store, "getRange");
  await readParquetMetadata(store, path);
  const footerReads = reads.mock.calls.map((args) => [...args]);
  reads.mockClear();
  await expect(readParquetObjects(store, path)).rejects.toMatchObject({
    code: "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
    details: { feature: "bson" },
  });
  expect(reads.mock.calls).toEqual(footerReads);
  reads.mockRestore();
  const pool = new MemoryBudget(1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const iterator = lake.path(path).batchSize(1).run().rows()[Symbol.asyncIterator]();
  await expect(iterator.next()).rejects.toMatchObject({
    code: "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
    details: { feature: "bson" },
  });
  expect(pool.usedBytes).toBe(0);
});
