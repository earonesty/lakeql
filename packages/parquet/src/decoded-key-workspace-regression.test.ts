import {
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  memoryStore,
  SharedMemoryCache,
} from "lakeql-core";
import { expect, it, vi } from "vitest";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { readParquetColumnBatches, writeParquet } from "./index.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it("reads real Parquet values without serializing an oversized optional cache identity", async () => {
  const store = memoryStore();
  await writeParquet(store, "fixture.parquet", {
    columnData: [{ name: "a", type: "INT32", data: [1, 2, 3] }],
  });
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool.child());
  const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 65536 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 65536, policy: "latency" });
  const stringify = vi.spyOn(JSON, "stringify");
  const rows = [];
  try {
    for await (const result of readParquetColumnBatches(store, "fixture.parquet", {
      columns: ["a"],
      batchSize: 2,
      allocationScope: scope,
      decodedColumnCache: cache,
      decodedColumnCacheKey: "x".repeat(128 * 1024),
    }))
      rows.push(...materializeBatchRows(result.batch));
    expect(rows).toEqual([{ a: 1 }, { a: 2 }, { a: 3 }]);
    expect(
      stringify.mock.calls.filter(([value]) => Array.isArray(value) && value[0] === "column-v2"),
    ).toHaveLength(0);
  } finally {
    stringify.mockRestore();
    scope.dispose();
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});

it("reads owned Parquet values when optional identities exceed the entire pool", async () => {
  const store = memoryStore();
  await writeParquet(store, "fixture.parquet", {
    columnData: [{ name: "a", type: "INT32", data: [1, 2, 3] }],
  });
  const file = await asyncBufferFromStore(store, "fixture.parquet");
  const metadata = await readParquetMetadataFromFile(file);
  const pool = new MemoryBudget(65536);
  const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 65536 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 65536, policy: "latency" });
  const stringify = vi.spyOn(JSON, "stringify");
  const rows = [];
  try {
    for await (const owner of readOwnedParquetVectorBatches(file, metadata, pool, {
      columns: ["a"],
      batchSize: 2,
      decodedColumnCache: cache,
      decodedColumnCacheKey: "x".repeat(128 * 1024),
    })) {
      try {
        rows.push(...materializeBatchRows(owner.value.batch));
      } finally {
        owner.release();
      }
    }
    expect(rows).toEqual([{ a: 1 }, { a: 2 }, { a: 3 }]);
    expect(
      stringify.mock.calls.filter(
        ([value]) =>
          Array.isArray(value) &&
          ["column-v2", "page-v2", "dictionary-v2", "page-header"].includes(value[0]),
      ),
    ).toHaveLength(0);
  } finally {
    stringify.mockRestore();
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});
