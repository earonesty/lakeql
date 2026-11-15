import { MemoryBudget, materializeBatchRows, memoryStore, SharedMemoryCache } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { writeParquet } from "./index.js";
import {
  type OwnedMetadataCache,
  readCachedParquetMetadata,
  readOwnedParquetMetadata,
  sharedParquetMetadataCache,
} from "./metadata-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import { readParquetVectorBatchesFromFile } from "./vector-batches.js";

async function fixture() {
  const store = memoryStore();
  await writeParquet(store, "fixture.parquet", {
    columnData: [{ name: "n", type: "INT32", data: [1, 2, 3] }],
  });
  return asyncBufferFromStore(store, "fixture.parquet");
}
function customCache(): OwnedMetadataCache {
  return {
    acquireOwned: vi.fn(() => undefined),
    setOwned: vi.fn(() => false),
    async get() {
      return undefined;
    },
    async set() {},
    async delete() {},
  };
}

it.each([
  "shared",
  "custom",
  "none",
  "borrowed-shared",
] as const)("reads real metadata without serializing an oversized optional identity (%s)", async (mode) => {
  const file = await fixture();
  const pool = new MemoryBudget(65536);
  const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 65536 });
  const cache =
    mode === "none"
      ? undefined
      : mode === "custom"
        ? customCache()
        : sharedParquetMetadataCache(shared);
  const stringify = vi.spyOn(JSON, "stringify");
  try {
    const result =
      mode === "borrowed-shared"
        ? await readCachedParquetMetadata("x".repeat(128 * 1024), file, cache)
        : await readOwnedParquetMetadata("x".repeat(128 * 1024), file, pool, cache);
    try {
      const metadata = "owner" in result ? result.owner.value : result.metadata;
      expect(metadata.num_rows).toBe(3n);
      const rows = [];
      for await (const batch of readParquetVectorBatchesFromFile(file, metadata, {
        columns: ["n"],
      }))
        rows.push(...materializeBatchRows(batch.batch));
      expect(rows).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
      expect(result.cached).toBe(false);
      expect(
        stringify.mock.calls.filter(
          ([value]) => Array.isArray(value) && value[0] === "parquet-metadata-v2",
        ),
      ).toHaveLength(0);
      if (mode === "custom") {
        expect(cache?.acquireOwned).not.toHaveBeenCalled();
        expect(cache?.setOwned).not.toHaveBeenCalled();
      }
    } finally {
      if ("owner" in result) result.owner.release();
    }
  } finally {
    stringify.mockRestore();
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "lookup",
  "insert",
] as const)("releases key and metadata ownership when a custom adapter fails at %s", async (phase) => {
  const file = await fixture();
  const pool = new MemoryBudget(65536);
  const failure = new Error("cache adapter failure");
  const cache = customCache();
  if (phase === "lookup")
    cache.acquireOwned = () => {
      throw failure;
    };
  else
    cache.setOwned = () => {
      throw failure;
    };
  await expect(readOwnedParquetMetadata("fixture", file, pool, cache)).rejects.toBe(failure);
  expect(pool.usedBytes).toBe(0);
});
