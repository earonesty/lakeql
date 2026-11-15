import { parquetWriteBuffer } from "hyparquet-writer";
import {
  MATERIALIZATION_CHARGES as c,
  MemoryBudget,
  materializeBatchRows,
  SharedMemoryCache,
  sharedCacheMetadataBytes,
} from "lakeql-core";
import { expect, it, vi } from "vitest";
import { borrowedRangeKeyBytes, fileCacheKey } from "./file-cache-key.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { cachedRangeBuffer } from "./range-cache.js";
import { readParquetVectorBatchesFromFile } from "./vector-batches.js";

it.each([
  false,
  true,
])("skips oversized optional keys and reads real Parquet values (shared=%s)", async (shared) => {
  const input = parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    columnData: [{ name: "n", type: "INT32", data: [11, 12] }],
  });
  const pool = new MemoryBudget(65536);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 65536 });
  let reads = 0;
  const file = cachedRangeBuffer(
    {
      byteLength: input.byteLength,
      async slice(start, end) {
        reads++;
        return input.slice(start, end);
      },
    },
    {
      maxBytes: 65536,
      cacheOptions: { memoryBudget: pool },
      ...(shared ? { sharedCache: cache } : {}),
    },
    "x".repeat(128 * 1024),
  );
  const stringify = vi.spyOn(JSON, "stringify");
  try {
    await file.prefetch?.([{ start: 0, end: input.byteLength }]);
    expect(reads).toBe(shared ? 0 : 1);
    const metadata = await readParquetMetadataFromFile(file);
    const rows = [];
    for await (const batch of readParquetVectorBatchesFromFile(file, metadata, { columns: ["n"] }))
      rows.push(...materializeBatchRows(batch.batch));
    expect(rows).toEqual([{ n: 11 }, { n: 12 }]);
    const keys = stringify.mock.calls.filter(
      ([value]) => Array.isArray(value) && value[0] === "scan-range-v2",
    );
    if (shared) expect(keys).toHaveLength(0);
    else {
      expect(pool.usedBytes).toBeGreaterThan(0);
      expect(keys.every(([value]) => Array.isArray(value) && value[1] === "")).toBe(true);
    }
  } finally {
    stringify.mockRestore();
    cache.deleteWhere(() => true);
    // Private entries participate in root reclamation even without a cache handle.
    const pressure = pool.reserve(pool.maxBytes, "release private fixture cache");
    pressure.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it("charges both pending keys until a deduplicated read settles", async () => {
  const keyBytes = borrowedRangeKeyBytes("fixture", 8, undefined, 0, 8);
  const retained =
    8 + sharedCacheMetadataBytes(fileCacheKey("scan-range-v2", "fixture", 8, undefined, 0, 8));
  const pool = new MemoryBudget(2 * keyBytes + retained);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: retained });
  let finish!: () => void;
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let reads = 0;
  const file = cachedRangeBuffer(
    {
      byteLength: 8,
      async slice() {
        reads++;
        await gate;
        return new Uint8Array(8).fill(7).buffer;
      },
    },
    { maxBytes: retained, coalesceBytes: 8, sharedCache: cache },
    "fixture",
  );
  const outputs = Promise.all([file.slice(0, 2), file.slice(2, 4)]);
  try {
    expect(reads).toBe(1);
    expect(pool.usedBytes).toBe(2 * keyBytes);
  } finally {
    finish();
  }
  expect((await outputs).map((bytes) => [...new Uint8Array(bytes)])).toEqual([
    [7, 7],
    [7, 7],
  ]);
  expect(pool.usedBytes).toBe(retained);
  cache.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(0);
});

it("admits prefetch planning, key and retained entry at the exact total-pool boundary", async () => {
  const keyBytes = borrowedRangeKeyBytes("fixture", 8, undefined, 0, 8);
  const retained =
    8 + sharedCacheMetadataBytes(fileCacheKey("scan-range-v2", "fixture", 8, undefined, 0, 8));
  const directory = 2 * c.array + c.object + 2 * c.property;
  const selection = c.object + 3 * c.property + c.string + 65 * c.stringCodeUnit + 4 * c.arraySlot;
  for (const shortage of [0, 1]) {
    const pool = new MemoryBudget(directory + selection + keyBytes + retained - shortage);
    const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: retained });
    let reads = 0;
    const file = cachedRangeBuffer(
      {
        byteLength: 8,
        async slice() {
          reads++;
          return new ArrayBuffer(8);
        },
      },
      { maxBytes: retained, coalesceBytes: 8, sharedCache: cache },
      "fixture",
    );
    await file.prefetch?.([{ start: 0, end: 1 }]);
    expect(reads).toBe(shortage ? 0 : 1);
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    cache.deleteWhere(() => true);
    expect(pool.usedBytes).toBe(0);
  }
});

it("declines unretainable read-ahead but preserves exact demand with a tiny cache", async () => {
  const pool = new MemoryBudget(65536);
  const ranges: number[][] = [];
  const file = cachedRangeBuffer(
    {
      byteLength: 8,
      async slice(start, end = 8) {
        ranges.push([start, end]);
        return new Uint8Array(end - start).fill(7).buffer;
      },
    },
    { maxBytes: 16, coalesceBytes: 8, cacheOptions: { memoryBudget: pool } },
    "fixture",
  );
  await file.prefetch?.([{ start: 0, end: 1 }]);
  expect(ranges).toEqual([]);
  expect([...new Uint8Array(await file.slice(1, 3))]).toEqual([7, 7]);
  expect(ranges).toEqual([[1, 3]]);
  expect(pool.usedBytes).toBe(0);
});
