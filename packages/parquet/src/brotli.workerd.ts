/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { env } from "cloudflare:test";
import { MemoryBudget, SharedMemoryCache } from "lakeql-core";
import { expect, it } from "vitest";
import { type R2BucketLike, R2ObjectStore } from "../../r2/src/index.js";
import { createParquetLake, readParquetObjects } from "./index.js";

it.each([64, 128])("decodes BROTLI through concurrent Worker R2 queries at %i MiB", async (mib) => {
  const bindings = env as unknown as { BROTLI_FIXTURE: string; MEMORY_TEST_BUCKET: R2BucketLike };
  expect(bindings.BROTLI_FIXTURE.length).toBeGreaterThan(0);
  const store = new R2ObjectStore(bindings.MEMORY_TEST_BUCKET);
  const path = `brotli-${mib}.parquet`;
  await store.put(
    path,
    Uint8Array.from(atob(bindings.BROTLI_FIXTURE), (c) => c.charCodeAt(0)),
  );
  const pool = new MemoryBudget(mib * 1024 * 1024);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 65536 });
  try {
    const expected = Array.from({ length: 513 }, (_, id) => ({
      id,
      text: id % 7 === 0 ? null : `dictionary text ${id % 19} 猫`,
    }));
    expect(await readParquetObjects(store, path)).toEqual(expected);
    const lake = createParquetLake({
      store,
      memoryPool: pool,
      budget: { maxAllocatedMemoryBytes: 1024 * 1024 },
      scanRangeCache: { maxBytes: 65536, coalesceBytes: 0, sharedCache: cache },
    });
    for (let round = 0; round < 2; round++) {
      const outputs = await Promise.all(
        [1, 64, 128, 256].map((batchSize) =>
          lake.path(path).batchSize(batchSize).run().toOwnedArray(),
        ),
      );
      try {
        for (const output of outputs) expect(output.value).toEqual(expected);
      } finally {
        for (const output of outputs) output.release();
      }
    }
    expect(pool.usedBytes).toBeGreaterThan(0);
    const iterator = lake.path(path).batchSize(1).run().rows()[Symbol.asyncIterator]();
    expect((await iterator.next()).value).toEqual(expected[0]);
    await iterator.return?.();
    cache.deleteWhere(() => true);
    expect(pool.usedBytes).toBe(0);
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
  } finally {
    cache.deleteWhere(() => true);
    await store.delete(path);
  }
});
