/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { env } from "cloudflare:test";
import { MemoryBudget, SharedMemoryCache } from "lakeql-core";
import { expect, it } from "vitest";
import { type R2BucketLike, R2ObjectStore } from "../../r2/src/index.js";
import { createParquetLake } from "./index.js";

it.each([64, 128])("decodes ALP through Worker R2 with concurrent %i MiB pools", async (mib) => {
  const bindings = env as unknown as { ALP_FIXTURE: string; MEMORY_TEST_BUCKET: R2BucketLike };
  expect(bindings.ALP_FIXTURE.length).toBeGreaterThan(0);
  const store = new R2ObjectStore(bindings.MEMORY_TEST_BUCKET);
  const path = `alp-${mib}.parquet`;
  await store.put(
    path,
    Uint8Array.from(atob(bindings.ALP_FIXTURE), (char) => char.charCodeAt(0)),
  );
  const pool = new MemoryBudget(mib * 1024 * 1024);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 512 * 1024 });
  try {
    const lake = createParquetLake({
      store,
      memoryPool: pool,
      budget: { maxAllocatedMemoryBytes: 8 * 1024 * 1024 },
      scanRangeCache: { maxBytes: 512 * 1024, coalesceBytes: 0, sharedCache: cache },
    });
    for (let round = 0; round < 2; round++) {
      await Promise.all(
        [64, 128, 256].map(async (batchSize) => {
          let count = 0;
          for await (const row of lake.path(path).batchSize(batchSize).run().rows()) {
            for (const type of ["float", "double"]) {
              for (const size of [32, 1024, 4096]) {
                if (!Object.is(row[`${type}_alp_${size}`], row[`${type}_plain`]))
                  throw Error(`ALP mismatch at ${count}, ${type}, ${size}`);
              }
            }
            count++;
          }
          expect(count).toBe(9032);
        }),
      );
    }
    const iterator = lake.path(path).batchSize(1).run().rows()[Symbol.asyncIterator]();
    expect((await iterator.next()).done).toBe(false);
    await iterator.return?.();
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    cache.deleteWhere(() => true);
    expect(pool.usedBytes).toBe(0);
  } finally {
    cache.deleteWhere(() => true);
    await store.delete(path);
  }
}, 30000);
