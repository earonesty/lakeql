/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { env } from "cloudflare:test";
import { parquetMetadata } from "hyparquet";
import {
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  type OwnedMemory,
  SharedMemoryCache,
} from "lakeql-core";
import { expect, it, vi } from "vitest";
import { type R2BucketLike, R2ObjectStore } from "../../r2/src/index.js";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { nestedListFile } from "./nested-list-worker-fixture.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import type { ParquetColumnBatch } from "./types.js";

const expected = [
  {
    a: [
      [["a", "b"], ["c"]],
      [null, ["d"]],
    ],
    b: 1,
  },
  {
    a: [
      [
        ["a", "b"],
        ["c", "d"],
      ],
      [null, ["e"]],
    ],
    b: 1,
  },
  {
    a: [
      [["a", "b"], ["c", "d"], ["e"]],
      [null, ["f"]],
    ],
    b: 1,
  },
];

for (const mib of [64, 128])
  it(`reads nested R2 row groups with concurrent cached jobs (${mib} MiB pool)`, async () => {
    const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
    const store = new R2ObjectStore(bucket);
    const path = `nested-row-group-${mib}.parquet`;
    await store.put(path, nestedListFile);
    const file = await asyncBufferFromStore(store, path);
    const metadata = parquetMetadata(nestedListFile.slice().buffer);
    const pool = new MemoryBudget(mib * 1024 * 1024);
    // Model other admitted jobs/cache consuming the pool; this is a reservation,
    // not a claim that the fixture exercises a whole-isolate heap limit.
    const occupied = pool.reserve((mib - 1) * 1024 * 1024, "other admitted work");
    const shared = new SharedMemoryCache({ maxBytes: 256 * 1024, memoryBudget: pool });
    const cache = new DecodedColumnCache(shared, { maxBytes: 256 * 1024, policy: "latency" });
    const cachedPages = vi.spyOn(cache, "getValue");
    const retained: OwnedMemory<ParquetColumnBatch>[] = [];
    const jobs = Array.from({ length: 4 }, () => pool.child(128 * 1024));
    try {
      for (const warm of [false, true]) {
        cachedPages.mockClear();
        await Promise.all(
          jobs.map(async (budget) => {
            const scope = new MemoryScope(budget);
            const rows: unknown[] = [];
            const offsets: number[] = [];
            try {
              for await (const owner of readOwnedParquetVectorBatches(
                file,
                metadata,
                scope.budget,
                {
                  batchSize: 2,
                  maxConcurrentReads: 2,
                  decodedColumnCache: cache,
                  decodedColumnCacheKey: path,
                },
              )) {
                retained.push(owner);
                offsets.push(owner.value.rowOffset);
                rows.push(...materializeBatchRows(owner.value.batch));
              }
              expect(rows).toEqual(expected);
              expect(offsets).toEqual([0, 2]);
            } finally {
              scope.dispose();
            }
            expect(budget.peakBytes).toBeLessThanOrEqual(128 * 1024);
          }),
        );
        if (warm) {
          expect(cachedPages.mock.results.length).toBeGreaterThan(0);
          expect(
            cachedPages.mock.results.every(
              (result) => result.type === "return" && result.value !== undefined,
            ),
          ).toBe(true);
        }
      }
      shared.deleteWhere(() => true);
      expect(retained.flatMap((owner) => materializeBatchRows(owner.value.batch))).toHaveLength(24);
      for (const owner of retained) {
        expect(materializeBatchRows(owner.value.batch)).toEqual(
          expected.slice(owner.value.rowOffset, owner.value.rowOffset + owner.value.batch.rowCount),
        );
      }
    } finally {
      for (const owner of retained) owner.release();
      shared.deleteWhere(() => true);
      occupied.release();
      await store.delete(path);
    }
    expect(jobs.map((budget) => budget.usedBytes)).toEqual([0, 0, 0, 0]);
    expect(pool.usedBytes).toBe(0);
    expect(pool.peakBytes).toBeLessThanOrEqual(mib * 1024 * 1024);
  });
