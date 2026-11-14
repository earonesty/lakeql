/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { env } from "cloudflare:test";
import { MemoryBudget, MemoryScope, type SpillAdapter, type SpillRef } from "lakeql-core";
import { expect, it } from "vitest";
import { type R2BucketLike, R2ObjectStore } from "../../r2/src/index.js";
import { createParquetLake, writeParquet } from "./index.js";

it.each([64, 128])("resumes real Parquet checkpoints from R2 with a %i MiB pool", async (mib) => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const path = `spill-continuation-${mib}.parquet`;
  const prefix = `spill-continuation-${mib}/`;
  const pool = new MemoryBudget(mib * 1024 * 1024);
  const entries = new Map<string, number>();
  let ownedReads = 0;
  const idOf = (ref: SpillRef | string) => (typeof ref === "string" ? ref : ref.id);
  // No stageWrite/beginWriteBatch: deliberately exercise the legacy publication
  // contract against an actual binding, without claiming multi-object atomicity.
  const spill: SpillAdapter = {
    async write(id, data) {
      expect(pool.usedBytes).toBeGreaterThanOrEqual(data.byteLength);
      await store.put(prefix + id, data);
      entries.set(id, data.byteLength);
      return { id, byteSize: data.byteLength };
    },
    async read() {
      throw new Error("Unowned spill read used");
    },
    async readOwned(ref, budget) {
      ownedReads++;
      const scope = new MemoryScope(budget);
      try {
        const path = prefix + idOf(ref);
        const head = await store.headOwned(path, scope);
        if (head === null) throw new Error("Missing spill fixture");
        const size = head.value.size;
        head.release();
        const owner = await store.getRangeOwned(path, { offset: 0, length: size }, scope);
        try {
          return owner.retain();
        } finally {
          owner.release();
        }
      } finally {
        scope.dispose();
      }
    },
    async delete(ref) {
      const id = idOf(ref);
      await store.delete(prefix + id);
      entries.delete(id);
    },
    async usage() {
      return { entries: entries.size, byteSize: [...entries.values()].reduce((a, b) => a + b, 0) };
    },
  };
  await writeParquet(store, path, {
    codec: "SNAPPY",
    rowGroupSize: [2, 2],
    columnData: [
      { name: "key", type: "INT32", data: [1, 2, 1, 2] },
      { name: "value", type: "INT32", data: [2, 3, 4, 5] },
    ],
  });
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  try {
    const spec = { total: { op: "sum" as const, column: "value" } };
    await lake.path(path).run().aggregateWithState(["key"], spec, { spill, spillId: "aggregate" });
    expect(pool.usedBytes).toBe(0);
    const aggregate = await lake
      .path(path)
      .run()
      .aggregateWithState(["key"], spec, {
        spill,
        operatorState: { spillRef: "aggregate" },
        maxGroups: 2,
      });
    expect(aggregate.rows).toEqual([
      { key: 1, total: 12 },
      { key: 2, total: 16 },
    ]);
    expect(pool.usedBytes).toBe(0);
    await lake
      .path(path)
      .orderBy([{ column: "value" }])
      .sortWithState({ spill, spillId: "sort" });
    expect(pool.usedBytes).toBe(0);
    const sorted = await lake
      .path(path)
      .orderBy([{ column: "value" }])
      .sortWithState({ spill, operatorState: { spillRef: "sort" } });
    expect(sorted.rows.map((row) => row.value)).toEqual([2, 2, 3, 3, 4, 4, 5, 5]);
    expect(ownedReads).toBeGreaterThanOrEqual(3);
    expect(pool.usedBytes).toBe(0);
    await store.put(`${prefix}sort-run-000000`, new TextEncoder().encode("[]"));
    await expect(
      lake
        .path(path)
        .orderBy([{ column: "value" }])
        .sortWithState({ spill, operatorState: { spillRef: "sort" } }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });
    expect(pool.usedBytes).toBe(0);
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
  } finally {
    await store.delete(path);
    for (const id of entries.keys()) await store.delete(prefix + id);
  }
});
