/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { env } from "cloudflare:test";
import { MemoryBudget, materializeBatchRows } from "lakeql-core";
import { expect, it } from "vitest";
import { type R2BucketLike, R2ObjectStore } from "../../r2/src/index.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it("measures ordinary R2 numeric scans against shared pinned headroom", async () => {
  const bindings = env as unknown as {
    NUMERIC_PRESSURE_FIXTURE: string;
    MEMORY_TEST_BUCKET: R2BucketLike;
  };
  const bytes = Uint8Array.from(atob(bindings.NUMERIC_PRESSURE_FIXTURE), (c) => c.charCodeAt(0));
  const store = new R2ObjectStore(bindings.MEMORY_TEST_BUCKET);
  const path = "numeric-shared-pressure.parquet";
  await store.put(path, bytes);
  const file = await asyncBufferFromStore(store, path);
  const metadata = await readParquetMetadataFromFile(file);
  try {
    for (const headroom of [8192, 1]) {
      const pool = new MemoryBudget(1024 * 1024 + headroom);
      const pinned = pool.reserve(1024 * 1024, "other job retained output");
      const job = pool.child(1024 * 1024);
      const read = async () => {
        const rows = [];
        for await (const output of readOwnedParquetVectorBatches(file, metadata, job, {
          columns: ["a", "b"],
          batchSize: 1,
          canStopEarly: true,
        })) {
          rows.push(...materializeBatchRows(output.value.batch));
          output.release();
        }
        return rows;
      };
      try {
        if (headroom === 8192) await expect(read()).resolves.toEqual([{ a: 0, b: null }]);
        else await expect(read()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
        expect(job.usedBytes).toBe(0);
        expect(pool.usedBytes).toBe(pinned.bytes);
      } finally {
        pinned.release();
      }
      expect(pool.usedBytes).toBe(0);
    }
  } finally {
    await store.delete(path);
  }
});
