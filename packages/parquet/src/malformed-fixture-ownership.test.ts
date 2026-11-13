import { existsSync, readFileSync } from "node:fs";
import { MemoryBudget, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { createParquetLake } from "./index.js";

for (const path of [
  "bad_data/ARROW-GH-47662.parquet",
  "bad_data/ARROW-RS-GH-6229-DICTHEADER.parquet",
  "bad_data/ARROW-RS-GH-6229-LEVELS.parquet",
  "bad_data/PARQUET-1481.parquet",
  "data/nation.dict-malformed.parquet",
]) {
  const fixture = new URL(`../../../fixtures/external/parquet-testing/${path}`, import.meta.url);
  it.skipIf(!existsSync(fixture))(
    `releases owned query allocations after rejecting ${path}`,
    async () => {
      const store = memoryStore();
      await store.put(path, readFileSync(fixture));
      const pool = new MemoryBudget(1024 * 1024);
      const lake = createParquetLake({
        store,
        memoryPool: pool,
        scanRangeCache: { maxBytes: 0 },
        budget: { maxAllocatedMemoryBytes: 1024 * 1024 },
      });
      let failure: unknown;
      try {
        // Stream to reach the malformed page rather than hitting the separate
        // collected-output limit first on a long corrupt fixture.
        for await (const _row of lake.path(path).batchSize(3).run().rows()) {
          /* consume */
        }
      } catch (error) {
        failure = error;
      }
      if (path.endsWith("ARROW-GH-47662.parquet")) {
        expect(failure).toMatchObject({
          code: "LAKEQL_VALIDATION_ERROR",
          message: expect.stringMatching(/Invalid PLAIN binary value range/),
        });
      } else if (path.endsWith("ARROW-RS-GH-6229-DICTHEADER.parquet")) {
        expect(failure).toMatchObject({
          code: "LAKEQL_PARQUET_READ_ERROR",
          message: expect.stringMatching(/Unexpected compact type/),
        });
      } else if (path.endsWith("ARROW-RS-GH-6229-LEVELS.parquet")) {
        // The bounded level decoder detects corruption before the upstream
        // decoder reaches an out-of-range DataView read.
        expect(failure).toMatchObject({
          code: "LAKEQL_VALIDATION_ERROR",
          message: "Malformed Parquet level runs",
        });
      } else {
        expect(failure).toMatchObject({
          code: expect.stringMatching(/^LAKEQL_(PARQUET_READ_ERROR|OBJECT_NOT_FOUND)$/),
        });
        if (!path.endsWith("PARQUET-1481.parquet")) {
          expect(failure).toMatchObject({ details: { path, cause: expect.any(RangeError) } });
        }
      }
      expect(pool.usedBytes).toBe(0);
      expect(pool.peakBytes).toBeGreaterThan(0);
      expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    },
  );
}
