import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import {
  batchFromColumns,
  Lake,
  MemoryBudget,
  MemoryScope,
  memoryStore,
} from "../packages/core/dist/index.js";

const results = [];
for (const rows of [128, 1024]) {
  for (const operation of ["grouped-count", "median", "count_distinct"]) {
    for (const cap of [16, 64 * 1024 * 1024]) {
      const store = memoryStore();
      await store.put("table", new Uint8Array(1));
      const pool = new MemoryBudget(cap);
      const scanner = {
        scan() {
          throw Error("unexpected row path");
        },
        async *scanOwnedVectorBatches(_path, options) {
          for (let i = 0; i < rows; i += 2) {
            const scope = new MemoryScope(options.memoryBudget);
            try {
              const batch = batchFromColumns({ id: [i, i + 1] }, scope);
              yield scope.retainAllocations({ rowOffset: i, batch });
            } finally {
              scope.dispose();
            }
          }
        },
      };
      const lake = new Lake({ store, scanner, memoryPool: pool });
      let outputGroups;
      let rejected = false;
      try {
        if (operation === "grouped-count") {
          const output = await lake
            .path("table")
            .groupBy(["id"])
            .aggregate({ n: { op: "count" } });
          assert.equal(output.length, rows);
          for (let i = 0; i < rows; i++) assert.deepEqual(output[i], { id: i, n: 1 });
          outputGroups = output.length;
        } else {
          const output = await lake
            .path("table")
            .groupBy([])
            .aggregate({ value: { op: operation, column: "id" } });
          assert.deepEqual(output, [{ value: operation === "median" ? (rows - 1) / 2 : rows }]);
        }
      } catch (error) {
        if (cap !== 16 || error.code !== "LAKEQL_BUDGET_EXCEEDED") throw error;
        rejected = true;
      }
      assert.equal(rejected, cap === 16);
      assert.equal(pool.usedBytes, 0);
      results.push({
        operation,
        rejected,
        inputRows: rows,
        outputGroups,
        configuredPoolBytes: pool.maxBytes,
        trackedPeakBytes: pool.peakBytes,
        remainingTrackedBytes: pool.usedBytes,
      });
    }
  }
}
writeFileSync(
  "docs/benchmarks/grouped-state-admission-gap-2026-11-13.json",
  `${JSON.stringify(
    {
      description:
        "Public grouped and buffering scalar queries using an owned two-row scanner. Checks that 16-byte pools reject aggregate-state admission and sufficient pools return every expected value with complete cleanup. Applies to the columnar continuation route; legacy and late-materialized routes require separate evidence.",
      results,
    },
    null,
    2,
  )}\n`,
);
console.log(results);
