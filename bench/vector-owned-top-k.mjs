import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import {
  batchFromColumns,
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  vectorTopKBatch,
  vectorTopKOwnedBatch,
} from "../packages/core/dist/index.js";

function measure(run) {
  for (let i = 0; i < 3; i++) run();
  const times = [];
  for (let i = 0; i < 7; i++) {
    const start = performance.now();
    run();
    times.push(performance.now() - start);
  }
  return times.sort((a, b) => a - b)[3];
}
for (const rows of [10000, 100000]) {
  for (const distribution of ["ascending", "descending", "mixed"]) {
    for (const limit of [10, 1000]) {
      const budget = new MemoryBudget(64 * 1024 * 1024);
      const scope = new MemoryScope(budget);
      const batch = batchFromColumns(
        {
          id: Array.from({ length: rows }, (_, i) => i),
          score: Array.from({ length: rows }, (_, i) =>
            distribution === "ascending"
              ? i
              : distribution === "descending"
                ? rows - i
                : (i * 7919) % rows,
          ),
        },
        scope,
      );
      const input = scope.retainAllocations(batch);
      scope.dispose();
      const baseline = () => vectorTopKBatch(batch, [{ column: "score" }], { limit });
      const owned = () => vectorTopKOwnedBatch(input, [{ column: "score" }], { limit });
      const expected = materializeBatchRows(baseline());
      const checked = owned();
      assert.deepEqual(materializeBatchRows(checked.value), expected);
      checked.release();
      const borrowedMs = measure(baseline);
      const ownedMs = measure(() => {
        const result = owned();
        result.release();
      });
      assert.equal(budget.usedBytes, input.bytes);
      console.log(
        JSON.stringify({
          rows,
          distribution,
          limit,
          borrowedMs,
          ownedMs,
          inputBytes: input.bytes,
          peakIncrementalBytes: budget.peakBytes - input.bytes,
        }),
      );
      input.release();
      assert.equal(budget.usedBytes, 0);
    }
  }
}
