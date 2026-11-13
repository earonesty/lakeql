import assert from "node:assert/strict";
import {
  MemoryBudget,
  MemoryScope,
  vectorFromValues,
  vectorValue,
} from "../packages/core/dist/index.js";
import { concatBatches } from "../packages/core/dist/vector-sort.js";

for (const rows of [1000, 10000]) {
  const vectors = [0, 1].map(() => ({
    type: "list",
    offsets: Int32Array.from({ length: rows + 1 }, (_, i) => i),
    child: {
      type: "map",
      offsets: Int32Array.from({ length: rows + 1 }, (_, i) => i),
      keys: {
        type: "utf8-view",
        data: new Uint8Array(rows).fill(107),
        fixedLength: 1,
        length: rows,
      },
      values: { type: "i32", values: new Int32Array(rows).fill(7) },
    },
  }));
  for (const implementation of ["row-fallback-emulation", "typed-children"]) {
    const samples = [];
    let peakTrackedBytes;
    for (let trial = 0; trial < 8; trial++) {
      const budget = new MemoryBudget(64 * 1024 * 1024);
      const scope = new MemoryScope(budget);
      try {
        const start = performance.now();
        const output =
          implementation === "typed-children"
            ? concatBatches(
                vectors.map((x) => ({ rowCount: rows, columns: { x } })),
                scope,
              ).columns.x
            : vectorFromValues(
                vectors.flatMap((v) => Array.from({ length: rows }, (_, i) => vectorValue(v, i))),
                scope,
              );
        const elapsed = performance.now() - start;
        for (let i = 0; i < rows * 2; i++) assert.deepEqual(vectorValue(output, i), [{ k: 7 }]);
        peakTrackedBytes = budget.peakBytes;
        if (trial >= 3) samples.push(elapsed);
      } finally {
        scope.dispose();
      }
      assert.equal(budget.usedBytes, 0);
    }
    samples.sort((a, b) => a - b);
    console.log(
      JSON.stringify({
        rows: rows * 2,
        implementation,
        medianMs: samples[2],
        peakTrackedBytes,
        node: process.version,
        scope:
          "Local borrowed-input list/map microbenchmark; uniform keys ensure legacy logical equivalence; JS objects excluded from tracked bytes; not cloud or heap proof",
      }),
    );
  }
}
