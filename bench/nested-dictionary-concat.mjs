import assert from "node:assert/strict";
import { planConcat } from "../packages/core/dist/concat-plan.js";
import {
  MemoryBudget,
  MemoryScope,
  vectorFromValues,
  vectorValue,
} from "../packages/core/dist/index.js";
import { concatBatches } from "../packages/core/dist/vector-sort.js";

for (const rows of [1000, 10000]) {
  const dictionaries = [0, 1].map(() => ({
    type: "list",
    offsets: Int32Array.from({ length: 32 + 1 }, (_, i) => i),
    child: {
      type: "map",
      offsets: Int32Array.from({ length: 32 + 1 }, (_, i) => i),
      keys: {
        type: "utf8-view",
        data: new Uint8Array(32).fill(107),
        fixedLength: 1,
        length: 32,
      },
      values: { type: "i32", values: new Int32Array(32).fill(7) },
    },
  }));
  const vectors = dictionaries.map((dictionary) => ({
    type: "dict",
    indices: Uint32Array.from({ length: rows }, (_, i) => i % 32),
    dictionary,
  }));
  for (const implementation of ["row-fallback-emulation", "typed-selection", "planned-selection"]) {
    const samples = [];
    let peakTrackedBytes;
    for (let trial = 0; trial < 8; trial++) {
      const budget = new MemoryBudget(64 * 1024 * 1024);
      const scope = new MemoryScope(budget);
      try {
        const start = performance.now();
        const batches = vectors.map((x) => ({ rowCount: rows, columns: { x } }));
        const plan = implementation === "planned-selection" ? planConcat(batches) : undefined;
        if (implementation === "planned-selection") {
          assert.ok(plan);
          scope.admit(plan.bytes, "recursive selected output");
        }
        const output =
          implementation !== "row-fallback-emulation"
            ? concatBatches(batches, scope, plan).columns.x
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
          "Local borrowed-input dictionary/list/map microbenchmark; uniform keys ensure legacy logical equivalence; JS objects excluded from tracked bytes; not cloud or heap proof",
      }),
    );
  }
}
