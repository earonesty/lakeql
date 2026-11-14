import assert from "node:assert/strict";
import {
  batchFromColumns,
  isIn,
  MemoryBudget,
  MemoryScope,
  predicateSelection,
} from "../packages/core/dist/index.js";

for (const rows of [10000, 100000]) {
  for (const candidates of [8, 128, 1024]) {
    const input = Array.from({ length: rows }, (_, i) =>
      i % 17 === 0 ? null : i % (candidates * 2),
    );
    const literals = Array.from({ length: candidates }, (_, i) => i);
    const batch = batchFromColumns({ value: input });
    const expression = isIn("value", [...literals, null]);
    const expected = Uint8Array.from(input, (value) =>
      value === null ? 2 : value < candidates ? 1 : 2,
    );
    for (const mode of ["no-scratch", "headroom"]) {
      const samples = [];
      let peakBytes;
      for (let trial = 0; trial < 8; trial++) {
        const budget = new MemoryBudget(rows + (mode === "headroom" ? candidates * 8 : 0));
        const scope = new MemoryScope(budget);
        const start = performance.now();
        const mask = predicateSelection(batch, expression, scope);
        const elapsed = performance.now() - start;
        assert.deepEqual(mask, expected);
        assert.equal(budget.usedBytes, rows);
        peakBytes = budget.peakBytes;
        scope.dispose();
        assert.equal(budget.usedBytes, 0);
        if (trial >= 3) samples.push(elapsed);
      }
      samples.sort((a, b) => a - b);
      console.log(
        JSON.stringify({
          rows,
          candidates,
          mode,
          medianMs: samples[2],
          peakBytes,
          samples: samples.length,
          scope:
            "production numeric IN kernel; borrowed inputs; typed mask/sorted lookup only; excludes JS heap and I/O",
        }),
      );
    }
  }
}
