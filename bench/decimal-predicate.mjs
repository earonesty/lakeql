import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { batchFromColumns, predicateSelection } from "../packages/core/dist/batch.js";
import { decimalFromParts } from "../packages/core/dist/decimal-value.js";
import { col, lit, lt } from "../packages/core/dist/expr.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

const results = [];
for (const rows of [100, 10000]) {
  for (const scale of [9, 1000, Number.MAX_SAFE_INTEGER]) {
    const batch = batchFromColumns({ value: Array(rows).fill(decimalFromParts(123n, scale)) });
    const expr = lt(col("value"), lit(decimalFromParts(1230n, scale - 1)));
    const samples = [];
    let peakBytes;
    let retainedBytes;
    for (let round = 0; round < 9; round++) {
      const pool = new MemoryBudget(1024 * 1024);
      const scope = new MemoryScope(pool);
      const start = performance.now();
      const mask = predicateSelection(batch, expr, scope);
      const elapsed = performance.now() - start;
      assert.ok(mask.every((value) => value === 1));
      peakBytes = pool.peakBytes;
      retainedBytes = pool.usedBytes;
      scope.dispose();
      assert.equal(pool.usedBytes, 0);
      if (round >= 2) samples.push(elapsed);
    }
    results.push({
      rows,
      scale,
      peakBytes,
      retainedBytes,
      scratchBytes: peakBytes - retainedBytes,
      medianMilliseconds: [...samples].sort((a, b) => a - b)[3],
      samples,
    });
  }
}
assert.equal(new Set(results.map((row) => row.scratchBytes)).size, 1);
const report = {
  description:
    "Exact cross-scale decimal vector predicates. Input vectors are constructed before timing and use a separate unaccounted input lifetime. Reported reservations cover predicate output and comparison scratch only. Seven measured samples after two warmups; no cloud, whole-heap or speedup claim.",
  node: process.version,
  sources: Object.fromEntries(
    [
      "packages/core/src/decimal-value.ts",
      "packages/core/src/batch.ts",
      "packages/core/src/evaluator.ts",
    ].map((path) => [path, createHash("sha256").update(readFileSync(path)).digest("hex")]),
  ),
  results,
};
writeFileSync(
  "docs/benchmarks/decimal-predicate-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results, null, 2));
