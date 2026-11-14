import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import {
  compareDecimalValues,
  decimalFromParts,
  decimalToString,
} from "../packages/core/dist/decimal-value.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

const results = [];
for (const scale of [4, 1000, Number.MAX_SAFE_INTEGER]) {
  const pool = new MemoryBudget(4096);
  const scope = new MemoryScope(pool);
  const left = decimalFromParts(123n, 3);
  const right = decimalFromParts(1n, scale);
  const samples = [];
  for (let round = 0; round < 9; round++) {
    const start = performance.now();
    for (let iteration = 0; iteration < 10000; iteration++)
      assert.equal(compareDecimalValues(left, right, scope), 1);
    if (round > 1) samples.push((performance.now() - start) / 10000);
  }
  assert.equal(pool.usedBytes, 0);
  const comparisonPeakBytes = pool.peakBytes;
  const text = decimalToString(right, scope);
  results.push({
    scale,
    text,
    comparisonPeakBytes,
    formattingRetainedBytes: pool.usedBytes,
    medianMilliseconds: [...samples].sort((a, b) => a - b)[3],
    samples,
  });
  scope.dispose();
  assert.equal(pool.usedBytes, 0);
}
assert.equal(new Set(results.map((row) => row.comparisonPeakBytes)).size, 1);
assert.equal(new Set(results.map((row) => row.formattingRetainedBytes)).size, 1);
const report = {
  node: process.version,
  description:
    "Exact-decimal kernel only, not integrated query or cloud throughput. Fixed coefficients and scales 4, 1000, MAX_SAFE_INTEGER; 2 warmup and 7 measured rounds of 10000 comparisons including assertions. Equal reservation sizes prove scale gaps do not expand modeled workspace. Native BigInt/GC/runtime metadata are not a total-heap guarantee.",
  sourceSha256: createHash("sha256")
    .update(readFileSync("packages/core/src/decimal-value.ts"))
    .digest("hex"),
  results,
};
writeFileSync(
  "docs/benchmarks/decimal-scale-workspace-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results, null, 2));
