import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { compareDecimalOperands } from "../packages/core/dist/decimal-comparison.js";
import { decimalFromParts } from "../packages/core/dist/decimal-value.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

const results = [];
for (const [name, value, expected] of [
  ["safe integer", 0, 1],
  ["fraction", 0.1, -1],
  ["subnormal", Number.MIN_VALUE, 1],
  ["largest finite", Number.MAX_VALUE, -1],
  ["bigint", 1n, -1],
]) {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const decimal = decimalFromParts(1n, 1);
  const samples = [];
  for (let round = 0; round < 9; round++) {
    const start = performance.now();
    for (let i = 0; i < 10000; i++)
      assert.equal(compareDecimalOperands(decimal, value, scope), expected);
    const elapsed = performance.now() - start;
    assert.equal(pool.usedBytes, 0);
    if (round >= 2) samples.push(elapsed);
  }
  results.push({
    name,
    comparisons: 10000,
    peakReservedBytes: pool.peakBytes,
    medianMilliseconds: [...samples].sort((a, b) => a - b)[3],
    samples,
  });
  scope.dispose();
}
const report = {
  description:
    "Mixed decimal/binary64/bigint scalar kernel, 10000 comparisons per sample including assertions. Two warmups and seven measured rounds. Input scalars excluded from scope. Tracks explicit workspace, not VM heap, native BigInt scratch, or cloud throughput. No speedup claim.",
  node: process.version,
  sources: Object.fromEntries(
    ["packages/core/src/decimal-comparison.ts", "packages/core/src/decimal-value.ts"].map(
      (path) => [path, createHash("sha256").update(readFileSync(path)).digest("hex")],
    ),
  ),
  results,
};
writeFileSync(
  "docs/benchmarks/decimal-numeric-comparison-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results, null, 2));
