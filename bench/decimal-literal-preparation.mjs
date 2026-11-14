import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { batchFromColumns, predicateSelection } from "../packages/core/dist/batch.js";
import { compareDecimalOperands } from "../packages/core/dist/decimal-comparison.js";
import { decimalFromParts } from "../packages/core/dist/decimal-value.js";
import { lt } from "../packages/core/dist/expr.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

const results = [];
for (const [name, literal] of [
  ["fraction", 0.1],
  ["subnormal", Number.MIN_VALUE],
  ["largest finite", Number.MAX_VALUE],
]) {
  for (const rows of [100, 10000]) {
    const value = decimalFromParts(1n, 1000);
    const batch = batchFromColumns({ value: Array(rows).fill(value) });
    const expr = lt("value", literal);
    for (const mode of ["scalar conversion loop", "prepared vector literal"]) {
      const samples = [];
      let peakBytes;
      let retainedBytes;
      for (let round = 0; round < 9; round++) {
        const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
        const start = performance.now();
        let mask;
        if (mode === "prepared vector literal") mask = predicateSelection(batch, expr, scope);
        else {
          mask = scope.allocate(
            rows,
            "reference predicate output",
            () => new Uint8Array(rows),
          ).value;
          for (let i = 0; i < rows; i++)
            mask[i] = compareDecimalOperands(value, literal, scope) < 0 ? 1 : 0;
        }
        const elapsed = performance.now() - start;
        assert.ok(mask.every((entry) => entry === 1));
        peakBytes = scope.budget.peakBytes;
        retainedBytes = scope.budget.usedBytes;
        scope.dispose();
        assert.equal(scope.budget.usedBytes, 0);
        if (round >= 2) samples.push(elapsed);
      }
      results.push({
        name,
        rows,
        mode,
        peakBytes,
        retainedBytes,
        scratchBytes: peakBytes - retainedBytes,
        medianMilliseconds: [...samples].sort((a, b) => a - b)[3],
        samples,
      });
    }
  }
}
const report = {
  description:
    "Identical decimal-less-than-number results, comparing a scoped scalar conversion loop against prepared vector literals. Inputs excluded from timing and output reservations. Both modes admit output and comparison storage. Two warmups, seven measured samples. This is a local controlled comparison, not an archived before/after revision, cloud throughput or VM-heap measurement.",
  node: process.version,
  sources: Object.fromEntries(
    [
      "packages/core/src/decimal-comparison.ts",
      "packages/core/src/decimal-value.ts",
      "packages/core/src/batch.ts",
    ].map((path) => [path, createHash("sha256").update(readFileSync(path)).digest("hex")]),
  ),
  results,
};
writeFileSync(
  "docs/benchmarks/decimal-literal-preparation-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results, null, 2));
