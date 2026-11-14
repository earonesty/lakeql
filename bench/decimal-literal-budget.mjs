import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { batchFromColumns, predicateSelection } from "../packages/core/dist/batch.js";
import { compareDecimalOperands } from "../packages/core/dist/decimal-comparison.js";
import { decimalFromParts } from "../packages/core/dist/decimal-value.js";
import { lt } from "../packages/core/dist/expr.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

const rows = 10000;
const value = decimalFromParts(1n, 1000);
const batch = batchFromColumns({ value: Array(rows).fill(value) });
const results = [];
for (const [name, literal] of [
  ["fraction", 0.1],
  ["subnormal", Number.MIN_VALUE],
]) {
  const expr = lt("value", literal);
  const scalar = new MemoryScope(new MemoryBudget(65536));
  compareDecimalOperands(value, literal, scalar);
  const scalarLimit = rows + scalar.budget.peakBytes;
  scalar.dispose();
  const probe = new MemoryScope(new MemoryBudget(65536));
  predicateSelection(batch, expr, probe);
  const preparedLimit = probe.budget.peakBytes;
  probe.dispose();
  for (const mib of [64, 128]) {
    for (const [mode, availableBytes] of [
      ["prepared", preparedLimit],
      ["scalar fallback", scalarLimit],
    ]) {
      const samples = [];
      let peakBytes;
      for (let round = 0; round < 9; round++) {
        const pool = new MemoryBudget(mib * 1024 * 1024);
        const retained = pool.reserve(pool.maxBytes - availableBytes, "retained peer reservation");
        const scope = new MemoryScope(pool);
        const start = performance.now();
        const mask = predicateSelection(batch, expr, scope);
        const elapsed = performance.now() - start;
        assert.ok(mask.every((entry) => entry === 1));
        assert.equal(pool.usedBytes, pool.maxBytes - availableBytes + rows);
        peakBytes = pool.peakBytes;
        assert.ok(peakBytes <= pool.maxBytes);
        scope.dispose();
        assert.equal(pool.usedBytes, pool.maxBytes - availableBytes);
        retained.release();
        assert.equal(pool.usedBytes, 0);
        if (round >= 2) samples.push(elapsed);
      }
      results.push({
        name,
        rows,
        poolMiB: mib,
        mode,
        availableBytes,
        peakBytes,
        medianMilliseconds: [...samples].sort((a, b) => a - b)[3],
        samples,
      });
    }
  }
}
const report = {
  description:
    "Prepared decimal predicates dynamically fall back when peer reservations leave only the scalar path's required headroom. 64/128 MiB accounting pools; peer reservation is synthetic and does not allocate a matching heap payload. Input vectors excluded. Two warmups, seven timed rounds; no cloud or total-heap claim.",
  node: process.version,
  sources: Object.fromEntries(
    [
      "packages/core/src/batch.ts",
      "packages/core/src/decimal-comparison.ts",
      "packages/core/src/decimal-value.ts",
    ].map((path) => [path, createHash("sha256").update(readFileSync(path)).digest("hex")]),
  ),
  results,
};
writeFileSync(
  "docs/benchmarks/decimal-literal-budget-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results, null, 2));
