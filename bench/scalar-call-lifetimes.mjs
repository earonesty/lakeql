import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { batchExprValues } from "../packages/core/dist/batch.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

// Isolate the eager COALESCE loop; both paths use the same current scalar readers.
// This is a Node scalar microbenchmark, not cloud latency or total runtime memory.
const rows = 4096;
const batch = {
  rowCount: rows,
  columns: {
    number: { type: "f64", values: new Float64Array(rows).fill(7) },
    text: {
      type: "utf8-view",
      data: new TextEncoder().encode("ß".repeat(rows)),
      fixedLength: 2,
      length: rows,
    },
  },
};
function run(kind, count, mode) {
  const expression =
    kind === "string"
      ? { kind: "call", fn: "upper", args: [{ kind: "column", name: "text" }] }
      : kind === "column"
        ? { kind: "column", name: "number" }
        : { kind: "literal", value: 7 };
  const args = Array(count).fill(expression);
  const readers = args.map((arg) => batchExprValues(batch, arg));
  const compiled = batchExprValues(batch, { kind: "call", fn: "coalesce", args });
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const start = performance.now();
  for (let index = 0; index < rows; index++) {
    const scope = new MemoryScope(pool);
    try {
      let result = null;
      if (mode === "scoped") result = compiled.valueAt(index, scope);
      else
        for (const reader of readers) {
          const value = reader.valueAt(index, scope);
          if (result === null && value !== null) result = value;
        }
      assert.equal(result, kind === "string" ? "SS" : 7);
    } finally {
      scope.dispose();
    }
  }
  assert.equal(pool.usedBytes, 0);
  return { milliseconds: performance.now() - start, peakAdmittedBytes: pool.peakBytes };
}
const results = [];
for (const kind of ["number", "column", "string"])
  for (const count of [2, 32]) {
    for (const mode of ["retained", "scoped"]) run(kind, count, mode);
    const samples = { retained: [], scoped: [] };
    // Alternate order to reduce systematic warmup/order bias.
    for (let round = 0; round < 5; round++)
      for (const mode of round % 2 ? ["scoped", "retained"] : ["retained", "scoped"])
        samples[mode].push(run(kind, count, mode));
    for (const mode of ["retained", "scoped"]) {
      const values = samples[mode];
      results.push({
        kind,
        count,
        mode,
        rows,
        medianMilliseconds: values.map((v) => v.milliseconds).sort((a, b) => a - b)[2],
        peakAdmittedBytes: values[0].peakAdmittedBytes,
        samples: values,
      });
    }
  }
const report = {
  node: process.version,
  description: "Eager COALESCE per-row scratch; current readers with retained versus scoped loop",
  results,
};
writeFileSync(
  "docs/benchmarks/scalar-call-lifetimes-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(
  JSON.stringify(
    results.map(({ samples, ...result }) => result),
    null,
    2,
  ),
);
