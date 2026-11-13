// Focused same-runtime allocation comparison; excludes decoder and I/O work.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

const url = process.env.LAKEQL_BUDGET_MODULE
  ? pathToFileURL(process.env.LAKEQL_BUDGET_MODULE)
  : new URL("../packages/core/dist/memory-budget.js", import.meta.url);
const { MemoryBudget } = await import(url);
const hash = createHash("sha256")
  .update(await readFile(url))
  .digest("hex");
for (const rows of [1, 128]) {
  const samples = [];
  for (let trial = 0; trial < 8; trial++) {
    const budget = new MemoryBudget(rows * 13);
    const start = performance.now();
    for (let stage = 0; stage < 10000; stage++) {
      const scope = new MemoryScope(budget);
      scope.admit(rows * 13, "stage output");
      scope.allocate(rows * 8, "values", () => new Float64Array(rows));
      scope.allocate(rows * 4, "indices", () => new Uint32Array(rows));
      scope.allocate(rows, "validity", () => new Uint8Array(rows));
      scope.dispose();
      assert.equal(budget.usedBytes, 0);
    }
    if (trial > 2) samples.push(performance.now() - start);
    assert.equal(budget.peakBytes, rows * 13);
  }
  console.log(
    JSON.stringify({
      rows,
      stages: 10000,
      medianMs: samples.sort((a, b) => a - b)[2],
      samples: 5,
      budgetSha256: hash,
      node: process.version,
      scope:
        "three output buffers per admitted scope; current runtime microbenchmark; excludes I/O, decoded inputs, heap-cap proof",
    }),
  );
}
