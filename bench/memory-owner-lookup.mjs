import assert from "node:assert/strict";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";

for (const count of [1000, 10000]) {
  for (const mode of ["linear-lookup", "indexed-lookup"]) {
    const samples = [];
    for (let trial = 0; trial < 8; trial++) {
      const budget = new MemoryBudget(count);
      const scope = new MemoryScope(budget);
      const entries = new Map();
      const values = Array.from({ length: count }, () => new Uint8Array(1));
      for (const value of values) {
        const owner = scope.allocate(1, "lookup benchmark", () => value);
        entries.set(owner.release, owner);
      }
      const started = performance.now();
      for (let index = count - 1; index >= 0; index--) {
        const value = values[index];
        if (mode === "indexed-lookup") scope.releaseAllocation(value);
        else {
          for (const [release, owner] of entries) {
            if (owner.value !== value) continue;
            entries.delete(release);
            release();
            break;
          }
        }
      }
      const elapsed = performance.now() - started;
      assert.equal(budget.usedBytes, 0);
      scope.dispose();
      if (trial >= 3) samples.push(elapsed);
    }
    samples.sort((a, b) => a - b);
    console.log(
      JSON.stringify({
        count,
        mode,
        medianMs: samples[2],
        samples: samples.length,
        scope:
          "reverse exact-release lookup kernel; linear scan emulates previous search using current owners; excludes allocation/index construction and JS bookkeeping memory",
      }),
    );
  }
}
