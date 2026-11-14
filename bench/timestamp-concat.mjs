import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { vectorFromValues, vectorValue } from "../packages/core/dist/batch.js";
import { planConcat } from "../packages/core/dist/concat-plan.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";
import { concatBatches } from "../packages/core/dist/vector-sort.js";

const sourceHashes = {};
for (const file of ["batch", "concat-plan", "selected-concat", "vector-sort"])
  sourceHashes[file] = createHash("sha256")
    .update(await readFile(new URL(`../packages/core/dist/${file}.js`, import.meta.url)))
    .digest("hex");
const reports = [];
for (const rows of [1024, 16384, 131072]) {
  for (const mixed of [false, true]) {
    const batches = Array.from({ length: 8 }, (_, part) => {
      const unit = mixed && part % 2 ? "micros" : "millis";
      const values = BigInt64Array.from(
        { length: rows / 8 },
        (_, i) =>
          BigInt(i - rows / 16) * (unit === "micros" ? 1000n : 1n) + (unit === "micros" ? 1n : 0n),
      );
      return {
        rowCount: values.length,
        columns: { at: { type: "timestamp", unit, isAdjustedToUTC: true, values } },
      };
    });
    const samples = { direct: [], materialized: [], planning: [] };
    for (let trial = -2; trial < 5; trial++) {
      // Alternate execution order to reduce systematic warmup/order bias.
      for (const mode of trial % 2 ? ["direct", "materialized"] : ["materialized", "direct"]) {
        const scope = new MemoryScope(new MemoryBudget(rows * 8));
        const start = performance.now();
        let result;
        if (mode === "direct") result = concatBatches(batches, scope).columns.at;
        else
          result = vectorFromValues(
            batches.flatMap((batch) =>
              Array.from({ length: batch.rowCount }, (_, i) => vectorValue(batch.columns.at, i)),
            ),
            scope,
          );
        const milliseconds = performance.now() - start;
        assert.equal(scope.budget.peakBytes, rows * 8);
        let offset = 0;
        for (const batch of batches) {
          const factor = result.unit === batch.columns.at.unit ? 1n : 1000n;
          for (const value of batch.columns.at.values)
            assert.equal(result.values[offset++], value * factor);
        }
        scope.dispose();
        assert.equal(scope.budget.usedBytes, 0);
        if (trial >= 0) samples[mode].push(milliseconds);
      }
      const start = performance.now();
      assert.equal(planConcat(batches).bytes, rows * 8);
      if (trial >= 0) samples.planning.push(performance.now() - start);
    }
    reports.push({
      rows,
      mixed,
      trackedOutputBytes: rows * 8,
      samples,
      medianMs: Object.fromEntries(
        Object.entries(samples).map(([key, values]) => [key, [...values].sort((a, b) => a - b)[2]]),
      ),
    });
  }
}
console.log(
  JSON.stringify(
    {
      node: process.version,
      sourceHashes,
      scope:
        "Synthetic local timestamp operator benchmark; two warmups, five samples, alternating mode order. Materialized baseline uses current lossless builder, not historical truncating behavior. Every output epoch and tracked cleanup asserted. Input backing is borrowed. No remote I/O, JS transient heap, or whole Worker cap measurement.",
      reports,
    },
    null,
    2,
  ),
);
