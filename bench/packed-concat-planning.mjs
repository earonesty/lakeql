import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { concatAllocationBytes } from "../packages/core/dist/concat-plan.js";
import { MemoryBudget, MemoryScope, vectorValue } from "../packages/core/dist/index.js";
import { concatBatches, concatOwnedBatches } from "../packages/core/dist/vector-sort.js";

const sourceSha256 = createHash("sha256")
  .update(await readFile("packages/core/dist/packed-gather.js"))
  .digest("hex");
for (const rowCount of [1000, 10000, 100000]) {
  for (const cardinality of [4, 1024]) {
    const batches = [0, 1].map((part) => ({
      rowCount: rowCount / 2,
      columns: {
        x: {
          type: "dict",
          indices: Uint32Array.from({ length: rowCount / 2 }, (_, i) => i % cardinality),
          dictionary: {
            type: "utf8",
            values: Array.from({ length: cardinality }, (_, i) => `${part}:猫:${i}`),
          },
        },
      },
    }));
    const expected = concatAllocationBytes(batches);
    assert.equal(typeof expected, "number");
    const medians = {};
    let peakBytes;
    for (const mode of ["planning", "incrementalExecution", "atomicExecution"]) {
      const samples = [];
      for (let trial = 0; trial < 8; trial++) {
        const budget = new MemoryBudget(expected);
        const scope = new MemoryScope(budget);
        const inputs = batches.map((batch) => scope.retainAllocations(batch));
        const start = performance.now();
        let output;
        let owner;
        if (mode === "planning") assert.equal(concatAllocationBytes(batches), expected);
        else if (mode === "incrementalExecution") output = concatBatches(batches, scope);
        else {
          owner = concatOwnedBatches(inputs);
          output = owner.value;
        }
        const elapsed = performance.now() - start;
        if (output) {
          for (const row of [0, rowCount / 2 - 1, rowCount / 2, rowCount - 1]) {
            const part = Math.floor(row / (rowCount / 2));
            assert.equal(
              vectorValue(output.columns.x, row),
              `${part}:猫:${(row % (rowCount / 2)) % cardinality}`,
            );
          }
          peakBytes = budget.peakBytes;
          assert.equal(peakBytes, expected);
        }
        owner?.release();
        for (const input of inputs) input.release();
        scope.dispose();
        assert.equal(budget.usedBytes, 0);
        if (trial >= 3) samples.push(elapsed);
      }
      samples.sort((a, b) => a - b);
      medians[mode] = samples[2];
    }
    console.log(
      JSON.stringify({
        rowCount,
        cardinality,
        expected,
        peakBytes,
        medianMs: medians,
        sourceSha256,
        scope:
          "Local packed concat; borrowed input payload; exact output/scratch cap; excludes cloud, decoding and whole heap",
      }),
    );
  }
}
