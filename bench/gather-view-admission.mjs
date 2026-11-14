// Local gather allocation scaling; borrowed fixture storage is excluded.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { MATERIALIZATION_CHARGES as c } from "../packages/core/dist/allocation-charges.js";
import { vectorValue } from "../packages/core/dist/batch.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";
import { gatherBatch } from "../packages/core/dist/vector-sort.js";

const cases = [];
for (const shape of ["shared-text", "numeric"]) {
  for (const columns of [1, 8, 32]) {
    for (const selectedRows of [0, 1, 1000]) {
      const values = Array.from({ length: 10000 }, (_, i) => i);
      const vector =
        shape === "numeric"
          ? { type: "f64", values: Float64Array.from(values) }
          : { type: "utf8", values: values.map((i) => `row-${i}-猫`) };
      const batch = {
        rowCount: values.length,
        columns: Object.fromEntries(Array.from({ length: columns }, (_, i) => [`c${i}`, vector])),
      };
      const indices = Uint32Array.from(
        { length: selectedRows },
        (_, i) => (i * 13) % values.length,
      );
      const expectedBytes =
        shape === "numeric" ? columns * (c.view + 8 * selectedRows) : c.view + 4 * selectedRows;
      const samples = [];
      for (let sample = 0; sample < 8; sample++) {
        const budget = new MemoryBudget(expectedBytes);
        const scope = new MemoryScope(budget);
        const start = performance.now();
        const output = gatherBatch(batch, indices, scope);
        const elapsedMs = performance.now() - start;
        assert.equal(budget.usedBytes, expectedBytes);
        assert.equal(budget.peakBytes, expectedBytes);
        for (const column of Object.values(output.columns)) {
          for (let row = 0; row < selectedRows; row++)
            assert.deepEqual(
              vectorValue(column, row),
              shape === "numeric" ? indices[row] : `row-${indices[row]}-猫`,
            );
        }
        const owner = scope.retainAllocations(output);
        scope.dispose();
        assert.equal(budget.usedBytes, expectedBytes);
        owner.release();
        assert.equal(budget.usedBytes, 0);
        if (sample >= 2) samples.push(elapsedMs);
      }
      const tight = new MemoryScope(new MemoryBudget(expectedBytes - 1));
      assert.throws(() => gatherBatch(batch, indices, tight), { code: "LAKEQL_BUDGET_EXCEEDED" });
      assert.equal(tight.budget.peakBytes, 0);
      tight.dispose();
      samples.sort((a, b) => a - b);
      cases.push({
        shape,
        columns,
        selectedRows,
        peakBytes: expectedBytes,
        retainedBytes: expectedBytes,
        afterReleaseBytes: 0,
        capMinusOnePeakBytes: 0,
        medianMs: (samples[2] + samples[3]) / 2,
      });
    }
  }
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local output construction; source storage, caller selection, descriptors, planner scratch and VM headroom excluded. No cloud or whole-process cap claim.",
      node: process.version,
      warmups: 2,
      samples: 6,
      sourceSha256: createHash("sha256")
        .update(await readFile(new URL("../packages/core/dist/vector-sort.js", import.meta.url)))
        .digest("hex"),
      cases,
    },
    null,
    2,
  ),
);
