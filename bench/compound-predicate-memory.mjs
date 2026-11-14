import assert from "node:assert/strict";
import { MATERIALIZATION_CHARGES as c } from "../packages/core/dist/allocation-charges.js";
import {
  and,
  batchFromColumns,
  col,
  MemoryBudget,
  MemoryScope,
  or,
  predicateSelection,
} from "../packages/core/dist/index.js";

for (const rows of [10000, 100000]) {
  const left = Array.from({ length: rows }, (_, index) =>
    index % 3 === 0 ? null : index % 3 === 1,
  );
  const right = Array.from({ length: rows }, (_, index) =>
    index % 5 === 0 ? null : index % 2 === 0,
  );
  const batch = batchFromColumns({ left, right });
  for (const operands of [2, 32, 128]) {
    for (const operation of ["and", "or"]) {
      const expression = (operation === "and" ? and : or)(
        col("left"),
        ...Array.from({ length: operands - 1 }, () => col("right")),
      );
      const expected = Uint8Array.from(left, (value, index) => {
        const other = right[index];
        const result =
          operation === "and"
            ? value === false || other === false
              ? false
              : value === null || other === null
                ? null
                : true
            : value === true || other === true
              ? true
              : value === null || other === null
                ? null
                : false;
        return result === null ? 2 : Number(result);
      });
      const samples = [];
      for (let trial = 0; trial < 8; trial++) {
        const budget = new MemoryBudget((c.view + rows) * 2);
        const scope = new MemoryScope(budget);
        const started = performance.now();
        const output = predicateSelection(batch, expression, scope);
        const elapsed = performance.now() - started;
        assert.deepEqual(output, expected);
        assert.equal(budget.peakBytes, (c.view + rows) * 2);
        assert.equal(budget.usedBytes, c.view + rows);
        scope.dispose();
        assert.equal(budget.usedBytes, 0);
        if (trial >= 3) samples.push(elapsed);
      }
      samples.sort((a, b) => a - b);
      console.log(
        JSON.stringify({
          rows,
          operands,
          operation,
          medianMs: samples[2],
          samples: samples.length,
          peakMaskBytes: (c.view + rows) * 2,
          retainedMaskBytes: c.view + rows,
          scope:
            "current production predicate kernel; borrowed input; typed mask backing and views only; all operands evaluated",
        }),
      );
    }
  }
}
