import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { col } from "../packages/core/dist/expr.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import {
  segmentTreeAggregateValues,
  segmentTreeWorkspaceBytes,
} from "../packages/core/dist/window-segment-tree.js";
import { partitionRows } from "../packages/core/dist/window-utils.js";

const expression = {
  fn: { aggregate: "sum" },
  args: [col("amount")],
  over: {
    partitionBy: [],
    orderBy: [{ expr: col("id") }],
    frame: {
      mode: "rows",
      start: { kind: "unbounded-preceding" },
      end: { kind: "current-row" },
    },
  },
};
const results = [];
for (const rows of [256, 1024, 2048]) {
  const partition = partitionRows(
    Array.from({ length: rows }, (_, id) => ({ id, amount: id + 1 })),
    expression,
  );
  const extent = segmentTreeWorkspaceBytes(rows);
  for (const budgetBytes of [extent - 1, extent]) {
    const samples = [];
    let peakBytes = 0;
    for (let round = 0; round < 5; round++) {
      const budget = new MemoryBudget(budgetBytes);
      const start = performance.now();
      const values = segmentTreeAggregateValues(partition, expression, undefined, budget);
      const elapsed = performance.now() - start;
      assert.deepEqual(
        values,
        Array.from({ length: rows }, (_, id) => ((id + 1) * (id + 2)) / 2),
      );
      assert.equal(budget.usedBytes, 0);
      peakBytes = budget.peakBytes;
      assert.equal(peakBytes, budgetBytes === extent ? extent : 0);
      if (round >= 2) samples.push(elapsed);
    }
    results.push({
      rows,
      budgetBytes,
      mode: budgetBytes === extent ? "tree" : "span",
      peakBytes,
      medianMilliseconds: [...samples].sort((a, b) => a - b)[1],
      samples,
    });
  }
}
const report = {
  description:
    "Running sum over presorted numeric input, two warmups and three timed rounds. Construction and independent triangular-number validation excluded. Tree workspace admission only; row storage, expression results, output arrays and total VM heap are not measured. Local execution, not cloud latency.",
  sourceHashes: Object.fromEntries(
    ["window-segment-tree", "window-aggregate", "window-backend"].map((name) => [
      name,
      createHash("sha256")
        .update(readFileSync(`packages/core/src/${name}.ts`))
        .digest("hex"),
    ]),
  ),
  results,
};
writeFileSync(
  process.env.LAKEQL_WINDOW_SEGMENT_REPORT ??
    "docs/benchmarks/window-segment-budget-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results));
