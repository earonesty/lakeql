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
  for (const workload of ["growing", "alternating"]) {
    const evaluatedExpression =
      workload === "growing"
        ? expression
        : {
            ...expression,
            over: {
              ...expression.over,
              frame: {
                mode: "rows",
                start: { kind: "preceding", offset: col("before") },
                end: { kind: "following", offset: col("after") },
              },
            },
          };
    const partition = partitionRows(
      Array.from({ length: rows }, (_, id) => ({
        id,
        amount: id + 1,
        before: id % 2 === 0 ? id : 0,
        after: id % 2 === 0 ? rows - id - 1 : 0,
      })),
      evaluatedExpression,
    );
    const extent = segmentTreeWorkspaceBytes(rows);
    for (const budgetBytes of [0, extent - 1, extent]) {
      const samples = [];
      let peakBytes = 0;
      for (let round = 0; round < 5; round++) {
        const budget = new MemoryBudget(budgetBytes);
        const start = performance.now();
        const values = segmentTreeAggregateValues(
          partition,
          evaluatedExpression,
          undefined,
          budget,
        );
        const elapsed = performance.now() - start;
        assert.deepEqual(
          values,
          Array.from({ length: rows }, (_, id) =>
            workload === "growing"
              ? ((id + 1) * (id + 2)) / 2
              : id % 2 === 0
                ? (rows * (rows + 1)) / 2
                : id + 1,
          ),
        );
        assert.equal(budget.usedBytes, 0);
        peakBytes = budget.peakBytes;
        assert.equal(peakBytes, workload === "alternating" && budgetBytes === extent ? extent : 0);
        if (round >= 2) samples.push(elapsed);
      }
      results.push({
        rows,
        workload,
        budgetBytes,
        mode: peakBytes > 0 ? "measured tree" : "incremental or recomputed spans",
        peakBytes,
        medianMilliseconds: [...samples].sort((a, b) => a - b)[1],
        samples,
      });
    }
  }
}
const report = {
  description:
    "Growing and alternating sum frames over presorted numeric input, two warmups and three timed rounds. Construction and independent frame-sum validation excluded. Tree workspace admission only; row storage, expression results, output arrays and total VM heap are not measured. Local execution, not cloud latency.",
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
    "docs/benchmarks/window-segment-measured-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results));
