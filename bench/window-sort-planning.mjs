import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { col } from "../packages/core/dist/expr.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";
import {
  compatibleWindowSortGroups,
  windowSortSpecsCompatible,
} from "../packages/core/dist/window-query.js";

function baseline(items) {
  const groups = [];
  const ordered = [...items].sort(
    (left, right) => right.expr.over.orderBy.length - left.expr.over.orderBy.length,
  );
  for (const item of ordered) {
    const group = groups.find((candidate) =>
      windowSortSpecsCompatible(candidate.sortExpr, item.expr),
    );
    if (group === undefined) groups.push({ sortExpr: item.expr, items: [item] });
    else group.items.push(item);
  }
  return groups;
}
const ids = (groups) => groups.map((group) => group.items.map((item) => item.id));
const results = [];
for (const count of [128, 512, 1024])
  for (const workload of ["compatible", "partitions", "order-branches"]) {
    const items = Array.from({ length: count }, (_, id) => ({
      id,
      expr: {
        fn: "row_number",
        args: [],
        over: {
          partitionBy: [col(workload === "partitions" ? `p${id}` : "p")],
          orderBy: [col("a"), col(workload === "order-branches" ? `b${id}` : "b")].map((expr) => ({
            expr,
          })),
        },
      },
    }));
    const expected = ids(baseline(items));
    for (const mode of ["baseline", "cached", "admitted"]) {
      const samples = [];
      let peakBytes = 0;
      for (let round = 0; round < 5; round++) {
        const budget = new MemoryBudget(64 * 1024 * 1024);
        const scope = new MemoryScope(budget);
        const start = performance.now();
        const groups =
          mode === "baseline"
            ? baseline(items)
            : compatibleWindowSortGroups(items, mode === "admitted" ? scope : undefined);
        const elapsed = performance.now() - start;
        assert.deepEqual(ids(groups), expected);
        peakBytes = Math.max(peakBytes, budget.peakBytes);
        scope.dispose();
        assert.equal(budget.usedBytes, 0);
        if (round >= 2) samples.push(elapsed);
      }
      results.push({
        count,
        workload,
        mode,
        peakBytes,
        medianMilliseconds: [...samples].sort((a, b) => a - b)[1],
        samples,
      });
    }
  }
const report = {
  description:
    "Local window sort planning, two warmups and three timed rounds. Baseline reproduces the previous pairwise compatibility loop using the unchanged standalone compatibility function. Cached and admitted modes use identical grouping semantics; output group membership/order is independently compared on every run. Input generation, validation and scope disposal excluded. Reservation peaks are not VM heap or cloud latency measurements.",
  sourceHashes: Object.fromEntries(
    ["window-query", "owned-checkpoint-encoding"].map((name) => [
      name,
      createHash("sha256")
        .update(readFileSync(`packages/core/src/${name}.ts`))
        .digest("hex"),
    ]),
  ),
  results,
};
writeFileSync(
  process.env.LAKEQL_WINDOW_PLANNING_REPORT ??
    "docs/benchmarks/window-sort-planning-2026-11-14.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results.filter((result) => result.count === 1024)));
