import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { col } from "../packages/core/dist/expr.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { createWindowAggregateAccumulator } from "../packages/core/dist/window-aggregate.js";

const results = [];
for (const length of [256, 4096, 16384]) {
  for (const workload of ["unique", "duplicates"]) {
    const rows = Array.from({ length }, (_, index) => ({
      value: workload === "unique" ? length - index : index % 8,
    }));
    for (const aggregate of ["median", "mode", "count_distinct"]) {
      const expected =
        aggregate === "median"
          ? workload === "unique"
            ? (length + 1) / 2
            : 3.5
          : aggregate === "mode"
            ? workload === "unique"
              ? 1
              : 0
            : workload === "unique"
              ? length
              : 8;
      for (const admitted of [false, true]) {
        const samples = [];
        let peakBytes = 0;
        let retainedBytes = 0;
        for (let round = 0; round < 7; round++) {
          const budget = admitted ? new MemoryBudget(64 * 1024 * 1024) : undefined;
          const state = createWindowAggregateAccumulator(
            { fn: { aggregate }, args: [col("value")], over: { partitionBy: [], orderBy: [] } },
            budget,
          );
          const start = performance.now();
          const actual = state.append(rows);
          const elapsed = performance.now() - start;
          assert.equal(actual, expected);
          peakBytes = Math.max(peakBytes, budget?.peakBytes ?? 0);
          retainedBytes = Math.max(retainedBytes, budget?.usedBytes ?? 0);
          state.reset();
          assert.equal(budget?.usedBytes ?? 0, 0);
          if (round >= 2) samples.push(elapsed);
        }
        results.push({
          length,
          workload,
          aggregate,
          admitted,
          medianMilliseconds: [...samples].sort((a, b) => a - b)[2],
          peakBytes,
          retainedBytes,
          samples,
        });
      }
    }
  }
}
const report = {
  description:
    "Local single-frame numeric aggregate scaling, two warmups and five samples. Same implementation with/without admission; unadmitted mode is a diagnostic baseline, not a before-change benchmark. Input construction and result validation excluded. Explicit reservation units only, not VM heap or cloud latency. Independent expected results checked on every run, with reset returning usage to zero.",
  sourceHashes: Object.fromEntries(
    ["window-aggregate", "json-scalar-key-memory", "memory-budget", "heap-sort"].map((name) => [
      name,
      createHash("sha256")
        .update(readFileSync(`packages/core/src/${name}.ts`))
        .digest("hex"),
    ]),
  ),
  results,
};
writeFileSync(
  process.env.LAKEQL_WINDOW_BUFFERED_REPORT ??
    "docs/benchmarks/window-buffered-aggregate-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results.map(({ samples, ...result }) => result)));
