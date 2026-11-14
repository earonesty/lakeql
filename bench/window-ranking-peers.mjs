import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { col } from "../packages/core/dist/expr.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { evaluateWindowRows } from "../packages/core/dist/window-backend.js";

const results = [];
for (const rows of [256, 1024, 2048]) {
  for (const groups of [1, 8]) {
    const partition = Array.from({ length: rows }, (_, index) => ({
      row: { key: Math.floor((index * groups) / rows) },
      originalIndex: index,
      partitionKey: [],
      orderKey: [Math.floor((index * groups) / rows)],
    }));
    const expr = {
      fn: "cume_dist",
      args: [],
      over: { partitionBy: [], orderBy: [{ expr: col("key") }] },
    };
    const samples = [];
    let peak = 0;
    for (let round = 0; round < 5; round++) {
      const pool = new MemoryBudget(64 * 1024 * 1024);
      const start = performance.now();
      const output = evaluateWindowRows(rows, partition, [{ start: 0, end: rows }], expr, {
        memoryBudget: pool,
      });
      const elapsed = performance.now() - start;
      for (let index = 0; index < rows; index++)
        assert.equal(output[index], (Math.floor((index * groups) / rows) + 1) / groups);
      assert.equal(pool.usedBytes, 0);
      peak = Math.max(peak, pool.peakBytes);
      if (round >= 2) samples.push(elapsed);
    }
    results.push({
      rows,
      groups,
      medianMilliseconds: [...samples].sort((a, b) => a - b)[1],
      samples,
      trackedPeerWorkspaceBytes: peak,
    });
  }
}
const report = {
  description:
    "Presorted cume_dist kernel; 2 warmups, 3 timed rounds; independent peer-end oracle. Row construction and output validation excluded. Tracked workspace is peer-index admission only, not output arrays or total VM heap. Local execution, not cloud throughput.",
  sourceHash: createHash("sha256")
    .update(readFileSync("packages/core/src/window-backend.ts"))
    .digest("hex"),
  results,
};
writeFileSync(process.env.LAKEQL_WINDOW_RANK_REPORT, `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(results));
