import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { col, lit } from "../packages/core/dist/expr.js";
import { evaluateWindowRows } from "../packages/core/dist/window-backend.js";

const results = [];
for (const rows of [256, 1024, 2048]) {
  for (const fn of ["first_value", "last_value", "nth_value"]) {
    const partition = Array.from({ length: rows }, (_, index) => ({
      row: { value: index === 0 || index === rows - 1 ? null : index },
      originalIndex: index,
      partitionKey: [],
      orderKey: [],
    }));
    const expr = {
      fn,
      args: fn === "nth_value" ? [col("value"), lit(3)] : [col("value")],
      ignoreNulls: true,
      over: {
        partitionBy: [],
        orderBy: [],
        frame: {
          mode: "rows",
          start: { kind: "unbounded-preceding" },
          end: { kind: "unbounded-following" },
        },
      },
    };
    const expected = fn === "first_value" ? 1 : fn === "last_value" ? rows - 2 : 3;
    const samples = [];
    for (let round = 0; round < 5; round++) {
      const start = performance.now();
      const output = evaluateWindowRows(rows, partition, [{ start: 0, end: rows }], expr, {});
      const elapsed = performance.now() - start;
      assert.deepEqual(output, Array(rows).fill(expected));
      if (round >= 2) samples.push(elapsed);
    }
    results.push({ rows, fn, medianMilliseconds: [...samples].sort((a, b) => a - b)[1], samples });
  }
}
const report = {
  description:
    "Full-partition first/last/nth value with IGNORE NULLS, two warmups and three timed samples. Construction and independent constant-output validation excluded. Local runtime; no whole-heap or cloud measurement.",
  compiledSourceHashes: Object.fromEntries(
    ["window-backend", "window-frame"].map((name) => [
      name,
      createHash("sha256")
        .update(readFileSync(`packages/core/dist/${name}.js`))
        .digest("hex"),
    ]),
  ),
  results,
};
writeFileSync(process.env.LAKEQL_FRAME_VALUES_REPORT, `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(results));
