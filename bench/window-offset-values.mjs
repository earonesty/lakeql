import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { col, lit } from "../packages/core/dist/expr.js";
import { evaluateWindowRows } from "../packages/core/dist/window-backend.js";

const results = [];
for (const rows of [256, 1024, 2048])
  for (const fn of ["lag", "lead"]) {
    const values = Array.from({ length: rows }, (_, i) =>
      fn === "lead" ? (i >= rows - 3 ? i : null) : i < 3 ? i : null,
    );
    const partition = values.map((value, i) => ({
      row: { value },
      originalIndex: i,
      partitionKey: [],
      orderKey: [],
    }));
    const expected = values.map(
      (_v, i) =>
        (fn === "lead" ? values.slice(i + 1) : values.slice(0, i).reverse()).filter(
          (v) => v !== null,
        )[2] ?? -1,
    );
    const expr = {
      fn,
      args: [col("value"), lit(3), lit(-1)],
      ignoreNulls: true,
      over: { partitionBy: [], orderBy: [] },
    };
    const samples = [];
    for (let round = 0; round < 5; round++) {
      const start = performance.now();
      const output = evaluateWindowRows(rows, partition, [{ start: 0, end: rows }], expr, {});
      const elapsed = performance.now() - start;
      assert.deepEqual(output, expected);
      if (round >= 2) samples.push(elapsed);
    }
    results.push({ rows, fn, medianMilliseconds: [...samples].sort((a, b) => a - b)[1], samples });
  }
writeFileSync(
  process.env.LAKEQL_OFFSET_REPORT,
  `${JSON.stringify({ description: "IGNORE NULLS offset 3 with sparse values. Two warmups, three local timed samples; construction and independent slice/filter oracle excluded. No whole-heap or cloud measurement.", compiledSourceHash: createHash("sha256").update(readFileSync("packages/core/dist/window-backend.js")).digest("hex"), results }, null, 2)}\n`,
);
console.log(JSON.stringify(results));
