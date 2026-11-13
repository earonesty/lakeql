import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import {
  batchExprValues,
  batchFromColumns,
  col,
  fn,
  materializeBatchRows,
  mul,
  vectorFromValues,
  vectorProjectBatch,
} from "../packages/core/dist/index.js";

function measure(run) {
  for (let i = 0; i < 3; i++) run();
  const times = [];
  for (let i = 0; i < 7; i++) {
    const start = performance.now();
    run();
    times.push(performance.now() - start);
  }
  return times.sort((a, b) => a - b)[3];
}
for (const rows of [10000, 100000]) {
  const batch = batchFromColumns({
    n: Array.from({ length: rows }, (_, i) => i),
    s: Array.from({ length: rows }, (_, i) => `  NAME ${i}  `),
  });
  for (const [kind, expr] of [
    ["numeric", mul(col("n"), 2)],
    ["string", fn("lower", fn("trim", col("s")))],
  ]) {
    const array = () => {
      const values = batchExprValues(batch, expr);
      return {
        rowCount: rows,
        columns: {
          result: vectorFromValues(Array.from({ length: rows }, (_, i) => values.valueAt(i))),
        },
      };
    };
    const indexed = () => {
      const projected = vectorProjectBatch(batch, ["n"], { result: expr });
      return { rowCount: rows, columns: { result: projected.columns.result } };
    };
    assert.deepEqual(materializeBatchRows(indexed()), materializeBatchRows(array()));
    console.log(
      JSON.stringify({ rows, kind, arrayMs: measure(array), indexedMs: measure(indexed) }),
    );
  }
}
