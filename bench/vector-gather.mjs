import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import {
  batchFromColumns,
  materializeBatchRows,
  vectorFromValues,
  vectorValue,
} from "../packages/core/dist/batch.js";
import { gatherBatch } from "../packages/core/dist/vector-sort.js";

for (const width of [8, 128]) {
  const rows = 2000;
  const batch = batchFromColumns({
    payload: Array.from({ length: rows }, (_, row) =>
      Array.from({ length: width }, (_, i) => row + i),
    ),
  });
  const indices = Array.from({ length: rows / 2 }, (_, i) => rows - i * 2 - 1);
  const rebuild = () => ({
    rowCount: indices.length,
    columns: {
      payload: vectorFromValues(indices.map((i) => vectorValue(batch.columns.payload, i))),
    },
  });
  const gather = () => gatherBatch(batch, indices);
  assert.deepEqual(materializeBatchRows(gather()), materializeBatchRows(rebuild()));
  const measure = (run) => {
    for (let i = 0; i < 3; i++) run();
    const timings = [];
    for (let i = 0; i < 9; i++) {
      const start = performance.now();
      run();
      timings.push(performance.now() - start);
    }
    return timings.sort((a, b) => a - b)[4];
  };
  console.log(
    JSON.stringify({
      rows,
      width,
      selected: indices.length,
      rebuildMs: measure(rebuild),
      gatherMs: measure(gather),
      sharedIndexBytes: indices.length * 4,
    }),
  );
}
