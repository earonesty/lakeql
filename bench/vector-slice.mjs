import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { sliceVector, vectorFromValues, vectorValue } from "../packages/core/dist/index.js";

// Compare the former nested materialization path with retained-offset slicing.
function materializedSlice(vector, start, end) {
  const values = [];
  for (let row = start; row < end; row++) values.push(vectorValue(vector, row));
  return vectorFromValues(values);
}
function medianRun(run) {
  for (let i = 0; i < 3; i++) run();
  const times = [];
  for (let i = 0; i < 7; i++) {
    const start = performance.now();
    run();
    times.push(performance.now() - start);
  }
  return times.sort((a, b) => a - b)[3];
}
for (const childWidth of [8, 128]) {
  const rows = 2000;
  const vector = vectorFromValues(
    Array.from({ length: rows }, (_, row) =>
      Array.from({ length: childWidth }, (_, index) => row + index),
    ),
  );
  const view = sliceVector(vector, 1, rows - 1);
  const materialized = materializedSlice(vector, 1, rows - 1);
  assert.equal(view.type, "list");
  assert.equal(vector.type, "list");
  assert.equal(view.child, vector.child);
  assert.equal(view.offsets.buffer, vector.offsets.buffer);
  for (let row = 0; row < rows - 2; row++)
    assert.deepEqual(vectorValue(view, row), vectorValue(materialized, row));
  const baselineMs = medianRun(() => materializedSlice(vector, 1, rows - 1));
  const viewMs = medianRun(() => sliceVector(vector, 1, rows - 1));
  console.log(JSON.stringify({ rows, childWidth, baselineMs, viewMs, sharedChild: true }));
}
