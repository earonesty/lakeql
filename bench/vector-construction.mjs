import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { vectorFromValues, vectorValue } from "../packages/core/dist/index.js";

function flattenedBaseline(rows) {
  const offsets = new Int32Array(rows.length + 1);
  const children = [];
  for (let row = 0; row < rows.length; row++) {
    for (const value of rows[row]) children.push(value);
    offsets[row + 1] = children.length;
  }
  return { type: "list", offsets, child: vectorFromValues(children) };
}
function median(run) {
  for (let i = 0; i < 3; i++) run();
  const samples = [];
  for (let i = 0; i < 7; i++) {
    const start = performance.now();
    run();
    samples.push(performance.now() - start);
  }
  return samples.sort((a, b) => a - b)[3];
}
for (const width of [8, 128]) {
  const rows = Array.from({ length: 2000 }, (_, row) =>
    Array.from({ length: width }, (_, child) => row + child),
  );
  const actual = vectorFromValues(rows);
  const expected = flattenedBaseline(rows);
  for (let row = 0; row < rows.length; row++)
    assert.deepEqual(vectorValue(actual, row), vectorValue(expected, row));
  console.log(
    JSON.stringify({
      rows: rows.length,
      width,
      flattenedMs: median(() => flattenedBaseline(rows)),
      segmentedMs: median(() => vectorFromValues(rows)),
      removedIntermediateSlots: rows.length * width,
    }),
  );
}

function flattenedMapBaseline(rows) {
  const offsets = new Int32Array(rows.length + 1);
  const keys = [];
  const values = [];
  for (let row = 0; row < rows.length; row++) {
    for (const [key, value] of rows[row]) {
      keys.push(key);
      values.push(value);
    }
    offsets[row + 1] = keys.length;
  }
  return { type: "map", offsets, keys: vectorFromValues(keys), values: vectorFromValues(values) };
}
for (const width of [8, 128]) {
  const rows = Array.from(
    { length: 2000 },
    (_, row) => new Map(Array.from({ length: width }, (_, index) => [`key-${index}`, row + index])),
  );
  const actual = vectorFromValues(rows);
  const expected = flattenedMapBaseline(rows);
  for (let row = 0; row < rows.length; row++)
    assert.deepEqual(vectorValue(actual, row), vectorValue(expected, row));
  console.log(
    JSON.stringify({
      kind: "map",
      rows: rows.length,
      width,
      flattenedMs: median(() => flattenedMapBaseline(rows)),
      cursorMs: median(() => vectorFromValues(rows)),
      removedIntermediateSlots: 2 * rows.length * width,
    }),
  );
}
