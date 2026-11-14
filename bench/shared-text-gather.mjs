// Run with --expose-gc. Source values are borrowed; this measures new gathered output.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { vectorValue } from "../packages/core/dist/batch.js";
import { gatherBatch } from "../packages/core/dist/vector-sort.js";

assert.equal(typeof global.gc, "function", "Run with --expose-gc");
const sourceSha256 = createHash("sha256")
  .update(await readFile(new URL("../packages/core/dist/vector-sort.js", import.meta.url)))
  .digest("hex");

function copyingGather(batch, indices) {
  // Emulates the prior non-null text/binary branch, without budget/ownership work.
  return {
    rowCount: indices.length,
    columns: Object.fromEntries(
      Object.entries(batch.columns).map(([name, vector]) => [
        name,
        { type: vector.type, values: Array.from(indices, (index) => vector.values[index]) },
      ]),
    ),
  };
}
function trial(run, batch, indices) {
  global.gc();
  const baseline = process.memoryUsage().heapUsed;
  const outputs = [];
  const start = performance.now();
  for (let i = 0; i < 100; i++) outputs.push(run(batch, indices));
  const elapsedMs = performance.now() - start;
  global.gc();
  const retainedHeapBytes = process.memoryUsage().heapUsed - baseline;
  const buffers = new Set();
  for (const output of outputs)
    for (const vector of Object.values(output.columns))
      if (vector.type === "dict") buffers.add(vector.indices.buffer);
  const typedBackingBytes = [...buffers].reduce((sum, buffer) => sum + buffer.byteLength, 0);
  // Check every value of one output outside the measured allocation interval.
  for (const [name, vector] of Object.entries(outputs[0].columns)) {
    for (let row = 0; row < indices.length; row++)
      assert.deepEqual(vectorValue(vector, row), vectorValue(batch.columns[name], indices[row]));
  }
  assert.equal(outputs.length, 100);
  return { elapsedMs, retainedHeapBytes, typedBackingBytes };
}
for (const columns of [1, 8, 32]) {
  const rows = 10000;
  const text = Array.from({ length: rows }, (_, i) => `value-${i}`);
  const binary = Array.from({ length: rows }, (_, i) => Uint8Array.of(i % 256));
  const batch = {
    rowCount: rows,
    columns: Object.fromEntries(
      Array.from({ length: columns }, (_, i) => [
        `c${i}`,
        i % 2 ? { type: "binary", values: binary } : { type: "utf8", values: text },
      ]),
    ),
  };
  const indices = Array.from({ length: 1000 }, (_, i) => (i * 13) % rows);
  for (const [implementation, run] of [
    ["prior-copy-emulation", copyingGather],
    ["shared-indices", gatherBatch],
  ]) {
    const samples = [];
    for (let i = 0; i < 6; i++) {
      const sample = trial(run, batch, indices);
      if (i > 0) samples.push(sample);
    }
    const median = (key) => samples.map((sample) => sample[key]).sort((a, b) => a - b)[2];
    console.log(
      JSON.stringify({
        implementation,
        columns,
        inputRows: rows,
        selectedRows: indices.length,
        retainedOutputs: 100,
        medianMs: median("elapsedMs"),
        retainedHeapMedianBytes: median("retainedHeapBytes"),
        distinctTypedBackingBytes: median("typedBackingBytes"),
        samples: 5,
        sourceSha256,
        node: process.version,
        scope:
          "borrowed source, non-null text/binary values, local output construction; Node heap excludes ArrayBuffer external memory; no whole-query or Worker-cap claim",
      }),
    );
  }
}
