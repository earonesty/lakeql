import assert from "node:assert/strict";
import { MemoryBudget, MemoryScope, vectorValue } from "../packages/core/dist/index.js";
import { readPackedBinaryValues } from "../packages/parquet/dist/binary-values.js";
import { readPlain } from "../packages/parquet/node_modules/hyparquet/src/plain.js";

for (const count of [1000, 10000]) {
  const bytes = new Uint8Array(count * 12);
  const view = new DataView(bytes.buffer);
  for (let index = 0; index < count; index++) {
    view.setUint32(index * 12, 8, true);
    view.setUint32(index * 12 + 4, index, true);
    view.setUint32(index * 12 + 8, 0xff00aa80, true);
  }
  for (const pages of [1, 32]) {
    for (const mode of ["view-array", "packed"]) {
      const samples = [];
      let peakBytes = 0;
      for (let trial = 0; trial < 8; trial++) {
        const scope = new MemoryScope(new MemoryBudget(count * 8));
        const start = performance.now();
        const reader = { view, offset: 0 };
        let vector;
        if (mode === "view-array") {
          const values = readPlain(reader, "BYTE_ARRAY", count);
          for (let page = 0; page < pages; page++) {
            vector = { type: "binary", values: values.slice().map((value) => value) };
          }
        } else {
          const packed = readPackedBinaryValues(reader, count, undefined, scope);
          const dictionary = {
            type: "binary-view",
            data: packed.payload,
            ranges: packed.ranges,
            length: count,
          };
          for (let page = 0; page < pages; page++) vector = dictionary;
        }
        const elapsed = performance.now() - start;
        assert.equal(reader.offset, bytes.length);
        for (let index = 0; index < count; index++) {
          assert.deepEqual(
            vectorValue(vector, index),
            bytes.subarray(index * 12 + 4, index * 12 + 12),
          );
        }
        peakBytes = scope.budget.peakBytes;
        scope.dispose();
        assert.equal(scope.budget.usedBytes, 0);
        if (trial >= 3) samples.push(elapsed);
      }
      samples.sort((a, b) => a - b);
      console.log(
        JSON.stringify({
          count,
          pages,
          mode,
          medianMs: samples[2],
          samples: samples.length,
          admittedTypedWorkspaceBytes: peakBytes,
          scope:
            "raw binary dictionary construction/reuse kernel; input borrowed; JS arrays/views untracked; excludes indices, decompression, I/O and value access",
        }),
      );
    }
  }
}
