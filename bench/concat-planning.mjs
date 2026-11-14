import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { concatAllocationBytes } from "../packages/core/dist/concat-plan.js";

const sourceSha256 = createHash("sha256")
  .update(await readFile(new URL("../packages/core/dist/concat-plan.js", import.meta.url)))
  .digest("hex");
const scalarKernelSha256 = createHash("sha256")
  .update(await readFile(new URL("../packages/core/dist/packed-gather.js", import.meta.url)))
  .digest("hex");
const dictionary = { type: "i32", values: Int32Array.of(1, 2) };
for (const shape of ["fixed", "shared-dictionary", "distinct-scalar-dictionary"]) {
  for (const columns of [4, 32]) {
    for (const batchCount of [16, 256, 4096]) {
      for (const rowsPerBatch of [1, 128]) {
        const batches = Array.from({ length: batchCount }, () => {
          const vector =
            shape === "fixed"
              ? { type: "f64", values: new Float64Array(rowsPerBatch) }
              : {
                  type: "dict",
                  indices: new Uint32Array(rowsPerBatch),
                  dictionary: shape === "shared-dictionary" ? dictionary : { ...dictionary },
                };
          return {
            rowCount: rowsPerBatch,
            columns: Object.fromEntries(
              Array.from({ length: columns }, (_, i) => [`c${i}`, vector]),
            ),
          };
        });
        const expectedBytes =
          batchCount *
          rowsPerBatch *
          (shape === "fixed" ? columns * 8 : shape === "shared-dictionary" ? 4 : columns * 4);
        const samples = [];
        const repeats = Math.max(1, Math.floor(16384 / (batchCount * columns)));
        for (let trial = 0; trial < 8; trial++) {
          const start = performance.now();
          let bytes;
          for (let i = 0; i < repeats; i++) bytes = concatAllocationBytes(batches);
          const elapsed = (performance.now() - start) / repeats;
          assert.equal(bytes, expectedBytes);
          if (trial >= 3) samples.push(elapsed);
        }
        samples.sort((a, b) => a - b);
        console.log(
          JSON.stringify({
            shape,
            columns,
            batchCount,
            rowsPerBatch,
            plannedBytes: expectedBytes,
            medianMs: samples[2],
            samples: 5,
            sourceSha256,
            scalarKernelSha256,
            node: process.version,
            scope:
              "Planning only; borrowed fixture inputs, no output allocation, no cloud or Worker heap proof",
          }),
        );
      }
    }
  }
}
