import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import {
  MemoryBudget,
  MemoryScope,
  vectorFromValues,
  vectorValue,
} from "../packages/core/dist/index.js";
import { concatBatches } from "../packages/core/dist/vector-sort.js";

const sourceSha256 = createHash("sha256")
  .update(await readFile(new URL("../packages/core/dist/packed-gather.js", import.meta.url)))
  .digest("hex");
for (const kind of ["bigint", "text"]) {
  for (const rows of [1000, 100000]) {
    const vectors = [0, 1].map((part) => ({
      type: "dict",
      indices: Uint32Array.from({ length: rows }, (_, i) => i % 32),
      dictionary:
        kind === "bigint"
          ? {
              type: "i64",
              values: BigInt64Array.from(
                { length: 32 },
                (_, i) => 9007199254740993n + BigInt(part * 100 + i),
              ),
            }
          : {
              type: "utf8",
              values: Array.from({ length: 32 }, (_, i) => `\ufeffé中-${part}-${i}`),
            },
    }));
    for (const implementation of ["row-fallback-emulation", "direct-concat"]) {
      const samples = [];
      let peakBytes;
      for (let trial = 0; trial < 8; trial++) {
        const budget = new MemoryBudget(64 * 1024 * 1024);
        const scope = new MemoryScope(budget);
        const start = performance.now();
        const output =
          implementation === "direct-concat"
            ? concatBatches(
                vectors.map((value) => ({ rowCount: rows, columns: { value } })),
                scope,
              ).columns.value
            : vectorFromValues(
                vectors.flatMap((vector) =>
                  Array.from({ length: rows }, (_, i) => vectorValue(vector, i)),
                ),
                scope,
              );
        const elapsedMs = performance.now() - start;
        for (let row = 0; row < rows * 2; row++)
          assert.deepEqual(
            vectorValue(output, row),
            vectorValue(vectors[Math.floor(row / rows)], row % rows),
          );
        peakBytes = budget.peakBytes;
        scope.dispose();
        assert.equal(budget.usedBytes, 0);
        if (trial > 2) samples.push(elapsedMs);
      }
      console.log(
        JSON.stringify({
          kind,
          rowsPerInput: rows,
          implementation,
          medianMs: samples.sort((a, b) => a - b)[2],
          peakTrackedBytes: peakBytes,
          samples: 5,
          sourceSha256,
          node: process.version,
          scope:
            "borrowed non-null dictionary inputs; fallback emulates prior row materialization; tracked bytes exclude JS arrays and strings; local microbenchmark, not Worker heap proof",
        }),
      );
    }
  }
}
