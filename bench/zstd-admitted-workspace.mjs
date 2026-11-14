import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { zstdCompressSync } from "node:zlib";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { scopedParquetCompressors } from "../packages/parquet/dist/compressors.js";

const sourceSha256 = createHash("sha256")
  .update(readFileSync(new URL("../packages/parquet/dist/compressors.js", import.meta.url)))
  .update(readFileSync(new URL("../packages/core/dist/vendor/zstd.js", import.meta.url)))
  .digest("hex");
function makeInput(length, seed) {
  let state = seed;
  return Uint8Array.from({ length }, (_, index) => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return (state >>> 16) % (3 + 13 * (Math.floor(index / 131072) % 16));
  });
}
function join(parts) {
  const result = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}
for (const frameCount of [1, 8]) {
  const parts = Array.from({ length: frameCount }, (_, index) =>
    makeInput(1024 * 1024, 12345 + index),
  );
  const input = join(parts);
  const compressed = join(parts.map((part) => zstdCompressSync(part)));
  function run(admitted, cap) {
    const budget = new MemoryBudget(cap);
    const scope = new MemoryScope(budget);
    if (admitted) scope.admit(cap, "measured complete codec allocation");
    try {
      const start = performance.now();
      const output = scopedParquetCompressors(scope).ZSTD(compressed, input.length);
      const elapsedMs = performance.now() - start;
      assert.deepEqual(output, input);
      return {
        elapsedMs,
        peakBytes: budget.peakBytes,
        allocatedBytes: admitted ? cap - scope.availableAllocationBytes : undefined,
      };
    } finally {
      scope.dispose();
      assert.equal(budget.usedBytes, 0);
    }
  }
  const measured = run(true, input.length * 2);
  const exactCap = measured.allocatedBytes;
  run(true, exactCap);
  assert.throws(() => run(true, exactCap - 1), /admitted|budget/);
  for (const admitted of [false, true]) {
    const times = [];
    let result;
    for (let repeat = 0; repeat < 8; repeat++) {
      result = run(admitted, admitted ? exactCap : input.length * 2);
      if (repeat >= 3) times.push(result.elapsedMs);
    }
    times.sort((a, b) => a - b);
    console.log(
      JSON.stringify({
        frameCount,
        mode: admitted ? "admitted-reuse" : "incremental-release",
        inputBytes: input.length,
        compressedBytes: compressed.length,
        workspacePeakBytes: result.peakBytes - input.length,
        totalNewWorkspaceBytes: admitted ? result.allocatedBytes - input.length : undefined,
        medianMs: times[2],
        samples: times.length,
        sourceSha256,
        scope:
          "Native-compressed synthetic multi-block payloads; borrowed encoded input; output and codec typed backing only; no cloud, Parquet page or whole-heap claim",
      }),
    );
  }
}
