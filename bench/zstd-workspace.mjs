import assert from "node:assert/strict";
import { zstdCompressSync } from "node:zlib";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { decompressInto } from "../packages/core/dist/vendor/zstd.js";
import { scopedParquetCompressors } from "../packages/parquet/dist/compressors.js";

const input = new Uint8Array(1024 * 1024);
let state = 12345;
for (let index = 0; index < input.length; index++) {
  state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
  input[index] = (state >>> 16) % (3 + 13 * Math.floor(index / 131072));
}
const compressed = zstdCompressSync(input);
for (const lifetime of ["frame", "block"]) {
  const times = [];
  let peakBytes = 0;
  for (let repeat = 0; repeat < 8; repeat++) {
    const budget = new MemoryBudget(input.length * 4);
    const scope = new MemoryScope(budget);
    try {
      const start = performance.now();
      const output =
        lifetime === "block"
          ? scopedParquetCompressors(scope).ZSTD(compressed, input.length)
          : decompressInto(
              compressed,
              scope.allocate(input.length, "output", () => new Uint8Array(input.length)).value,
              (bytes) =>
                scope.allocate(bytes, "frame workspace", () => new Uint8Array(bytes)).value,
            );
      const elapsed = performance.now() - start;
      assert.deepEqual(output, input);
      peakBytes = budget.peakBytes;
      if (repeat >= 3) times.push(elapsed);
    } finally {
      scope.dispose();
    }
    assert.equal(budget.usedBytes, 0);
  }
  times.sort((a, b) => a - b);
  console.log(
    JSON.stringify({
      lifetime,
      inputBytes: input.length,
      compressedBytes: compressed.length,
      peakBytes,
      workspacePeakBytes: peakBytes - input.length,
      medianMs: times[2],
      samples: times.length,
      scope: "admitted typed output and codec workspace; excludes input and JS heap",
    }),
  );
}
