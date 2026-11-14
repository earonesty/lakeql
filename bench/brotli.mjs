// Reproducible codec cost/ownership probe. Run after `pnpm exec tsc -b`.
// Native Brotli is a CPU reference only: its internal allocations are untracked.

import { performance } from "node:perf_hooks";
import { brotliCompressSync, brotliDecompressSync, constants } from "node:zlib";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { scopedParquetCompressors } from "../packages/parquet/dist/compressors.js";

const results = [];
for (const bytes of [65536, 262144, 1048576]) {
  for (const pattern of ["text", "binary"]) {
    let seed = 12345;
    const phrase = new TextEncoder().encode("dictionary and bounded parallel range reads 猫\n");
    const input = Uint8Array.from({ length: bytes }, (_, index) => {
      seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
      return pattern === "binary" ? seed >>> 24 : phrase[index % phrase.length];
    });
    const compressed = brotliCompressSync(input, {
      params: { [constants.BROTLI_PARAM_QUALITY]: 4 },
    });
    const times = [];
    const nativeTimes = [];
    let peak = 0;
    for (let iteration = -3; iteration < 15; iteration++) {
      const budget = new MemoryBudget(64 * 1024 * 1024);
      const scope = new MemoryScope(budget);
      const start = performance.now();
      const output = scopedParquetCompressors(scope).BROTLI(compressed, bytes);
      const elapsed = performance.now() - start;
      if (Buffer.compare(output, input) !== 0) throw Error("Decoded bytes differ");
      if (budget.usedBytes !== bytes) throw Error("Scratch ownership retained");
      peak = Math.max(peak, budget.peakBytes);
      scope.dispose();
      if (budget.usedBytes !== 0) throw Error("Output ownership retained");
      const nativeStart = performance.now();
      const native = brotliDecompressSync(compressed);
      const nativeElapsed = performance.now() - nativeStart;
      if (Buffer.compare(native, input) !== 0) throw Error("Native reference differs");
      if (iteration >= 0) {
        times.push(elapsed);
        nativeTimes.push(nativeElapsed);
      }
    }
    times.sort((a, b) => a - b);
    nativeTimes.sort((a, b) => a - b);
    results.push({
      bytes,
      pattern,
      compressedBytes: compressed.length,
      medianMs: times[7],
      p95Ms: times[14],
      nativeMedianMs: nativeTimes[7],
      peakReservedBytes: peak,
      scratchAndMetadataPeakBytes: peak - bytes,
      retainedAfterReleaseBytes: 0,
    });
  }
}
console.log(
  JSON.stringify(
    {
      runtime: process.version,
      samples: 15,
      warmups: 3,
      compressionQuality: 4,
      capBytes: 64 * 1024 * 1024,
      scope:
        "Local codec cost and reservation accounting; not cloud throughput, total RSS or hosted Worker certification. Native decoder internal memory is untracked.",
      results,
    },
    null,
    2,
  ),
);
