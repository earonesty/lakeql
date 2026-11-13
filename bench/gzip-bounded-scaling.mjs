import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { gzipSync } from "node:zlib";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { scopedParquetCompressors } from "../packages/parquet/dist/compressors.js";
import { GZIP_WORKSPACE_BYTES } from "../packages/parquet/dist/gzip.js";

const reports = [];
for (const size of [1024, 65536, 262144, 1048576, 4194304]) {
  for (const pattern of ["runs", "random"]) {
    let seed = 89123;
    const expected = Uint8Array.from({ length: size }, (_, i) => {
      seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
      return pattern === "random" ? seed >>> 24 : Math.floor(i / 127) % 19;
    });
    const encoded = gzipSync(expected);
    const cap = encoded.length + size + GZIP_WORKSPACE_BYTES;
    const samples = [];
    for (let trial = 0; trial < 7; trial++) {
      const budget = new MemoryBudget(cap);
      const inputScope = new MemoryScope(budget);
      const input = inputScope.allocate(encoded.length, "owned gzip input", () =>
        Uint8Array.from(encoded),
      );
      const decodeScope = new MemoryScope(budget);
      decodeScope.admit(size + GZIP_WORKSPACE_BYTES, "complete gzip decode");
      const codec = scopedParquetCompressors(decodeScope).GZIP;
      const started = performance.now();
      const output = codec(input.value, size);
      const elapsedMs = performance.now() - started;
      assert.deepEqual(output, expected);
      assert.equal(budget.peakBytes, cap);
      assert.equal(budget.usedBytes, encoded.length + size);
      const owner = decodeScope.retainAllocation(output);
      inputScope.dispose();
      decodeScope.dispose();
      assert.equal(budget.usedBytes, size);
      owner.release();
      assert.equal(budget.usedBytes, 0);
      if (trial >= 2) samples.push(elapsedMs);
    }
    const sorted = [...samples].sort((a, b) => a - b);
    reports.push({
      size,
      pattern,
      inputBytes: encoded.length,
      workspaceBytes: GZIP_WORKSPACE_BYTES,
      capBytes: cap,
      peakBytes: cap,
      medianMs: sorted[2],
      samplesMs: samples,
    });
  }
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local Node codec benchmark with owned encoded input, exact output and fixed scratch; excludes compression/oracle generation, JS objects and runtime heap; no remote latency or whole-worker claim",
      decoderSha256: createHash("sha256")
        .update(await readFile(new URL("../packages/parquet/dist/gzip.js", import.meta.url)))
        .digest("hex"),
      reports,
    },
    null,
    2,
  ),
);
