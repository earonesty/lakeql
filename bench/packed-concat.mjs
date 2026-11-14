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
for (const type of ["utf8-view", "binary-view"])
  for (const rows of [1000, 100000]) {
    const payload =
      type === "utf8-view" ? new TextEncoder().encode("\ufeffé中") : Uint8Array.of(255, 0, 128);
    const vectors = [0, 1].map(() => {
      const data = new Uint8Array(rows * payload.length);
      for (let i = 0; i < rows; i++) data.set(payload, i * payload.length);
      return { type, data, fixedLength: payload.length, length: rows };
    });
    const expected = vectorValue(vectors[0], 0);
    for (const implementation of ["row-fallback-emulation", "packed-copy"]) {
      const samples = [];
      let peakTrackedBytes = 0;
      for (let trial = 0; trial < 8; trial++) {
        const budget = new MemoryBudget(64 * 1024 * 1024);
        const scope = new MemoryScope(budget);
        try {
          const start = performance.now();
          const output =
            implementation === "packed-copy"
              ? concatBatches(
                  vectors.map((x) => ({ rowCount: rows, columns: { x } })),
                  scope,
                ).columns.x
              : vectorFromValues(
                  vectors.flatMap((v) => Array.from({ length: rows }, (_, i) => vectorValue(v, i))),
                  scope,
                );
          const elapsed = performance.now() - start;
          for (let i = 0; i < rows * 2; i++) assert.deepEqual(vectorValue(output, i), expected);
          peakTrackedBytes = budget.peakBytes;
          if (trial >= 3) samples.push(elapsed);
        } finally {
          scope.dispose();
        }
        assert.equal(budget.usedBytes, 0);
      }
      samples.sort((a, b) => a - b);
      console.log(
        JSON.stringify({
          type,
          rows: rows * 2,
          implementation,
          medianMs: samples[2],
          peakTrackedBytes,
          sourceSha256,
          node: process.version,
          scope:
            "Local microbenchmark; borrowed inputs; legacy JS strings/view arrays excluded from tracked bytes; not cloud or whole heap proof",
        }),
      );
    }
  }
