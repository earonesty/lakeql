import assert from "node:assert/strict";
import { memorySpillAdapter } from "../packages/core/dist/index.js";

const results = [];
for (const entries of [1000, 10000]) {
  for (const mode of ["scan-baseline", "incremental"]) {
    const samples = [];
    for (let round = 0; round < 4; round++) {
      const map = new Map();
      const spill = memorySpillAdapter({ maxBytes: entries * 8 });
      const bytes = new Uint8Array(8);
      let total = 0;
      const start = performance.now();
      for (let i = 0; i < entries; i++) {
        if (mode === "incremental") await spill.write(String(i), bytes);
        else {
          total = 0;
          for (const value of map.values()) total += value.byteLength;
          total += bytes.byteLength;
          map.set(String(i), new Uint8Array(bytes));
          await Promise.resolve();
        }
      }
      const elapsed = performance.now() - start;
      assert.equal(mode === "incremental" ? (await spill.usage()).byteSize : total, entries * 8);
      if (round > 0) samples.push(elapsed);
    }
    samples.sort((a, b) => a - b);
    results.push({ entries, mode, medianMs: samples[1] });
  }
}
console.log(
  JSON.stringify(
    {
      warmups: 1,
      samples: 3,
      scope:
        "Local 8-byte in-memory spill writes; modeled previous full-map byte accounting baseline; no disk/cloud or heap-cap measurement.",
      results,
    },
    null,
    2,
  ),
);
