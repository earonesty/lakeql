import assert from "node:assert/strict";
import { MATERIALIZATION_CHARGES as c } from "../packages/core/dist/allocation-charges.js";
import { vectorValue } from "../packages/core/dist/batch.js";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";

// Policy experiment only: identical row construction, varying decoded-key reuse.
// The budget measures the optional reference directory, not fixture/output heap.
const encoder = new TextEncoder();
const rows = 32768;
const cases = [];
const median = (a) => [...a].sort((x, y) => x - y)[Math.floor(a.length / 2)];
for (const cardinality of [16, 1024, 32768]) {
  for (const width of [8, 256]) {
    for (const stride of [1, 64]) {
      const data = new Uint8Array(cardinality * width);
      for (let i = 0; i < cardinality; i++) {
        const value = encoder.encode(`${i % 2 ? "界" : "é"}${i}`);
        data.fill(120, i * width, (i + 1) * width);
        data.set(value.subarray(0, width), i * width);
      }
      // Exercise independent value decoding, including BOM and malformed UTF-8.
      data.set([239, 187, 191, 255], 0);
      const dictionary = { type: "utf8-view", data, fixedLength: width, length: cardinality };
      const indices = Uint32Array.from({ length: rows }, (_, i) => (i * 17) % cardinality);
      const required = c.array + cardinality * c.arraySlot;
      function run(cached, allowance = required) {
        const pool = new MemoryBudget(allowance);
        const scope = new MemoryScope(pool);
        let cache;
        let decodes = 0;
        const start = performance.now();
        try {
          if (cached && required <= pool.availableBytes)
            cache = scope.allocate(
              required,
              "optional dictionary reference directory",
              () => new Array(cardinality),
            ).value;
          const output = [];
          for (let row = 0; row < rows; row += stride) {
            const index = indices[row];
            let text = null;
            if (row % 11 !== 0) {
              text = cache?.[index];
              if (text === undefined) {
                text = vectorValue(dictionary, index);
                decodes++;
                if (cache) cache[index] = text;
              }
            }
            output.push({ id: row, text });
          }
          return {
            output,
            elapsedMs: performance.now() - start,
            decodes,
            workspacePeak: pool.peakBytes,
          };
        } finally {
          scope.dispose();
          assert.equal(pool.usedBytes, 0);
        }
      }
      const expected = run(false).output;
      const measurements = { direct: [], cached: [] };
      for (let round = 0; round < 8; round++) {
        for (const kind of round % 2 ? ["cached", "direct"] : ["direct", "cached"]) {
          const result = run(kind === "cached");
          assert.deepEqual(result.output, expected);
          if (round >= 2) {
            const { output: _, ...metrics } = result;
            measurements[kind].push(metrics);
          }
        }
      }
      const fallback = run(true, required - 1);
      assert.deepEqual(fallback.output, expected);
      assert.equal(fallback.workspacePeak, 0);
      cases.push({
        cardinality,
        width,
        stride,
        rows,
        selectedRows: expected.length,
        measurements,
        directMedianMs: median(measurements.direct.map((x) => x.elapsedMs)),
        cachedMedianMs: median(measurements.cached.map((x) => x.elapsedMs)),
        fallbackDecodes: fallback.decodes,
      });
    }
  }
}
console.log(
  JSON.stringify(
    {
      node: process.version,
      scope:
        "Local dictionary materialization policy experiment; cache directory admission only. Input/output allocation excluded. No runtime/query/cloud speed or memory-cap claim. UTF-8 values checked against the existing decoder on every run; one-byte workspace shortage falls back to direct access.",
      cases,
    },
    null,
    2,
  ),
);
