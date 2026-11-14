import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { MATERIALIZATION_CHARGES as c } from "../packages/core/dist/allocation-charges.js";
import {
  isNull,
  MemoryBudget,
  MemoryScope,
  predicateSelection,
  vectorValue,
} from "../packages/core/dist/index.js";

const results = [];
const decode = TextDecoder.prototype.decode;
let calls = 0;
let decodedBytes = 0;
TextDecoder.prototype.decode = function (input, options) {
  calls++;
  decodedBytes += input?.byteLength ?? 0;
  return decode.call(this, input, options);
};
try {
  for (const shape of ["packed", "list"])
    for (const width of [32, 4096])
      for (const rows of [256, 4096]) {
        const count = shape === "list" ? rows * 2 : rows;
        const valid = Uint8Array.from({ length: rows }, (_, i) => (i % 7 === 0 ? 0 : 1));
        const packed = {
          type: "utf8-view",
          data: new Uint8Array(count * width).fill(97),
          fixedLength: width,
          length: count,
        };
        const vector =
          shape === "packed"
            ? { ...packed, valid }
            : {
                type: "list",
                offsets: Int32Array.from({ length: rows + 1 }, (_, i) => i * 2),
                child: packed,
                valid,
              };
        const batch = { rowCount: rows, columns: { value: vector } };
        const expected = Array.from(valid, (value) => (value === 0 ? 1 : 0));
        const samples = { materializedReference: [], metadata: [] };
        for (let round = 0; round < 8; round++) {
          for (const metadata of round % 2 ? [true, false] : [false, true]) {
            const pool = new MemoryBudget(c.view + rows);
            const scope = new MemoryScope(pool);
            calls = 0;
            decodedBytes = 0;
            try {
              const start = performance.now();
              let mask;
              if (metadata) mask = predicateSelection(batch, isNull("value"), scope);
              else {
                mask = scope.allocate(
                  c.view + rows,
                  "reference predicate mask",
                  () => new Uint8Array(rows),
                ).value;
                for (let index = 0; index < rows; index++)
                  mask[index] = vectorValue(vector, index) === null ? 1 : 0;
              }
              const ms = performance.now() - start;
              assert.deepEqual([...mask], expected);
              assert.equal(pool.peakBytes, c.view + rows);
              if (metadata) assert.equal(calls, 0);
              if (round >= 2)
                samples[metadata ? "metadata" : "materializedReference"].push({
                  ms,
                  decodeCalls: calls,
                  decodedBytes,
                  chargedPeak: pool.peakBytes,
                });
            } finally {
              scope.dispose();
            }
            assert.equal(pool.usedBytes, 0);
          }
        }
        const variants = Object.fromEntries(
          Object.entries(samples).map(([name, observations]) => {
            const ordered = observations.map((sample) => sample.ms).sort((a, b) => a - b);
            return [
              name,
              {
                medianMs: (ordered[2] + ordered[3]) / 2,
                ...observations[0],
                samplesMs: observations.map((sample) => sample.ms),
              },
            ];
          }),
        );
        results.push({
          shape,
          rows,
          width,
          borrowedSourceBytes:
            packed.data.byteLength +
            valid.byteLength +
            (shape === "list" ? vector.offsets.byteLength : 0),
          variants,
        });
      }
} finally {
  TextDecoder.prototype.decode = decode;
}
const report = {
  runtimeBase: "14a038e",
  node: process.version,
  harnessSha256: createHash("sha256")
    .update(readFileSync(import.meta.filename))
    .digest("hex"),
  batchModuleSha256: createHash("sha256")
    .update(readFileSync(new URL("../packages/core/dist/batch.js", import.meta.url)))
    .digest("hex"),
  description:
    "Metadata-only IS NULL versus the removed value-materializing loop with the current view charge applied to both. Two warmups/six samples, alternating order. Independent validity oracle, exact mask-only pool and complete cleanup. Packed text and two-element lists, source construction excluded.",
  limitations: [
    "Reference reproduces the removed loop using current vectorValue, not a historical whole-revision build.",
    "Source is borrowed and excluded from the predicate pool. The reference reports the same mask charge despite allocating untracked decoded strings/list payload; decode volume exposes that accounting gap.",
    "Local Node kernel, not HTTP/cloud latency or whole-runtime memory.",
  ],
  results,
};
writeFileSync(
  process.env.LAKEQL_NULL_MASK_REPORT ?? "docs/benchmarks/vector-null-mask-2026-11-14.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(
  JSON.stringify({
    scenarios: results.length,
    eliminatedDecodeBytes: results.map((r) => r.variants.materializedReference.decodedBytes),
  }),
);
