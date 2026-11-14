import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { materializeBatchRows } from "../packages/core/dist/batch.js";
import { jsonSafeValue } from "../packages/core/dist/evaluator.js";
import { stableStringify } from "../packages/core/dist/manifest.js";
import { serializedBatchBytes } from "../packages/core/dist/serialized-size.js";

const encoder = new TextEncoder();
const sourceSha256 = createHash("sha256")
  .update(await readFile(new URL("../packages/core/dist/serialized-size.js", import.meta.url)))
  .digest("hex");
for (const rowCount of [1000, 10000, 50000]) {
  const batch = {
    rowCount,
    columns: {
      id: { type: "i32", values: Int32Array.from({ length: rowCount }, (_, i) => i) },
      text: {
        type: "utf8",
        values: Array.from({ length: rowCount }, (_, i) => (i % 2 ? "猫😀\n" : "plain")),
      },
      binary: {
        type: "binary",
        values: new Array(rowCount).fill(Uint8Array.from({ length: 16 }, (_, i) => i)),
      },
    },
  };
  const old = () =>
    encoder.encode(stableStringify(jsonSafeValue(materializeBatchRows(batch)))).length;
  const direct = () => serializedBatchBytes(batch);
  const expected = old();
  assert.equal(direct(), expected);
  const medians = {};
  for (const [name, run] of [
    ["serializedCopy", old],
    ["directCount", direct],
  ]) {
    const samples = [];
    for (let i = 0; i < 8; i++) {
      const start = performance.now();
      const actual = run();
      const elapsed = performance.now() - start;
      assert.equal(actual, expected);
      if (i >= 3) samples.push(elapsed);
    }
    samples.sort((a, b) => a - b);
    medians[name] = samples[2];
  }
  console.log(
    JSON.stringify({
      rowCount,
      serializedBytes: expected,
      medianMs: medians,
      samples: 5,
      sourceSha256,
      scope:
        "Local serialized-budget accounting only, borrowed typed fixture inputs; no whole-query latency, heap cap, or cloud claim",
    }),
  );
}
