import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import {
  batchFromColumns,
  decimalFromParts,
  gatherOwnedBatch,
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
} from "../packages/core/dist/index.js";

const results = [];
for (const count of [100, 10000]) {
  const value = decimalFromParts(123456789987654321n, 9);
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const producer = new MemoryScope(pool);
  const input = producer.retainAllocations(
    batchFromColumns(
      {
        value: Array.from({ length: count }, (_, index) => (index % 3 === 0 ? null : value)),
      },
      producer,
    ),
  );
  producer.dispose();
  const inputBytes = input.bytes;
  const output = gatherOwnedBatch(input, [1, 0, 2]);
  input.release();
  assert.deepEqual(materializeBatchRows(output.value), [{ value }, { value: null }, { value }]);
  assert.equal(pool.usedBytes, output.bytes);
  results.push({
    rows: count,
    inputBytes,
    outputBytes: output.bytes,
    peakReservedBytes: pool.peakBytes,
  });
  output.release();
  assert.equal(pool.usedBytes, 0);
}
assert.equal(results[0].outputBytes, results[1].outputBytes);
const sources = [
  "batch",
  "decimal-value",
  "selected-concat",
  "vector-selection",
  "vector-sort",
  "packed-gather",
];
const report = {
  description:
    "Owned decimal vector compaction with nulls and real class/value assertions. Input arrays supplied by the caller are outside the pool. Reservation units are not measured VM heap. No IO or cloud claim.",
  node: process.version,
  sources: Object.fromEntries(
    sources.map((name) => [
      name,
      createHash("sha256")
        .update(readFileSync(`packages/core/src/${name}.ts`))
        .digest("hex"),
    ]),
  ),
  results,
};
writeFileSync(
  "docs/benchmarks/decimal-vector-compaction-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results, null, 2));
