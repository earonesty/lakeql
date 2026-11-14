import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import {
  batchFromColumns,
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  timestampFromEpoch,
  vectorTopKOwnedBatch,
} from "../packages/core/dist/index.js";

const results = [];
for (const kind of ["bigint", "timestamp96", "packed text"]) {
  for (const rows of [100, 10000]) {
    const samples = [];
    let workspaceBytes;
    let inputBytes;
    for (let round = 0; round < 9; round++) {
      const pool = new MemoryBudget(64 * 1024 * 1024);
      const scope = new MemoryScope(pool);
      let batch;
      let expected;
      if (kind === "packed text") {
        const data = scope.allocate(
          rows * 128,
          "packed source",
          () => new Uint8Array(rows * 128),
        ).value;
        const encoder = new TextEncoder();
        const text = (value) => String(value).padStart(8, "0").padEnd(128, "x");
        for (let i = 0; i < rows; i++) data.set(encoder.encode(text((i * 17) % rows)), i * 128);
        batch = {
          rowCount: rows,
          columns: { key: { type: "utf8-view", data, fixedLength: 128, length: rows } },
        };
        expected = [0, 1, 2].map((value) => ({ key: text(value) }));
      } else {
        const convert = (value) =>
          kind === "bigint"
            ? BigInt(value)
            : timestampFromEpoch((1n << 70n) + BigInt(value), "nanos");
        batch = batchFromColumns(
          { key: Array.from({ length: rows }, (_, i) => convert((i * 17) % rows)) },
          scope,
        );
        expected = [0, 1, 2].map((value) => ({ key: convert(value) }));
      }
      const input = scope.retainAllocations(batch);
      scope.dispose();
      inputBytes = input.bytes;
      const start = performance.now();
      const output = vectorTopKOwnedBatch(input, [{ column: "key" }], { limit: 3 });
      const elapsed = performance.now() - start;
      assert.deepEqual(materializeBatchRows(output.value), expected);
      workspaceBytes = pool.peakBytes - inputBytes;
      output.release();
      input.release();
      assert.equal(pool.usedBytes, 0);
      if (round >= 2) samples.push(elapsed);
    }
    results.push({
      kind,
      rows,
      limit: 3,
      inputBytes,
      workspaceBytes,
      medianMilliseconds: [...samples].sort((a, b) => a - b)[3],
      samples,
    });
  }
}
for (const kind of ["bigint", "timestamp96", "packed text"])
  assert.equal(
    new Set(results.filter((result) => result.kind === kind).map((result) => result.workspaceBytes))
      .size,
    1,
  );
const report = {
  description:
    "Owned top-3 selection over real typed/packed buffers, including admitted scalar reads. Input creation excluded from timing; reported workspace is peak minus retained input backing. Two warmups, seven samples. No local/cloud equivalence, before/after speedup, native-GC or total-heap claim.",
  node: process.version,
  sources: Object.fromEntries(
    [
      "packages/core/src/vector-sort.ts",
      "packages/core/src/batch.ts",
      "packages/core/src/scalar-order.ts",
      "packages/core/src/scalar-read-workspace.ts",
      "packages/core/src/vector-buffer.ts",
      "packages/core/src/memory-budget.ts",
    ].map((path) => [path, createHash("sha256").update(readFileSync(path)).digest("hex")]),
  ),
  results,
};
writeFileSync(
  process.env.LAKEQL_TOP_K_SCALAR_REPORT ??
    "docs/benchmarks/top-k-scalar-workspace-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results, null, 2));
