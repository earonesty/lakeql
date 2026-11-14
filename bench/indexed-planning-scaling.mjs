import assert from "node:assert/strict";
import { MemoryBudget, memoryStore } from "../packages/core/dist/index.js";
import { planOwnedIndexedRowGroups } from "../packages/parquet/dist/indexed-row-group-plan.js";
import { readOwnedParquetMetadata } from "../packages/parquet/dist/metadata-cache.js";
import { planRowGroupsFromMetadata } from "../packages/parquet/dist/row-group-plan.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/store-buffer.js";
import { parquetWriteBuffer } from "../packages/parquet/node_modules/hyparquet-writer/src/index.js";

const reports = [];
for (const [width, groups] of [
  [32, 32],
  [128, 64],
  [256, 128],
]) {
  const input = parquetWriteBuffer({
    rowGroupSize: Array(groups).fill(2),
    columnData: Array.from({ length: width }, (_, i) => ({
      name: `column_${i}`,
      type: "INT32",
      data: Array.from({ length: groups * 2 }, (_, j) => i + j),
    })),
  });
  const store = memoryStore();
  await store.put("fixture.parquet", new Uint8Array(input));
  const file = await asyncBufferFromStore(store, "fixture.parquet");
  const capacity = input.byteLength * 4 + groups * 8;
  let oracle;
  for (const mode of ["full", "indexed"]) {
    const samples = [];
    for (let trial = -2; trial < 5; trial++) {
      const budget = new MemoryBudget(capacity);
      const start = performance.now();
      let plan;
      if (mode === "full") {
        const { owner } = await readOwnedParquetMetadata("fixture.parquet", file, budget);
        try {
          plan = planRowGroupsFromMetadata(owner.value);
        } finally {
          owner.release();
        }
      } else {
        plan = await planOwnedIndexedRowGroups(file, budget);
      }
      const milliseconds = performance.now() - start;
      oracle ??= plan;
      assert.deepEqual(plan, oracle);
      assert.equal(budget.usedBytes, 0);
      assert.ok(budget.peakBytes <= capacity);
      if (trial >= 0) samples.push({ milliseconds, trackedPeakBytes: budget.peakBytes });
    }
    reports.push({ width, groups, mode, inputBytes: input.byteLength, capacity, samples });
  }
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local memory-store owned acquisition and planning on generated real Parquet files. Two warmups and five measured samples per mode; full mode first. Plan parity and tracked cleanup asserted. No cloud latency, JS heap, GC peak, or whole-query memory guarantee.",
      node: process.version,
      reports,
    },
    null,
    2,
  ),
);
