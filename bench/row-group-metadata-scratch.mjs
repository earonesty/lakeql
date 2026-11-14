// Run after tsc -b. Optional reference module is compiled from the baseline source.
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import {
  footerRowGroupView,
  indexFooterRowGroups,
} from "../packages/parquet/dist/footer-directory.js";
import { decodeRowGroupMetadata } from "../packages/parquet/dist/row-group-metadata.js";

const { parquetWriteBuffer } = await import(
  "../packages/parquet/node_modules/hyparquet-writer/src/index.js"
);
const { parquetMetadata } = await import("../packages/parquet/node_modules/hyparquet/src/index.js");
const variants = { current: decodeRowGroupMetadata };
if (process.env.LAKEQL_METADATA_REFERENCE)
  variants.reference = (await import(process.env.LAKEQL_METADATA_REFERENCE)).decodeRowGroupMetadata;
const reports = [];
for (const width of [1, 8, 32]) {
  const bytes = parquetWriteBuffer({
    rowGroupSize: [2, 2],
    columnData: Array.from({ length: width }, (_, i) => ({
      name: `c${String(i).padStart(2, "0")}`,
      type: "INT32",
      data: [null, i, -i, i + 1],
    })),
  });
  const metadata = parquetMetadata(bytes);
  const leaves = metadata.schema.filter((leaf) => leaf.type);
  const length = new DataView(bytes).getUint32(bytes.byteLength - 8, true);
  const view = new DataView(bytes, bytes.byteLength - length - 8, length);
  const directoryScope = new MemoryScope(new MemoryBudget(16));
  const group = footerRowGroupView(view, indexFooterRowGroups(view, directoryScope), 1);
  directoryScope.dispose();
  for (const [variant, decode] of Object.entries(variants)) {
    const samples = [];
    let peak = 0,
      retained = 0;
    for (let round = 0; round < 7; round++) {
      const start = performance.now();
      for (let iteration = 0; iteration < 100; iteration++) {
        const budget = new MemoryBudget(1024 * 1024);
        const scope = new MemoryScope(budget);
        try {
          const result = decode(group, leaves, true, scope);
          if (iteration === 0) assert.deepEqual(result, metadata.row_groups[1]);
          peak = budget.peakBytes;
          retained = budget.usedBytes;
        } finally {
          scope.dispose();
        }
        assert.equal(budget.usedBytes, 0);
      }
      if (round >= 2) samples.push((performance.now() - start) / 100);
    }
    reports.push({
      width,
      variant,
      peakBytes: peak,
      retainedBytes: retained,
      scratchOverRetainedBytes: peak - retained,
      samplesMs: samples,
      medianMs: [...samples].sort((a, b) => a - b)[2],
    });
  }
}
const result = {
  conditions: {
    warmupRounds: 2,
    sampleRounds: 5,
    decodesPerRound: 100,
    fixture: "real written two-row-group nullable INT32 Parquet",
    timing: "decode, scope cleanup and zero-used check; reference equality checked once per round",
  },
  reports,
};
await writeFile(
  process.env.LAKEQL_METADATA_REPORT ?? "/tmp/row-group-metadata-scratch.json",
  `${JSON.stringify(result, null, 2)}\n`,
);
console.log(JSON.stringify(result, null, 2));
