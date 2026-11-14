import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  MATERIALIZATION_CHARGES as c,
  MemoryBudget,
  MemoryScope,
} from "../packages/core/dist/index.js";
import { readAdmittedCompactHeader } from "../packages/parquet/dist/compact-allocation.js";
import {
  footerRowGroupView,
  indexFooterRowGroups,
} from "../packages/parquet/dist/footer-directory.js";
import { readParquetFooterEnvelope } from "../packages/parquet/dist/incremental-metadata.js";
import {
  COLUMN_CHUNK_PROJECTION,
  convertColumnChunkMetadata,
  convertRowGroupMetadata,
  decodeRowGroupMetadata,
  ROW_GROUP_FIELDS_PROJECTION,
} from "../packages/parquet/dist/row-group-metadata.js";
import { parquetWriteBuffer } from "../packages/parquet/node_modules/hyparquet-writer/src/index.js";

// Frozen pre-partition lifetime policy, sharing current conversion kernels.
// This isolates scratch/alias ownership overhead, not unrelated historical changes.
function retainedRawBaseline(view, leaves, scope) {
  const columns = scope.allocate(c.array, "converted column list", () => []).value;
  const reader = { view, offset: 0 };
  const raw = readAdmittedCompactHeader(reader, scope, {
    1: {
      list: COLUMN_CHUNK_PROJECTION,
      visit(value, index) {
        scope.allocate(c.arraySlot, "converted column slot", () =>
          columns.push(convertColumnChunkMetadata(value, leaves[index], true, scope)),
        );
      },
    },
    ...ROW_GROUP_FIELDS_PROJECTION,
  });
  assert.equal(reader.offset, view.byteLength);
  return convertRowGroupMetadata(raw, leaves, columns, scope);
}
const groups = 256,
  repetitions = 4;
const input = parquetWriteBuffer({
  rowGroupSize: Array(groups).fill(8),
  columnData: [
    { name: "id", type: "INT32", data: Array.from({ length: groups * 8 }, (_, i) => i) },
    {
      name: "label",
      type: "STRING",
      data: Array.from({ length: groups * 8 }, (_, i) => `row-${i}`),
    },
  ],
});
const envelope = readParquetFooterEnvelope(input);
const leaves = envelope.metadata.schema.filter((element) => element.type);
const indexScope = new MemoryScope(new MemoryBudget(1024 * 1024));
const directory = indexFooterRowGroups(envelope.view, indexScope, groups);
const views = Array.from({ length: groups }, (_, index) =>
  footerRowGroupView(envelope.view, directory, index),
);
const samples = { retainedRaw: [], partitioned: [] };
function run(decode) {
  let rows = 0n,
    maximum = 0;
  const started = performance.now();
  for (let repeat = 0; repeat < repetitions; repeat++) {
    const scope = new MemoryScope(new MemoryBudget(64 * 1024 * 1024));
    try {
      for (const view of views) {
        const group = decode(view, leaves, scope);
        rows += group.num_rows;
        maximum = group.columns[0].meta_data.statistics.max_value;
      }
    } finally {
      scope.dispose();
    }
    assert.equal(scope.budget.usedBytes, 0);
  }
  const ms = performance.now() - started;
  assert.equal(rows, BigInt(groups * 8 * repetitions));
  assert.equal(maximum, groups * 8 - 1);
  return ms;
}
const decoders = {
  retainedRaw: retainedRawBaseline,
  partitioned: (view, schema, scope) => decodeRowGroupMetadata(view, schema, true, scope),
};
for (let round = 0; round < 16; round++) {
  const order = round % 2 ? ["partitioned", "retainedRaw"] : ["retainedRaw", "partitioned"];
  for (const mode of order) {
    const ms = run(decoders[mode]);
    if (round >= 4) samples[mode].push(ms);
  }
}
indexScope.dispose();
const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b);
  return (sorted[5] + sorted[6]) / 2;
};
const retainedRawMs = median(samples.retainedRaw),
  partitionedMs = median(samples.partitioned);
console.log(
  JSON.stringify(
    {
      node: process.version,
      groups,
      repetitions,
      warmupRounds: 4,
      measuredRounds: 12,
      fixtureSha256: createHash("sha256").update(new Uint8Array(input)).digest("hex"),
      scope:
        "Local generated Parquet metadata; alternating modes; same current conversion kernels. Baseline retains all raw allocations; partitioned uses current scratch/alias lifetimes. Includes scope disposal, excludes file reads, envelope and directory. No cloud latency or production heap claim.",
      samples,
      retainedRawMs,
      partitionedMs,
      ratio: partitionedMs / retainedRawMs,
    },
    null,
    2,
  ),
);
