import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { planCompactAllocation } from "../packages/parquet/dist/compact-allocation.js";
import {
  footerRowGroupView,
  indexFooterRowGroups,
} from "../packages/parquet/dist/footer-directory.js";
import { readParquetFooterEnvelope } from "../packages/parquet/dist/incremental-metadata.js";
import { openIndexedParquetEnvelope } from "../packages/parquet/dist/indexed-footer.js";
import { decodeOwnedFullMetadata } from "../packages/parquet/dist/owned-full-metadata.js";
import {
  COLUMN_CHUNK_PROJECTION,
  ROW_GROUP_FIELDS_PROJECTION,
} from "../packages/parquet/dist/row-group-metadata.js";
import { parquetWriteBuffer } from "../packages/parquet/node_modules/hyparquet-writer/src/index.js";

const results = [];
for (const groups of [1, 16, 256]) {
  const rows = groups * 8;
  const input = parquetWriteBuffer({
    rowGroupSize: Array(groups).fill(8),
    columnData: [
      { name: "id", type: "INT32", data: Array.from({ length: rows }, (_, i) => i) },
      { name: "label", type: "STRING", data: Array.from({ length: rows }, (_, i) => `row-${i}`) },
    ],
  });
  const footerBytes = new DataView(input).getUint32(input.byteLength - 8, true) + 8;
  const footer = input.slice(input.byteLength - footerBytes);
  const analysisScope = new MemoryScope(new MemoryBudget(64 * 1024 * 1024));
  const envelope = readParquetFooterEnvelope(footer);
  const directory = indexFooterRowGroups(envelope.view, analysisScope, groups);
  let rawGroupCharges = 0;
  for (let index = 0; index < groups; index++) {
    rawGroupCharges += planCompactAllocation(
      { view: footerRowGroupView(envelope.view, directory, index), offset: 0 },
      {
        1: { list: COLUMN_CHUNK_PROJECTION, visit() {} },
        ...ROW_GROUP_FIELDS_PROJECTION,
      },
    );
  }
  analysisScope.dispose();
  const modes = {};
  for (const mode of ["full", "indexed"]) {
    const budget = new MemoryBudget(64 * 1024 * 1024);
    const scope = new MemoryScope(budget);
    const backing = scope.allocate(footerBytes, "fixture footer", () => footer.slice(0)).value;
    let seenRows = 0n;
    let finalBound;
    if (mode === "full") {
      const metadata = decodeOwnedFullMetadata(backing, scope);
      assert.equal(metadata.row_groups.length, groups);
      for (const group of metadata.row_groups) {
        seenRows += group.num_rows;
        finalBound = group.columns[0].meta_data.statistics.max_value;
      }
    } else {
      const envelope = readParquetFooterEnvelope(backing, undefined, scope);
      const source = openIndexedParquetEnvelope(envelope, scope);
      assert.equal(source.rowGroupCount, groups);
      for (let index = 0; index < groups; index++) {
        const groupScope = scope.childScope();
        try {
          const group = source.readRowGroup(index, groupScope);
          seenRows += group.num_rows;
          finalBound = group.columns[0].meta_data.statistics.max_value;
        } finally {
          groupScope.dispose();
        }
      }
    }
    assert.equal(seenRows, BigInt(rows));
    assert.equal(finalBound, rows - 1);
    modes[mode] = { peakBytes: budget.peakBytes, retainedBytes: budget.usedBytes };
    scope.dispose();
    assert.equal(budget.usedBytes, 0);
  }
  results.push({
    groups,
    rows,
    footerBytes,
    rawGroupCharges,
    fixtureSha256: createHash("sha256").update(new Uint8Array(input)).digest("hex"),
    modes,
  });
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local generated real Parquet footer decoding. Explicit allocation model, not measured VM heap. Both modes admit exact footer backing; full retains all groups, indexed releases each decoded group before advancing. No timing or cloud transport claim.",
      results,
    },
    null,
    2,
  ),
);
