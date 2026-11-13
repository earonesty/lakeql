import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { inspectCompactAllocations } from "../packages/parquet/dist/compact-header.js";
import {
  footerRowGroupView,
  indexFooterRowGroups,
} from "../packages/parquet/dist/footer-directory.js";
import {
  incrementalParquetMetadata,
  readParquetFooterEnvelope,
} from "../packages/parquet/dist/incremental-metadata.js";
import { openIndexedParquetFooter } from "../packages/parquet/dist/indexed-footer.js";
import {
  COLUMN_CHUNK_PROJECTION,
  ROW_GROUP_FIELDS_PROJECTION,
} from "../packages/parquet/dist/row-group-metadata.js";
import { parquetWriteBuffer } from "../packages/parquet/node_modules/hyparquet-writer/src/index.js";

if (process.argv[2] === "child") {
  const mode = process.argv[3],
    width = Number(process.argv[4]),
    groups = Number(process.argv[5]);
  const input = parquetWriteBuffer({
    rowGroupSize: Array(groups).fill(2),
    columnData: Array.from({ length: width }, (_, i) => ({
      name: `column_${i}`,
      type: "INT32",
      data: Array.from({ length: groups * 2 }, (_, j) => i + j),
    })),
  });
  const parse = () => {
    const scope = new MemoryScope(new MemoryBudget(groups * 8));
    if (mode === "full") return { metadata: incrementalParquetMetadata(input), scope };
    const source = openIndexedParquetFooter(input, scope);
    let group,
      rows = 0n;
    for (let i = 0; i < groups; i++) {
      group = source.readRowGroup(i);
      rows += group.num_rows;
    }
    assert.equal(rows, BigInt(groups * 2));
    return { source, group, scope };
  };
  // Warm up without retaining an earlier result in the measured scope.
  for (let i = 0; i < 2; i++) parse().scope.dispose();
  await new Promise(setImmediate);
  global.gc();
  global.gc();
  const before = process.memoryUsage();
  const start = performance.now();
  globalThis.retainedMetadata = parse();
  const elapsedMs = performance.now() - start;
  const afterParse = process.memoryUsage();
  await new Promise(setImmediate);
  global.gc();
  global.gc();
  const afterGc = process.memoryUsage();
  const retained = globalThis.retainedMetadata;
  if (mode === "full") {
    assert.equal(retained.metadata.row_groups.length, groups);
    assert.equal(retained.metadata.row_groups[0].columns.length, width);
  } else {
    assert.equal(retained.source.rowGroupCount, groups);
    assert.equal(retained.group.columns.length, width);
    assert.equal(retained.scope.budget.usedBytes, groups * 8);
  }
  // Profile after both heap snapshots so instrumentation cannot inflate them.
  const emptyCounts = () => ({
    structs: 0,
    fields: 0,
    lists: 0,
    listSlots: 0,
    binaryViews: 0,
    borrowedBytes: 0,
  });
  const envelopeCounts = emptyCounts();
  const envelope = readParquetFooterEnvelope(input, (event) => {
    switch (event.kind) {
      case "struct":
        envelopeCounts.structs++;
        break;
      case "field":
        envelopeCounts.fields++;
        break;
      case "list":
        envelopeCounts.lists++;
        envelopeCounts.listSlots += event.length;
        break;
      case "binary-view":
        envelopeCounts.binaryViews++;
        envelopeCounts.borrowedBytes += event.borrowedBytes;
        break;
    }
  });
  const profileScope = new MemoryScope(new MemoryBudget(groups * 8));
  const directory = indexFooterRowGroups(envelope.view, profileScope, groups);
  const allGroupCounts = emptyCounts();
  let lastGroupCounts;
  for (let i = 0; i < groups; i++) {
    lastGroupCounts = inspectCompactAllocations(
      { view: footerRowGroupView(envelope.view, directory, i), offset: 0 },
      { 1: { list: COLUMN_CHUNK_PROJECTION, visit() {} }, ...ROW_GROUP_FIELDS_PROJECTION },
    );
    for (const key of Object.keys(allGroupCounts)) allGroupCounts[key] += lastGroupCounts[key];
  }
  profileScope.dispose();
  retained.scope.dispose();
  assert.equal(retained.scope.budget.usedBytes, 0);
  console.log(
    JSON.stringify({
      mode,
      width,
      groups,
      inputBytes: input.byteLength,
      rawAllocationCounts: {
        envelope: envelopeCounts,
        allGroups: allGroupCounts,
        lastGroup: lastGroupCounts,
      },
      elapsedMs,
      heapAfterParseDelta: afterParse.heapUsed - before.heapUsed,
      retainedHeapDelta: afterGc.heapUsed - before.heapUsed,
      arrayBufferDelta: afterGc.arrayBuffers - before.arrayBuffers,
    }),
  );
} else {
  const reports = [];
  for (const [width, groups] of [
    [32, 32],
    [128, 64],
    [256, 128],
  ])
    for (const mode of ["full", "indexed"]) {
      const samples = [];
      for (let trial = 0; trial < 3; trial++) {
        const result = spawnSync(
          process.execPath,
          [
            "--expose-gc",
            fileURLToPath(import.meta.url),
            "child",
            mode,
            String(width),
            String(groups),
          ],
          { encoding: "utf8", timeout: 60000 },
        );
        assert.equal(result.status, 0, result.stderr);
        samples.push(JSON.parse(result.stdout));
      }
      reports.push({ mode, width, groups, samples });
    }
  console.log(
    JSON.stringify(
      {
        scope:
          "Isolated local Node processes; generated real Parquet files, two warmup parses, event-loop boundary and double GC, then one retained parse per process. Full retains every group; indexed visits all groups and retains source plus last decoded group and directory. Three processes per case. Post-parse heap delta is not peak; post-GC delta estimates retained V8 heap, not a portable bound. Input/writer generation excluded. Raw structural allocation profiles are gathered after heap snapshots and exclude converted objects, strings, bigint work and decoder runtime; counts are not bytes. No I/O or whole-query memory bound measured; timing includes visiting every group, but not planning.",
        node: process.version,
        reports,
      },
      null,
      2,
    ),
  );
}
