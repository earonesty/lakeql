import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { incrementalParquetMetadata } from "../packages/parquet/dist/incremental-metadata.js";
import { parquetMetadata } from "../packages/parquet/node_modules/hyparquet/src/metadata.js";
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
  const parse = mode === "upstream" ? parquetMetadata : incrementalParquetMetadata;
  // Warm up without retaining an earlier result in the measured scope.
  for (let i = 0; i < 2; i++) parse(input);
  await new Promise(setImmediate);
  global.gc();
  global.gc();
  const before = process.memoryUsage();
  const start = performance.now();
  globalThis.retainedMetadata = parse(input);
  const elapsedMs = performance.now() - start;
  const afterParse = process.memoryUsage();
  await new Promise(setImmediate);
  global.gc();
  global.gc();
  const afterGc = process.memoryUsage();
  assert.equal(globalThis.retainedMetadata.row_groups.length, groups);
  assert.equal(globalThis.retainedMetadata.row_groups[0].columns.length, width);
  assert.equal(globalThis.retainedMetadata.num_rows, BigInt(groups * 2));
  console.log(
    JSON.stringify({
      mode,
      width,
      groups,
      inputBytes: input.byteLength,
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
    for (const mode of ["upstream", "incremental"]) {
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
          "Isolated local Node processes; generated real Parquet files, two warmup parses, event-loop boundary and double GC, then one retained parse per process. Three processes per case. Post-parse heap delta is not peak; post-GC delta estimates retained V8 heap, not a portable bound. Input/writer generation excluded. No I/O or decimal normalization measured.",
        node: process.version,
        reports,
      },
      null,
      2,
    ),
  );
}
