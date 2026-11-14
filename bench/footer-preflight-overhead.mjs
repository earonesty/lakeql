import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { inspectCompactStructure } from "../packages/parquet/dist/compact-header.js";
import { incrementalParquetMetadata } from "../packages/parquet/dist/incremental-metadata.js";
import { parquetMetadata } from "../packages/parquet/node_modules/hyparquet/src/metadata.js";

const reports = [];
for (const name of [
  "byte_stream_split_extended.gzip.parquet",
  "concatenated_gzip_members.parquet",
  "rle_boolean_encoding.parquet",
]) {
  const bytes = await readFile(
    new URL(`../fixtures/external/parquet-testing/data/${name}`, import.meta.url),
  );
  const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length);
  const length = new DataView(buffer).getUint32(buffer.byteLength - 8, true);
  const samples = { baseline: [], preflight: [], incremental: [] };
  const oracle = parquetMetadata(buffer);
  for (let trial = 0; trial < 7; trial++)
    for (const mode of trial % 2
      ? ["incremental", "preflight", "baseline"]
      : ["baseline", "preflight", "incremental"]) {
      const start = performance.now();
      let output;
      for (let iteration = 0; iteration < 100; iteration++) {
        if (mode !== "baseline") {
          const reader = {
            view: new DataView(buffer, buffer.byteLength - 8 - length, length),
            offset: 0,
          };
          inspectCompactStructure(reader);
          assert.equal(reader.offset, length);
        }
        output =
          mode === "incremental" ? incrementalParquetMetadata(buffer) : parquetMetadata(buffer);
      }
      const elapsed = (performance.now() - start) / 100;
      assert.deepEqual(output, oracle);
      if (trial >= 2) samples[mode].push(elapsed);
    }
  reports.push({
    name,
    footerBytes: length,
    samplesMs: samples,
    medianMs: Object.fromEntries(
      Object.entries(samples).map(([mode, values]) => [mode, [...values].sort((a, b) => a - b)[2]]),
    ),
  });
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local external footer parse benchmark; upstream parse, preflight plus upstream, and preflight plus incremental parse, alternating order, two warmups/five samples each 100 parses. Excludes I/O and LakeQL normalization; not heap or cloud performance.",
      node: process.version,
      reports,
    },
    null,
    2,
  ),
);
