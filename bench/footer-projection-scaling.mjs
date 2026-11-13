import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import {
  FOOTER_STATISTICS_PROJECTION,
  readCompactHeader,
} from "../packages/parquet/dist/compact-header.js";

function listHeader(count) {
  if (count < 15) return [count * 16 + 12];
  const bytes = [0xfc];
  do {
    const byte = count & 127;
    count >>>= 7;
    bytes.push(byte | (count ? 128 : 0));
  } while (count);
  return bytes;
}
function fixture(groups, columns) {
  const bounds = [0x18, 4, 0, 15, 81, 82, 0x18, 4, 0, 15, 81, 83, 0];
  const column = [0x3c, 0xcc, ...bounds, 0, 0];
  const group = [
    0x19,
    ...listHeader(columns),
    ...Array.from({ length: columns }, () => column).flat(),
    0,
  ];
  return Uint8Array.from([
    0x49,
    ...listHeader(groups),
    ...Array.from({ length: groups }, () => group).flat(),
    0,
  ]);
}
function parse(bytes, incremental) {
  let count = 0;
  let checksum = 0;
  const consume = (column) => {
    const bounds = column.field_3.field_12;
    for (const value of Object.values(bounds)) {
      assert.equal(value.buffer, bytes.buffer);
      count++;
      checksum += value[3];
    }
  };
  const projection = incremental
    ? {
        4: {
          list: {
            1: {
              list: { 3: { 12: { 1: "binary", 2: "binary" } } },
              visit: consume,
            },
          },
          visit: () => {},
        },
      }
    : FOOTER_STATISTICS_PROJECTION;
  const reader = { view: new DataView(bytes.buffer), offset: 0 };
  const result = readCompactHeader(reader, projection);
  assert.equal(reader.offset, bytes.length);
  if (!incremental)
    for (const group of result.field_4) for (const column of group.field_1) consume(column);
  return { result, count, checksum };
}
const reports = [];
for (const groups of [16, 128, 1024, 4096]) {
  const columns = 16;
  const bytes = fixture(groups, columns);
  for (const incremental of [false, true]) {
    const samples = [];
    let retainedColumnObjects;
    for (let trial = 0; trial < 7; trial++) {
      global.gc?.();
      const start = performance.now();
      const output = parse(bytes, incremental);
      const elapsed = performance.now() - start;
      assert.equal(output.count, groups * columns * 2);
      assert.equal(output.checksum, groups * columns * 165);
      if (incremental) assert.deepEqual(output.result, { field_4: 0 });
      if (trial >= 2) samples.push(elapsed);
      retainedColumnObjects = incremental
        ? 0
        : output.result.field_4.reduce((sum, group) => sum + group.field_1.length, 0);
      assert.equal(retainedColumnObjects, incremental ? 0 : groups * columns);
    }
    reports.push({
      groups,
      columns,
      inputBytes: bytes.length,
      incremental,
      samplesMs: samples,
      medianMs: [...samples].sort((a, b) => a - b)[2],
      retainedColumnObjects,
    });
  }
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local Node synthetic compact footer projection only; two warmups/five samples; both paths inspect all bounds. Retained projected column objects are counted structurally; process heap and peak allocations are not measured. Input generation excluded.",
      node: process.version,
      gcEnabled: typeof global.gc === "function",
      parserSha256: createHash("sha256")
        .update(
          await readFile(new URL("../packages/parquet/dist/compact-header.js", import.meta.url)),
        )
        .digest("hex"),
      reports,
    },
    null,
    2,
  ),
);
