import { performance } from "node:perf_hooks";
import { vectorValue } from "../packages/core/dist/batch.js";
import { setRecordValue } from "../packages/core/dist/record-value.js";

const rows = 50000;
const fields = Object.fromEntries(
  Array.from({ length: 8 }, (_, i) => [
    `f${i}`,
    { type: "i32", values: Int32Array.from({ length: rows }, (_, r) => r + i) },
  ]),
);
const vector = { type: "struct", fields, length: rows };
const previous = (index) => {
  const out = {};
  for (const [name, field] of Object.entries(fields))
    setRecordValue(out, name, vectorValue(field, index));
  return out;
};
const current = (index) => vectorValue(vector, index);
const run = (fn) => {
  let checksum = 0;
  const start = performance.now();
  for (let i = 0; i < rows; i++) {
    const row = fn(i);
    checksum += row.f0 + row.f7;
  }
  return { ms: performance.now() - start, checksum };
};
for (let i = 0; i < 4; i++) {
  run(previous);
  run(current);
}
const samples = [];
for (let i = 0; i < 12; i++) {
  const order =
    i % 2
      ? [
          ["current", current],
          ["previous", previous],
        ]
      : [
          ["previous", previous],
          ["current", current],
        ];
  const result = {};
  for (const [key, fn] of order) result[key] = run(fn);
  if (result.current.checksum !== result.previous.checksum) throw Error("mismatch");
  samples.push(result);
}
console.log(
  JSON.stringify(
    {
      node: process.version,
      rows,
      fields: 8,
      scope:
        "Local microbenchmark of one-level numeric struct materialization only; alternating order after warmup; not real Parquet or end-to-end latency evidence.",
      samples,
    },
    null,
    2,
  ),
);
