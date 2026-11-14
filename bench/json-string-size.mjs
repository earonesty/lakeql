import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { jsonStringUtf8Bytes } from "../packages/core/dist/index.js";

const encoder = new TextEncoder();
const ascii = /^[ -~]*$/;
const oldSize = (value) => {
  const json = JSON.stringify(value);
  return ascii.test(json) ? json.length : encoder.encode(json).byteLength;
};
for (const [kind, token] of [
  ["ascii", "abcdef"],
  ["unicode", "é猫😀"],
  ["escaped", '"\\\n\ud800'],
]) {
  const values = Array.from({ length: 1000 }, (_, i) => `${i}:${token.repeat(128)}`);
  for (const value of values) assert.equal(jsonStringUtf8Bytes(value), oldSize(value));
  const measure = (size) => {
    const run = () => values.reduce((sum, value) => sum + size(value), 0);
    for (let i = 0; i < 3; i++) run();
    const times = [];
    for (let i = 0; i < 7; i++) {
      const start = performance.now();
      run();
      times.push(performance.now() - start);
    }
    return times.sort((a, b) => a - b)[3];
  };
  console.log(
    JSON.stringify({
      kind,
      values: values.length,
      serializedMs: measure(oldSize),
      measuredMs: measure(jsonStringUtf8Bytes),
    }),
  );
}
