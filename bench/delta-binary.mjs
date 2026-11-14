import assert from "node:assert/strict";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { readPackedDeltaBinaryValues } from "../packages/parquet/dist/delta-binary-values.js";
import {
  deltaLengthByteArray as decodeLengths,
  deltaByteArray as decodePrefix,
} from "../packages/parquet/node_modules/hyparquet/src/delta.js";
import { ByteWriter } from "../packages/parquet/node_modules/hyparquet-writer/src/bytewriter.js";
import {
  deltaLengthByteArray as encodeLengths,
  deltaByteArray as encodePrefix,
} from "../packages/parquet/node_modules/hyparquet-writer/src/delta.js";

const results = [];
for (const prefixes of [false, true]) {
  const count = 8192;
  const values = Array.from({ length: count }, (_, index) =>
    new TextEncoder().encode(`${"prefix-é-".repeat(16)}${index}`),
  );
  const writer = new ByteWriter();
  (prefixes ? encodePrefix : encodeLengths)(writer, values);
  const encoded = writer.getBuffer();
  const samples = { upstream: [], packed: [] };
  let peakBytes = 0,
    retainedBytes = 0;
  function run(mode) {
    const scope = new MemoryScope(new MemoryBudget(64 * 1024 * 1024));
    const reader = { view: new DataView(encoded), offset: 0 };
    const started = performance.now();
    let output;
    if (mode === "upstream") {
      output = new Array(count);
      (prefixes ? decodePrefix : decodeLengths)(reader, count, output);
    } else {
      output = readPackedDeltaBinaryValues(reader, count, prefixes, scope);
      peakBytes = scope.budget.peakBytes;
      retainedBytes = scope.budget.usedBytes;
    }
    const elapsed = performance.now() - started;
    assert.equal(reader.offset, encoded.byteLength);
    for (let i = 0; i < count; i++)
      assert.deepEqual(mode === "upstream" ? output[i] : output.value(i), values[i]);
    scope.dispose();
    assert.equal(scope.budget.usedBytes, 0);
    return elapsed;
  }
  for (let round = 0; round < 16; round++) {
    for (const mode of round % 2 ? ["packed", "upstream"] : ["upstream", "packed"]) {
      const elapsed = run(mode);
      if (round >= 4) samples[mode].push(elapsed);
    }
  }
  const median = (values) => {
    const sorted = [...values].sort((a, b) => a - b);
    return (sorted[5] + sorted[6]) / 2;
  };
  results.push({
    prefixes,
    count,
    encodedBytes: encoded.byteLength,
    decodedBytes: values.reduce((sum, value) => sum + value.length, 0),
    peakBytes,
    retainedBytes,
    upstreamMedianMs: median(samples.upstream),
    packedMedianMs: median(samples.packed),
    samples,
  });
}
console.log(
  JSON.stringify(
    {
      node: process.version,
      scope:
        "Local delta payload decoding only; four warmup and twelve alternating measured rounds; all decoded bytes checked outside timing. Packed includes allocation admission; upstream does not. Allocation figures cover the explicit packed model, not upstream or total VM heap. Input owned by harness. No cloud latency claim.",
      results,
    },
    null,
    2,
  ),
);
