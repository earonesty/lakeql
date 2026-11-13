// Run after building core and parquet. CPU kernels only, not cloud latency.
import assert from "node:assert/strict";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";
import { readDeltaValues } from "../packages/parquet/dist/delta-values.js";
import { deltaBinaryUnpack } from "../packages/parquet/node_modules/hyparquet/src/delta.js";
import { ByteWriter } from "../packages/parquet/node_modules/hyparquet-writer/src/bytewriter.js";
import { deltaBinaryPack } from "../packages/parquet/node_modules/hyparquet-writer/src/delta.js";

function medianTime(run) {
  for (let index = 0; index < 3; index += 1) run();
  const times = [];
  for (let index = 0; index < 7; index += 1) {
    const start = performance.now();
    run();
    times.push(performance.now() - start);
  }
  return times.sort((a, b) => a - b)[3];
}

const reports = [];
for (const count of [5000, 50000]) {
  for (const kind of ["int32-linear", "int32-varying", "int64-varying"]) {
    const int32 = !kind.startsWith("int64");
    const values = Array.from({ length: count }, (_, index) =>
      kind.endsWith("linear")
        ? index * 3
        : int32
          ? (index * index) % 1000000
          : 9007199254740993n + BigInt((index * index) % 1000000),
    );
    const writer = new ByteWriter();
    deltaBinaryPack(writer, values);
    const buffer = writer.getBuffer();
    const baseline = () => {
      const output = int32 ? new Int32Array(count) : new BigInt64Array(count);
      deltaBinaryUnpack({ view: new DataView(buffer), offset: 0 }, count, output);
      return output;
    };
    const admitted = () => {
      const budget = new MemoryBudget(count * (int32 ? 4 : 8));
      const scope = new MemoryScope(budget);
      try {
        const output = readDeltaValues(
          { view: new DataView(buffer), offset: 0 },
          count,
          int32,
          scope,
        );
        assert.equal(budget.peakBytes, count * (int32 ? 4 : 8));
        return output;
      } finally {
        scope.dispose();
        assert.equal(budget.usedBytes, 0);
      }
    };
    assert.deepEqual([...baseline()], values);
    assert.deepEqual([...admitted()], values);
    reports.push({
      kind,
      count,
      encodedBytes: buffer.byteLength,
      baselineMs: medianTime(baseline),
      admittedMs: medianTime(admitted),
    });
  }
}
console.log(JSON.stringify({ node: process.version, reports }, null, 2));
