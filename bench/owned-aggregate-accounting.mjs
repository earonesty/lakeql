import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { OwnedAggregateState } from "../packages/core/dist/owned-aggregate-state.js";

assert.equal(typeof globalThis.gc, "function", "Run with --expose-gc");
const results = [];
for (const rows of [1024, 8192, 65536]) {
  globalThis.gc();
  const before = process.memoryUsage().heapUsed;
  const pool = new MemoryBudget(64 * 1024 * 1024);
  let reservations = 0;
  const reserve = pool.reserve.bind(pool);
  pool.reserve = (...args) => {
    reservations++;
    return reserve(...args);
  };
  const parent = new MemoryScope(pool);
  const owner = new OwnedAggregateState(parent, { value: { op: "median" } });
  const created = reservations;
  const started = performance.now();
  for (let i = 0; i < rows; i++) owner.updateValue("value", i);
  const updateMs = performance.now() - started;
  globalThis.gc();
  const heapGrowthBytes = process.memoryUsage().heapUsed - before;
  const retainedBytes = pool.usedBytes;
  const growthReservations = reservations - created;
  const output = owner.finish();
  assert.deepEqual(output.value, { value: (rows - 1) / 2 });
  output.release();
  parent.dispose();
  assert.equal(pool.usedBytes, 0);
  results.push({
    rows,
    growthReservations,
    retainedAdmittedBytes: retainedBytes,
    heapGrowthBytes,
    updateMs,
    remainingBytes: pool.usedBytes,
  });
}
const report = {
  description:
    "Diagnostic Node retained heap after explicit GC and exact reservation-call counts for owned median growth. One timing sample; not a Worker heap guarantee or public query benchmark.",
  node: process.version,
  results,
};
writeFileSync(
  process.argv[2] ?? "docs/benchmarks/owned-aggregate-accounting-before-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(results);
