import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { AggregateStateMemory } from "../packages/core/dist/aggregate-state-memory.js";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { OwnedAggregateState } from "../packages/core/dist/owned-aggregate-state.js";

assert.equal(typeof globalThis.gc, "function", "Run with --expose-gc");
const results = [];
for (const directories of [128, 1024, 8192]) {
  for (const kind of ["separate-owners", "shared-arena"]) {
    globalThis.gc();
    const before = process.memoryUsage().heapUsed;
    const pool = new MemoryBudget(64 * 1024 * 1024);
    const scope = new MemoryScope(pool);
    let reservations = 0;
    const reserve = pool.reserve.bind(pool);
    pool.reserve = (...args) => {
      reservations++;
      return reserve(...args);
    };
    const owners = [],
      states = [];
    const arena = kind === "shared-arena" ? AggregateStateMemory.allocate(scope) : undefined;
    const start = performance.now();
    for (let i = 0; i < directories; i++) {
      if (arena) {
        const state = arena.create({ n: { op: "count" } });
        arena.updateValue(state.n, i);
        states.push(state);
      } else {
        const owner = new OwnedAggregateState(scope, { n: { op: "count" } });
        owner.updateValue("n", i);
        owners.push(owner);
      }
    }
    const constructionMs = performance.now() - start;
    globalThis.gc();
    const heapGrowthBytes = process.memoryUsage().heapUsed - before;
    const admittedBytes = pool.usedBytes;
    const constructionReservations = reservations;
    for (const state of states) assert.equal(state.n.count, 1);
    for (const owner of owners) {
      const output = owner.finish();
      assert.deepEqual(output.value, { n: 1 });
      output.release();
    }
    scope.dispose();
    owners.length = states.length = 0;
    assert.equal(pool.usedBytes, 0);
    results.push({
      directories,
      kind,
      constructionReservations,
      admittedBytes,
      heapGrowthBytes,
      constructionMs,
      remainingBytes: pool.usedBytes,
    });
  }
}
writeFileSync(
  "docs/benchmarks/aggregate-arena-accounting-2026-11-13.json",
  `${JSON.stringify({ description: "Diagnostic directory ownership comparison. Node heap after explicit GC; one timing sample. Not grouped-query integration or a Worker heap guarantee.", node: process.version, results }, null, 2)}\n`,
);
console.log(results);
