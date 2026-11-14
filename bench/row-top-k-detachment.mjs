import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { memoryStore } from "../packages/core/dist/memory-store.js";
import { Lake } from "../packages/core/dist/query.js";

const results = [];
for (const width of [64, 65536]) {
  const samples = [];
  let allocatedBinaryBytes = 0;
  let peakBytes = 0;
  for (let round = 0; round < 5; round++) {
    const pool = new MemoryBudget(64 * 1024 * 1024);
    const store = memoryStore();
    await store.put("table", new Uint8Array(1));
    const bytes = new Uint8Array(width);
    const lake = new Lake({
      store,
      memoryPool: pool,
      scanner: {
        async *scan() {
          for (let id = 0; id < 4096; id++) yield [{ id, bytes }];
        },
      },
    });
    let copied = 0;
    const original = MemoryBudget.prototype.reserve;
    MemoryBudget.prototype.reserve = function (size, purpose) {
      if (purpose === "detached row bytes") copied += size;
      return original.call(this, size, purpose);
    };
    try {
      const start = performance.now();
      const rows = await lake
        .path("table")
        .orderBy([{ column: "id" }])
        .limit(8)
        .toArray();
      const elapsed = performance.now() - start;
      assert.deepEqual(
        rows.map((row) => row.id),
        [0, 1, 2, 3, 4, 5, 6, 7],
      );
      assert.equal(pool.usedBytes, 0);
      if (round >= 2) samples.push(elapsed);
      allocatedBinaryBytes = copied;
      peakBytes = pool.peakBytes;
    } finally {
      MemoryBudget.prototype.reserve = original;
    }
  }
  results.push({
    width,
    rows: 4096,
    k: 8,
    medianMilliseconds: [...samples].sort((a, b) => a - b)[1],
    samples,
    allocatedBinaryBytes,
    peakBytes,
  });
}
const report = {
  description:
    "Local borrowed row scanner, ascending input, two warmups and three timed rounds. Eight winners, all later rows rejected. Binary allocation charge includes view overhead. Input payload is shared/borrowed and outside the pool; this measures detached copies, not total VM heap or remote latency.",
  sourceHashes: Object.fromEntries(
    ["query", "owned-bounded-top-k", "bounded-top-k", "owned-row-detachment"].map((name) => [
      name,
      createHash("sha256")
        .update(readFileSync(`packages/core/src/${name}.ts`))
        .digest("hex"),
    ]),
  ),
  results,
};
writeFileSync(
  process.env.LAKEQL_ROW_TOP_K_REPORT ??
    "docs/benchmarks/row-top-k-detachment-before-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results));
