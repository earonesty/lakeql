import assert from "node:assert/strict";
import { batchFromColumns, Lake, MemoryBudget, memoryStore } from "../packages/core/dist/index.js";

const results = [];
const batchRows = 256;
const payloadBytes = 128;
for (const inputOrder of ["ascending", "descending"]) {
  for (const k of [1, 64]) {
    for (const rows of [4096, 16384, 65536]) {
      const times = [];
      let peakReservedBytes = 0;
      let retainedOutputBytes = 0;
      for (let round = 0; round < 4; round++) {
        const budget = new MemoryBudget(64 * 1024 * 1024);
        const store = memoryStore();
        await store.put("table", new Uint8Array(1));
        let batches = 0;
        const lake = new Lake({
          store,
          memoryPool: budget,
          scanner: {
            scan() {
              throw Error("Unexpected row fallback");
            },
            async *scanColumns() {
              for (let start = 0; start < rows; start += batchRows) {
                const count = Math.min(batchRows, rows - start);
                const score = Array.from({ length: count }, (_, i) =>
                  inputOrder === "descending" ? rows - start - i - 1 : start + i,
                );
                const payload = score.map((value) =>
                  new Uint8Array(payloadBytes).fill(value % 251),
                );
                batches++;
                yield batchFromColumns({ score, payload });
                // A borrowed scanner is allowed to reuse every yielded source.
                for (const value of payload) value.fill(255);
              }
            },
          },
        });
        const start = performance.now();
        const output = await lake
          .path("table")
          .orderBy([{ column: "score" }])
          .limit(k)
          .batchSize(batchRows)
          .run()
          .toOwnedArray();
        const elapsed = performance.now() - start;
        try {
          assert.equal(output.value.length, k);
          for (let i = 0; i < k; i++) {
            assert.equal(output.value[i].score, i);
            assert.deepEqual(output.value[i].payload, new Uint8Array(payloadBytes).fill(i % 251));
          }
          assert.equal(batches, rows / batchRows);
          peakReservedBytes = budget.peakBytes;
          retainedOutputBytes = budget.usedBytes;
        } finally {
          output.release();
        }
        assert.equal(budget.usedBytes, 0);
        if (round > 0) times.push(elapsed);
      }
      times.sort((a, b) => a - b);
      results.push({
        inputOrder,
        rows,
        k,
        batches: rows / batchRows,
        medianMs: times[1],
        peakReservedBytes,
        retainedOutputBytes,
      });
    }
  }
}
for (const inputOrder of ["ascending", "descending"]) {
  for (const k of [1, 64]) {
    const runs = results.filter((result) => result.k === k && result.inputOrder === inputOrder);
    assert.equal(
      new Set(runs.map((run) => run.peakReservedBytes)).size,
      1,
      "Peak reservations must not grow with total input batches",
    );
    assert.equal(new Set(runs.map((run) => run.retainedOutputBytes)).size, 1);
  }
}
console.log(
  JSON.stringify(
    {
      runtime: { node: process.version, platform: process.platform, arch: process.arch },
      warmups: 1,
      samples: 3,
      batchRows,
      payloadBytes,
      poolBytes: 64 * 1024 * 1024,
      inputOrders: "ascending keeps early winners; descending replaces winners every batch",
      scope:
        "Local borrowed column scanner; current implementation scaling, not a before/after speedup. Scanner-created input and runtime heap are outside reservation measurements. No cloud latency claim.",
      results,
    },
    null,
    2,
  ),
);
