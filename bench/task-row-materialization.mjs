import {
  batchFromVectors,
  MemoryBudget,
  MemoryScope,
  planBatchMaterialization,
  planBatchRowMaterialization,
} from "../packages/core/dist/index.js";

const data = new TextEncoder().encode("界".repeat(4096));
const ranges = new Uint32Array(512 * 2);
for (let i = 0; i < 512; i++) ranges[i * 2 + 1] = data.length;
const batch = batchFromVectors({ text: { type: "utf8-view", data, length: 512, ranges } });
const results = [];
for (const mode of ["batch", "row"]) {
  const times = [];
  let peak = 0;
  for (let round = 0; round < 6; round++) {
    const budget = new MemoryBudget(64 * 1024 * 1024);
    const start = performance.now();
    for (let i = 0; i < (mode === "batch" ? 1 : 512); i++) {
      const scope = new MemoryScope(budget);
      try {
        const rows = (
          mode === "batch" ? planBatchMaterialization(batch) : planBatchRowMaterialization(batch, i)
        ).execute(scope);
        rows.release();
      } finally {
        scope.dispose();
      }
    }
    if (budget.usedBytes !== 0) throw new Error("leak");
    if (round > 0) times.push(performance.now() - start);
    peak = budget.peakBytes;
  }
  times.sort((a, b) => a - b);
  results.push({ mode, medianMs: times[2], peakReservedBytes: peak });
}
console.log(
  JSON.stringify({ rows: 512, codeUnitsPerRow: 4096, warmups: 1, samples: 5, results }, null, 2),
);
