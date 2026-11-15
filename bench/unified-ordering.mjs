// Run pnpm bench:ordering. Fixtures/checks are outside timed regions.
import assert from "node:assert/strict";
import { BoundedSelection } from "../packages/core/dist/query-sort.js";

const rows = 8192;
const reports = [];
for (const k of [8, 64, 512]) {
  for (const distribution of ["ascending", "descending", "mixed"]) {
    const values = Array.from({ length: rows }, (_, i) =>
      distribution === "ascending"
        ? i
        : distribution === "descending"
          ? rows - i
          : (i * 7919) % rows,
    );
    const expected = [...values].sort((a, b) => a - b).slice(0, k);
    const samples = { scan: [], queue: [] };
    let counts;
    for (let round = 0; round < 8; round++) {
      counts = {};
      for (const mode of round % 2 ? ["queue", "scan"] : ["scan", "queue"]) {
        let comparisons = 0;
        const compare = (a, b) => {
          comparisons++;
          return a - b;
        };
        const start = performance.now();
        let result;
        if (mode === "scan") {
          const kept = [];
          for (const value of values) {
            if (kept.length < k) {
              kept.push(value);
              continue;
            }
            let worst = 0;
            for (let i = 1; i < kept.length; i++) if (compare(kept[i], kept[worst]) > 0) worst = i;
            if (compare(value, kept[worst]) < 0) kept[worst] = value;
          }
          result = kept.sort(compare);
        } else {
          const selection = new BoundedSelection(k, compare);
          for (const value of values) selection.add(value);
          result = selection.finish();
        }
        const elapsed = performance.now() - start;
        assert.deepEqual(result, expected);
        counts[mode] = comparisons;
        if (round >= 2) samples[mode].push(elapsed);
      }
    }
    const median = (a) => {
      const sorted = [...a].sort((a, b) => a - b);
      return (sorted[2] + sorted[3]) / 2;
    };
    reports.push({
      k,
      distribution,
      scanMs: median(samples.scan),
      queueMs: median(samples.queue),
      comparisons: counts,
      retainedCandidates: k,
    });
  }
}
console.log(
  JSON.stringify(
    {
      node: process.version,
      rows,
      scope:
        "Alternating bounded-selection kernels; 2 warmups and 6 samples. Independent full-sort parity checked after timing. Counts include final ordering. Retained candidate bound is k plus fixed bookkeeping; this does not measure total heap or cloud throughput.",
      reports,
    },
    null,
    2,
  ),
);
