import { writeFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import {
  col,
  detachOwnedRow,
  evaluateWindowWorkUnitPartials,
  indexImmutableBacking,
  MemoryBudget,
  MemoryScope,
  partitionWindowWorkUnitRows,
} from "../packages/core/dist/index.js";

const windows = {
  rn: { fn: "row_number", args: [], over: { partitionBy: [], orderBy: [{ expr: col("id") }] } },
};
const results = [];
for (const phase of ["partition", "evaluate"])
  for (const characters of [32, 4096])
    for (const count of [128, 512])
      for (const indexed of [false, true]) {
        const samples = [];
        let peakBytes = 0;
        for (let repeat = 0; repeat < 5; repeat++) {
          const pool = new MemoryBudget(128 * 1024 * 1024);
          const source = new MemoryScope(pool);
          const output = new MemoryScope(pool);
          const rows = Array.from({ length: count }, (_, id) => ({
            ordinal: { task: 0, row: id },
            row: source.adopt(detachOwnedRow({ id, text: `${id}:${"x".repeat(characters)}` }, pool))
              .value,
          }));
          try {
            const start = performance.now();
            let index;
            if (indexed) {
              const owner = source.retainAllocations(undefined);
              try {
                index = indexImmutableBacking(owner, output);
              } finally {
                owner.release();
              }
            }
            const options = {
              resultScope: output,
              ...(index === undefined ? {} : { immutableSource: index }),
            };
            const result =
              phase === "partition"
                ? partitionWindowWorkUnitRows(rows, windows, 1, options).buckets[0].rows.map(
                    (entry) => entry.row,
                  )
                : await evaluateWindowWorkUnitPartials(
                    [{ bucketCount: 1, buckets: [{ bucket: 0, rows }] }],
                    windows,
                    options,
                  );
            const elapsed = performance.now() - start;
            if (
              result.length !== count ||
              result[count - 1].text !== rows[count - 1].row.text ||
              (phase === "evaluate" && result[count - 1].rn !== count)
            )
              throw new Error("Incorrect result");
            if (repeat > 0) samples.push(elapsed);
            peakBytes = pool.peakBytes;
          } finally {
            output.dispose();
            source.dispose();
          }
          if (pool.usedBytes !== 0) throw new Error("Leaked reservation");
        }
        samples.sort((a, b) => a - b);
        results.push({
          phase,
          characters,
          count,
          indexed,
          medianMs: (samples[1] + samples[2]) / 2,
          peakBytes,
        });
      }
await writeFile(
  "docs/benchmarks/window-task-ownership-2026-11-14.json",
  `${JSON.stringify(
    {
      scope:
        "Local core kernel; includes stage index construction, excludes fixture construction, correctness checks and disposal. One warmup and four samples. Reservations are not VM heap measurements.",
      results,
    },
    null,
    2,
  )}\n`,
);
