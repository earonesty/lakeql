import { writeFileSync } from "node:fs";
import { vectorFromValues, vectorValue } from "../packages/core/dist/batch.js";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { concatOwnedBatches } from "../packages/core/dist/vector-sort.js";

function input(pool, values) {
  const scope = new MemoryScope(pool);
  try {
    const vector = vectorFromValues(values, scope);
    return scope.retainAllocations({ rowCount: values.length, columns: { value: vector } });
  } finally {
    scope.dispose();
  }
}
function depth(vector) {
  if (vector.type !== "union") return 0;
  return 1 + Math.max(0, ...vector.children.map(depth));
}
const results = [];
for (const appends of [32, 128, 512]) {
  const modes = {};
  for (const mode of ["oneShot", "incremental"]) {
    const pool = new MemoryBudget(64 * 1024 * 1024);
    let output;
    const start = performance.now();
    if (mode === "oneShot") {
      const inputs = [input(pool, ["seed", 0])];
      try {
        for (let i = 1; i <= appends; i++) inputs.push(input(pool, [i]));
        output = concatOwnedBatches(inputs, pool);
      } finally {
        for (const owner of inputs) owner.release();
      }
    } else {
      output = input(pool, ["seed", 0]);
      for (let i = 1; i <= appends; i++) {
        const next = input(pool, [i]);
        let combined;
        try {
          combined = concatOwnedBatches([output, next], pool);
        } finally {
          output.release();
          next.release();
        }
        output = combined;
      }
    }
    const elapsedMs = performance.now() - start;
    const vector = output.value.columns.value;
    if (vectorValue(vector, 0) !== "seed") throw Error("Missing seed");
    for (let i = 0; i <= appends; i++)
      if (vectorValue(vector, i + 1) !== i) throw Error("Incorrect appended value");
    modes[mode] = {
      elapsedMs,
      retainedBytes: pool.usedBytes,
      peakBytes: pool.peakBytes,
      unionDepth: depth(vector),
    };
    output.release();
    if (pool.usedBytes !== 0) throw Error("Concat ownership leak");
  }
  results.push({ appends, rows: appends + 2, modes });
}
writeFileSync(
  new URL(
    process.argv[2] ?? "../docs/benchmarks/union-concat-scaling-2026-11-13.json",
    import.meta.url,
  ),
  `${JSON.stringify(
    {
      scope:
        "Local owned concatenation including source construction; explicit allocation accounting, not measured VM heap. One diagnostic run per case, no timing speedup claim. Every output value checked after timing; all owners release to zero. Compare one-shot versus repeated appends with identical values. No cloud transport.",
      results,
    },
    null,
    2,
  )}\n`,
);
