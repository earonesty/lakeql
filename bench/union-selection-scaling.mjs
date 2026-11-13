import { writeFileSync } from "node:fs";
import { isDeepStrictEqual } from "node:util";
import { vectorFromValues, vectorValue } from "../packages/core/dist/batch.js";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { concatOwnedBatches, gatherOwnedBatch } from "../packages/core/dist/vector-sort.js";

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
  if (vector.type === "dict") return 1 + depth(vector.dictionary);
  if (vector.type !== "union") return 0;
  return 1 + Math.max(0, ...vector.children.map(depth));
}
const results = [];
for (const appends of [32, 128, 512]) {
  const modes = {};
  for (const mode of ["selected", "strings", "binary", "struct", "list"]) {
    const valueAt = (i) => {
      if (mode === "struct") return { id: i, nested: { values: [i, `row-${i}`, null] } };
      if (mode === "list") return [i, `row-${i}`, [i + 1, null]];
      if (mode === "strings") return `row-${i}-${"x".repeat(128)}`;
      if (mode === "binary") {
        const backing = new Uint8Array(256).fill(i % 251);
        new DataView(backing.buffer).setUint32(16, i, true);
        return backing.subarray(16, 144);
      }
      return i;
    };
    const pool = new MemoryBudget(64 * 1024 * 1024);
    let output;
    const start = performance.now();
    output = input(pool, ["seed", 0]);
    for (let i = 1; i <= appends; i++) {
      const next = input(pool, [valueAt(i)]);
      let combined;
      try {
        combined = concatOwnedBatches([output, next], pool);
      } finally {
        output.release();
        next.release();
      }
      try {
        output = gatherOwnedBatch(combined, [2, 0], pool);
      } finally {
        combined.release();
      }
    }
    const elapsedMs = performance.now() - start;
    const vector = output.value.columns.value;
    if (
      !isDeepStrictEqual(vectorValue(vector, 0), valueAt(appends)) ||
      !isDeepStrictEqual(vectorValue(vector, 1), valueAt(appends - 1))
    )
      throw Error("Incorrect selected values");
    modes[mode] = {
      elapsedMs,
      retainedBytes: pool.usedBytes,
      peakBytes: pool.peakBytes,
      unionDepth: depth(vector),
    };
    output.release();
    if (pool.usedBytes !== 0) throw Error("Concat ownership leak");
  }
  results.push({ appends, rows: 2, modes });
}
writeFileSync(
  new URL(
    process.argv[2] ?? "../docs/benchmarks/union-selection-scaling-2026-11-13.json",
    import.meta.url,
  ),
  `${JSON.stringify(
    {
      scope:
        "Local owned concatenation including source construction; explicit allocation accounting, not measured VM heap. One diagnostic run per case, no timing speedup claim. Every output value checked after timing; all owners release to zero. Repeated append followed by selecting [newest, previous] to keep exactly two rows. No cloud transport.",
      results,
    },
    null,
    2,
  )}\n`,
);
