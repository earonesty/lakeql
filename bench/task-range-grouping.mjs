import assert from "node:assert/strict";
import { groupAggregateTasks } from "../packages/parquet/dist/task.js";

// Preserve the previous unadmitted clone in the benchmark baseline only.
function cloneTaskWithRanges(task, ranges) {
  return {
    ...task,
    rowGroupRanges: ranges.map((range) => ({ ...range })),
    partitionValues: { ...task.partitionValues },
  };
}

function baseline(tasks) {
  const groups = new Map();
  for (const task of tasks) {
    const key = JSON.stringify({ path: task.path, partitionValues: task.partitionValues });
    const existing = groups.get(key);
    if (!existing) {
      groups.set(key, cloneTaskWithRanges(task, task.rowGroupRanges));
      continue;
    }
    const sorted = [...existing.rowGroupRanges, ...task.rowGroupRanges]
      .map((r) => ({ ...r }))
      .sort((a, b) => a.start - b.start || a.end - b.end);
    const merged = [];
    for (const range of sorted) {
      const previous = merged.at(-1);
      if (previous && previous.end >= range.start) previous.end = Math.max(previous.end, range.end);
      else merged.push(range);
    }
    existing.rowGroupRanges = merged;
  }
  return [...groups.values()];
}
const results = [];
for (const count of [1000, 4000]) {
  const tasks = Array.from({ length: count }, (_, i) => ({
    path: "data",
    partitionValues: {},
    rowGroupRanges: [{ start: (count - i) * 2, end: (count - i) * 2 + 1 }],
  }));
  assert.deepEqual(groupAggregateTasks(tasks), baseline(tasks));
  for (const [name, run] of [
    ["repeated-merge-baseline", baseline],
    ["single-merge", groupAggregateTasks],
  ]) {
    const samples = [];
    for (let round = 0; round < 4; round++) {
      const start = performance.now();
      run(tasks);
      if (round > 0) samples.push(performance.now() - start);
    }
    samples.sort((a, b) => a - b);
    results.push({ count, name, medianMs: samples[1] });
  }
}
console.log(
  JSON.stringify(
    {
      warmups: 1,
      samples: 3,
      scope:
        "Local grouping only; baseline reproduces previous range merge with simplified equivalent keys for these empty-partition fixtures. No I/O or heap-cap measurement.",
      results,
    },
    null,
    2,
  ),
);
