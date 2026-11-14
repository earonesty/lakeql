import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { batchFromColumns } from "../packages/core/dist/batch.js";
import { materializeOwnedBatchRows } from "../packages/core/dist/materialization-plan.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";
import { OwnedRowCollector } from "../packages/core/dist/owned-row-collector.js";
import { applyWindowsToVectorBatches } from "../packages/core/dist/window-vector.js";

assert.equal(typeof globalThis.gc, "function", "Run Node with --expose-gc");
const totalRows = 1024;
const cases = [];
function collect(chunks) {
  const budget = new MemoryBudget(16 * 1024 * 1024);
  const collector = new OwnedRowCollector(budget);
  const directories = [];
  for (let index = 0; index < chunks; index++) {
    const input = new MemoryScope(budget);
    try {
      const batch = batchFromColumns({
        id: Array.from(
          { length: totalRows / chunks },
          (_, row) => (index * totalRows) / chunks + row,
        ),
      });
      // Borrowed fixture construction is outside this output-ownership probe.
      const source = input.allocate(0, "borrowed fixture handle", () => batch);
      const rows = materializeOwnedBatchRows(source);
      try {
        directories.push(new WeakRef(rows.value));
        collector.append(rows);
      } finally {
        rows.release();
      }
    } finally {
      input.dispose();
    }
  }
  const output = collector.finish();
  assert.deepEqual(
    output.value.map((row) => row.id),
    Array.from({ length: totalRows }, (_, i) => i),
  );
  return { budget, output, directories };
}
async function collectGarbage() {
  for (let i = 0; i < 8; i++) {
    await delay(0);
    globalThis.gc();
  }
}
function alive(references) {
  return references.filter((reference) => reference.deref() !== undefined).length;
}
for (const chunks of [1, 4, 64]) {
  const { budget, output, directories } = collect(chunks);
  await collectGarbage();
  const retainedDirectories = alive(directories);
  const retainedBytes = budget.usedBytes;
  const peakBytes = budget.peakBytes;
  output.release();
  assert.equal(budget.usedBytes, 0);
  await collectGarbage();
  const afterReleaseDirectories = alive(directories);
  assert.equal(afterReleaseDirectories, 0);
  cases.push({
    route: "collector",
    chunks,
    totalRows,
    retainedDirectories,
    afterReleaseDirectories,
    retainedBytes,
    peakBytes,
    finalBytes: budget.usedBytes,
  });
}
for (const chunks of [1, 4, 64]) {
  const budget = new MemoryBudget(16 * 1024 * 1024);
  const scope = new MemoryScope(budget);
  const inputs = Array.from({ length: chunks }, (_, chunk) => ({
    batch: batchFromColumns({
      id: Array.from(
        { length: totalRows / chunks },
        (_, row) => (chunk * totalRows) / chunks + row,
      ),
    }),
  }));
  const rows = await applyWindowsToVectorBatches(
    inputs,
    {},
    { memoryBudget: budget, resultScope: scope },
  );
  assert.deepEqual(
    rows.map((row) => row.id),
    Array.from({ length: totalRows }, (_, i) => i),
  );
  const retainedBytes = budget.usedBytes;
  const peakBytes = budget.peakBytes;
  scope.dispose();
  assert.equal(budget.usedBytes, 0);
  cases.push({
    route: "vector-window",
    chunks,
    totalRows,
    retainedBytes,
    peakBytes,
    finalBytes: budget.usedBytes,
  });
}
console.log(
  JSON.stringify(
    {
      node: process.version,
      scope:
        "Local output-ownership diagnostic on identical numeric rows at different batch fragmentation. Borrowed input construction excluded. Forced GC tests reachability only, not production reclamation or total-runtime caps. Collector source directories are weakly observed while final output is pinned and after release; one-chunk collection returns the original directory legitimately. Vector-window values and admission are measured without a reachability claim. No wall-clock performance claim.",
      cases,
    },
    null,
    2,
  ),
);
