// Run with node --expose-gc. Optional module path supports same-runtime comparisons.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const budgetModuleUrl = process.env.LAKEQL_BUDGET_MODULE
  ? pathToFileURL(process.env.LAKEQL_BUDGET_MODULE)
  : new URL("../packages/core/dist/memory-budget.js", import.meta.url);
const { MemoryBudget } = await import(budgetModuleUrl);
const budgetSourceSha256 = createHash("sha256")
  .update(await readFile(budgetModuleUrl))
  .digest("hex");

const moduleUrl = process.env.LAKEQL_SCOPE_MODULE
  ? pathToFileURL(process.env.LAKEQL_SCOPE_MODULE)
  : new URL("../packages/core/dist/memory-scope.js", import.meta.url);
const { MemoryScope } = await import(moduleUrl);
const sourceSha256 = createHash("sha256")
  .update(await readFile(moduleUrl))
  .digest("hex");
assert.equal(typeof global.gc, "function", "Run with --expose-gc");
for (const count of [10000, 50000]) {
  const samples = [];
  for (let trial = 0; trial < 6; trial++) {
    global.gc();
    const baseline = process.memoryUsage().heapUsed;
    const budget = new MemoryBudget(count);
    const scope = new MemoryScope(budget);
    const owners = [];
    const start = performance.now();
    for (let index = 0; index < count; index++)
      owners.push(scope.allocate(1, "bookkeeping", () => new Uint8Array(1)));
    const allocateMs = performance.now() - start;
    global.gc();
    const liveHeapDelta = process.memoryUsage().heapUsed - baseline;
    const releaseStart = performance.now();
    for (let index = owners.length - 1; index >= 0; index--)
      scope.releaseAllocation(owners[index].value);
    const releaseMs = performance.now() - releaseStart;
    assert.equal(budget.usedBytes, 0);
    scope.dispose();
    owners.length = 0;
    if (trial > 0) samples.push({ allocateMs, liveHeapDelta, releaseMs });
  }
  const median = (key) => samples.map((x) => x[key]).sort((a, b) => a - b)[2];
  console.log(
    JSON.stringify({
      count,
      sourceSha256,
      budgetSourceSha256,
      node: process.version,
      allocateMedianMs: median("allocateMs"),
      releaseMedianMs: median("releaseMs"),
      liveHeapDeltaMedianBytes: median("liveHeapDelta"),
      samples: samples.length,
      scope:
        "Node V8 post-GC heap delta for live owners, values, reservations and indexes; one-byte typed buffer per owner; not a Worker heap-cap proof",
    }),
  );
}
