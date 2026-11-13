import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { MemoryBudget } from "../packages/core/dist/index.js";

const moduleUrl = process.env.LAKEQL_SCOPE_MODULE
  ? pathToFileURL(process.env.LAKEQL_SCOPE_MODULE)
  : new URL("../packages/core/dist/memory-scope.js", import.meta.url);
const { MemoryScope } = await import(moduleUrl);
const sourceSha256 = createHash("sha256")
  .update(await readFile(moduleUrl))
  .digest("hex");
assert.equal(typeof global.gc, "function");
const mode = process.env.LAKEQL_RETAIN_MODE ?? "composite";
assert.ok(["composite", "snapshot"].includes(mode));
function sample(buffers) {
  global.gc();
  const budget = new MemoryBudget(buffers);
  const scope = new MemoryScope(budget);
  for (let i = 0; i < buffers; i++) scope.allocate(1, "backing", () => new Uint8Array(1));
  const composite = scope.retainAllocations({ buffers });
  if (mode === "composite") scope.dispose();
  global.gc();
  const baseline = process.memoryUsage().heapUsed;
  const start = performance.now();
  const handles = Array.from({ length: 100 }, () =>
    mode === "composite" ? composite.retain() : scope.retainAllocations({ buffers }),
  );
  const retainMs = performance.now() - start;
  global.gc();
  const heapDelta = process.memoryUsage().heapUsed - baseline;
  composite.release();
  scope.dispose();
  assert.equal(budget.usedBytes, buffers);
  for (const handle of handles) handle.release();
  assert.equal(budget.usedBytes, 0);
  return { retainMs, heapDelta };
}
for (const buffers of [10, 100, 1000]) {
  const samples = [];
  for (let trial = 0; trial < 6; trial++) {
    const result = sample(buffers);
    if (trial > 0) samples.push(result);
  }
  const median = (key) => samples.map((x) => x[key]).sort((a, b) => a - b)[2];
  console.log(
    JSON.stringify({
      buffers,
      mode,
      retains: 100,
      sourceSha256,
      node: process.version,
      retainMedianMs: median("retainMs"),
      heapDeltaMedianBytes: median("heapDelta"),
      samples: 5,
      scope:
        "same-budget ownership retention (see mode); preexisting backing excluded; Node post-GC heap delta; no I/O or Worker heap-cap claim",
    }),
  );
}
