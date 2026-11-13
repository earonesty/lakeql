import assert from "node:assert/strict";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

assert.equal(typeof global.gc, "function", "Run with node --expose-gc");
const handles = [];
const observations = [];
for (const kind of ["scoped", "retained", "composite", "sibling"]) {
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  const input = scope.allocate(1024 * 1024, "payload", () => new Uint8Array(1024 * 1024));
  let handle;
  if (kind === "scoped") handle = input;
  else if (kind === "retained") handle = input.retain();
  else {
    handle = scope.retainAllocations({ payload: input.value });
    if (kind === "sibling") {
      const first = handle;
      handle = first.retain();
      first.release();
      handles.push(first);
    }
  }
  const payload = new WeakRef(input.value);
  const container = new WeakRef(handle.value);
  scope.dispose();
  handle.release();
  handles.push(input, handle);
  assert.equal(scope.budget.usedBytes, 0);
  assert.throws(() => handle.value, /released/);
  observations.push({ kind, payload, container });
}
// Keep released handles live across turns and real GC. This isolates ownership
// retention from caller aliases; normal runtime code must not depend on forced GC.
for (let attempt = 0; attempt < 10; attempt++) {
  await new Promise(setImmediate);
  global.gc();
  global.gc();
  if (
    observations.every(
      ({ payload, container }) => payload.deref() === undefined && container.deref() === undefined,
    )
  )
    break;
}
for (const observation of observations) {
  assert.equal(observation.payload.deref(), undefined, `${observation.kind} retained payload`);
  assert.equal(observation.container.deref(), undefined, `${observation.kind} retained container`);
}
assert.equal(handles.length, 9);
console.log(
  JSON.stringify(
    {
      node: process.version,
      retainedReleasedHandles: handles.length,
      verified: observations.map(({ kind }) => kind),
      scope:
        "Real forced Node GC while released handles remain strongly held. Verifies no handle-to-payload retention; does not prove collection timing or Worker heap bounds.",
    },
    null,
    2,
  ),
);
