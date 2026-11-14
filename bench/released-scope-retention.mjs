import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

assert.equal(typeof global.gc, "function", "Run with node --expose-gc");
const held = [];
function scopedHandle() {
  const scope = new MemoryScope(new MemoryBudget(16));
  const handle = scope.allocate(16, "small value", () => ({ value: 1 }));
  const weak = new WeakRef(scope);
  scope.dispose();
  held.push(handle);
  return { kind: "released-scoped-handle", weak };
}
function disposedChild() {
  const parent = new MemoryScope(new MemoryBudget(16));
  const child = parent.childScope();
  const weak = new WeakRef(parent);
  parent.dispose();
  held.push(child);
  return { kind: "disposed-child", weak };
}
function adoptedOwner() {
  const scope = new MemoryScope(new MemoryBudget(16));
  const reservation = scope.budget.reserve(16, "external payload");
  const owner = {
    value: new Uint8Array(1024 * 1024),
    bytes: 16,
    budget: scope.budget,
    retain() {
      throw new Error("unused retain");
    },
    retainIn() {
      throw new Error("unused transfer");
    },
    release() {
      reservation.release();
    },
  };
  const weak = new WeakRef(owner.value);
  const handle = scope.adopt(owner);
  scope.dispose();
  assert.equal(scope.budget.usedBytes, 0);
  held.push(handle);
  return { kind: "released-adopted-payload", weak };
}
const observations = [scopedHandle(), disposedChild(), adoptedOwner()];
for (let attempt = 0; attempt < 10; attempt++) {
  await new Promise(setImmediate);
  global.gc();
  global.gc();
  if (observations.every(({ weak }) => weak.deref() === undefined)) break;
}
const results = observations.map(({ kind, weak }) => ({
  kind,
  collected: weak.deref() === undefined,
}));
assert.equal(held.length, 3);
console.log(
  JSON.stringify(
    {
      node: process.version,
      sourceSha256: createHash("sha256")
        .update(await readFile(new URL("../packages/core/dist/memory-scope.js", import.meta.url)))
        .digest("hex"),
      conditions:
        "Released handles and disposed child remain strongly held across event-loop turns and diagnostic Node GC. The external payload is deliberately larger than its illustrative charge; this tests reachability, not admission coverage or GC timing in production.",
      results,
    },
    null,
    2,
  ),
);
if (process.env.LAKEQL_RETENTION_OBSERVE !== "1")
  assert.ok(results.every(({ collected }) => collected));
