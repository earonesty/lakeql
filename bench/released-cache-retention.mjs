import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { MemoryBudget, MemoryScope, SharedMemoryCache } from "../packages/core/dist/index.js";

assert.equal(typeof global.gc, "function", "Run with node --expose-gc");
const held = [];
function prepare(owned) {
  const budget = new MemoryBudget(2 * 1024 * 1024);
  const cache = new SharedMemoryCache({ memoryBudget: budget });
  const scope = new MemoryScope(budget);
  const payload = new Uint8Array(1024 * 1024);
  if (owned) {
    const owner = scope.allocate(payload.byteLength, "cache payload", () => payload);
    cache.setOwned("payload", owner);
  } else cache.set("payload", payload, payload.byteLength);
  const lease = cache.acquire("payload");
  assert.ok(lease);
  const weak = new WeakRef(lease.value);
  scope.dispose();
  cache.delete("payload");
  assert.equal(budget.usedBytes, payload.byteLength);
  lease.release();
  lease.release();
  assert.equal(budget.usedBytes, 0);
  held.push(lease);
  return { kind: owned ? "owned-entry" : "reserved-entry", weak };
}
const observations = [prepare(false), prepare(true)];
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
console.log(
  JSON.stringify(
    {
      node: process.version,
      sourceSha256: createHash("sha256")
        .update(
          await readFile(new URL("../packages/core/src/object-store-cache.ts", import.meta.url)),
        )
        .digest("hex"),
      heldReleasedLeases: held.length,
      results,
      finalAdmittedBytes: 0,
      limitation:
        "Forced-GC Node reachability diagnostic, not a production GC requirement or runtime cap.",
    },
    null,
    2,
  ),
);
if (process.env.LAKEQL_EXPECT_CACHE_COLLECTION === "1")
  assert.ok(results.every(({ collected }) => collected));
