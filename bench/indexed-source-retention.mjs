import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createNumericPressureFixture } from "../fixtures/src/numeric-pressure.ts";
import { MemoryBudget, MemoryScope, memoryStore } from "../packages/core/dist/index.js";
import { acquireIndexedParquetSource } from "../packages/parquet/dist/indexed-source.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/store-buffer.js";

assert.equal(typeof global.gc, "function", "Run with node --expose-gc");
const held = [];
async function prepare(close) {
  const store = memoryStore();
  await store.put("footer.parquet", createNumericPressureFixture());
  const file = await asyncBufferFromStore(store, "footer.parquet");
  const observations = [];
  const traced = {
    ...file,
    async sliceOwned(...args) {
      const owner = await file.sliceOwned(...args);
      observations.push({ close, kind: "footer-buffer", weak: new WeakRef(owner.value) });
      return owner;
    },
  };
  const pool = new MemoryBudget(1024 * 1024);
  const parent = new MemoryScope(pool);
  const source = await acquireIndexedParquetSource(traced, parent);
  observations.push({ close, kind: "metadata", weak: new WeakRef(source.metadata) });
  observations.push({ close, kind: "input-source", weak: new WeakRef(traced) });
  if (close === "source") source.dispose();
  parent.dispose();
  assert.equal(pool.usedBytes, 0);
  assert.throws(() => source.metadata, /closed/);
  held.push(source);
  return observations;
}
const observations = [...(await prepare("source")), ...(await prepare("parent"))];
for (let attempt = 0; attempt < 10; attempt++) {
  await new Promise(setImmediate);
  global.gc();
  global.gc();
  if (observations.every(({ weak }) => weak.deref() === undefined)) break;
}
const results = observations.map(({ close, kind, weak }) => ({
  close,
  kind,
  collected: weak.deref() === undefined,
}));
console.log(
  JSON.stringify(
    {
      node: process.version,
      sourceSha256: createHash("sha256")
        .update(
          await readFile(new URL("../packages/parquet/src/indexed-source.ts", import.meta.url)),
        )
        .digest("hex"),
      heldClosedSources: held.length,
      results,
      finalAdmittedBytes: 0,
      limitation:
        "Forced-GC Node reachability diagnostic, not a production GC requirement or total-runtime cap.",
    },
    null,
    2,
  ),
);
if (process.env.LAKEQL_EXPECT_INDEXED_COLLECTION === "1")
  assert.ok(results.every(({ collected }) => collected));
