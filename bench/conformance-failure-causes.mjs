import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { MemoryBudget, memoryStore } from "../packages/core/dist/index.js";
import { createParquetLake, readParquetObjects } from "../packages/parquet/dist/index.js";

// Diagnostic only: the full conformance gate remains authoritative and failing.
const inventory = JSON.parse(
  readFileSync("docs/benchmarks/conformance-owned-2026-11-13.json", "utf8"),
);
const files = [
  ...new Set(
    inventory.failures.map((name) => name.match(/(?:data|shredded_variant)\/.*?\.parquet/)[0]),
  ),
];
const results = [];
for (const file of files)
  for (const reader of ["public", "owned"]) {
    const store = memoryStore();
    await store.put(file, readFileSync(`fixtures/external/parquet-testing/${file}`));
    const pool = new MemoryBudget(64 * 1024 * 1024);
    let rows = 0;
    let failure;
    try {
      if (reader === "public") rows = (await readParquetObjects(store, file)).length;
      else {
        const lake = createParquetLake({
          store,
          memoryPool: pool,
          scanRangeCache: { maxBytes: 0 },
          budget: { maxAllocatedMemoryBytes: pool.maxBytes },
        });
        for await (const _row of lake.path(file).batchSize(128).run().rows()) rows++;
      }
    } catch (error) {
      failure = [];
      for (
        let cause = error;
        cause && failure.length < 8;
        cause = cause.details?.cause ?? cause.cause
      ) {
        failure.push({ name: cause.name, code: cause.code, message: cause.message });
      }
    }
    assert.equal(pool.usedBytes, 0);
    results.push({
      file,
      reader,
      rowsBeforeFailure: rows,
      failure,
      peakAdmittedBytes: reader === "owned" ? pool.peakBytes : undefined,
      remainingBytes: reader === "owned" ? pool.usedBytes : undefined,
    });
  }
writeFileSync(
  "docs/benchmarks/conformance-failure-causes-2026-11-13.json",
  `${JSON.stringify({ description: "Current causes for the currently listed failing conformance cases; no skips, expected-failure changes or support claims. Public reader is unscoped; owned reader uses a 64 MiB pool and verifies cleanup.", node: process.version, results }, null, 2)}\n`,
);
console.log(
  JSON.stringify(
    results.map(({ file, reader, failure }) => ({ file, reader, cause: failure?.at(-1) })),
    null,
    2,
  ),
);
