import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { MemoryBudget, memoryStore } from "../packages/core/dist/index.js";
import { createParquetLake, readParquetObjects } from "../packages/parquet/dist/index.js";

const root = "fixtures/external/parquet-testing/shredded_variant/";
const cases = JSON.parse(readFileSync(`${root}cases.json`, "utf8"));
const selected = new Set([26, 27, 28, 29, 70, 71, 72, 73, 112, 113, 114, 115]);
const results = [];
for (const item of cases.filter((item) => selected.has(item.case_number))) {
  const expected = /value=(-?\d+\.\d+)\)\)/.exec(item.variant)?.[1];
  assert.ok(expected, "fixture documentation must supply exact decimal digits");
  const bytes = readFileSync(`${root}${item.parquet_file}`);
  const store = memoryStore();
  await store.put(item.parquet_file, bytes);
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const outcomes = {};
  for (const mode of ["public", "owned"]) {
    let owner;
    try {
      if (mode === "owned")
        owner = await lake.path(item.parquet_file).batchSize(1).run().toOwnedArray();
      const rows = owner?.value ?? (await readParquetObjects(store, item.parquet_file));
      assert.equal(rows.length, 1);
      const actual = String(rows[0].var);
      outcomes[mode] = { actual, type: typeof rows[0].var, exact: actual === expected };
    } catch (error) {
      outcomes[mode] = { code: error.code, message: error.message, exact: false };
    } finally {
      owner?.release();
      assert.equal(pool.usedBytes, 0);
    }
  }
  results.push({
    fixture: item.parquet_file,
    sha256: createHash("sha256").update(bytes).digest("hex"),
    documentedValue: expected,
    outcomes,
    peakReservedBytes: pool.peakBytes,
  });
}
const report = {
  description:
    "Independent documented decimal digits from vendored cases.json, never converted to Number for expected values. Both readers over memoryStore; no cloud/HTTP or whole-heap claim. String comparison is exact for these fixed-form fixture values.",
  node: process.version,
  sources: Object.fromEntries(
    [
      "packages/parquet/src/parsers.ts",
      "packages/parquet/src/schema.ts",
      "packages/parquet/src/decoded-rows.ts",
      "packages/parquet/src/object-batches.ts",
      "packages/parquet/src/column-batches.ts",
      "patches/hyparquet@1.26.0.patch",
    ].map((path) => [path, createHash("sha256").update(readFileSync(path)).digest("hex")]),
  ),
  casesSha256: createHash("sha256")
    .update(readFileSync(`${root}cases.json`))
    .digest("hex"),
  results,
};
writeFileSync(
  process.env.LAKEQL_VARIANT_DECIMAL_REPORT ??
    "docs/benchmarks/variant-decimal-oracle-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(results, null, 2));
