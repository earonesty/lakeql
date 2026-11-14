import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { encodeOwnedCsvOutput } from "../packages/core/dist/csv-output-encoding.js";
import { jsonSafeValue } from "../packages/core/dist/evaluator.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

const encoder = new TextEncoder();
const cases = {
  small: { id: 42, name: "example", active: true },
  ascii: { text: "abcdefghij".repeat(10000) },
  unicode: { text: '雪😀\n"'.repeat(20000) },
  binary: { data: Uint8Array.from({ length: 100000 }, (_, i) => i & 255) },
  nested: { rows: Array.from({ length: 1000 }, (_, i) => ({ id: i, values: [i, null, "雪"] })) },
};
const results = [];
for (const [name, value] of Object.entries(cases)) {
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const scope = new MemoryScope(pool);
  const columns = Object.keys(value);
  const legacy = () =>
    encoder.encode(
      `${columns
        .map((key) => {
          const safe = jsonSafeValue(value[key]);
          if (safe === null || safe === undefined) return "";
          const text =
            typeof safe === "string" || typeof safe === "number" || typeof safe === "boolean"
              ? String(safe)
              : JSON.stringify(safe);
          return /[",\n\r]/u.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
        })
        .join(",")}\n`,
    );
  const owned = () => encodeOwnedCsvOutput(value, columns, false, scope);
  const expected = legacy();
  const actual = owned();
  assert.deepEqual(actual.value, expected);
  actual.release();
  const samples = { legacy: [], owned: [] };
  const iterations = name === "small" ? 1000 : 20;
  for (let round = 0; round < 9; round++) {
    for (const variant of round % 2 ? ["owned", "legacy"] : ["legacy", "owned"]) {
      const start = performance.now();
      for (let i = 0; i < iterations; i++) {
        if (variant === "legacy") legacy();
        else owned().release();
      }
      if (round > 1) samples[variant].push((performance.now() - start) / iterations);
    }
  }
  assert.equal(pool.usedBytes, 0);
  scope.dispose();
  results.push({
    name,
    bytes: expected.length,
    peakAdmittedBytes: pool.peakBytes,
    iterations,
    samples,
    medianMilliseconds: Object.fromEntries(
      Object.entries(samples).map(([key, values]) => [key, [...values].sort((a, b) => a - b)[3]]),
    ),
  });
}
const sources = [
  "csv-output-encoding",
  "utf8-size",
  "json-output-encoding",
  "json-output-size",
  "json-string-size",
  "evaluator",
].map((name) => [
  `${name}.ts`,
  createHash("sha256")
    .update(readFileSync(new URL(`../packages/core/src/${name}.ts`, import.meta.url)))
    .digest("hex"),
]);
const report = {
  node: process.version,
  description:
    "Synthetic CSV row encoding only; byte parity checked before timing. Two warm-up rounds and seven measured alternating-order rounds. Owned includes admission and release; legacy converts JSON-safe cells, escapes/joins the row string, and encodes without admission. No IO or total heap claim.",
  sources: Object.fromEntries(sources),
  results,
};
writeFileSync(
  new URL(
    process.env.LAKEQL_CSV_OUTPUT_REPORT ?? "../docs/benchmarks/csv-output-2026-11-13.json",
    import.meta.url,
  ),
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(
  JSON.stringify(
    results.map(({ name, medianMilliseconds }) => ({ name, ...medianMilliseconds })),
    null,
    2,
  ),
);
