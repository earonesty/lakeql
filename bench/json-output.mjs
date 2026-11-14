import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { decimalFromParts } from "../packages/core/dist/decimal-value.js";
import { jsonSafeValue } from "../packages/core/dist/evaluator.js";
import { encodeOwnedJsonOutput } from "../packages/core/dist/json-output-encoding.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

const encoder = new TextEncoder();
const cases = {
  small: { id: 42, name: "example", active: true },
  decimal: {
    values: Array.from({ length: 1000 }, (_, i) =>
      decimalFromParts(123456789987654321n + BigInt(i), 9),
    ),
  },
  fractional: { values: Array.from({ length: 1000 }, (_, i) => i + 0.25) },
  ascii: { text: "abcdefghij".repeat(10000) },
  unicode: { text: '雪😀\n"'.repeat(20000) },
  binary: { data: Uint8Array.from({ length: 100000 }, (_, i) => i & 255) },
  nested: { rows: Array.from({ length: 1000 }, (_, i) => ({ id: i, values: [i, null, "雪"] })) },
};
const results = [];
for (const [name, value] of Object.entries(cases)) {
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const scope = new MemoryScope(pool);
  const legacy = () => encoder.encode(JSON.stringify(jsonSafeValue(value)));
  const owned = () => encodeOwnedJsonOutput(value, scope);
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
    iterations,
    samples,
    medianMilliseconds: Object.fromEntries(
      Object.entries(samples).map(([key, values]) => [key, [...values].sort((a, b) => a - b)[3]]),
    ),
  });
}
const sources = [
  "decimal-value",
  "json-output-encoding",
  "json-scalar-output",
  "json-integer-output",
  "json-output-size",
  "json-string-size",
  "evaluator",
  "scalar-cast-allocation",
  "scalar-allocation",
  "interval",
  "timestamp",
].map((name) => [
  `${name}.ts`,
  createHash("sha256")
    .update(readFileSync(new URL(`../packages/core/src/${name}.ts`, import.meta.url)))
    .digest("hex"),
]);
const report = {
  node: process.version,
  description:
    "Synthetic output encoding only; byte parity checked before timing. Two warm-up rounds and seven measured alternating-order rounds. Owned includes admission and release; legacy clones JSON-safe values, stringifies, and encodes without admission. No IO or total heap claim.",
  sources: Object.fromEntries(sources),
  results,
};
writeFileSync(
  new URL(
    process.env.LAKEQL_JSON_OUTPUT_REPORT ?? "../docs/benchmarks/json-output-2026-11-13.json",
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
