import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";
import { encodeOwnedJsonOutput } from "../packages/core/dist/json-output-encoding.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

// Compare the selected reference with current built output using identical
// inputs, budgets, runtime, and byte verification. Timing includes both passes.
const reference = process.env.LAKEQL_JSON_REFERENCE ?? "5e4d280";
const referenceModules = [
  "json-output-size",
  "json-output-encoding",
  ...(process.env.LAKEQL_JSON_REFERENCE_HELPERS ?? "").split(",").filter(Boolean),
];
if (referenceModules.some((name) => !/^[a-z-]+$/.test(name)))
  throw new Error("Invalid reference module name");
if (process.env.LAKEQL_JSON_REFERENCE_BUDGET === "1") referenceModules.push("memory-budget");
const scratch = await mkdtemp(join(tmpdir(), "lakeql-json-traversal-"));
const dist = pathToFileURL(`${resolve("packages/core/dist")}/`).href;
try {
  for (const name of referenceModules) {
    const source = execFileSync("git", ["show", `${reference}:packages/core/src/${name}.ts`], {
      encoding: "utf8",
    });
    const compiled = ts
      .transpileModule(source, {
        compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
      })
      .outputText.replaceAll(
        /from "\.\/([^"]+)"/g,
        (_match, file) =>
          `from "${referenceModules.some((name) => file === `${name}.js`) ? pathToFileURL(join(scratch, file)).href : dist + file}"`,
      );
    await writeFile(join(scratch, `${name}.js`), compiled);
  }
  const old = (await import(pathToFileURL(join(scratch, "json-output-encoding.js")).href))
    .encodeOwnedJsonOutput;
  const ReferenceBudget = referenceModules.includes("memory-budget")
    ? (await import(pathToFileURL(join(scratch, "memory-budget.js")).href)).MemoryBudget
    : MemoryBudget;
  let deep = "雪😀";
  for (let i = 0; i < 20000; i++) deep = [deep];
  const cases = [
    ["small-row", { id: 1, name: "snow 雪", value: 2.25 }, 1000],
    ["wide-numbers", Array.from({ length: 10000 }, (_, i) => i + 0.25), 20],
    [
      "row-array",
      Array.from({ length: 1000 }, (_, i) => ({ id: i, text: "雪😀", flag: true })),
      20,
    ],
    [
      "mixed-siblings",
      Array.from({ length: 1000 }, (_, i) =>
        i % 3 === 0 ? [i, null, "雪😀"] : i % 3 === 1 ? { nested: { id: i } } : {},
      ),
      20,
    ],
    ["deep-20000", deep, 2],
  ];
  const results = [];
  for (const [name, value, iterations] of cases) {
    const expected = new TextEncoder().encode(
      name === "deep-20000"
        ? `${"[".repeat(20000)}"雪😀"${"]".repeat(20000)}`
        : JSON.stringify(value),
    );
    const encoders = [
      ["reference", old, ReferenceBudget],
      ["current", encodeOwnedJsonOutput, MemoryBudget],
    ];
    const variants = Object.fromEntries(
      encoders.map(([variant]) => [variant, { samplesMs: [], peakTrackedBytes: 0 }]),
    );
    // Alternate first runner to reduce systematic JIT/GC ordering bias.
    for (let round = 0; round < 12; round++) {
      for (const [variant, encode, Budget] of round % 2 === 0
        ? encoders
        : [...encoders].reverse()) {
        const result = variants[variant];
        if (result.failure) continue;
        const start = performance.now();
        try {
          for (let i = 0; i < iterations; i++) {
            const budget = new Budget(64 * 1024 * 1024);
            const scope = new MemoryScope(budget);
            let output;
            try {
              output = encode(value, scope);
              if (i === 0) assert.deepEqual(output.value, expected);
              result.peakTrackedBytes = Math.max(result.peakTrackedBytes, budget.peakBytes);
            } finally {
              output?.release();
              scope.dispose();
              assert.equal(budget.usedBytes, 0);
            }
          }
        } catch (error) {
          result.failure = `${error.name}: ${error.message}`;
          continue;
        }
        if (round >= 4) result.samplesMs.push((performance.now() - start) / iterations);
      }
    }
    results.push({ name, iterations, outputBytes: expected.length, variants });
  }
  const report = {
    reference,
    referenceModules,
    current: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
    node: process.version,
    builtModuleHashes: Object.fromEntries(
      await Promise.all(
        [
          "json-output-size",
          "json-output-encoding",
          "json-traversal-budget",
          "json-container-entries",
          "json-standard-scalars",
          "json-scalar-output",
          "memory-capacity",
          "memory-budget",
        ].map(async (name) => [
          name,
          createHash("sha256")
            .update(await readFile(`packages/core/dist/${name}.js`))
            .digest("hex"),
        ]),
      ),
    ),
    results,
    limitations: [
      "Kernel benchmark; not query or cloud throughput",
      "Tracked reservation units are not heap measurements",
      "Alternating variant order; eight measured rounds after four warmup rounds",
    ],
  };
  const json = `${JSON.stringify(report, null, 2)}\n`;
  if (process.env.LAKEQL_JSON_REPORT) await writeFile(process.env.LAKEQL_JSON_REPORT, json);
  else process.stdout.write(json);
} finally {
  await rm(scratch, { recursive: true, force: true });
}
