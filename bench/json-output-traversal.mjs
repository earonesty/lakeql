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
const scratch = await mkdtemp(join(tmpdir(), "lakeql-json-traversal-"));
const dist = pathToFileURL(`${resolve("packages/core/dist")}/`).href;
try {
  for (const name of ["json-output-size", "json-output-encoding"]) {
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
          `from "${file === "json-output-size.js" ? pathToFileURL(join(scratch, file)).href : dist + file}"`,
      );
    await writeFile(join(scratch, `${name}.js`), compiled);
  }
  const old = (await import(pathToFileURL(join(scratch, "json-output-encoding.js")).href))
    .encodeOwnedJsonOutput;
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
    ["deep-20000", deep, 2],
  ];
  const results = [];
  for (const [name, value, iterations] of cases) {
    const expected = new TextEncoder().encode(
      name === "deep-20000"
        ? `${"[".repeat(20000)}"雪😀"${"]".repeat(20000)}`
        : JSON.stringify(value),
    );
    const variants = {};
    for (const [variant, encode] of [
      ["reference", old],
      ["current", encodeOwnedJsonOutput],
    ]) {
      const samples = [];
      let peak = 0;
      let failure;
      for (let round = 0; round < 6; round++) {
        const start = performance.now();
        try {
          for (let i = 0; i < iterations; i++) {
            const budget = new MemoryBudget(64 * 1024 * 1024);
            const scope = new MemoryScope(budget);
            let output;
            try {
              output = encode(value, scope);
              if (i === 0) assert.deepEqual(output.value, expected);
              peak = Math.max(peak, budget.peakBytes);
            } finally {
              output?.release();
              scope.dispose();
              assert.equal(budget.usedBytes, 0);
            }
          }
        } catch (error) {
          failure = `${error.name}: ${error.message}`;
          break;
        }
        if (round > 0) samples.push((performance.now() - start) / iterations);
      }
      variants[variant] = {
        samplesMs: samples,
        peakTrackedBytes: peak,
        ...(failure ? { failure } : {}),
      };
    }
    results.push({ name, iterations, outputBytes: expected.length, variants });
  }
  const report = {
    reference,
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
      "Variants run sequentially; five measured rounds after warmup",
    ],
  };
  const json = `${JSON.stringify(report, null, 2)}\n`;
  if (process.env.LAKEQL_JSON_REPORT) await writeFile(process.env.LAKEQL_JSON_REPORT, json);
  else process.stdout.write(json);
} finally {
  await rm(scratch, { recursive: true, force: true });
}
