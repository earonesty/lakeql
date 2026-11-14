import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";
import { rejectUnsupportedParquetSchema as current } from "../packages/parquet/dist/schema.js";

const baselineCommit = "1d05228";
const baselineSource = execFileSync(
  "git",
  ["show", `${baselineCommit}:packages/parquet/src/schema.ts`],
  { encoding: "utf8" },
);
const currentSource = readFileSync(
  new URL("../packages/parquet/src/schema.ts", import.meta.url),
  "utf8",
);
const temporary = mkdtempSync(join(tmpdir(), "lakeql-schema-bench-"));
const results = [];
try {
  const baselineFile = join(temporary, "baseline.mjs");
  const source = baselineSource.replace(
    'from "lakeql-core"',
    `from ${JSON.stringify(new URL("../packages/core/dist/index.js", import.meta.url).href)}`,
  );
  writeFileSync(
    baselineFile,
    ts.transpileModule(source, {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
    }).outputText,
  );
  const { rejectUnsupportedParquetSchema: baseline } = await import(
    pathToFileURL(baselineFile).href
  );
  for (const depth of [16, 128, 512, 2048, 8192, 20000]) {
    const metadata = {
      schema: [
        { name: "root", num_children: 1 },
        ...Array.from({ length: depth }, (_, index) => ({ name: `g${index}`, num_children: 1 })),
        { name: "value", type: "INT32" },
      ],
    };
    for (const selected of [true, false]) {
      const options = selected ? {} : { columns: [] };
      const measurements = {};
      // Alternate order across sizes to reduce a fixed before/after timing bias.
      const readers =
        depth === 128 || depth === 2048 || depth === 20000
          ? [
              ["current", current],
              ["baseline", baseline],
            ]
          : [
              ["baseline", baseline],
              ["current", current],
            ];
      for (const [name, validate] of readers) {
        try {
          validate(metadata, options);
          const samples = [];
          for (let sample = 0; sample < 5; sample++) {
            const started = performance.now();
            for (let iteration = 0; iteration < 10; iteration++) validate(metadata, options);
            samples.push((performance.now() - started) / 10);
          }
          samples.sort((a, b) => a - b);
          measurements[name] = { medianMilliseconds: samples[2], samples };
        } catch (error) {
          if (name === "current") throw error;
          measurements[name] = { error: String(error) };
        }
      }
      assert.equal(measurements.current.error, undefined);
      results.push({ depth, selected, measurements });
    }
  }
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
const report = {
  node: process.version,
  baselineCommit,
  baselineSourceSha256: createHash("sha256").update(baselineSource).digest("hex"),
  currentSourceSha256: createHash("sha256").update(currentSource).digest("hex"),
  description:
    "Synthetic schema-only traversal, construction outside timed regions. Five samples of ten validations after warm-up. No IO, codec, heap-cap, or whole-query speed claim. Current implementation uses one mutable path and child-count stack; baseline copied paths and joined every node.",
  results,
};
writeFileSync(
  new URL(
    process.env.LAKEQL_SCHEMA_BENCH_REPORT ??
      "../docs/benchmarks/schema-validation-2026-11-13.json",
    import.meta.url,
  ),
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(
  JSON.stringify(
    results.map(({ depth, selected, measurements }) => ({
      depth,
      selected,
      baseline: measurements.baseline.medianMilliseconds ?? measurements.baseline.error,
      current: measurements.current.medianMilliseconds,
    })),
    null,
    2,
  ),
);
