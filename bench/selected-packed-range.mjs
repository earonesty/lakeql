// Compare the prior tuple range helper with current shared scalar range access.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";
import { vectorValue } from "../packages/core/dist/batch.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";
import { compactGatherPlan } from "../packages/core/dist/packed-gather.js";
import { planSelectedPacked } from "../packages/core/dist/selected-packed.js";
import { selectVector } from "../packages/core/dist/vector-selection.js";

const baseline = "c6a848a";
const source = execFileSync("git", ["show", `${baseline}:packages/core/src/selected-packed.ts`], {
  encoding: "utf8",
});
const compiled = ts
  .transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
  })
  .outputText.replaceAll(
    /from "(\.\/[^"]+)"/g,
    (_, path) => `from "${new URL(path, new URL("../packages/core/dist/", import.meta.url)).href}"`,
  );
const directory = await mkdtemp(join(tmpdir(), "lakeql-packed-range-"));
try {
  const file = join(directory, "baseline.mjs");
  await writeFile(file, compiled);
  const prior = (await import(pathToFileURL(file).href)).planSelectedPacked;
  const priorGatherSource = execFileSync(
    "git",
    ["show", `${baseline}:packages/core/src/packed-gather.ts`],
    { encoding: "utf8" },
  );
  const priorGatherCompiled = ts
    .transpileModule(priorGatherSource, {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
    })
    .outputText.replaceAll(
      /from "(\.\/[^"]+)"/g,
      (_, path) =>
        `from "${new URL(path, new URL("../packages/core/dist/", import.meta.url)).href}"`,
    );
  const gatherFile = join(directory, "baseline-gather.mjs");
  await writeFile(gatherFile, priorGatherCompiled);
  const priorGather = (await import(pathToFileURL(gatherFile).href)).compactGatherPlan;
  const malformed = {
    rowCount: 1,
    columns: {
      value: {
        type: "binary-view",
        data: Uint8Array.of(1),
        ranges: Uint32Array.of(1, 1),
        length: 1,
      },
    },
  };
  const oldPlan = priorGather(malformed, [0]);
  assert.ok(oldPlan);
  assert.throws(() => compactGatherPlan(malformed, [0]), { code: "LAKEQL_VALIDATION_ERROR" });
  const negativeControl = {
    baselineGatherSourceSha256: createHash("sha256").update(priorGatherSource).digest("hex"),
    baselineAcceptedOutOfBoundsRange: true,
    currentRejectsBeforeAllocation: true,
  };
  const cases = [];
  for (const shape of ["utf8", "binary", "utf8-view"]) {
    for (const rows of [256, 4096]) {
      for (const width of [8, 256]) {
        const text = `猫${"a".repeat(width - 3)}`;
        const encoded = new TextEncoder().encode(text);
        const valid = Uint8Array.from({ length: rows }, (_, i) => (i % 7 === 0 ? 0 : 1));
        const vector =
          shape === "utf8"
            ? { type: shape, values: Array(rows).fill(text), valid }
            : shape === "binary"
              ? { type: shape, values: Array(rows).fill(encoded), valid }
              : {
                  type: shape,
                  data: Uint8Array.from({ length: rows * width }, (_, i) => encoded[i % width]),
                  fixedLength: width,
                  length: rows,
                  valid,
                };
        const inputs = [selectVector(vector)];
        const samples = { prior: [], current: [] };
        let peakBytes = 0;
        for (let round = 0; round < 40; round++) {
          for (const name of round % 2 ? ["current", "prior"] : ["prior", "current"]) {
            const start = performance.now();
            const plan = (name === "prior" ? prior : planSelectedPacked)(inputs, false);
            assert.ok(plan);
            const scope = new MemoryScope(new MemoryBudget(plan.bytes));
            scope.admit(plan.bytes, "packed output");
            const output = plan.execute(scope);
            const elapsed = performance.now() - start;
            assert.equal(scope.budget.peakBytes, plan.bytes);
            if (peakBytes !== 0) assert.equal(plan.bytes, peakBytes);
            peakBytes = plan.bytes;
            for (let row = 0; row < rows; row++)
              assert.deepEqual(
                vectorValue(output, row),
                valid[row] === 0 ? null : shape === "binary" ? encoded : text,
              );
            scope.dispose();
            assert.equal(scope.budget.usedBytes, 0);
            if (round >= 20) samples[name].push(elapsed);
          }
        }
        const median = (values) => {
          values.sort((a, b) => a - b);
          return (values[9] + values[10]) / 2;
        };
        cases.push({
          shape,
          rows,
          width,
          peakBytes,
          afterReleaseBytes: 0,
          priorMs: median(samples.prior),
          currentMs: median(samples.current),
        });
      }
    }
  }
  console.log(
    JSON.stringify(
      {
        baseline,
        negativeControl,
        baselineSourceSha256: createHash("sha256").update(source).digest("hex"),
        currentSourceSha256: createHash("sha256")
          .update(
            await readFile(new URL("../packages/core/dist/selected-packed.js", import.meta.url)),
          )
          .digest("hex"),
        node: process.version,
        warmups: 20,
        samples: 20,
        scope:
          "Local selected packed planning plus execution, alternating variants. Prior selected-packed module uses current dependencies, not a complete historical build. Borrowed source/oracle and runtime heap overhead excluded; no cloud performance claim.",
        cases,
      },
      null,
      2,
    ),
  );
} finally {
  await rm(directory, { recursive: true, force: true });
}
