import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";
import { planBatchMaterialization } from "../packages/core/dist/materialization-plan.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

const baseline = process.env.LAKEQL_MATERIALIZATION_BASELINE ?? "5457712";
const source = execFileSync(
  "git",
  ["show", `${baseline}:packages/core/src/materialization-plan.ts`],
  { encoding: "utf8" },
);
const compiled = ts
  .transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
  })
  .outputText.replaceAll(
    /from "(\.\/[^"]+)"/g,
    (_, path) => `from "${new URL(path, new URL("../packages/core/dist/", import.meta.url)).href}"`,
  );
const directory = await mkdtemp(join(tmpdir(), "lakeql-traversal-"));
try {
  const file = join(directory, "baseline.mjs");
  await writeFile(file, compiled);
  const prior = (await import(pathToFileURL(file).href)).planBatchMaterialization;
  const cases = [];
  for (const shape of ["numeric", "nested", "packed-text", "sparse-wide"]) {
    for (const rows of [256, 4096, 32768]) {
      let vector =
        shape === "packed-text"
          ? {
              type: "utf8-view",
              data: new Uint8Array(rows * 256).fill(97),
              fixedLength: 256,
              length: rows,
            }
          : { type: "i32", values: new Int32Array(rows) };
      if (shape === "nested")
        for (let depth = 1; depth < 4; depth++)
          vector = { type: "struct", fields: { value: vector }, length: rows };
      const columns = { value: vector };
      const selection = shape === "sparse-wide" ? new Uint8Array(rows) : undefined;
      if (selection) {
        selection[rows - 1] = 1;
        for (let column = 1; column < 16; column++) columns[`value${column}`] = vector;
      }
      const batch = { rowCount: rows, columns };
      const sample = { prior: [], current: [] };
      const measurements = {};
      for (let round = 0; round < 12; round++) {
        for (const name of round % 2 ? ["current", "prior"] : ["prior", "current"]) {
          const budget = new MemoryBudget(4096);
          const scope = new MemoryScope(budget);
          const start = performance.now();
          const plan = (name === "prior" ? prior : planBatchMaterialization)(
            batch,
            selection,
            scope,
          );
          const elapsedMs = performance.now() - start;
          assert.equal(plan.rows, selection ? 1 : rows);
          assert.equal(budget.usedBytes, 0);
          scope.dispose();
          measurements[name] = {
            planningPeakBytes: budget.peakBytes,
            allocationBytes: plan.allocationBytes,
            retainedBytes: plan.retainedBytes,
          };
          if (round >= 4) sample[name].push(elapsedMs);
        }
      }
      assert.deepEqual(measurements.prior, measurements.current);
      const calls = {};
      for (const [name, plan] of [
        ["prior", prior],
        ["current", planBatchMaterialization],
      ]) {
        const budget = new MemoryBudget(4096);
        const scope = new MemoryScope(budget);
        const reserve = budget.reserve;
        let count = 0;
        budget.reserve = function (...args) {
          count++;
          return reserve.apply(this, args);
        };
        try {
          plan(batch, selection, scope);
          calls[name] = count;
        } finally {
          budget.reserve = reserve;
          scope.dispose();
        }
        assert.equal(budget.usedBytes, 0);
      }
      assert.equal(calls.current, shape === "nested" ? 5 : 2);
      assert.ok(calls.prior > 0);
      const median = (values) => {
        values.sort((a, b) => a - b);
        return (values[3] + values[4]) / 2;
      };
      cases.push({
        shape,
        rows,
        ...measurements.current,
        reservationCalls: calls,
        priorMedianMs: median(sample.prior),
        currentMedianMs: median(sample.current),
        afterReleaseBytes: 0,
      });
    }
  }
  console.log(
    JSON.stringify(
      {
        baseline,
        baselineSourceSha256: createHash("sha256").update(source).digest("hex"),
        currentSourceSha256: createHash("sha256")
          .update(
            await readFile(
              new URL("../packages/core/dist/materialization-plan.js", import.meta.url),
            ),
          )
          .digest("hex"),
        node: process.version,
        warmups: 4,
        samples: 8,
        scope:
          "Local planning-only comparison; prior materialization module uses current dependencies. Borrowed inputs and subsequent row construction excluded. Alternating variants, reservation counts measured separately from timings; no whole-heap or cloud claim.",
        cases,
      },
      null,
      2,
    ),
  );
} finally {
  await rm(directory, { recursive: true, force: true });
}
