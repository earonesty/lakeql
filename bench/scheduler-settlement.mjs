import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryStageScheduler } from "../packages/core/dist/memory-stage-scheduler.js";

const reference = "25c924e";
const directory = await mkdtemp(join(tmpdir(), "lakeql-stage-settlement-"));
try {
  const source = execFileSync(
    "git",
    ["show", `${reference}:packages/core/src/memory-stage-scheduler.ts`],
    { encoding: "utf8" },
  );
  const compiled = ts
    .transpileModule(source, {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
    })
    .outputText.replaceAll(
      /from "(\.\/[^"]+)"/g,
      (_, path) =>
        `from "${new URL(path, new URL("../packages/core/dist/", import.meta.url)).href}"`,
    );
  const file = join(directory, "scheduler.mjs");
  await writeFile(file, compiled);
  const Prior = (await import(pathToFileURL(file).href)).MemoryStageScheduler;
  const results = [];
  for (const buffers of [0, 1, 8]) {
    const variants = [
      ["reference", Prior],
      ["current", MemoryStageScheduler],
    ];
    const samples = { reference: [], current: [] };
    for (let round = 0; round < 12; round++) {
      for (const [name, Scheduler] of round % 2 === 0 ? variants : [...variants].reverse()) {
        const pool = new MemoryBudget(buffers * 64);
        const job = pool.child();
        const scheduler = new Scheduler(pool);
        const start = performance.now();
        for (let i = 0; i < 1000; i++) {
          const output = await scheduler.run({
            budget: job,
            bytes: buffers * 64,
            purpose: "workspace",
            execute(scope) {
              for (let buffer = 0; buffer < buffers; buffer++)
                scope.allocate(64, "buffer", () => new Uint8Array(64));
              return buffers;
            },
          });
          assert.equal(output.value, buffers);
          output.release();
          assert.equal(pool.usedBytes, 0);
          assert.equal(scheduler.activeStages, 0);
          assert.equal(scheduler.queuedStages, 0);
        }
        if (round >= 4) samples[name].push(performance.now() - start);
        assert.equal(pool.peakBytes, buffers * 64);
      }
    }
    results.push({ buffers, stages: 1000, samplesMs: samples });
  }
  process.stdout.write(
    `${JSON.stringify(
      {
        reference,
        node: process.version,
        currentModuleSha256: createHash("sha256")
          .update(
            await readFile(
              new URL("../packages/core/dist/memory-stage-scheduler.js", import.meta.url),
            ),
          )
          .digest("hex"),
        scope:
          "Same-runtime scheduler-only comparison with current dependencies. Four warmup and eight measured alternating-order rounds. Serial independent stages with held output until caller release; exact admission and zero final charges checked. No cloud, decode, concurrent throughput or total-heap claim.",
        results,
      },
      null,
      2,
    )}\n`,
  );
} finally {
  await rm(directory, { recursive: true, force: true });
}
