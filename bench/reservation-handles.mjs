import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import ts from "typescript";

if (process.argv[2] === "--child") {
  const { MemoryBudget } = await import(process.argv[3]);
  const mode = process.argv[4];
  const count = 32768;
  const pool = new MemoryBudget(count * 16);
  const handles = [];
  global.gc();
  const before = process.memoryUsage().heapUsed;
  let sampledHeap = before;
  const start = performance.now();
  if (mode === "retained") {
    for (let i = 0; i < count; i++) {
      handles.push(pool.reserve(16, "retained workspace"));
      if (i % 1024 === 0) sampledHeap = Math.max(sampledHeap, process.memoryUsage().heapUsed);
    }
    assert.equal(pool.usedBytes, count * 16);
  } else {
    let capacity = pool.reserve(0, "capacity");
    for (let i = 0; i < count; i++) {
      capacity = capacity.merge(pool.reserve(16, "growth"));
      if (i % 1024 === 0) sampledHeap = Math.max(sampledHeap, process.memoryUsage().heapUsed);
    }
    handles.push(capacity);
    assert.equal(capacity.bytes, count * 16);
    assert.equal(pool.usedBytes, count * 16);
  }
  const elapsedMs = performance.now() - start;
  sampledHeap = Math.max(sampledHeap, process.memoryUsage().heapUsed);
  // Diagnostic collection only after the timed operation, with handles live.
  global.gc();
  const retainedHeapDelta = process.memoryUsage().heapUsed - before;
  for (const handle of handles) handle.release();
  handles.length = 0;
  assert.equal(pool.usedBytes, 0);
  global.gc();
  process.stdout.write(
    JSON.stringify({
      elapsedMs,
      sampledHeapDelta: sampledHeap - before,
      retainedHeapDelta,
      releasedHeapDelta: process.memoryUsage().heapUsed - before,
      admittedPeak: pool.peakBytes,
      finalUsedBytes: pool.usedBytes,
    }),
  );
} else {
  const reference = "995120b";
  const directory = await mkdtemp(join(tmpdir(), "lakeql-handles-"));
  try {
    const source = execFileSync(
      "git",
      ["show", `${reference}:packages/core/src/memory-budget.ts`],
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
    const baseline = join(directory, "memory-budget.mjs");
    await writeFile(baseline, compiled);
    const current = new URL("../packages/core/dist/memory-budget.js", import.meta.url);
    const results = [];
    for (const mode of ["retained", "growth"]) {
      const variants = [
        ["reference", pathToFileURL(baseline).href],
        ["current", current.href],
      ];
      const runs = [];
      for (let round = 0; round < 4; round++)
        for (const [variant, module] of round % 2 === 0 ? variants : [...variants].reverse())
          runs.push({
            round,
            variant,
            ...JSON.parse(
              execFileSync(
                process.execPath,
                ["--expose-gc", fileURLToPath(import.meta.url), "--child", module, mode],
                { encoding: "utf8" },
              ),
            ),
          });
      results.push({ mode, count: 32768, runs });
    }
    process.stdout.write(
      `${JSON.stringify(
        {
          reference,
          node: process.version,
          currentModuleSha256: createHash("sha256")
            .update(await readFile(current))
            .digest("hex"),
          conditions:
            "Four alternating fresh-process pairs per case. Timings include admission and sparse heap sampling. Diagnostic forced GC occurs before timing and after measured operations, never during them. Node kernel only; sampled heap is not an exact peak or Worker guarantee. Prior budget module uses current error dependency.",
          results,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
