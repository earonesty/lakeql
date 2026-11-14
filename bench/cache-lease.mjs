import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";
import { MemoryBudget, MemoryScope, SharedMemoryCache } from "../packages/core/dist/index.js";

const reference = "955bebc";
const dir = await mkdtemp(join(tmpdir(), "lakeql-cache-lease-"));
try {
  const source = execFileSync(
    "git",
    ["show", `${reference}:packages/core/src/object-store-cache.ts`],
    { encoding: "utf8" },
  );
  const dist = pathToFileURL(`${resolve("packages/core/dist")}/`).href;
  const code = ts
    .transpileModule(source, {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
    })
    .outputText.replaceAll(/from "\.\/([^"]+)"/g, (_, file) => `from "${dist + file}"`);
  const path = join(dir, "cache.mjs");
  await writeFile(path, code);
  const Previous = (await import(pathToFileURL(path).href)).SharedMemoryCache;
  const results = [];
  for (const owned of [false, true]) {
    const samples = { reference: [], current: [] };
    for (let round = 0; round < 12; round++) {
      const variants = [
        ["reference", Previous],
        ["current", SharedMemoryCache],
      ];
      for (const [name, Cache] of round % 2 ? variants.reverse() : variants) {
        const pool = new MemoryBudget(65536);
        const cache = new Cache({ memoryBudget: pool });
        const scope = new MemoryScope(pool);
        const payload = new Uint8Array([1, 2, 3]);
        if (owned)
          cache.setOwned(
            "payload",
            scope.allocate(3, "payload", () => payload),
          );
        else cache.set("payload", payload, 3);
        scope.dispose();
        let sum = 0;
        const start = performance.now();
        for (let i = 0; i < 10000; i++) {
          const lease = cache.acquire("payload");
          sum += lease.value[0];
          lease.release();
        }
        const elapsed = performance.now() - start;
        assert.equal(sum, 10000);
        assert.equal(pool.usedBytes, 3);
        cache.delete("payload");
        assert.equal(pool.usedBytes, 0);
        if (round >= 4) samples[name].push(elapsed);
      }
    }
    const median = (values) => {
      const sorted = [...values].sort((a, b) => a - b);
      return (sorted[3] + sorted[4]) / 2;
    };
    results.push({
      owned,
      samplesMs: samples,
      medianMsPer10000Leases: Object.fromEntries(
        Object.entries(samples).map(([key, value]) => [key, median(value)]),
      ),
    });
  }
  console.log(
    JSON.stringify(
      {
        reference,
        node: process.version,
        warmupRounds: 4,
        measuredRounds: 8,
        results,
        limitations: [
          "Cache lease kernel only; not cloud/query throughput or total heap.",
          "Reference module with current dependencies; alternating variant order.",
        ],
      },
      null,
      2,
    ),
  );
} finally {
  await rm(dir, { recursive: true, force: true });
}
