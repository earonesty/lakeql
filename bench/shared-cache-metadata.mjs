import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";
import { MemoryBudget, MemoryScope, SharedMemoryCache } from "../packages/core/dist/index.js";

const baseline = process.env.LAKEQL_COMPARE_BASELINE ?? "47606b7";
const directory = await mkdtemp(join(tmpdir(), "lakeql-cache-metadata-"));
const results = [];
try {
  const source = execFileSync(
    "git",
    ["show", `${baseline}:packages/core/src/object-store-cache.ts`],
    { encoding: "utf8" },
  );
  const dist = pathToFileURL(`${resolve("packages/core/dist")}/`).href;
  const compiled = ts
    .transpileModule(source, {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
    })
    .outputText.replaceAll(/from "\.\/([^"]+)"/g, (_, file) => `from "${dist + file}"`);
  const path = join(directory, "baseline.mjs");
  await writeFile(path, compiled);
  const Previous = (await import(pathToFileURL(path).href)).SharedMemoryCache;
  for (const owned of [false, true])
    for (const payloadLength of [64, 4096])
      for (const keyLength of [16, 1024]) {
        const keys = Array.from({ length: 64 }, (_, i) => String(i).padStart(keyLength, "k"));
        const samples = { baseline: [], current: [] };
        for (let round = 0; round < 12; round++) {
          const variants = [
            ["baseline", Previous],
            ["current", SharedMemoryCache],
          ];
          for (const [variant, Cache] of round % 2 ? variants.reverse() : variants) {
            const pool = new MemoryBudget(128 * 1024);
            const scope = new MemoryScope(pool);
            const cache = new Cache({ memoryBudget: pool, maxBytes: pool.maxBytes });
            const start = performance.now();
            let checksum = 0;
            for (let index = 0; index < 512; index++) {
              const key = keys[index % keys.length];
              const allocate = () => new Uint8Array(payloadLength).fill(index & 255);
              if (owned) {
                const owner = scope.allocate(128 + payloadLength, "payload and view", allocate);
                try {
                  assert.equal(cache.setOwned(key, owner), true);
                } finally {
                  owner.release();
                }
              } else assert.equal(cache.setAllocated(key, 128 + payloadLength, allocate), true);
              const lease = cache.acquire(key);
              assert.equal(lease.value.length, payloadLength);
              assert.equal(lease.value[0], index & 255);
              checksum += lease.value[0];
              lease.release();
            }
            const elapsedMs = performance.now() - start;
            assert.equal(checksum, 65280);
            const retainedBytes = pool.usedBytes;
            const peakBytes = pool.peakBytes;
            let retainedEntries = 0;
            cache.deleteWhere(() => {
              retainedEntries++;
              return true;
            });
            scope.dispose();
            assert.equal(pool.usedBytes, 0);
            assert.ok(peakBytes <= pool.maxBytes);
            if (round >= 4)
              samples[variant].push({ elapsedMs, retainedBytes, peakBytes, retainedEntries });
          }
        }
        const medians = Object.fromEntries(
          Object.entries(samples).map(([name, values]) => {
            const sorted = values.map((value) => value.elapsedMs).sort((a, b) => a - b);
            return [name, (sorted[3] + sorted[4]) / 2];
          }),
        );
        results.push({
          owned,
          payloadLength,
          keyLength,
          operationsPerSample: 512,
          samples,
          medianMs: medians,
        });
      }
  console.log(
    JSON.stringify(
      {
        baseline,
        node: process.version,
        warmupRounds: 4,
        measuredRounds: 8,
        results,
        limitations: [
          "Cache insertion, acquisition and eviction kernel with current dependencies; not query or cloud throughput.",
          "Equal 128 KiB modeled pools include retained metadata only in the candidate, so entry residency differs.",
          "Caller-created keys and benchmark/runtime overhead are outside modeled pool metrics.",
        ],
      },
      null,
      2,
    ),
  );
} finally {
  await rm(directory, { recursive: true, force: true });
}
