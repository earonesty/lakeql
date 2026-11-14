import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { MATERIALIZATION_CHARGES as c } from "../packages/core/dist/allocation-charges.js";
import {
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  vectorTopKOwnedBatch,
} from "../packages/core/dist/index.js";

const results = [];
for (const width of [16, 128, 1024])
  for (const rows of [100, 10000])
    for (const limit of [3, 100]) {
      const variants = {};
      for (const cached of [false, true]) {
        const samples = [];
        let incrementalPeak;
        for (let round = 0; round < 7; round++) {
          const pool = new MemoryBudget(64 * 1024 * 1024);
          const source = new MemoryScope(pool);
          const data = source.allocate(
            c.view + rows * width,
            "packed fixture",
            () => new Uint8Array(rows * width),
          ).value;
          const text = (value) => String(value).padStart(8, "0").padEnd(width, "x");
          const encoder = new TextEncoder();
          for (let row = 0; row < rows; row++)
            data.set(encoder.encode(text((row * 17) % rows)), row * width);
          const input = source
            .allocate(3 * c.object + 7 * c.property, "fixture descriptors", () => ({
              rowCount: rows,
              columns: { key: { type: "utf8-view", data, fixedLength: width, length: rows } },
            }))
            .retain();
          const backing = source.retainAllocations(undefined);
          source.dispose();
          const baseline = pool.usedBytes;
          const cache = new MemoryScope(pool);
          let cachedInput;
          const start = performance.now();
          try {
            if (cached) {
              // Bulk admission avoids making per-key ownership bookkeeping the
              // deciding cost. ASCII fixture width bounds decoded UTF16 storage.
              const batch = cache.allocate(
                c.array +
                  rows * (c.arraySlot + c.string + width * c.stringCodeUnit) +
                  4 * c.object +
                  4 * c.property +
                  c.view,
                "decoded key cache and construction workspace",
                () => {
                  const values = new Array(rows);
                  const decoder = new TextDecoder();
                  for (let row = 0; row < rows; row++)
                    values[row] = decoder.decode(data.subarray(row * width, (row + 1) * width));
                  return { rowCount: rows, columns: { key: { type: "utf8", values } } };
                },
              ).value;
              cachedInput = cache.retainAllocations(batch);
            }
            const output = vectorTopKOwnedBatch(cachedInput ?? input, [{ column: "key" }], {
              limit,
            });
            const elapsed = performance.now() - start;
            incrementalPeak = pool.peakBytes - baseline;
            assert.deepEqual(
              materializeBatchRows(output.value),
              Array.from({ length: limit }, (_, key) => ({ key: text(key) })),
            );
            output.release();
            if (round >= 2) samples.push(elapsed);
          } finally {
            cachedInput?.release();
            cache.dispose();
            input.release();
            backing.release();
          }
          assert.equal(pool.usedBytes, 0);
        }
        variants[cached ? "cachedIncludingConstruction" : "direct"] = {
          incrementalPeak,
          samplesMs: samples,
          medianMs: [...samples].sort((a, b) => a - b)[2],
        };
      }
      results.push({ width, rows, limit, variants });
    }
const report = {
  sourceCommit: "a8ba6db",
  node: process.version,
  harnessSha256: createHash("sha256")
    .update(readFileSync(import.meta.filename))
    .digest("hex"),
  description:
    "Experimental whole-batch decoded-key cache versus direct packed reads using the same owned top-k operator. Cache construction included; source creation excluded. ASCII fixed-width permutation fixture, two warmups/five samples, 64 MiB tracked pool. Both source and cache remain charged through selection.",
  limitations: [
    "One key column; no dictionary/null/multi-column/cache-reuse model.",
    "Reservation units exclude VM/native decoder and GC overhead.",
    "Experimental benchmark only; no production cache/default change.",
    "Variants run sequentially; no cloud throughput claim.",
  ],
  results,
};
writeFileSync(
  process.env.LAKEQL_KEY_CACHE_REPORT ?? "docs/benchmarks/top-k-key-cache-2026-11-14.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
