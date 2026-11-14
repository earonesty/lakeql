import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { MATERIALIZATION_CHARGES as c } from "../packages/core/dist/allocation-charges.js";
import {
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  vectorTopKOwnedBatch,
} from "../packages/core/dist/index.js";

const encoder = new TextEncoder();
const prefixes = ["é", "界", "🙂", "A", "𐀀", "\uE000"];
function text(key, width) {
  const prefix = prefixes[key % prefixes.length] + String(key).padStart(8, "0");
  return prefix + "x".repeat(width - encoder.encode(prefix).length);
}
function record(scope, value) {
  return scope.allocate(
    c.object + Object.keys(value).length * c.property,
    "fixture/cache descriptor",
    () => value,
  ).value;
}
function source(pool, config) {
  const scope = new MemoryScope(pool);
  try {
    const columns = record(scope, {});
    scope.allocate((config.keys + 1) * c.property, "fixture column slots", () => undefined);
    columns.id = record(scope, {
      type: "i32",
      values: scope.allocate(c.view + config.rows * 4, "row identities", () =>
        Int32Array.from({ length: config.rows }, (_, i) => i),
      ).value,
    });
    for (let key = 0; key < config.keys; key++) {
      const count = config.shape === "dictionary" ? 64 : config.rows;
      const data = scope.allocate(
        c.view + count * config.width,
        "packed source keys",
        () => new Uint8Array(count * config.width),
      ).value;
      for (let index = 0; index < count; index++)
        data.set(encoder.encode(text(index, config.width)), index * config.width);
      const valid = scope.allocate(c.view + config.rows, "key validity", () =>
        Uint8Array.from({ length: config.rows }, (_, i) => (i % (key ? 11 : 7) === 0 ? 0 : 1)),
      ).value;
      // Packed columns permute payload; dictionary columns permute indices.
      if (config.shape === "packed") {
        for (let row = 0; row < config.rows; row++)
          data.set(
            encoder.encode(text((row * (key ? 31 : 17)) % count, config.width)),
            row * config.width,
          );
        columns[`key${key}`] = record(scope, {
          type: "utf8-view",
          data,
          fixedLength: config.width,
          length: count,
          valid,
        });
      } else {
        const dictionary = record(scope, {
          type: "utf8-view",
          data,
          fixedLength: config.width,
          length: count,
        });
        const indices = scope.allocate(c.view + config.rows * 4, "dictionary indices", () =>
          Int32Array.from({ length: config.rows }, (_, row) => (row * (key ? 31 : 17)) % count),
        ).value;
        columns[`key${key}`] = record(scope, { type: "dict", indices, dictionary, valid });
      }
    }
    return scope.retainAllocations(record(scope, { rowCount: config.rows, columns }));
  } finally {
    scope.dispose();
  }
}
function decodeKeys(input, scope, keys) {
  scope.adopt(input.retain());
  const decoder = scope.allocate(
    c.object + c.view,
    "decoder and transient input view",
    () => new TextDecoder(),
  ).value;
  const columns = scope.allocate(
    c.object + (keys + 1) * c.property,
    "cached column directory",
    () => ({ ...input.value.columns }),
  ).value;
  let decodedValues = 0;
  for (let key = 0; key < keys; key++) {
    const original = columns[`key${key}`];
    const packed = original.type === "dict" ? original.dictionary : original;
    const values = scope.allocate(
      c.array +
        packed.length * (c.arraySlot + c.string) +
        packed.data.byteLength * c.stringCodeUnit,
      "decoded key array and Unicode upper bound",
      () => {
        const result = new Array(packed.length);
        for (let index = 0; index < packed.length; index++) {
          if (packed.valid?.[index] === 0) result[index] = "";
          else {
            result[index] = decoder.decode(
              packed.data.subarray(index * packed.fixedLength, (index + 1) * packed.fixedLength),
            );
            decodedValues++;
          }
        }
        return result;
      },
    ).value;
    const decoded = record(scope, {
      type: "utf8",
      values,
      ...(packed.valid ? { valid: packed.valid } : {}),
    });
    columns[`key${key}`] =
      original.type === "dict" ? record(scope, { ...original, dictionary: decoded }) : decoded;
  }
  return {
    input: scope.retainAllocations(record(scope, { rowCount: input.value.rowCount, columns })),
    decodedValues,
  };
}
function expected(config, order) {
  const count = config.shape === "dictionary" ? 64 : config.rows;
  return Array.from({ length: config.rows }, (_, id) => {
    const row = { id };
    for (let key = 0; key < config.keys; key++)
      row[`key${key}`] =
        id % (key ? 11 : 7) === 0 ? null : text((id * (key ? 31 : 17)) % count, config.width);
    return row;
  })
    .sort((a, b) => {
      for (const term of order) {
        const x = a[term.column],
          y = b[term.column];
        if (x === y) continue;
        if (x === null) return term.nulls === "first" ? -1 : 1;
        if (y === null) return term.nulls === "first" ? 1 : -1;
        return (x < y ? -1 : 1) * (term.direction === "desc" ? -1 : 1);
      }
      return a.id - b.id;
    })
    .slice(0, config.limit);
}
function run(config, cached, oracle, order) {
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const input = source(pool, config);
  const baseline = pool.usedBytes;
  const scope = new MemoryScope(pool);
  const outputs = [];
  let prepared;
  const start = performance.now();
  try {
    if (cached) prepared = decodeKeys(input, scope, config.keys);
    for (let job = 0; job < config.reuse; job++)
      outputs.push(vectorTopKOwnedBatch(prepared?.input ?? input, order, { limit: config.limit }));
    const elapsedMs = performance.now() - start;
    const incrementalPeak = pool.peakBytes - baseline;
    for (const output of outputs) assert.deepEqual(materializeBatchRows(output.value), oracle);
    return { elapsedMs, incrementalPeak, decodedValues: prepared?.decodedValues ?? null };
  } finally {
    for (const output of outputs) output.release();
    prepared?.input.release();
    scope.dispose();
    input.release();
    assert.equal(pool.usedBytes, 0);
  }
}
const results = [];
for (const shape of ["packed", "dictionary"])
  for (const keys of [1, 2])
    for (const width of [32, 256])
      for (const limit of [3, 100])
        for (const reuse of [1, 4])
          for (const nulls of ["first", "last"]) {
            const config = { shape, keys, width, limit, reuse, nulls, rows: 10000 };
            const order = Array.from({ length: keys }, (_, key) => ({
              column: `key${key}`,
              direction: key ? "desc" : "asc",
              nulls: key ? "last" : nulls,
            }));
            const oracle = expected(config, order);
            const samples = { direct: [], cached: [] };
            for (let round = 0; round < 8; round++) {
              for (const cached of round % 2 ? [true, false] : [false, true]) {
                const observation = run(config, cached, oracle, order);
                if (round >= 2) samples[cached ? "cached" : "direct"].push(observation);
              }
            }
            const variants = Object.fromEntries(
              Object.entries(samples).map(([name, observations]) => {
                const sorted = observations.map((sample) => sample.elapsedMs).sort((a, b) => a - b);
                return [
                  name,
                  {
                    medianMs: (sorted[2] + sorted[3]) / 2,
                    incrementalPeak: Math.max(
                      ...observations.map((sample) => sample.incrementalPeak),
                    ),
                    decodedValues: observations[0].decodedValues,
                    samplesMs: observations.map((sample) => sample.elapsedMs),
                  },
                ];
              }),
            );
            results.push({ ...config, variants });
          }
const report = {
  sourceCommit: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  node: process.version,
  harnessSha256: createHash("sha256")
    .update(readFileSync(import.meta.filename))
    .digest("hex"),
  description:
    "Unicode nullable packed and dictionary keys; one/two ordering terms, widths 32/256, k 3/100, one/four selections sharing preparation with all results retained. Two warmups/six samples, alternating variant order. Cache construction included, fixture construction and independent stable-sort oracle excluded. 64 MiB modeled pool; dictionaries decode unique values only.",
  limitations: [
    "Sequential synchronous Node selections with retained outputs model reuse/overlap, not asynchronous cloud jobs.",
    "Unicode allocation uses a conservative UTF16 upper bound derived from UTF8 bytes; runtime/native/GC overhead and oracle memory are outside reservation accounting.",
    "Experimental cache only; no production policy or timing threshold inferred from one run.",
  ],
  results,
};
writeFileSync(
  process.env.LAKEQL_KEY_CACHE_REPORT ?? "docs/benchmarks/top-k-key-cache-shapes-2026-11-14.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(
  JSON.stringify({
    scenarios: results.length,
    cachedFaster: results.filter((r) => r.variants.cached.medianMs < r.variants.direct.medianMs)
      .length,
  }),
);
