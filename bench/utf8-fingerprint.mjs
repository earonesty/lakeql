import { performance } from "node:perf_hooks";
import { fnv1a64Utf8 } from "../packages/core/dist/utf8-fingerprint.js";

function encodedReference(input) {
  let hash = 0xcbf29ce484222325n;
  for (const byte of new TextEncoder().encode(input)) {
    hash = BigInt.asUintN(64, (hash ^ BigInt(byte)) * 0x100000001b3n);
  }
  return hash;
}
const cases = [];
for (const [name, unit] of [
  ["ascii", "task-plan"],
  ["unicode", "猫😀"],
]) {
  for (const repeats of [128, 8192, 65536]) {
    const input = unit.repeat(repeats);
    const expected = encodedReference(input);
    for (let warmup = 0; warmup < 10; warmup++) {
      encodedReference(input);
      fnv1a64Utf8(input);
    }
    const timings = { encoded: [], incremental: [] };
    for (let round = 0; round < 7; round++) {
      for (const [key, fn] of round % 2 === 0
        ? [
            ["encoded", encodedReference],
            ["incremental", fnv1a64Utf8],
          ]
        : [
            ["incremental", fnv1a64Utf8],
            ["encoded", encodedReference],
          ]) {
        const start = performance.now();
        const result = fn(input);
        timings[key].push(performance.now() - start);
        if (result !== expected) throw Error("Fingerprint mismatch");
      }
    }
    cases.push({
      name,
      codeUnits: input.length,
      avoidedEncodedBytes: new TextEncoder().encode(input).byteLength,
      medianMs: Object.fromEntries(
        Object.entries(timings).map(([key, values]) => [key, [...values].sort((a, b) => a - b)[3]]),
      ),
      samplesMs: timings,
    });
  }
}
console.log(
  JSON.stringify(
    {
      runtime: process.version,
      platform: process.platform,
      note: "Kernel comparison; excludes stable JSON serialization. Avoided bytes are encoded buffer extent, not measured process peak.",
      cases,
    },
    null,
    2,
  ),
);
