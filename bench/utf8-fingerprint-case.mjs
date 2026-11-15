import assert from "node:assert/strict";
import { fnv1a64Utf8 } from "../packages/core/dist/utf8-fingerprint.js";

const cases = [];
for (const [kind, token] of [
  ["ascii", "0123456789abcdef"],
  ["unicode", "é雪😀"],
  ["mixed", "ascii-prefix-雪😀"],
  ["surrogates", "\ud800x\udc00"],
]) {
  for (const repeats of [1, 64, 4096]) {
    const text = token.repeat(repeats);
    const bytes = new TextEncoder().encode(text);
    let expected = 0xcbf29ce484222325n;
    for (const byte of bytes)
      expected = BigInt.asUintN(64, (expected ^ BigInt(byte)) * 0x100000001b3n);
    assert.equal(fnv1a64Utf8(text), expected);
    const iterations = Math.max(4, Math.floor(200000 / text.length));
    const samples = [];
    let digest;
    for (let round = 0; round < 7; round++) {
      const start = performance.now();
      for (let i = 0; i < iterations; i++) digest = fnv1a64Utf8(text);
      if (round >= 2) samples.push((performance.now() - start) / iterations);
    }
    assert.equal(digest, expected);
    samples.sort((a, b) => a - b);
    cases.push({
      kind,
      codeUnits: text.length,
      utf8Bytes: bytes.length,
      iterations,
      digest: digest.toString(16),
      elapsedMs: samples[2],
    });
  }
}
console.log(
  JSON.stringify({
    cases,
    elapsedMs: cases.reduce((sum, entry) => sum + entry.elapsedMs, 0),
    scope:
      "Local UTF8 hashing kernel; independent TextEncoder/BigInt oracle checks every case. No encoded copy is made by the implementation. No cloud throughput or total heap claim.",
  }),
);
