import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { fingerprint } from "./manifest.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { fnv1a64Utf8 } from "./utf8-fingerprint.js";

function reference(input: string): bigint {
  let hash = 0xcbf29ce484222325n;
  for (const byte of new TextEncoder().encode(input)) {
    hash = BigInt.asUintN(64, (hash ^ BigInt(byte)) * 0x100000001b3n);
  }
  return hash;
}

it("matches encoded hashing across every UTF-16 code unit, including lone surrogates", () => {
  for (let start = 0; start < 65536; start += 256) {
    let input = "";
    for (let point = start; point < start + 256; point++) input += String.fromCharCode(point);
    expect(fnv1a64Utf8(input)).toBe(reference(input));
  }
});

it.each([
  "",
  "ASCII\0",
  "猫😀",
  "\ud800\udc00",
  "\udbff\udfff",
  "\ud800x\udc00",
  "\ud800\ud800\udc00",
  "\udc00\ud800",
  "猫😀".repeat(10000),
])("preserves UTF-8 fingerprint bytes for case %#", (input) => {
  expect(fnv1a64Utf8(input)).toBe(reference(input));
});

it("admits fingerprint serialization and retains only the digest, with transactional failure", () => {
  const run = (capacity: number) => {
    const pool = new MemoryBudget(capacity);
    const parent = new MemoryScope(pool);
    parent.allocate(8, "sibling", () => undefined);
    const value = { binary: new Uint8Array([0, 255]), nested: [null, "猫😀", 4n] };
    try {
      expect(fingerprint(value, parent)).toBe(fingerprint(value));
      expect(pool.usedBytes).toBe(8 + c.string + 19 * c.stringCodeUnit);
      return pool.peakBytes;
    } catch (error) {
      expect(pool.usedBytes).toBe(8);
      throw error;
    } finally {
      parent.dispose();
      expect(pool.usedBytes).toBe(0);
    }
  };
  const peak = run(65536);
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
});

it("preserves carries and wrapping over deterministic mixed UTF16 sequences", () => {
  let seed = 0x9e3779b9;
  for (const length of [1, 7, 31, 255, 4097, 65536]) {
    let text = "";
    for (let i = 0; i < length; i++) {
      seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
      text += String.fromCharCode(seed >>> 16);
    }
    expect(fnv1a64Utf8(text)).toBe(reference(text));
  }
});

it.each([
  0, 1, 31, 1024, 65536,
])("preserves carries across an ASCII/UTF8 transition after %i code units", (length) => {
  const prefix = "\0\x7fAZ09".repeat(Math.ceil(length / 6)).slice(0, length);
  for (const suffix of ["", "雪😀\ud800"]) {
    const input = prefix + suffix;
    expect(fnv1a64Utf8(input)).toBe(reference(input));
  }
});
