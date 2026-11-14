import { constants, crc32, gzipSync } from "node:zlib";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { scopedParquetCompressors } from "./compressors.js";
import { decodeGzipInto, GZIP_WORKSPACE_BYTES } from "./gzip.js";

function decode(input: Uint8Array, length: number) {
  return decodeGzipInto(input, new Uint8Array(length), new Uint8Array(GZIP_WORKSPACE_BYTES));
}

it.each(
  [0, 1, 7, 128, 4096, 65536].flatMap((length) =>
    (["runs", "random"] as const).map((pattern) => ({ length, pattern })),
  ),
)("decodes stored/fixed/dynamic blocks: $length bytes, $pattern", ({ length, pattern }) => {
  let seed = 12345;
  const input = Uint8Array.from({ length }, (_, i) => {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    return pattern === "random" ? seed >>> 24 : Math.floor(i / 31) % 17;
  });
  for (const strategy of [
    constants.Z_DEFAULT_STRATEGY,
    constants.Z_FIXED,
    constants.Z_HUFFMAN_ONLY,
    constants.Z_RLE,
  ]) {
    for (const level of [0, 1, 9] as const) {
      const encoded = gzipSync(input, { level, strategy });
      expect(decode(encoded, length)).toEqual(input);
    }
  }
});

it("decodes concatenated members without allowing cross-member history", () => {
  const parts = [Uint8Array.of(1, 2, 3), new Uint8Array(0), Uint8Array.of(4, 5)];
  const encoded = Uint8Array.from(parts.flatMap((part) => [...gzipSync(part)]));
  expect(decode(encoded, 5)).toEqual(Uint8Array.of(1, 2, 3, 4, 5));
});

it("rejects truncated input, checksums, incorrect sizes and reserved fields", () => {
  const encoded = gzipSync(Uint8Array.of(1, 2, 3, 4, 5));
  for (let end = 0; end < encoded.length; end++)
    expect(() => decode(encoded.subarray(0, end), 5)).toThrow(/Invalid GZIP/);
  for (const offset of [0, 2, encoded.length - 8, encoded.length - 4]) {
    const changed = Uint8Array.from(encoded);
    changed[offset] = (changed[offset] ?? 0) ^ 1;
    expect(() => decode(changed, 5)).toThrow(/Invalid GZIP/);
  }
  const flags = Uint8Array.from(encoded);
  flags[3] = 32;
  expect(() => decode(flags, 5)).toThrow(/reserved header flags/);
  expect(() => decode(encoded, 4)).toThrow(/output exceeds/);
  expect(() => decode(encoded, 6)).toThrow(/output size/);
});

it("does not write beyond a short output view and validates workspace before decoding", () => {
  const encoded = gzipSync(new Uint8Array(1000).fill(7));
  const backing = new Uint8Array(20).fill(99);
  expect(() =>
    decodeGzipInto(encoded, backing.subarray(5, 15), new Uint8Array(GZIP_WORKSPACE_BYTES)),
  ).toThrow(/output exceeds/);
  expect(backing.subarray(0, 5)).toEqual(new Uint8Array(5).fill(99));
  expect(backing.subarray(15)).toEqual(new Uint8Array(5).fill(99));
  expect(() =>
    decodeGzipInto(encoded, new Uint8Array(1000), new Uint8Array(GZIP_WORKSPACE_BYTES - 1)),
  ).toThrow(/workspace/);
  expect(() => decodeGzipInto(encoded, encoded, new Uint8Array(GZIP_WORKSPACE_BYTES))).toThrow(
    /overlapping/,
  );
});

it("validates optional header fields and their checksum", () => {
  const value = Uint8Array.of(9, 8, 7);
  const original = gzipSync(value);
  const header = Uint8Array.from([...original.subarray(0, 10), 3, 0, 1, 2, 3, 110, 0, 99, 0]);
  header[3] = 30;
  const checksum = crc32(header) & 0xffff;
  const encoded = Uint8Array.from([
    ...header,
    checksum & 255,
    checksum >>> 8,
    ...original.subarray(10),
  ]);
  expect(decode(encoded, value.length)).toEqual(value);
  encoded[header.length] = (encoded[header.length] ?? 0) ^ 1;
  expect(() => decode(encoded, value.length)).toThrow(/header checksum/);
});

it("admits exact output plus fixed workspace and releases failed decode allocations", () => {
  const input = new Uint8Array(10000).fill(7);
  const encoded = gzipSync(input);
  const bound = input.length + GZIP_WORKSPACE_BYTES;
  const scope = new MemoryScope(new MemoryBudget(bound));
  scope.admit(bound, "complete gzip");
  const codec = scopedParquetCompressors(scope).GZIP;
  if (!codec) throw new Error("Missing GZIP codec");
  expect(codec(encoded, input.length)).toEqual(input);
  expect(scope.budget.peakBytes).toBe(bound);
  expect(scope.budget.usedBytes).toBe(input.length);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  for (const corrupt of [false, true]) {
    const budget = new MemoryBudget(corrupt ? bound : bound - 1);
    const failed = new MemoryScope(budget);
    const codec = scopedParquetCompressors(failed).GZIP;
    if (!codec) throw new Error("Missing GZIP codec");
    const bytes = Uint8Array.from(encoded);
    if (corrupt) bytes[bytes.length - 8] = (bytes[bytes.length - 8] ?? 0) ^ 1;
    expect(() => codec(bytes, input.length)).toThrow();
    expect(budget.usedBytes).toBe(0);
    failed.dispose();
  }
});

it("rejects backreferences before a member even when earlier members produced bytes", () => {
  const empty = gzipSync(new Uint8Array(0));
  // Final fixed block: length symbol 257, distance zero, without a prior literal.
  const invalid = Uint8Array.from([...empty.subarray(0, 10), 3, 2, 0, ...new Uint8Array(8)]);
  expect(() => decode(invalid, 3)).toThrow(/distance before member output/);
  const joined = Uint8Array.from([...gzipSync(Uint8Array.of(1, 2, 3)), ...invalid]);
  expect(() => decode(joined, 6)).toThrow(/distance before member output/);
});
