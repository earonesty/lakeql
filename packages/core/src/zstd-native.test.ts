import assert from "node:assert/strict";
import { constants, zstdCompressSync } from "node:zlib";
import { expect, it } from "vitest";
import { decompressInto } from "./vendor/zstd.js";

it("rejects oversized Huffman tables before allocating their backing", () => {
  // Single-segment frame, one compressed block, direct Huffman weights.
  // Each individual weight is legal, but 128 weights of 11 exceed the
  // supported table depth. The output destination is deliberately tiny.
  const literals = [0x12, 0x40, 0x10, 255, ...Array(64).fill(0xbb), 0];
  const blockHeader = (literals.length << 3) | 5;
  const frame = Uint8Array.from([
    0x28,
    0xb5,
    0x2f,
    0xfd,
    0x20,
    1,
    blockHeader & 255,
    (blockHeader >>> 8) & 255,
    blockHeader >>> 16,
    ...literals,
  ]);
  const allocations: number[] = [];
  expect(() =>
    decompressInto(frame, new Uint8Array(1), (bytes) => {
      allocations.push(bytes);
      return new Uint8Array(bytes);
    }),
  ).toThrow();
  expect(allocations).toEqual([12, 292]);
});

it.each([1, 3, 9])("decodes native ZSTD frames directly at level %s", (level) => {
  for (const length of [0, 1, 31, 1024, 131072, 262145]) {
    for (const pattern of ["constant", "mixed", "random"]) {
      const input = new Uint8Array(length);
      let state = 1729;
      for (let index = 0; index < length; index++) {
        state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
        input[index] =
          pattern === "constant"
            ? 65
            : pattern === "mixed" && index % 97 < 80
              ? index % 7
              : state >>> 24;
      }
      const frame = zstdCompressSync(input, {
        params: { [constants.ZSTD_c_compressionLevel]: level },
      });
      assert.deepEqual(decompressInto(frame, new Uint8Array(length)), input);
      const joined = new Uint8Array(frame.length * 2);
      joined.set(frame);
      joined.set(frame, frame.length);
      const expected = new Uint8Array(length * 2);
      expected.set(input);
      expected.set(input, length);
      assert.deepEqual(decompressInto(joined, new Uint8Array(length * 2)), expected);
      expect(() =>
        decompressInto(frame.subarray(0, frame.length - 1), new Uint8Array(length)),
      ).toThrow();
      expect(() => decompressInto(frame, new Uint8Array(length + 1))).toThrow();
      if (length > 0) expect(() => decompressInto(frame, new Uint8Array(length - 1))).toThrow();
    }
  }
});
