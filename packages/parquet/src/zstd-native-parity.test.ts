import assert from "node:assert/strict";
import { constants, zstdCompressSync } from "node:zlib";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { scopedParquetCompressors } from "./compressors.js";

it.each([1, 3, 9])("matches native ZSTD payloads at compression level %s", (level) => {
  for (const length of [0, 1, 31, 1024, 131072, 131073]) {
    for (const pattern of ["repeated", "structured", "random"] as const) {
      const input = new Uint8Array(length);
      let state = 1729;
      for (let index = 0; index < length; index++) {
        state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
        input[index] =
          pattern === "repeated"
            ? 65
            : pattern === "structured"
              ? index % 97 < 80
                ? index % 7
                : state >>> 24
              : state >>> 24;
      }
      const compressed = zstdCompressSync(input, {
        params: { [constants.ZSTD_c_compressionLevel]: level },
      });
      const budget = new MemoryBudget(length + 256 * 1024);
      const scope = new MemoryScope(budget);
      try {
        const output = scopedParquetCompressors(scope).ZSTD(compressed, length);
        assert.deepEqual(output, input, `${level}/${length}/${pattern}`);
        expect(budget.usedBytes).toBe(length);
      } finally {
        scope.dispose();
      }
      expect(budget.usedBytes).toBe(0);
      // Cut actual compressed bytes, including the last block, without relying on output length mismatch.
      for (const cut of new Set([1, Math.min(5, compressed.length - 1), compressed.length - 1])) {
        const failed = new MemoryBudget(length + 256 * 1024);
        const failedScope = new MemoryScope(failed);
        try {
          expect(() =>
            scopedParquetCompressors(failedScope).ZSTD(compressed.subarray(0, cut), length),
          ).toThrow();
          expect(failed.usedBytes).toBe(0);
        } finally {
          failedScope.dispose();
        }
      }
    }
  }
});
