import assert from "node:assert/strict";
import { constants, zstdCompressSync } from "node:zlib";
import { decompressZstdInto as decompressInto, MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { scopedParquetCompressors, ZSTD_REUSABLE_WORKSPACE_BYTES } from "./compressors.js";

it("releases replaced entropy tables across native-compressed ZSTD blocks", () => {
  const input = new Uint8Array(1024 * 1024);
  let state = 12345;
  for (let index = 0; index < input.length; index++) {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    const alphabet = 3 + 13 * Math.floor(index / 131072);
    input[index] = (state >>> 16) % alphabet;
  }
  const compressed = zstdCompressSync(input);
  expect(compressed.length).toBeLessThan(input.length);
  // Comparison keeps all frame allocations, matching the earlier ownership lifetime.
  const baselineBudget = new MemoryBudget(4 * input.length);
  const baselineScope = new MemoryScope(baselineBudget);
  try {
    assert.deepEqual(
      decompressInto(
        compressed,
        new Uint8Array(input.length),
        (bytes) =>
          baselineScope.allocate(bytes, "retained frame workspace", () => new Uint8Array(bytes))
            .value,
      ),
      input,
    );
  } finally {
    baselineScope.dispose();
  }
  function run(cap: number) {
    const budget = new MemoryBudget(cap);
    const scope = new MemoryScope(budget);
    try {
      assert.deepEqual(scopedParquetCompressors(scope).ZSTD(compressed, input.length), input);
      expect(budget.usedBytes).toBe(input.length);
      return budget.peakBytes;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  }
  const peak = run(4 * input.length);
  expect(peak - input.length).toBeLessThan(baselineBudget.peakBytes);
  run(peak);
  expect(() => run(peak - 1)).toThrow(/budget/);
});

it("reuses admitted entropy workspace across concatenated native frames", () => {
  const input = Uint8Array.from(
    { length: 131072 },
    (_, index) => (Math.imul(index, index + 17) >>> 5) % 29,
  );
  const frame = zstdCompressSync(input);
  let workspaceBytes = 0;
  assert.deepEqual(
    decompressInto(frame, new Uint8Array(input.length), (bytes) => {
      workspaceBytes += bytes;
      return new Uint8Array(bytes);
    }),
    input,
  );
  expect(workspaceBytes).toBeGreaterThan(12);
  const copies = 8;
  const compressed = new Uint8Array(frame.length * copies);
  const expected = new Uint8Array(input.length * copies);
  for (let index = 0; index < copies; index++) {
    compressed.set(frame, index * frame.length);
    expected.set(input, index * input.length);
  }
  const cap = expected.length + workspaceBytes;
  const budget = new MemoryBudget(cap);
  const scope = new MemoryScope(budget);
  scope.admit(cap, "complete repeated-frame decode");
  try {
    assert.deepEqual(scopedParquetCompressors(scope).ZSTD(compressed, expected.length), expected);
    expect(budget.peakBytes).toBe(cap);
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("cleans admitted reusable workspace after truncated input or exhausted admission", () => {
  const input = Uint8Array.from(
    { length: 131072 },
    (_, index) => (Math.imul(index, index + 17) >>> 5) % 29,
  );
  const frame = zstdCompressSync(input);
  for (const truncated of [false, true]) {
    const cap = input.length + (truncated ? 65536 : 12);
    const budget = new MemoryBudget(cap);
    const scope = new MemoryScope(budget);
    scope.admit(cap, "failing decode");
    try {
      expect(() =>
        scopedParquetCompressors(scope).ZSTD(
          truncated ? frame.subarray(0, frame.length - 1) : frame,
          input.length,
        ),
      ).toThrow();
    } finally {
      scope.dispose();
    }
    expect(budget.usedBytes).toBe(0);
    expect(budget.peakBytes).toBe(cap);
  }
});

it("admits changing multi-block entropy tables within the static reusable bound", () => {
  const input = new Uint8Array(2 * 1024 * 1024);
  let state = 1729;
  for (let index = 0; index < input.length; index++) {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    input[index] = (state >>> 16) % (3 + 13 * Math.floor(index / 131072));
  }
  for (const level of [1, 3, 9]) {
    const compressed = zstdCompressSync(input, {
      params: { [constants.ZSTD_c_compressionLevel]: level },
    });
    const cap = input.length + ZSTD_REUSABLE_WORKSPACE_BYTES;
    const budget = new MemoryBudget(cap);
    const parent = new MemoryScope(budget);
    parent.admit(cap, "static complete ZSTD workspace");
    const scope = parent.childScope();
    try {
      assert.deepEqual(scopedParquetCompressors(scope).ZSTD(compressed, input.length), input);
      expect(cap - parent.availableAllocationBytes - input.length).toBeLessThanOrEqual(
        ZSTD_REUSABLE_WORKSPACE_BYTES,
      );
    } finally {
      scope.dispose();
      parent.dispose();
    }
    expect(budget.usedBytes).toBe(0);
  }
});
