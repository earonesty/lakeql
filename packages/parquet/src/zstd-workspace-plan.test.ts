import assert from "node:assert/strict";
import { constants, zstdCompressSync } from "node:zlib";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { scopedParquetCompressors, ZSTD_REUSABLE_WORKSPACE_BYTES } from "./compressors.js";
import { planZstdWorkspaceBytes } from "./zstd-workspace-plan.js";

it("plans raw, RLE, skippable and concatenated framing without decoding payloads", () => {
  const raw = Uint8Array.of(40, 181, 47, 253, 32, 5, 41, 0, 0, 104, 101, 108, 108, 111);
  const rle = Uint8Array.of(40, 181, 47, 253, 32, 5, 43, 0, 0, 65);
  const skip = Uint8Array.of(0x50, 0x2a, 0x4d, 0x18, 2, 0, 0, 0, 7, 8);
  expect(planZstdWorkspaceBytes(new Uint8Array())).toBe(0);
  expect(planZstdWorkspaceBytes(skip)).toBe(0);
  expect(planZstdWorkspaceBytes(raw)).toBe(12);
  expect(planZstdWorkspaceBytes(rle)).toBe(12);
  expect(planZstdWorkspaceBytes(Uint8Array.of(...raw, ...skip, ...rle))).toBe(12);
  for (const frame of [raw, rle, skip]) {
    for (let length = 1; length < frame.length; length++)
      expect(() => planZstdWorkspaceBytes(frame.subarray(0, length))).toThrow();
  }
  const reserved = raw.slice();
  reserved[6] = 47;
  expect(() => planZstdWorkspaceBytes(reserved)).toThrow(/Reserved/);
});

it.each([1, 3, 9])("bounds actual native codec allocations at compression level %s", (level) => {
  for (const length of [0, 1, 8192, 262145, 1048576]) {
    let state = 1729;
    const input = Uint8Array.from({ length }, (_, index) => {
      state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
      return (state >>> 16) % (3 + 13 * Math.floor(index / 131072));
    });
    const frame = zstdCompressSync(input, {
      params: { [constants.ZSTD_c_compressionLevel]: level, [constants.ZSTD_c_checksumFlag]: 1 },
    });
    const backing = new Uint8Array(frame.length + 1);
    backing[0] = 99;
    backing.set(frame, 1);
    const encoded = backing.subarray(1);
    const workspace = planZstdWorkspaceBytes(encoded);
    expect(workspace).toBeLessThanOrEqual(ZSTD_REUSABLE_WORKSPACE_BYTES);
    if (length <= 8192) expect(workspace).toBeLessThanOrEqual(11824);
    const budget = new MemoryBudget(length + workspace);
    const scope = new MemoryScope(budget);
    scope.admit(length + workspace, "framing-derived extent");
    try {
      assert.deepEqual(scopedParquetCompressors(scope).ZSTD(encoded, length), input);
    } finally {
      scope.dispose();
    }
    expect(budget.usedBytes).toBe(0);
  }
});
