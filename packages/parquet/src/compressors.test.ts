import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { lakeqlParquetCompressors, scopedParquetCompressors } from "./compressors.js";

const zstdHello = Uint8Array.of(40, 181, 47, 253, 32, 5, 41, 0, 0, 104, 101, 108, 108, 111);
// Snappy uncompressed-length varint, literal-length tag, then five literal bytes.
const snappyHello = Uint8Array.of(5, 16, 104, 101, 108, 108, 111);

it.each([1, 2, 100])("consumes pre-admitted output and workspace for %s ZSTD frames", (count) => {
  const input = new Uint8Array(zstdHello.length * count);
  for (let index = 0; index < count; index++) input.set(zstdHello, index * zstdHello.length);
  // All frames reuse one offset buffer; admission still counts total new backing.
  const extent = count * 5 + 12;
  for (const admitted of [extent, extent - 1]) {
    const budget = new MemoryBudget(extent);
    const scope = new MemoryScope(budget);
    scope.admit(admitted, "complete decode extent");
    try {
      if (admitted < extent) {
        expect(() => scopedParquetCompressors(scope).ZSTD(input, count * 5)).toThrow(/extent/);
      } else {
        const output = scopedParquetCompressors(scope).ZSTD(input, count * 5);
        expect(new TextDecoder().decode(output)).toBe("hello".repeat(count));
        expect(scope.availableAllocationBytes).toBe(0);
        expect(budget.usedBytes).toBe(count * 5);
        const retained = scope.retainAllocation(output);
        scope.dispose();
        expect(budget.usedBytes).toBe(count * 5);
        retained.release();
      }
    } finally {
      scope.dispose();
    }
    expect(budget.usedBytes).toBe(0);
  }
});

it.each(["ZSTD", "SNAPPY"] as const)("reserves %s page output before decoding", (codec) => {
  const required = codec === "ZSTD" ? 17 : 5;
  const budget = new MemoryBudget(required);
  const scope = new MemoryScope(budget);
  const decompress = scopedParquetCompressors(scope)[codec];
  const input = codec === "ZSTD" ? zstdHello : snappyHello;
  const output = decompress(input, 5);
  expect(new TextDecoder().decode(output)).toBe("hello");
  expect(budget.usedBytes).toBe(5);
  expect(() => decompress(input, 5)).toThrow("Allocation exceeds memory budget");
  expect(budget.peakBytes).toBe(required);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it.each([
  "ZSTD",
  "SNAPPY",
] as const)("rejects oversize %s output before touching malformed input", (codec) => {
  const budget = new MemoryBudget(4);
  const scope = new MemoryScope(budget);
  expect(() => scopedParquetCompressors(scope)[codec](new Uint8Array(), 5)).toThrow(
    "Allocation exceeds memory budget",
  );
  expect(budget.peakBytes).toBe(0);
  expect(budget.usedBytes).toBe(0);
});

it("releases admission when decompression fails", () => {
  const budget = new MemoryBudget(8);
  const scope = new MemoryScope(budget);
  expect(() => scopedParquetCompressors(scope).ZSTD(Uint8Array.of(1, 2, 3, 4), 8)).toThrow();
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it.each([
  Uint8Array.of(6, ...snappyHello.subarray(1)),
  Uint8Array.of(0, ...snappyHello.subarray(1)),
  Uint8Array.of(0x80, 0x80, 0x80, 0x80, 0x80, 0),
  Uint8Array.of(0xff, 0xff, 0xff, 0xff, 0x10),
])("rejects inconsistent or oversized Snappy length preambles", (input) => {
  const budget = new MemoryBudget(5);
  const scope = new MemoryScope(budget);
  try {
    expect(() => scopedParquetCompressors(scope).SNAPPY(input, 5)).toThrow(/Snappy length/);
    expect(budget.usedBytes).toBe(0);
    expect(() => lakeqlParquetCompressors.SNAPPY(input, 5)).toThrow(/Snappy length/);
  } finally {
    scope.dispose();
  }
});

it("accepts empty Snappy payloads without retained allocation", () => {
  const budget = new MemoryBudget(0);
  const scope = new MemoryScope(budget);
  expect(scopedParquetCompressors(scope).SNAPPY(Uint8Array.of(0), 0)).toEqual(new Uint8Array());
  expect(scopedParquetCompressors(scope).SNAPPY(new Uint8Array(), 0)).toEqual(new Uint8Array());
  expect(lakeqlParquetCompressors.SNAPPY(new Uint8Array(), 0)).toEqual(new Uint8Array());
  expect(() => lakeqlParquetCompressors.SNAPPY(new Uint8Array(), 1)).toThrow();
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("validates ZSTD actual lengths, multiple frames, unknown sizes, and skippable frames", () => {
  const decode = lakeqlParquetCompressors.ZSTD;
  expect(() => decode(zstdHello, 6)).toThrow(/length/);
  expect(() => decode(zstdHello, 4)).toThrow();
  const unknownSize = Uint8Array.of(...zstdHello.subarray(0, 4), 0, 0, ...zstdHello.subarray(6));
  expect(new TextDecoder().decode(decode(unknownSize, 5))).toBe("hello");
  expect(() => decode(unknownSize, 6)).toThrow(/length/);
  const frames = Uint8Array.of(...zstdHello, ...unknownSize);
  expect(new TextDecoder().decode(decode(frames, 10))).toBe("hellohello");
  const skipped = Uint8Array.of(0x50, 0x2a, 0x4d, 0x18, 3, 0, 0, 0, 1, 2, 3, ...frames);
  expect(new TextDecoder().decode(decode(skipped, 10))).toBe("hellohello");
});

it("admits ZSTD workspace with the output and releases it on failure", () => {
  const budget = new MemoryBudget(16);
  const scope = new MemoryScope(budget);
  expect(() => scopedParquetCompressors(scope).ZSTD(zstdHello, 5)).toThrow(/budget/);
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it.each([1, 100])("releases completed ZSTD frame workspace (%s frames)", (count) => {
  const input = new Uint8Array(zstdHello.length * count);
  for (let index = 0; index < count; index++) input.set(zstdHello, index * zstdHello.length);
  const outputBytes = 5 * count;
  const budget = new MemoryBudget(outputBytes + 12);
  const scope = new MemoryScope(budget);
  try {
    const output = scopedParquetCompressors(scope).ZSTD(input, outputBytes);
    expect(new TextDecoder().decode(output)).toBe("hello".repeat(count));
    expect(budget.peakBytes).toBe(outputBytes + 12);
    expect(budget.usedBytes).toBe(outputBytes);
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
  const tightBudget = new MemoryBudget(outputBytes + 11);
  const tightScope = new MemoryScope(tightBudget);
  try {
    expect(() => scopedParquetCompressors(tightScope).ZSTD(input, outputBytes)).toThrow(/budget/);
    expect(tightBudget.usedBytes).toBe(0);
  } finally {
    tightScope.dispose();
  }
});

it("cleans up ZSTD output and workspace when a later frame is truncated", () => {
  const input = Uint8Array.of(...zstdHello, ...zstdHello.subarray(0, -1));
  const budget = new MemoryBudget(22);
  const scope = new MemoryScope(budget);
  expect(() => scopedParquetCompressors(scope).ZSTD(input, 10)).toThrow();
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it("does not truncate ZSTD eight-byte content lengths or treat declared zero as unknown", () => {
  const large = Uint8Array.of(
    ...zstdHello.subarray(0, 4),
    0xe0,
    5,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    ...zstdHello.subarray(6),
  );
  expect(() => lakeqlParquetCompressors.ZSTD(large, 5)).toThrow(/exceeds destination/);
  const zero = Uint8Array.from(zstdHello);
  zero[5] = 0;
  expect(() => lakeqlParquetCompressors.ZSTD(zero, 5)).toThrow(/content length/);
  const validWide = Uint8Array.from(large);
  validWide[9] = 0;
  expect(new TextDecoder().decode(lakeqlParquetCompressors.ZSTD(validWide, 5))).toBe("hello");
});

it("rejects truncated ZSTD frame headers and overflowing content sizes without retained admission", () => {
  const inputs = [
    zstdHello.subarray(0, 4),
    zstdHello.subarray(0, 5),
    Uint8Array.of(...zstdHello.subarray(0, 4), 0xe0, 5),
    Uint8Array.of(...zstdHello.subarray(0, 4), 0xe0, 0, 0, 0, 0, 0, 0, 0, 0x80),
    Uint8Array.of(0x50, 0x2a, 0x4d, 0x18),
  ];
  for (const input of inputs) {
    const budget = new MemoryBudget(64);
    const scope = new MemoryScope(budget);
    expect(() => scopedParquetCompressors(scope).ZSTD(input, 5)).toThrow();
    expect(budget.usedBytes).toBe(0);
    scope.dispose();
  }
});
