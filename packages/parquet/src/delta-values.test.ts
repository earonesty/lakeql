import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { readDeltaValues } from "./delta-values.js";

function unsigned(value: bigint): number[] {
  const result: number[] = [];
  do {
    const part = Number(value & 127n);
    value >>= 7n;
    result.push(part | (value ? 128 : 0));
  } while (value);
  return result;
}

it.each([
  -(1n << 63n),
  (1n << 63n) - 1n,
  9007199254740993n,
])("preserves singleton INT64 value %s", (value) => {
  const bytes = Uint8Array.from([128, 1, 4, 1, ...unsigned((value << 1n) ^ (value >> 63n))]);
  const scope = new MemoryScope(new MemoryBudget(8));
  const reader = { view: new DataView(bytes.buffer), offset: 0 };
  expect(readDeltaValues(reader, 1, false, scope)).toEqual(BigInt64Array.of(value));
  expect(reader.offset).toBe(bytes.length);
  expect(scope.budget.usedBytes).toBe(8);
  scope.dispose();
});

it("handles zero-width miniblocks without workspace allocations", () => {
  const bytes = Uint8Array.of(128, 1, 4, 3, 20, 4, 0, 0, 0, 0);
  const scope = new MemoryScope(new MemoryBudget(12));
  const reader = { view: new DataView(bytes.buffer), offset: 0 };
  expect(readDeltaValues(reader, 3, true, scope)).toEqual(Int32Array.of(10, 12, 14));
  expect(scope.budget.peakBytes).toBe(12);
  scope.dispose();
  const tight = new MemoryScope(new MemoryBudget(11));
  const rejected = { view: new DataView(bytes.buffer), offset: 0 };
  expect(() => readDeltaValues(rejected, 3, true, tight)).toThrow(/budget/);
  expect(rejected.offset).toBe(0);
  expect(tight.budget.peakBytes).toBe(0);
  tight.dispose();
});

it("consumes a valid empty stream header without allocating payload", () => {
  const bytes = Uint8Array.of(128, 1, 4, 0, 0);
  const scope = new MemoryScope(new MemoryBudget(0));
  const reader = { view: new DataView(bytes.buffer), offset: 0 };
  expect(readDeltaValues(reader, 0, true, scope)).toEqual(new Int32Array());
  expect(reader.offset).toBe(bytes.length);
  scope.dispose();
});

it.each([
  { bytes: [0, 4, 3, 20] },
  { bytes: [128, 1, 0, 3, 20] },
  { bytes: [128, 1, 4, 2, 20] },
  { bytes: [128, 1, 4, 3, 128] },
  { bytes: [128, 1, 4, 3, 20, 4, 33, 0, 0, 0] },
  { bytes: [128, 1, 4, 3, 20, 4, 8, 0, 0, 0] },
])("rejects malformed delta input %j without committing its cursor", ({ bytes }) => {
  const input = Uint8Array.from(bytes);
  const scope = new MemoryScope(new MemoryBudget(12));
  const reader = { view: new DataView(input.buffer), offset: 0 };
  expect(() => readDeltaValues(reader, 3, true, scope)).toThrow(/Malformed/);
  expect(reader.offset).toBe(0);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("preserves INT32 wraparound in zero-width delta miniblocks", () => {
  for (const increment of [2n, -2n, (1n << 40n) + 1n]) {
    const first = 2147483647n;
    const zigzag = (value: bigint) => unsigned((value << 1n) ^ (value >> 63n));
    const bytes = Uint8Array.from([
      128,
      1,
      4,
      3,
      ...zigzag(first),
      ...zigzag(increment),
      0,
      0,
      0,
      0,
    ]);
    const scope = new MemoryScope(new MemoryBudget(12));
    const actual = readDeltaValues({ view: new DataView(bytes.buffer), offset: 0 }, 3, true, scope);
    expect(actual).toEqual(
      Int32Array.from(
        [0n, 1n, 2n].map((index) => Number(BigInt.asIntN(32, first + index * increment))),
      ),
    );
    scope.dispose();
  }
});

it("decodes full-width unsigned packed deltas with exact INT32 wraparound", () => {
  const bytes = new Uint8Array(10 + 128);
  bytes.set([128, 1, 4, 3, 0, 0, 32, 0, 0, 0]);
  const view = new DataView(bytes.buffer);
  view.setUint32(10, 0xffffffff, true);
  view.setUint32(14, 2, true);
  const scope = new MemoryScope(new MemoryBudget(12));
  expect(readDeltaValues({ view, offset: 0 }, 3, true, scope)).toEqual(Int32Array.of(0, -1, 1));
  scope.dispose();
});
