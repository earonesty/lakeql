import { expect, it } from "vitest";
import { allocateInt96Storage, readInt96, writeInt96 } from "./int96-storage.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("round-trips signed boundaries in one exactly admitted backing allocation", () => {
  const values = [
    -(1n << 95n),
    -(1n << 64n) - 1n,
    -(1n << 63n),
    -1n,
    0n,
    1n,
    1n << 63n,
    (1n << 95n) - 1n,
  ];
  const scope = new MemoryScope(new MemoryBudget(values.length * 12));
  const storage = allocateInt96Storage(values.length, scope);
  expect(storage.low.buffer).toBe(storage.high.buffer);
  expect(storage.high.byteOffset).toBe(values.length * 8);
  for (const [index, value] of values.entries()) writeInt96(storage, index, value);
  expect(values.map((_, index) => readInt96(storage, index))).toEqual(values);
  expect(scope.budget.usedBytes).toBe(values.length * 12);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(values.length * 12 - 1));
  expect(() => allocateInt96Storage(values.length, tight)).toThrow(/budget/);
  expect(tight.budget.peakBytes).toBe(0);
  tight.dispose();
});

it("validates capacities and indexes and rejects overflow before changing storage", () => {
  for (const length of [-1, 1.5, Infinity, Number.MAX_SAFE_INTEGER])
    expect(() => allocateInt96Storage(length)).toThrow(/length/);
  const storage = allocateInt96Storage(3);
  writeInt96(storage, 1, 123n);
  for (const value of [-(1n << 95n) - 1n, 1n << 95n]) {
    expect(() => writeInt96(storage, 1, value)).toThrow(/capacity/);
    expect(readInt96(storage, 1)).toBe(123n);
  }
  for (const index of [-1, 3, NaN, 0.5]) {
    expect(() => readInt96(storage, index)).toThrow(/index/);
    expect(() => writeInt96(storage, index, 0n)).toThrow(/index/);
  }
  expect(allocateInt96Storage(0).low.byteLength).toBe(0);
});

it("shares parent capacity between independently owned 96-bit buffers", () => {
  const pool = new MemoryBudget(36);
  const first = new MemoryScope(pool.child(24));
  const second = new MemoryScope(pool.child(24));
  const storage = allocateInt96Storage(2, first);
  const owner = first.retainAllocations(storage);
  first.dispose();
  expect(() => allocateInt96Storage(2, second)).toThrow(/budget/);
  const other = allocateInt96Storage(1, second);
  writeInt96(other, 0, -1n);
  expect(pool.usedBytes).toBe(36);
  owner.release();
  expect(pool.usedBytes).toBe(12);
  second.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("gathers duplicate selections and concatenates borrowed slices without bigint intermediates", async () => {
  const { gatherInt96Storage, concatInt96Storage, sliceInt96Storage } = await import(
    "./int96-storage.js"
  );
  const input = allocateInt96Storage(3);
  const values = [-(1n << 95n), 9n, (1n << 95n) - 1n];
  for (const [index, value] of values.entries()) writeInt96(input, index, value);
  const scope = new MemoryScope(new MemoryBudget(60));
  const gathered = gatherInt96Storage(input, Uint32Array.of(2, 0, 2), scope);
  expect([0, 1, 2].map((index) => readInt96(gathered, index))).toEqual([
    values[2],
    values[0],
    values[2],
  ]);
  const borrowed = sliceInt96Storage(gathered, 1, 2);
  expect(borrowed.low.buffer).toBe(gathered.low.buffer);
  const combined = concatInt96Storage([borrowed, sliceInt96Storage(input, 1, 2)], scope);
  expect([readInt96(combined, 0), readInt96(combined, 1)]).toEqual([values[0], 9n]);
  expect(scope.budget.usedBytes).toBe(60);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(35));
  expect(() => gatherInt96Storage(input, [0, 1, 2], tight)).toThrow(/budget/);
  expect(tight.budget.peakBytes).toBe(0);
  expect(() => gatherInt96Storage(input, [0, 3], tight)).toThrow(/index/);
  expect(tight.budget.peakBytes).toBe(0);
  expect(() => sliceInt96Storage(input, 2, 1)).toThrow(/range/);
  expect(concatInt96Storage([]).low.length).toBe(0);
  tight.dispose();
});
