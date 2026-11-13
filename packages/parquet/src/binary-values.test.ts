import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { readPackedBinaryValues } from "./binary-values.js";

it("admits only ranges and aliases nonzero-offset binary payloads", () => {
  const values = [new Uint8Array(), Uint8Array.of(0, 255, 1), new TextEncoder().encode("é😀")];
  const bytes = new Uint8Array(3 + values.reduce((sum, value) => sum + 4 + value.length, 0));
  const view = new DataView(bytes.buffer);
  let offset = 3;
  for (const value of values) {
    view.setUint32(offset, value.length, true);
    bytes.set(value, offset + 4);
    offset += value.length + 4;
  }
  const scope = new MemoryScope(new MemoryBudget(24));
  const reader = { view: new DataView(bytes.buffer, 1), offset: 2 };
  const packed = readPackedBinaryValues(reader, 3, undefined, scope);
  expect(packed.count).toBe(3);
  for (let index = 0; index < values.length; index += 1) {
    expect(packed.value(index)).toEqual(values[index]);
    expect(packed.value(index).buffer).toBe(bytes.buffer);
  }
  expect(scope.budget.usedBytes).toBe(24);
  expect(reader.offset).toBe(bytes.length - 1);
  expect(() => packed.value(3)).toThrow(/range/);
  const tight = new MemoryScope(new MemoryBudget(23));
  const rejected = { view, offset: 3 };
  expect(() => readPackedBinaryValues(rejected, 3, undefined, tight)).toThrow(/budget/);
  expect(rejected.offset).toBe(3);
  expect(tight.budget.peakBytes).toBe(0);
  scope.dispose();
  tight.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("uses implicit offsets for fixed-width payloads without a ranges allocation", () => {
  const bytes = Uint8Array.of(9, 1, 2, 3, 4);
  const scope = new MemoryScope(new MemoryBudget(0));
  const reader = { view: new DataView(bytes.buffer), offset: 1 };
  const packed = readPackedBinaryValues(reader, 2, 2, scope);
  expect(packed.value(0)).toEqual(Uint8Array.of(1, 2));
  expect(packed.value(1)).toEqual(Uint8Array.of(3, 4));
  expect(packed.ranges).toBeUndefined();
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});

it.each([
  { bytes: [255, 255, 255, 255], count: 1 },
  { bytes: [1, 0, 0], count: 1 },
  { bytes: [2, 0, 0, 0, 1], count: 1 },
  { bytes: [0, 0, 0, 0], count: 2 },
])("validates complete binary lengths before allocating %j", ({ bytes, count }) => {
  const input = Uint8Array.from(bytes);
  const scope = new MemoryScope(new MemoryBudget(64));
  const reader = { view: new DataView(input.buffer), offset: 0 };
  expect(() => readPackedBinaryValues(reader, count, undefined, scope)).toThrow(/range/);
  expect(reader.offset).toBe(0);
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});
