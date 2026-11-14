import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { readDictionaryIndices } from "./dictionary-indices.js";

it("decodes zero-bit dictionaries without reading any payload values", () => {
  const scope = new MemoryScope(new MemoryBudget(12));
  const reader = { view: new DataView(Uint8Array.of(0).buffer), offset: 0 };
  expect(Array.from(readDictionaryIndices(reader, 3, 1, scope))).toEqual([0, 0, 0]);
  expect(reader.offset).toBe(1);
  expect(scope.budget.usedBytes).toBe(12);
  scope.dispose();
});

it("rejects invalid dictionary extents before allocation", () => {
  for (const [count, length] of [
    [-1, 1],
    [1, -1],
    [1, 1.5],
    [1, 0x100000000],
  ]) {
    const scope = new MemoryScope(new MemoryBudget(65536));
    const reader = { view: new DataView(Uint8Array.of(0).buffer), offset: 0 };
    expect(() => readDictionaryIndices(reader, count, length, scope)).toThrow(/extent/);
    expect(scope.budget.usedBytes).toBe(0);
    expect(reader.offset).toBe(0);
    scope.dispose();
  }
});

it.each([
  { bytes: [0, 3], count: 3 }, // One padded bit-packed group, no payload bits.
  { bytes: [0, 6], count: 3 }, // RLE run of three zeros, no payload bytes.
  { bytes: [0, 2, 4], count: 3 }, // Two RLE runs.
])("consumes zero-width hybrid run headers $bytes", ({ bytes, count }) => {
  const scope = new MemoryScope(new MemoryBudget(count * 4));
  try {
    const reader = { view: new DataView(Uint8Array.from(bytes).buffer), offset: 0 };
    expect(Array.from(readDictionaryIndices(reader, count, 1, scope))).toEqual([0, 0, 0]);
    expect(reader.offset).toBe(bytes.length);
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it.each([
  [0, 0],
  [0, 128],
  [0, 8],
  [0, 2],
])("rejects malformed zero-width runs %j", (...bytes) => {
  const scope = new MemoryScope(new MemoryBudget(12));
  try {
    const reader = { view: new DataView(Uint8Array.from(bytes).buffer), offset: 0 };
    expect(() => readDictionaryIndices(reader, 3, 1, scope)).toThrow(/Malformed/);
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("consumes an optional width byte for zero indices and rejects trailing payload", () => {
  const scope = new MemoryScope(new MemoryBudget(0));
  for (const bytes of [[], [0], [32]]) {
    const reader = { view: new DataView(Uint8Array.from(bytes).buffer), offset: 0 };
    expect(readDictionaryIndices(reader, 0, 0, scope).length).toBe(0);
    expect(reader.offset).toBe(bytes.length);
  }
  for (const bytes of [[33], [0, 3]]) {
    const reader = { view: new DataView(Uint8Array.from(bytes).buffer), offset: 0 };
    expect(() => readDictionaryIndices(reader, 0, 0, scope)).toThrow(/empty dictionary/);
  }
  scope.dispose();
});
