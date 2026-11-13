import { expect, it } from "vitest";
import { selectedRowIndexBuffer } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("admits only selected indices and preserves SQL unknown exclusions", () => {
  const budget = new MemoryBudget(8);
  const scope = new MemoryScope(budget);
  try {
    expect(selectedRowIndexBuffer(5, new Uint8Array([2, 1, 0, 1, 2]), scope)).toEqual(
      new Uint32Array([1, 3]),
    );
    expect(budget.usedBytes).toBe(8);
    expect(budget.peakBytes).toBe(8);
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("rejects one byte below the index allocation before charging or constructing output", () => {
  const budget = new MemoryBudget(7);
  const scope = new MemoryScope(budget);
  try {
    expect(() => selectedRowIndexBuffer(2, undefined, scope)).toThrow();
    expect(budget.usedBytes).toBe(0);
  } finally {
    scope.dispose();
  }
});

it("handles empty and unfiltered selections", () => {
  expect(selectedRowIndexBuffer(0)).toEqual(new Uint32Array());
  expect(selectedRowIndexBuffer(3)).toEqual(new Uint32Array([0, 1, 2]));
  expect(selectedRowIndexBuffer(3, new Uint8Array([0, 2, 0]))).toEqual(new Uint32Array());
});

it.each([
  -1,
  0.5,
  Number.NaN,
  Number.POSITIVE_INFINITY,
  0x100000001,
])("rejects invalid index dimensions before allocation: %s", (rowCount) => {
  const budget = new MemoryBudget(1);
  const scope = new MemoryScope(budget);
  try {
    expect(() => selectedRowIndexBuffer(rowCount, undefined, scope)).toThrow(/capacity/);
    expect(budget.peakBytes).toBe(0);
  } finally {
    scope.dispose();
  }
});

it("rejects both short and long masks", () => {
  for (const length of [1, 3]) {
    expect(() => selectedRowIndexBuffer(2, new Uint8Array(length))).toThrow(/length/);
  }
});
