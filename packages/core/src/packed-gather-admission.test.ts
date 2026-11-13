import { expect, it, vi } from "vitest";
import type { PackedByteVector } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { compactPackedGather } from "./packed-gather.js";

const source: PackedByteVector = {
  type: "binary-view",
  data: Uint8Array.of(10, 20, 30),
  ranges: Uint32Array.of(0, 2, 2, 1),
  valid: Uint8Array.of(1, 0),
  length: 2,
};

it("admits payload, ranges and validity atomically at exact capacity", () => {
  // Repeated first value, followed by null: 24 range + 4 payload + 3 validity bytes.
  const budget = new MemoryBudget(31);
  const scope = new MemoryScope(budget);
  const output = compactPackedGather(source, [0, 0, 1], scope);
  expect([...output.data]).toEqual([10, 20, 10, 20]);
  expect([...(output.ranges ?? [])]).toEqual([0, 2, 2, 2, 4, 0]);
  expect([...(output.valid ?? [])]).toEqual([1, 1, 0]);
  expect(budget.usedBytes).toBe(31);
  expect(budget.peakBytes).toBe(31);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("rejects cap-minus-one before allocating any partial output", () => {
  const budget = new MemoryBudget(30);
  const scope = new MemoryScope(budget);
  const allocate = vi.spyOn(scope, "allocateReserved");
  expect(() => compactPackedGather(source, [0, 0, 1], scope)).toThrow("budget");
  expect(allocate).not.toHaveBeenCalled();
  expect(budget.usedBytes).toBe(0);
  expect(budget.peakBytes).toBe(0);
  scope.dispose();
});

it("releases successful partial buffers and unused admission after allocation failure", () => {
  const budget = new MemoryBudget(31);
  const scope = new MemoryScope(budget);
  const allocate = scope.allocateReserved.bind(scope);
  let calls = 0;
  vi.spyOn(scope, "allocateReserved").mockImplementation((admission, create) => {
    if (++calls === 2) throw new Error("allocator failed");
    return allocate(admission, create);
  });
  expect(() => compactPackedGather(source, [0, 0, 1], scope)).toThrow("allocator failed");
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it("handles empty output without a validity allocation", () => {
  const budget = new MemoryBudget(0);
  const scope = new MemoryScope(budget);
  const output = compactPackedGather(source, [], scope);
  expect(output.length).toBe(0);
  expect(output.valid).toBeUndefined();
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});
