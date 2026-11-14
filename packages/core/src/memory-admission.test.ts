import { expect, it, vi } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  "admit",
  "reserve",
  "allocate",
  "allocateAsync",
] as const)("cleans up %s when cache reclamation cancels the scope", async (operation) => {
  const budget = new MemoryBudget(4);
  const cached = budget.reserve(4, "cache");
  const controller = new AbortController();
  const scope = new MemoryScope(budget, [controller.signal]);
  budget.registerReclaimer(() => {
    cached.release();
    controller.abort();
  });
  const allocate = vi.fn(() => new Uint8Array(4));
  if (operation === "allocateAsync") {
    await expect(scope.allocateAsync(4, "output", async () => allocate())).rejects.toThrow();
  } else {
    expect(() => {
      if (operation === "allocate") scope.allocate(4, "output", allocate);
      else scope[operation](4, "output");
    }).toThrow();
  }
  expect(allocate).not.toHaveBeenCalled();
  expect(scope.disposed).toBe(true);
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("does not overwrite or bypass admission installed by a reentrant cache reclaimer", () => {
  const budget = new MemoryBudget(8);
  const cached = budget.reserve(8, "cache");
  const scope = new MemoryScope(budget);
  budget.registerReclaimer(() => {
    cached.release();
    scope.admit(4, "reentrant stage");
  });
  expect(() => scope.admit(4, "outer stage")).toThrow("changed during reclamation");
  expect(budget.usedBytes).toBe(4);
  const output = scope.allocate(4, "inner output", () => new Uint8Array(4));
  expect(output.bytes).toBe(4);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("limits stage allocations to admitted capacity and releases unused capacity on disposal", () => {
  const budget = new MemoryBudget(100);
  const scope = new MemoryScope(budget);
  scope.admit(12, "output");
  expect(() => scope.admit(1, "duplicate")).toThrow("already");
  const first = scope.allocate(8, "values", () => new Uint8Array(8));
  const allocate = vi.fn(() => new Uint8Array(5));
  expect(() => scope.allocate(5, "underestimated shape", allocate)).toThrow("admitted");
  expect(allocate).not.toHaveBeenCalled();
  expect(budget.usedBytes).toBe(12);
  const retained = first.retain();
  scope.dispose();
  expect(budget.usedBytes).toBe(8);
  retained.release();
  expect(budget.usedBytes).toBe(0);
});

it("does not alter stage admission for invalid allocation sizes", () => {
  const budget = new MemoryBudget(4);
  const scope = new MemoryScope(budget);
  scope.admit(4, "stage");
  for (const bytes of [-1, NaN, Infinity, 0.5]) {
    expect(() => scope.reserve(bytes, "invalid")).toThrow();
    expect(budget.usedBytes).toBe(4);
  }
  const owner = scope.allocate(4, "all output", () => new Uint8Array(4));
  expect(owner.bytes).toBe(4);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("tracks completed asynchronous admission and releases synchronous disposal failures", async () => {
  const budget = new MemoryBudget(4);
  const scope = new MemoryScope(budget);
  const owner = await scope.allocateReservedAsync(
    budget.reserve(4, "completed"),
    async () => new Uint8Array(4),
  );
  expect(owner.value.byteLength).toBe(4);
  expect(budget.usedBytes).toBe(4);
  owner.release();
  expect(() =>
    scope.allocateReserved(budget.reserve(4, "closing"), () => {
      scope.dispose();
      return new Uint8Array(4);
    }),
  ).toThrow();
  expect(budget.usedBytes).toBe(0);
});

it("allocates partitioned working-set extents without admitting capacity twice", () => {
  const budget = new MemoryBudget(12);
  const scope = new MemoryScope(budget);
  const parts = budget.reserve(12, "stage").partition([8, 4]);
  const scratchAdmission = parts[0];
  const outputAdmission = parts[1];
  if (!scratchAdmission || !outputAdmission) throw new Error("Missing admission");
  const scratch = scope.allocateReserved(scratchAdmission, () => new Uint8Array(8));
  const output = scope.allocateReserved(outputAdmission, () => new Uint8Array(4));
  scratchAdmission.release();
  outputAdmission.release();
  expect(budget.usedBytes).toBe(12);
  scratch.release();
  const retained = output.retain();
  scope.dispose();
  expect(budget.usedBytes).toBe(4);
  retained.release();
  expect(budget.usedBytes).toBe(0);
  expect(budget.peakBytes).toBe(12);
});

it("preserves caller admission when validation fails before allocation", () => {
  const budget = new MemoryBudget(4);
  const scope = new MemoryScope(budget);
  const sibling = budget.child();
  const wrong = sibling.reserve(4, "wrong owner");
  const allocate = vi.fn(() => new Uint8Array(4));
  expect(() => scope.allocateReserved(wrong, allocate)).toThrow("same memory budget");
  expect(budget.usedBytes).toBe(4);
  wrong.release();
  const admission = budget.reserve(4, "shared");
  const alias = admission.retain();
  expect(() => scope.allocateReserved(admission, allocate)).toThrow("exclusive");
  alias.release();
  scope.dispose();
  expect(() => scope.allocateReserved(admission, allocate)).toThrow();
  expect(allocate).not.toHaveBeenCalled();
  expect(budget.usedBytes).toBe(4);
  admission.release();
  expect(budget.usedBytes).toBe(0);
});

it("releases consumed admission on allocator failure and forbids reuse", () => {
  const budget = new MemoryBudget(4);
  const scope = new MemoryScope(budget);
  const admission = budget.reserve(4, "failure");
  expect(() =>
    scope.allocateReserved(admission, () => {
      throw new Error("failed");
    }),
  ).toThrow("failed");
  expect(budget.usedBytes).toBe(0);
  const allocate = vi.fn();
  expect(() => scope.allocateReserved(admission, allocate)).toThrow("released");
  expect(allocate).not.toHaveBeenCalled();
  scope.dispose();
});

it.each([
  false,
  true,
])("keeps pending admission charged after cancellation, rejection=%s", async (reject) => {
  const budget = new MemoryBudget(4);
  const controller = new AbortController();
  const scope = new MemoryScope(budget, [controller.signal]);
  const admission = budget.reserve(4, "pending");
  let settle = () => {};
  const work = scope.allocateReservedAsync(
    admission,
    () =>
      new Promise<Uint8Array>((resolve, fail) => {
        settle = () => (reject ? fail(new Error("transport failed")) : resolve(new Uint8Array(4)));
      }),
  );
  const checked = expect(work).rejects.toThrow();
  controller.abort();
  admission.release();
  expect(budget.usedBytes).toBe(4);
  expect(budget.tryReserve(1, "competing job")).toBeUndefined();
  settle();
  await checked;
  expect(budget.usedBytes).toBe(0);
});

it("transfers admitted capacity into child stages without double charging and drains pending cancellation", async () => {
  const budget = new MemoryBudget(16);
  const parent = new MemoryScope(budget);
  parent.admit(16, "whole stage");
  expect(budget.availableBytes).toBe(0);
  expect(parent.availableAllocationBytes).toBe(16);
  const child = parent.childStage(12, "decode");
  expect(parent.availableAllocationBytes).toBe(4);
  expect(child.availableAllocationBytes).toBe(12);
  let finish = () => {};
  const pending = child.allocateAsync(
    8,
    "pending bytes",
    () =>
      new Promise<Uint8Array>((resolve) => {
        finish = () => resolve(new Uint8Array(8));
      }),
  );
  const checked = expect(pending).rejects.toThrow();
  expect(child.availableAllocationBytes).toBe(4);
  expect(budget.usedBytes).toBe(16);
  parent.dispose();
  expect(child.disposed).toBe(true);
  expect(child.availableAllocationBytes).toBe(0);
  expect(budget.usedBytes).toBe(8);
  finish();
  await checked;
  expect(budget.usedBytes).toBe(0);
  expect(budget.peakBytes).toBe(16);
});
