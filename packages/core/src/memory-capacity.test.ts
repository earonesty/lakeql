import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { reserveMemoryCapacity } from "./memory-capacity.js";
import { MemoryScope } from "./memory-scope.js";

it.each([false, true])("grows to the same cap with fixed stage=%s", (fixed) => {
  const scope = new MemoryScope(new MemoryBudget(100));
  if (fixed) scope.admit(100, "workspace");
  let capacity = reserveMemoryCapacity(scope, undefined, 20, "workspace");
  capacity = reserveMemoryCapacity(scope, capacity, 100, "workspace");
  expect(reserveMemoryCapacity(scope, capacity, 50, "workspace")).toBe(capacity);
  expect(scope.budget.peakBytes).toBe(100);
  expect(() => reserveMemoryCapacity(scope, capacity, 101, "workspace")).toThrow(/budget|extent/i);
  expect(capacity.bytes).toBe(100);
  capacity.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("preserves existing capacity when merge rejects a shared reservation", () => {
  const scope = new MemoryScope(new MemoryBudget(100));
  const first = reserveMemoryCapacity(scope, undefined, 20, "workspace");
  const alias = first.retain();
  expect(() => reserveMemoryCapacity(scope, first, 100, "workspace")).toThrow(/exclusive/i);
  expect(scope.budget.usedBytes).toBe(20);
  alias.release();
  const grown = reserveMemoryCapacity(scope, first, 100, "workspace");
  expect(grown.bytes).toBe(100);
  grown.release();
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("rejects invalid capacity even when a prior reservation exists", () => {
  const scope = new MemoryScope(new MemoryBudget(100));
  const first = reserveMemoryCapacity(scope, undefined, 20, "workspace");
  for (const bytes of [-1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])
    expect(() => reserveMemoryCapacity(scope, first, bytes, "workspace")).toThrow(/capacity/);
  expect(scope.budget.usedBytes).toBe(20);
  first.release();
  scope.dispose();
});
