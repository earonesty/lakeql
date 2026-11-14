import { expect, it } from "vitest";
import { JsonTraversalBudget } from "./json-traversal-budget.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("retains depth capacity across siblings and releases it at traversal completion", () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  const traversal = new JsonTraversalBudget(scope, 100, 100);
  traversal.admit(3);
  const highWater = scope.budget.usedBytes;
  expect(highWater).toBeGreaterThan(300);
  for (let i = 0; i < 10000; i++) traversal.admit((i % 3) + 1);
  expect(scope.budget.usedBytes).toBe(highWater);
  expect(scope.budget.peakBytes).toBe(highWater);
  traversal.dispose();
  traversal.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("releases admitted depths when a deeper admission fails", () => {
  const scope = new MemoryScope(new MemoryBudget(500));
  const traversal = new JsonTraversalBudget(scope, 100, 100);
  expect(() => traversal.admit(10)).toThrow(/budget/i);
  expect(scope.budget.usedBytes).toBe(0);
  traversal.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("admits deep traversal once without retaining a reservation directory", () => {
  const capacity = 100 + 10000 * 100;
  const scope = new MemoryScope(new MemoryBudget(capacity));
  scope.admit(capacity, "deep traversal");
  const traversal = new JsonTraversalBudget(scope, 100, 100);
  traversal.admit(1);
  traversal.admit(10000);
  for (let i = 0; i < 1000; i++) traversal.admit(2);
  expect(scope.budget.usedBytes).toBe(capacity);
  expect(() => traversal.admit(10001)).toThrow(/extent|budget/i);
  traversal.dispose();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
