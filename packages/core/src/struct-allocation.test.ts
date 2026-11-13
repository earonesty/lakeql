import { expect, it } from "vitest";
import { vectorFromValues, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("admits wide struct discovery and descriptors with exact-budget rollback", () => {
  const row: Record<string, unknown> = Object.create(null);
  for (let i = 100; i >= 0; i--) row[`field${i}`] = i;
  Object.defineProperty(row, "__proto__", { value: 42, enumerable: true });
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  const vector = vectorFromValues([row], scope);
  expect(vectorValue(vector, 0)).toEqual(row);
  if (vector.type !== "struct") throw Error("Expected struct");
  expect(Object.keys(vector.fields)).toEqual(Object.keys(row).sort());
  const peak = scope.budget.peakBytes;
  const retained = scope.budget.usedBytes;
  expect(peak).toBeGreaterThan(retained);
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(vectorValue(vectorFromValues([row], exact), 0)).toEqual(row);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => vectorFromValues([row], tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});
