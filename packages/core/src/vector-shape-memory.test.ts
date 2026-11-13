import { expect, it } from "vitest";
import { vectorFromValues } from "./batch.js";
import { MATERIALIZATION_CHARGES as c } from "./materialization-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("admits borrowed string slots and validity together after inference", () => {
  const values = ["snow 雪", null, ""];
  const bytes = c.array + values.length * c.arraySlot + values.length;
  const scope = new MemoryScope(new MemoryBudget(bytes));
  expect(vectorFromValues(values, scope)).toEqual({
    type: "utf8",
    values: ["snow 雪", "", ""],
    valid: Uint8Array.of(1, 0, 1),
  });
  expect(scope.budget.usedBytes).toBe(bytes);
  scope.dispose();
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => vectorFromValues(values, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  expect(tight.budget.peakBytes).toBeLessThan(bytes);
  tight.dispose();
});

it("rolls back inference allocations for unsupported mixed shapes", () => {
  for (const values of [
    [null, "text", Symbol("invalid")],
    [null, -1n, 0xffffffffffffffffn],
  ]) {
    const scope = new MemoryScope(new MemoryBudget(65536));
    expect(() => vectorFromValues(values, scope)).toThrow();
    expect(scope.budget.usedBytes).toBe(0);
    scope.dispose();
  }
});
