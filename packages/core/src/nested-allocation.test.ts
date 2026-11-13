import { expect, it } from "vitest";
import { vectorFromValues, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  "list",
  "map",
])("admits %s descriptors and rolls back failed child construction", (kind) => {
  const value =
    kind === "list"
      ? [1, "text", { id: 3 }]
      : new Map<unknown, unknown>([
          ["id", 3],
          ["nested", [1, null]],
        ]);
  const expected = kind === "list" ? value : { id: 3, nested: [1, null] };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const output = vectorFromValues([value], scope);
  expect(vectorValue(output, 0)).toEqual(expected);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(vectorValue(vectorFromValues([value], exact), 0)).toEqual(expected);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => vectorFromValues([value], tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  const invalid =
    kind === "list"
      ? [1, () => 2]
      : new Map<unknown, unknown>([
          ["ok", 1],
          ["bad", () => 2],
        ]);
  const failed = new MemoryScope(new MemoryBudget(65536));
  expect(() => vectorFromValues([invalid], failed)).toThrow();
  expect(failed.budget.usedBytes).toBe(0);
  failed.dispose();
});
