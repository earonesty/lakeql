import { expect, it } from "vitest";
import { admittedColumnNames } from "./column-names.js";
import { col, eq } from "./expr.js";
import { collectExprColumns } from "./expr-utils.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  false,
  true,
])("admits deduplicated column lists including ordering overlap (sorted=%s)", (sorted) => {
  const run = (capacity: number) => {
    const budget = new MemoryBudget(capacity);
    const scope = new MemoryScope(budget);
    try {
      const names = admittedColumnNames(
        scope,
        (columns) => {
          columns.add("猫");
          columns.add("__proto__");
          collectExprColumns(eq(col("猫"), col("constructor")), columns);
        },
        sorted,
      );
      expect(names).toEqual(
        sorted ? ["__proto__", "constructor", "猫"] : ["猫", "__proto__", "constructor"],
      );
      expect(budget.usedBytes).toBeGreaterThan(0);
      return budget.peakBytes;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  };
  const peak = run(65536);
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
});

it("releases partially planned names on failure without disposing caller ownership", () => {
  const budget = new MemoryBudget(4096);
  const parent = new MemoryScope(budget);
  parent.allocate(64, "sibling", () => undefined);
  expect(() =>
    admittedColumnNames(parent, (columns) => {
      columns.add("first");
      columns.add("x".repeat(4096));
    }),
  ).toThrowError(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
  expect(budget.usedBytes).toBe(64);
  expect(() =>
    admittedColumnNames(parent, (columns) => {
      columns.add("first");
      throw new Error("traversal failed");
    }),
  ).toThrow("traversal failed");
  expect(budget.usedBytes).toBe(64);
  parent.dispose();
  expect(budget.usedBytes).toBe(0);
});
