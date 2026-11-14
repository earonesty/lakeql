import { expect, it } from "vitest";
import { col, lit } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import type { WindowExpr } from "./window.js";
import { compatibleWindowSortGroups, windowSortGroupCount } from "./window-query.js";

function expr(partition: string, order: string[]): WindowExpr {
  return {
    fn: "row_number",
    args: [],
    over: { partitionBy: [col(partition)], orderBy: order.map((name) => ({ expr: col(name) })) },
  };
}

it("preserves stable longest-prefix grouping and partition separation", () => {
  const items = [
    { id: "short", expr: expr("p", ["a"]) },
    { id: "left", expr: expr("p", ["a", "b"]) },
    { id: "right", expr: expr("p", ["a", "c"]) },
    { id: "other", expr: expr("q", ["a"]) },
    { id: "empty", expr: expr("p", []) },
  ];
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  try {
    const groups = compatibleWindowSortGroups(items, scope);
    expect(groups.map((group) => group.items.map((item) => item.id))).toEqual([
      ["left", "short", "empty"],
      ["right"],
      ["other"],
    ]);
    expect(items.map((item) => item.id)).toEqual(["short", "left", "right", "other", "empty"]);
    expect(
      windowSortGroupCount(Object.fromEntries(items.map((item) => [item.id, item.expr]))),
    ).toBe(3);
    expect(budget.usedBytes).toBeGreaterThan(0);
    expect(budget.usedBytes).toBeLessThan(budget.peakBytes);
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("admits planning at the exact peak and rejects one byte below", () => {
  const items = Array.from({ length: 20 }, (_, index) => ({
    expr: expr(`p${index % 3}`, ["a", `b${index}`]),
  }));
  const run = (budget: MemoryBudget) => {
    const scope = new MemoryScope(budget);
    try {
      return compatibleWindowSortGroups(items, scope);
    } finally {
      scope.dispose();
    }
  };
  const measured = new MemoryBudget(1024 * 1024);
  expect(run(measured)).toHaveLength(20);
  expect(run(new MemoryBudget(measured.peakBytes))).toHaveLength(20);
  const small = new MemoryBudget(measured.peakBytes - 1);
  expect(() => run(small)).toThrow(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
  expect(small.usedBytes).toBe(0);
});

it("rejects oversized serialized sort expressions before retaining a plan", () => {
  const budget = new MemoryBudget(4096);
  const sibling = budget.reserve(100, "sibling");
  const scope = new MemoryScope(budget);
  try {
    const window = expr("p", []);
    window.over.orderBy = [{ expr: lit("x".repeat(10000)) }];
    expect(() => compatibleWindowSortGroups([{ expr: window }], scope)).toThrow(
      expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
    );
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(100);
  sibling.release();
});
