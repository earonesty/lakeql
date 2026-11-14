import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { col, type Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import type { WindowExpr } from "./window.js";
import { applyWindowsToRows, applyWindowsToRuns, windowRowsForExpr } from "./window-backend.js";
import type { WindowRow } from "./window-utils.js";
import { applyWindowsToVectorBatches } from "./window-vector.js";

for (const key of ["partition", "order"] as const) {
  it.each(["rows", "runs", "vectors"])(`admits ${key} key expressions for %s`, async (route) => {
    for (const expand of [false, true]) {
      const pool = new MemoryBudget(16384);
      const sibling = pool.reserve(1024, "sibling");
      const expression: Expr = {
        kind: "call",
        fn: "replace",
        args: [
          col("text"),
          { kind: "literal", value: "x" },
          { kind: "literal", value: expand ? "a".repeat(10000) : "a" },
        ],
      };
      const window: WindowExpr = {
        fn: "row_number",
        args: [],
        over: {
          partitionBy: key === "partition" ? [expression] : [],
          orderBy: [{ expr: key === "order" ? expression : col("id") }],
        },
      };
      const rows = [
        { id: 2, text: "x2" },
        { id: 1, text: "x1" },
      ];
      const run = async () =>
        route === "rows"
          ? applyWindowsToRows(rows, { rn: window }, { memoryBudget: pool })
          : route === "runs"
            ? applyWindowsToRuns(
                rows.map((row) => [row]),
                { rn: window },
                { memoryBudget: pool },
              ).flat()
            : applyWindowsToVectorBatches(
                [{ batch: batchFromColumns({ id: [2, 1], text: ["x2", "x1"] }) }],
                { rn: window },
                { memoryBudget: pool },
              );
      if (expand) await expect(run()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
      else
        expect(await run()).toEqual([
          { id: 2, text: "x2", rn: key === "partition" ? 1 : 2 },
          { id: 1, text: "x1", rn: 1 },
        ]);
      expect(pool.usedBytes).toBe(sibling.bytes);
      sibling.release();
    }
  });
}

it("reuses full-order indexes and admits prefix copies without mutating shared order", () => {
  const full: WindowExpr = {
    fn: "row_number",
    args: [],
    over: { partitionBy: [], orderBy: [{ expr: col("a") }, { expr: col("b") }] },
  };
  const prefix: WindowExpr = {
    ...full,
    over: { ...full.over, orderBy: full.over.orderBy.slice(0, 1) },
  };
  const sorted: WindowRow[] = [
    { row: {}, originalIndex: 2, partitionKey: [], orderKey: [1, 0] },
    { row: {}, originalIndex: 0, partitionKey: [], orderKey: [1, 1] },
    { row: {}, originalIndex: 1, partitionKey: [], orderKey: [2, 0] },
  ];
  const pool = new MemoryBudget(16384);
  const scope = new MemoryScope(pool);
  expect(windowRowsForExpr(sorted, full, full, scope)).toBe(sorted);
  expect(pool.usedBytes).toBe(0);
  const projected = windowRowsForExpr(sorted, prefix, full, scope);
  expect(projected.map((row) => row.originalIndex)).toEqual([0, 2, 1]);
  expect(projected.map((row) => row.orderKey)).toEqual([[1], [1], [2]]);
  expect(sorted.map((row) => row.originalIndex)).toEqual([2, 0, 1]);
  expect(pool.usedBytes).toBeGreaterThan(0);
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
  const tiny = new MemoryScope(new MemoryBudget(1));
  expect(() => windowRowsForExpr(sorted, prefix, full, tiny)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
  tiny.dispose();
  expect(tiny.budget.usedBytes).toBe(0);
});
