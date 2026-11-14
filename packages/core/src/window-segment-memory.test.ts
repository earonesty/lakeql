import { expect, it } from "vitest";
import { col, gt } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import type { WindowExpr } from "./window.js";
import { applyWindowsToRows } from "./window-backend.js";
import { WindowPeers } from "./window-peers.js";
import { segmentTreeAggregateValues, segmentTreeWorkspaceBytes } from "./window-segment-tree.js";
import { partitionRows } from "./window-utils.js";

const expr: WindowExpr = {
  fn: { aggregate: "sum" },
  args: [col("amount")],
  over: {
    partitionBy: [],
    orderBy: [{ expr: col("id") }],
    frame: {
      mode: "rows",
      start: { kind: "unbounded-preceding" },
      end: { kind: "current-row" },
    },
  },
};

it("selects tree or span recomputation within exact shared-budget availability", () => {
  for (const length of [1, 7, 16]) {
    const required = segmentTreeWorkspaceBytes(length);
    for (const cap of [0, required - 1, required]) {
      const pool = new MemoryBudget(cap + 37);
      const retained = pool.child(37).reserve(37, "other query output");
      const budget = pool.child(cap);
      let reads = 0;
      const rows = Array.from({ length }, (_, id) => ({
        id,
        get amount() {
          reads++;
          expect(budget.usedBytes).toBe(cap === required ? required : 0);
          return id + 1;
        },
      }));
      const partition = partitionRows(rows, expr);
      const peers = new WindowPeers(partition, budget);
      try {
        expect(segmentTreeAggregateValues(partition, expr, peers, budget)).toEqual(
          Array.from({ length }, (_, id) => ((id + 1) * (id + 2)) / 2),
        );
        expect(reads).toBe(cap === required ? length : (length * (length + 1)) / 2);
        expect(budget.usedBytes).toBe(0);
        expect(budget.peakBytes).toBe(cap === required ? required : 0);
        expect(pool.usedBytes).toBe(37);
      } finally {
        peers.dispose();
        retained.release();
      }
      expect(pool.usedBytes).toBe(0);
    }
  }
});

it("releases admitted tree workspace after an expression failure", () => {
  const partition = partitionRows(
    [
      { id: 1, amount: 1 },
      { id: 2, amount: "invalid" },
    ],
    expr,
  );
  const budget = new MemoryBudget(segmentTreeWorkspaceBytes(partition.length));
  expect(() => segmentTreeAggregateValues(partition, expr, undefined, budget)).toThrow(/numeric/);
  expect(budget.usedBytes).toBe(0);
  expect(budget.peakBytes).toBe(budget.maxBytes);
  expect(segmentTreeAggregateValues([], expr, undefined, new MemoryBudget(0))).toEqual([]);
  for (const size of [-1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER])
    expect(() => segmentTreeWorkspaceBytes(size)).toThrow();
});

it("propagates query budget to window aggregates and keeps null and filter semantics", () => {
  const rows = [
    { id: 0, amount: null },
    { id: 1, amount: 2 },
    { id: 2, amount: 3 },
  ];
  for (const cap of [0, segmentTreeWorkspaceBytes(rows.length)]) {
    const memoryBudget = new MemoryBudget(cap);
    expect(
      applyWindowsToRows(rows, { total: expr }, { memoryBudget }).map((row) => row.total),
    ).toEqual([null, 2, 5]);
    expect(
      applyWindowsToRows(rows, { total: { ...expr, filter: gt("id", 1) } }, { memoryBudget }).map(
        (row) => row.total,
      ),
    ).toEqual([null, null, 3]);
    expect(memoryBudget.peakBytes).toBe(cap);
    expect(memoryBudget.usedBytes).toBe(0);
  }
});
