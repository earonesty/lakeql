import { expect, it } from "vitest";
import { col } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import type { WindowExpr, WindowRankingFn } from "./window.js";
import { applyWindowsToRows, applyWindowsToRuns, evaluateWindowRows } from "./window-backend.js";
import type { WindowRow } from "./window-utils.js";

const functions = ["rank", "dense_rank", "percent_rank", "cume_dist"] as const;
function expression(fn: WindowRankingFn): WindowExpr {
  return { fn, args: [], over: { partitionBy: [], orderBy: [{ expr: col("key") }] } };
}

it.each(functions)("uses existing peer keys in linear work for %s", (fn) => {
  for (const length of [0, 1, 64, 4096]) {
    for (const groupSize of [1, 7, Math.max(length, 1)]) {
      let keyReads = 0;
      const partition: WindowRow[] = Array.from({ length }, (_, index) => ({
        row: {
          get key(): never {
            throw new Error("Ranking reevaluated its input expression");
          },
        },
        originalIndex: index,
        partitionKey: [],
        get orderKey() {
          keyReads++;
          return [Math.floor(index / groupSize)];
        },
      }));
      const budget = new MemoryBudget(0);
      const values = evaluateWindowRows(
        length,
        partition,
        [{ start: 0, end: length }],
        expression(fn),
        { memoryBudget: budget },
      );
      const expected = Array.from({ length }, (_, index) => {
        const group = Math.floor(index / groupSize);
        const start = group * groupSize;
        if (fn === "rank") return start + 1;
        if (fn === "dense_rank") return group + 1;
        if (fn === "percent_rank") return length <= 1 ? 0 : start / (length - 1);
        return Math.min(length, start + groupSize) / length;
      });
      expect(values).toEqual(expected);
      expect(keyReads).toBeLessThanOrEqual(4 * length);
      expect(budget.peakBytes).toBe(0);
      expect(budget.usedBytes).toBe(0);
    }
  }
});

it("preserves partitions, null peers and compatible sort prefixes across row runs", () => {
  const rows = [
    { id: 0, part: "a", key: 2 },
    { id: 1, part: "b", key: null },
    { id: 2, part: "a", key: null },
    { id: 3, part: "a", key: 2 },
    { id: 4, part: "a", key: 1 },
    { id: 5, part: "b", key: 3 },
  ];
  const windows: Record<string, WindowExpr> = {};
  for (const fn of functions)
    windows[fn] = {
      ...expression(fn),
      over: {
        partitionBy: [col("part")],
        orderBy: [{ expr: col("key"), direction: "desc", nulls: "first" }],
      },
    };
  windows.ordinal = {
    ...expression("row_number"),
    over: {
      partitionBy: [col("part")],
      orderBy: [
        { expr: col("key"), direction: "desc", nulls: "first" },
        { expr: col("id"), direction: "desc" },
      ],
    },
  };
  const expected = [
    [2, 2, 1 / 3, 3 / 4, 3],
    [1, 1, 0, 1 / 2, 1],
    [1, 1, 0, 1 / 4, 1],
    [2, 2, 1 / 3, 3 / 4, 2],
    [4, 3, 1, 1, 4],
    [2, 2, 1, 1, 2],
  ];
  const pool = new MemoryBudget(0);
  const outputs = [
    applyWindowsToRows(rows, windows, { memoryBudget: pool }),
    applyWindowsToRuns([rows.slice(0, 2), [], rows.slice(2, 5), rows.slice(5)], windows, {
      memoryBudget: pool,
    }).flat(),
  ];
  for (const output of outputs)
    expect(output.map((row) => [...functions.map((fn) => row[fn]), row.ordinal])).toEqual(expected);
  expect(pool.usedBytes).toBe(0);
});
