import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { col } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import { applyWindowsToRows, applyWindowsToRuns } from "./window-backend.js";
import { applyWindowsToVectorBatches } from "./window-vector.js";

const windows: Record<string, WindowExpr> = {
  rank: {
    fn: "dense_rank",
    args: [],
    over: { partitionBy: [col("p")], orderBy: [{ expr: col("key") }] },
  },
  total: {
    fn: { aggregate: "sum" },
    args: [col("amount")],
    over: {
      partitionBy: [col("p")],
      orderBy: [{ expr: col("key") }, { expr: col("id") }],
      frame: {
        mode: "rows",
        start: { kind: "unbounded-preceding" },
        end: { kind: "current-row" },
      },
    },
  },
};
const fixture: Row[] = [
  { id: 0, p: "b", key: 2, amount: 4 },
  { id: 1, p: "a", key: 1, amount: null },
  { id: 2, p: "b", key: 1, amount: 3 },
  { id: 3, p: "a", key: 1, amount: 5 },
  { id: 4, p: "a", key: 2, amount: -2 },
];
const expected: Row[] = [
  { ...fixture[0], rank: 2, total: 7 },
  { ...fixture[1], rank: 1, total: null },
  { ...fixture[2], rank: 1, total: 3 },
  { ...fixture[3], rank: 1, total: 5 },
  { ...fixture[4], rank: 2, total: 3 },
];

it.each([
  "rows",
  "runs",
  "vectors",
] as const)("%s window lifecycle retains only output and recovers after exact-boundary failure", async (route) => {
  const execute = async (budget: MemoryBudget, resultScope: MemoryScope) => {
    const options = { memoryBudget: budget, resultScope };
    if (route === "rows") return applyWindowsToRows(fixture, windows, options);
    // Partition peers deliberately span run/batch boundaries.
    const runs = [fixture.slice(0, 2), [], fixture.slice(2)];
    if (route === "runs") return applyWindowsToRuns(runs, windows, options).flat();
    return applyWindowsToVectorBatches(
      runs.map((rows) => ({
        batch: batchFromColumns(
          Object.fromEntries(
            ["id", "p", "key", "amount"].map((key) => [key, rows.map((r) => r[key])]),
          ),
        ),
      })),
      windows,
      options,
    );
  };
  const measure = new MemoryBudget(1024 * 1024);
  const measuredOutput = new MemoryScope(measure);
  expect(await execute(measure, measuredOutput)).toEqual(expected);
  const peak = measure.peakBytes;
  const outputBytes = measure.usedBytes;
  expect(outputBytes).toBeGreaterThan(0);
  expect(outputBytes).toBeLessThan(peak);
  measuredOutput.dispose();
  expect(measure.usedBytes).toBe(0);

  const pool = new MemoryBudget(peak + 37);
  const sibling = pool.reserve(37, "other job's pinned output");
  const budget = pool.child(peak);
  const output = new MemoryScope(budget);
  // One unavailable byte must reject mandatory storage, then release all work.
  const pressure = budget.reserve(1, "temporary pressure");
  await expect(execute(budget, output)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(budget.usedBytes).toBe(1);
  expect(pool.usedBytes).toBe(38);
  pressure.release();
  for (let attempt = 0; attempt < 3; attempt++) {
    const retained = output.childScope();
    expect(await execute(budget, retained)).toEqual(expected);
    expect(budget.usedBytes).toBe(outputBytes);
    expect(pool.usedBytes).toBe(outputBytes + 37);
    retained.dispose();
    expect(budget.usedBytes).toBe(0);
  }
  output.dispose();
  sibling.release();
  expect(pool.usedBytes).toBe(0);
  expect(fixture.map((row) => Object.keys(row))).toEqual(
    Array(5).fill(["id", "p", "key", "amount"]),
  );
});
