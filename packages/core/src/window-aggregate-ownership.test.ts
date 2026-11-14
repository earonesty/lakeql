import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { col, type Expr, eq, lit } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import type { WindowExpr } from "./window.js";
import { createWindowAggregateAccumulator } from "./window-aggregate.js";
import { applyWindowsToRows, applyWindowsToRuns } from "./window-backend.js";
import { WindowPeers } from "./window-peers.js";
import { segmentTreeAggregateValues, segmentTreeWorkspaceBytes } from "./window-segment-tree.js";
import { partitionRows } from "./window-utils.js";
import { applyWindowsToVectorBatches } from "./window-vector.js";

const replacement = (size: number): Expr => ({
  kind: "call",
  fn: "replace",
  args: [col("text"), lit("x"), lit("a".repeat(size))],
});
const expression = (
  aggregate: "min" | "max" | "first" | "last" | "any" | "mode" | "count",
  size: number,
): WindowExpr => ({
  fn: { aggregate },
  args: [replacement(size)],
  over: {
    partitionBy: [],
    orderBy: [],
    frame: {
      mode: "rows",
      start: { kind: "unbounded-preceding" },
      end: { kind: "unbounded-following" },
    },
  },
});

for (const aggregate of ["min", "max", "first", "last", "any", "mode", "count"] as const) {
  it.each([
    "rows",
    "runs",
    "vectors",
  ])(`admits ${aggregate} expression expansion (%s)`, async (route) => {
    for (const size of [2, 100000]) {
      const budget = new MemoryBudget(65536);
      const sibling = budget.reserve(1000, "sibling");
      const rows = [{ text: "x" }, { text: null }, { text: "x" }];
      const windows = { value: expression(aggregate, size) };
      const run = async () =>
        route === "rows"
          ? applyWindowsToRows(rows, windows, { memoryBudget: budget })
          : route === "runs"
            ? applyWindowsToRuns([rows], windows, { memoryBudget: budget }).flat()
            : applyWindowsToVectorBatches(
                [{ batch: batchFromColumns({ text: ["x", null, "x"] }) }],
                windows,
                { memoryBudget: budget },
              );
      if (size > 2) await expect(run()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
      else
        expect((await run()).map((row) => row.value)).toEqual(
          Array(3).fill(aggregate === "count" ? 2 : "aa"),
        );
      expect(budget.usedBytes).toBe(1000);
      sibling.release();
    }
  });
}

it("releases replaced aggregate state while retaining published values", () => {
  const budget = new MemoryBudget(65536);
  const output = new MemoryScope(budget);
  const expr = expression("last", 1000);
  const state = createWindowAggregateAccumulator(expr, budget, output);
  for (let i = 0; i < 100; i++) expect(state.append([{ text: "x" }])).toBe("a".repeat(1000));
  expect(budget.usedBytes).toBeLessThan(5000);
  state.reset();
  expect(budget.usedBytes).toBeGreaterThanOrEqual(2000);
  output.dispose();
  expect(budget.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("retains adaptive min-tree payloads or falls back when payloads exceed tree headroom (tight=%s)", (tight) => {
  const length = 32;
  const budget = new MemoryBudget(tight ? segmentTreeWorkspaceBytes(length) + 2500 : 1024 * 1024);
  const resultScope = new MemoryScope(budget);
  const expr: WindowExpr = {
    ...expression("min", 100),
    over: {
      partitionBy: [],
      orderBy: [{ expr: col("id") }],
      frame: {
        mode: "rows",
        start: { kind: "preceding", offset: col("before") },
        end: { kind: "following", offset: col("after") },
      },
    },
  };
  let treeReads = 0;
  let fallbackReads = 0;
  const rows = Array.from({ length }, (_, id) => ({
    id,
    get text() {
      if (budget.usedBytes >= segmentTreeWorkspaceBytes(length)) treeReads++;
      else if (treeReads > 0) fallbackReads++;
      return `x${id.toString().padStart(2, "0")}`;
    },
    before: id % 2 === 0 ? id : 0,
    after: id % 2 === 0 ? length - id - 1 : 0,
  }));
  const partition = partitionRows(rows, expr);
  const peers = new WindowPeers(partition, budget);
  try {
    const values = segmentTreeAggregateValues(partition, expr, peers, budget, resultScope);
    expect(values).toEqual(
      rows.map(
        (row) => "a".repeat(100) + (row.id % 2 === 0 ? "00" : row.id.toString().padStart(2, "0")),
      ),
    );
    expect(treeReads).toBeGreaterThan(0);
    if (tight) expect(fallbackReads).toBeGreaterThan(0);
    else expect(fallbackReads).toBe(0);
    expect(budget.peakBytes).toBeGreaterThan(segmentTreeWorkspaceBytes(length));
    expect(budget.usedBytes).toBeLessThan(segmentTreeWorkspaceBytes(length));
  } finally {
    peers.dispose();
    resultScope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("admits aggregate filters and short-circuits rejected rows before argument expansion", () => {
  const budget = new MemoryBudget(4096);
  const expanded = expression("count", 100000);
  const filtered = createWindowAggregateAccumulator({ ...expanded, filter: lit(false) }, budget);
  expect(filtered.append([{ text: "x" }])).toBe(0);
  filtered.reset();
  const rejected = createWindowAggregateAccumulator(
    { ...expression("count", 2), filter: eq(replacement(100000), lit("x")) },
    budget,
  );
  expect(() => rejected.append([{ text: "x" }])).toThrow(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
  expect(budget.usedBytes).toBe(0);
});
