import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { col, type Expr, eq, lit } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import type { WindowExpr } from "./window.js";
import { applyWindowsToRows, applyWindowsToRuns } from "./window-backend.js";
import { frameSpan } from "./window-frame.js";
import { WindowPeers } from "./window-peers.js";
import { partitionRows } from "./window-utils.js";
import { applyWindowsToVectorBatches } from "./window-vector.js";

const expansion = (size: number): Expr => ({
  kind: "call",
  fn: "replace",
  args: [col("text"), lit("x"), lit("a".repeat(size))],
});
const offset = (size: number): Expr => ({
  kind: "case",
  whens: [{ when: eq(expansion(size), lit("a".repeat(size))), value: lit(1) }],
  else: lit(0),
});
const expression = (
  mode: "rows" | "groups" | "range",
  size: number,
  aggregate: boolean,
): WindowExpr => ({
  fn: aggregate ? { aggregate: "sum" } : "first_value",
  args: [col("id")],
  over: {
    partitionBy: [],
    orderBy: [{ expr: col("id") }],
    frame: {
      mode,
      exclude: "no-others",
      start: { kind: "preceding", offset: offset(size) },
      end: { kind: "current-row" },
    },
  },
});

for (const mode of ["rows", "groups", "range"] as const)
  for (const aggregate of [false, true]) {
    it.each([
      "rows",
      "runs",
      "vectors",
    ])(`admits ${mode} frame expression (aggregate=${aggregate}, %s)`, async (route) => {
      for (const size of [2, 100000]) {
        const budget = new MemoryBudget(65536);
        const sibling = budget.reserve(1000, "sibling");
        const rows = [
          { id: 1, text: "x" },
          { id: 2, text: "x" },
        ];
        const windows = { value: expression(mode, size, aggregate) };
        const run = async () =>
          route === "rows"
            ? applyWindowsToRows(rows, windows, { memoryBudget: budget })
            : route === "runs"
              ? applyWindowsToRuns([rows], windows, { memoryBudget: budget }).flat()
              : applyWindowsToVectorBatches(
                  [{ batch: batchFromColumns({ id: [1, 2], text: ["x", "x"] }) }],
                  windows,
                  { memoryBudget: budget },
                );
        if (size > 2) await expect(run()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
        else expect((await run()).map((row) => row.value)).toEqual(aggregate ? [1, 3] : [1, 1]);
        expect(budget.usedBytes).toBe(1000);
        sibling.release();
      }
    });
  }

it("releases bound-expression scratch after every frame calculation", () => {
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  const expr = expression("rows", 10000, false);
  const partition = partitionRows([{ id: 1, text: "x" }], expr);
  const peers = new WindowPeers(partition, budget);
  try {
    const live = budget.usedBytes;
    for (let index = 0; index < 100; index++) {
      expect(frameSpan(partition, 0, expr, peers, scope)).toEqual({ start: 0, end: 1 });
      expect(budget.usedBytes).toBe(live);
    }
  } finally {
    peers.dispose();
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("QUALIFY scratch does not accumulate across rejected rows", async () => {
  const budget = new MemoryBudget(128 * 1024);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: budget,
    scanner: {
      async *scan() {
        yield Array.from({ length: 100 }, (_, id) => ({ id, text: "x" }));
      },
    },
  });
  const rows = await lake
    .path("table")
    .window("rn", {
      fn: "row_number",
      args: [],
      over: { partitionBy: [], orderBy: [{ expr: col("id") }] },
    })
    .qualify(eq(expansion(1000), lit("different")))
    .toArray();
  expect(rows).toEqual([]);
  expect(budget.usedBytes).toBe(0);
});
