import { expect, it } from "vitest";
import { col, gt } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import type { WindowExpr } from "./window.js";
import { applyWindowsToRows, evaluateWindowRows } from "./window-backend.js";
import { WINDOW_PEER_METADATA_BYTES as metadataBytes, WindowPeers } from "./window-peers.js";
import { segmentTreeAggregateValues, segmentTreeWorkspaceBytes } from "./window-segment-tree.js";
import { partitionRows, partitionSpans } from "./window-utils.js";

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

it("keeps growing frames incremental within exact shared-budget availability", () => {
  for (const length of [1, 7, 16]) {
    const required = segmentTreeWorkspaceBytes(length);
    for (const cap of [0, required - 1, required]) {
      const pool = new MemoryBudget(metadataBytes + cap + 37);
      const retained = pool.child(37).reserve(37, "other query output");
      const budget = pool.child(metadataBytes + cap);
      let reads = 0;
      const rows = Array.from({ length }, (_, id) => ({
        id,
        get amount() {
          reads++;
          expect(budget.usedBytes).toBe(metadataBytes);
          return id + 1;
        },
      }));
      const partition = partitionRows(rows, expr);
      const peers = new WindowPeers(partition, budget);
      try {
        expect(segmentTreeAggregateValues(partition, expr, peers, budget)).toEqual(
          Array.from({ length }, (_, id) => ((id + 1) * (id + 2)) / 2),
        );
        expect(reads).toBe(length);
        expect(budget.usedBytes).toBe(metadataBytes);
        expect(budget.peakBytes).toBe(metadataBytes);
        expect(pool.usedBytes).toBe(metadataBytes + 37);
      } finally {
        peers.dispose();
        retained.release();
      }
      expect(pool.usedBytes).toBe(0);
    }
  }
});

function alternatingExpression(): WindowExpr {
  return {
    ...expr,
    over: {
      ...expr.over,
      frame: {
        mode: "rows",
        start: { kind: "preceding", offset: col("before") },
        end: { kind: "following", offset: col("after") },
      },
    },
  };
}

it("admits a tree only after repeated visits and releases it on completion or failure", () => {
  const length = 32;
  const required = segmentTreeWorkspaceBytes(length);
  for (const cap of [required - 1, required]) {
    for (const fail of [false, true]) {
      const pool = new MemoryBudget(metadataBytes + cap + 37);
      const retained = pool.child(37).reserve(37, "concurrent output");
      const budget = pool.child(metadataBytes + cap);
      let visits = 0;
      let firstAdmittedVisit = 0;
      const rows = Array.from({ length }, (_, id) => ({
        id,
        before: id % 2 === 0 ? id : 0,
        after: id % 2 === 0 ? length - id - 1 : 0,
        get amount() {
          visits++;
          if (budget.usedBytes > metadataBytes) {
            firstAdmittedVisit ||= visits;
            if (fail) throw new Error("admitted leaf failed");
          }
          return id + 1;
        },
      }));
      const dynamic = alternatingExpression();
      const run = () =>
        segmentTreeAggregateValues(partitionRows(rows, dynamic), dynamic, undefined, budget);
      if (fail && cap === required) expect(run).toThrow("admitted leaf failed");
      else
        expect(run()).toEqual(
          rows.map((_row, id) => (id % 2 === 0 ? (length * (length + 1)) / 2 : id + 1)),
        );
      expect(budget.peakBytes).toBe(metadataBytes + (cap === required ? required : 0));
      expect(budget.usedBytes).toBe(0);
      if (cap === required) expect(firstAdmittedVisit).toBeGreaterThan(length * 3);
      else expect(firstAdmittedVisit).toBe(0);
      expect(pool.usedBytes).toBe(37);
      retained.release();
      expect(pool.usedBytes).toBe(0);
    }
  }
  expect(segmentTreeAggregateValues([], expr, undefined, new MemoryBudget(0))).toEqual([]);
  for (const size of [-1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER])
    expect(() => segmentTreeWorkspaceBytes(size)).toThrow();
});

it("propagates kernel budget and keeps null and filter semantics", () => {
  const rows = [
    { id: 0, amount: null },
    { id: 1, amount: 2 },
    { id: 2, amount: 3 },
  ];
  const indexed = partitionRows(rows, expr);
  const spans = partitionSpans(indexed);
  for (const cap of [0, segmentTreeWorkspaceBytes(rows.length)]) {
    const memoryBudget = new MemoryBudget(metadataBytes + cap);
    expect(evaluateWindowRows(rows.length, indexed, spans, expr, { memoryBudget })).toEqual([
      null,
      2,
      5,
    ]);
    expect(
      evaluateWindowRows(
        rows.length,
        indexed,
        spans,
        { ...expr, filter: gt("id", 1) },
        { memoryBudget },
      ),
    ).toEqual([null, null, 3]);
    expect(memoryBudget.peakBytes).toBe(metadataBytes);
    expect(memoryBudget.usedBytes).toBe(0);
  }
  const zero = new MemoryBudget(0);
  expect(() => applyWindowsToRows(rows, { total: expr }, { memoryBudget: zero })).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
  expect(zero.usedBytes).toBe(0);
});

it("reuses growing frames and resets correctly when dynamic bounds shrink or move", async () => {
  const { aggregateWindowValue } = await import("./window-aggregate.js");
  const { frameRows } = await import("./window-frame.js");
  const rows = [0, 1, 2, 0, 4, 2, 1, 7].map((before, id) => ({
    id,
    before,
    after: [0, 2, 0, 0, 1, 0, 0, 0][id] ?? 0,
    amount: id === 2 ? null : id - 3,
  }));
  for (const aggregate of [
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "var_pop",
    "var_samp",
    "stddev_pop",
    "stddev_samp",
  ] as const) {
    for (const endKind of ["following", "preceding"] as const) {
      const dynamic: WindowExpr = {
        ...expr,
        fn: { aggregate },
        filter: gt("id", 0),
        over: {
          ...expr.over,
          frame: {
            mode: "rows",
            start: { kind: "preceding", offset: col("before") },
            end: { kind: endKind, offset: col("after") },
          },
        },
      };
      const partition = partitionRows(rows, dynamic);
      const expected = partition.map((_row, index) =>
        aggregateWindowValue(frameRows(partition, index, dynamic), dynamic),
      );
      const budget = new MemoryBudget(metadataBytes);
      expect(segmentTreeAggregateValues(partition, dynamic, undefined, budget)).toEqual(expected);
      expect(budget.usedBytes).toBe(0);
    }
  }
});
