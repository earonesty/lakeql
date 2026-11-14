import { expect, it, vi } from "vitest";
import { col } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { TimestampValue, timestampFromEpoch } from "./timestamp.js";
import type { WindowExpr, WindowRankingFn } from "./window.js";
import { applyWindowsToRows, applyWindowsToRuns, evaluateWindowRows } from "./window-backend.js";
import { WINDOW_PEER_METADATA_BYTES } from "./window-peers.js";
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
      const budget = new MemoryBudget(WINDOW_PEER_METADATA_BYTES);
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
      expect(budget.peakBytes).toBe(WINDOW_PEER_METADATA_BYTES);
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
  const pool = new MemoryBudget(65536);
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

it("uses mathematical timestamp peers across units without serializing keys", () => {
  const rows = [
    { id: 0, part: timestampFromEpoch(0n, "millis"), key: timestampFromEpoch(1000n, "millis") },
    { id: 1, part: timestampFromEpoch(0n, "micros"), key: timestampFromEpoch(1000000n, "micros") },
    { id: 2, part: timestampFromEpoch(0n, "nanos"), key: timestampFromEpoch(2000000000n, "nanos") },
    { id: 3, part: timestampFromEpoch(1n, "millis"), key: timestampFromEpoch(1000n, "millis") },
  ];
  const windows: Record<string, WindowExpr> = {};
  for (const fn of functions)
    windows[fn] = {
      ...expression(fn),
      over: { partitionBy: [col("part")], orderBy: [{ expr: col("key") }] },
    };
  windows.ordinal = {
    ...expression("row_number"),
    over: {
      partitionBy: [col("part")],
      orderBy: [{ expr: col("key") }, { expr: col("id"), direction: "desc" }],
    },
  };
  const serialization = vi.spyOn(TimestampValue.prototype, "toJSON").mockImplementation(() => {
    throw new Error("Peer detection must not serialize timestamp keys");
  });
  try {
    const outputs = [
      applyWindowsToRows(rows, windows),
      applyWindowsToRuns(
        [rows.slice(0, 1), [...rows.slice(1, 2), ...rows.slice(3)], rows.slice(2, 3)],
        windows,
      )
        .flat()
        .sort((a, b) => Number(a.id) - Number(b.id)),
    ];
    for (const output of outputs)
      expect(output.map((row) => [...functions.map((fn) => row[fn]), row.ordinal])).toEqual([
        [1, 1, 0, 2 / 3, 2],
        [1, 1, 0, 2 / 3, 1],
        [3, 2, 1, 1, 3],
        [1, 1, 0, 1, 1],
      ]);
    expect(serialization).not.toHaveBeenCalled();
  } finally {
    serialization.mockRestore();
  }
});
