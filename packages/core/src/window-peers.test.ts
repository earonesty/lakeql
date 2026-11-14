import { expect, it } from "vitest";
import { col } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import type { WindowExpr } from "./window.js";
import { applyWindowsToRows } from "./window-backend.js";
import { frameRows, frameSpan } from "./window-frame.js";
import { WindowPeers } from "./window-peers.js";
import type { WindowRow } from "./window-utils.js";

function rows(keys: number[]): WindowRow[] {
  return keys.map((key, index) => ({
    originalIndex: index,
    row: { key, before: index % 3, after: (7 - index) % 3 },
    orderKey: [key],
    partitionKey: [],
  }));
}
function expression(mode: "groups" | "range"): WindowExpr {
  return {
    fn: { aggregate: "sum" },
    args: [col("key")],
    over: {
      partitionBy: [],
      orderBy: [{ expr: col("key"), direction: "asc" }],
      frame: {
        mode,
        start:
          mode === "groups"
            ? { kind: "preceding", offset: col("before") }
            : { kind: "unbounded-preceding" },
        end:
          mode === "groups" ? { kind: "following", offset: col("after") } : { kind: "current-row" },
        exclude: "no-others",
      },
    },
  };
}

it.each([
  "groups",
  "range",
] as const)("matches uncached %s spans and exclusions for arbitrary access order", (mode) => {
  // Repeated equality classes are intentionally noncontiguous: no binary-search
  // assumption about representation equality is valid here.
  const partition = rows([1, 1, 2, 1, 1, 3, 3]);
  for (const exclude of ["no-others", "current-row", "group", "ties"] as const) {
    const expr = expression(mode);
    if (expr.over.frame !== undefined) expr.over.frame.exclude = exclude;
    const peers = new WindowPeers(partition);
    for (const index of [0, 1, 6, 2, 4, 0, 5, 3, 6]) {
      expect(frameSpan(partition, index, expr, peers)).toEqual(frameSpan(partition, index, expr));
      expect(frameRows(partition, index, expr, peers)).toEqual(frameRows(partition, index, expr));
    }
  }
});

it("bounds peer-key visits for sequential GROUPS endpoints without a partition index", () => {
  let visits = 0;
  const partition = rows(Array.from({ length: 10_000 }, (_, index) => Math.floor(index / 5)));
  for (const row of partition) {
    const key = row.orderKey;
    Object.defineProperty(row, "orderKey", {
      get() {
        visits += 1;
        return key;
      },
    });
    row.row.before = 2;
    row.row.after = 2;
  }
  const expr = expression("groups");
  const peers = new WindowPeers(partition);
  for (let index = 0; index < partition.length; index += 1) {
    const group = Math.floor(index / 5);
    expect(frameSpan(partition, index, expr, peers)).toEqual({
      start: Math.max(0, group - 2) * 5,
      end: Math.min(2000, group + 3) * 5,
    });
  }
  expect(visits).toBeLessThan(100_000);
});

it("handles empty and single-peer partitions", () => {
  const empty = new WindowPeers([]);
  expect(empty.groupCount).toBe(0);
  expect(frameSpan([], 0, expression("groups"), empty)).toEqual({ start: 0, end: 0 });
  const partition = rows([1, 1, 1]);
  const peers = new WindowPeers(partition);
  expect(peers.groupCount).toBe(1);
  for (let index = 0; index < 3; index += 1)
    expect(peers.peer(index)).toMatchObject({ start: 0, end: 3, group: 0 });
});

it("admits an adaptive peer index only after repeated traversal, with bounded key visits", () => {
  const partition = rows(Array.from({ length: 1000 }, (_, index) => index));
  let visits = 0;
  for (const row of partition) {
    const key = row.orderKey;
    Object.defineProperty(row, "orderKey", {
      get() {
        visits += 1;
        return key;
      },
    });
  }
  const budget = new MemoryBudget(4004);
  const peers = new WindowPeers(partition, budget);
  for (let index = 0; index < 1000; index += 1) peers.group(index, "start");
  expect(budget.peakBytes).toBe(0);
  for (let index = 0; index < 1000; index += 1) {
    const target = index % 2 ? 0 : 999;
    expect(peers.group(target, "start")).toEqual({ start: target, end: target + 1, group: target });
    expect(peers.peer(target)).toEqual({ start: target, end: target + 1, group: target });
  }
  expect(budget.usedBytes).toBe(4004);
  expect(visits).toBeLessThan(40000);
  peers.dispose();
  expect(budget.usedBytes).toBe(0);
  const tight = new MemoryBudget(4003);
  const fallback = new WindowPeers(partition, tight);
  for (let index = 0; index < 20; index += 1) {
    const target = index % 2 ? 0 : 999;
    expect(fallback.group(target, "end").start).toBe(target);
  }
  expect(tight.peakBytes).toBe(0);
  fallback.dispose();
});

it("releases adaptive index allocations after normal window evaluation and errors", () => {
  const input = Array.from({ length: 800 }, (_, key) => ({
    key,
    before: key % 2 ? key : 0,
    after: 0,
  }));
  const expr = expression("groups");
  const budget = new MemoryBudget(1024 * 1024);
  const output = applyWindowsToRows(input, { total: expr }, { memoryBudget: budget });
  expect(output.map((row) => row.total)).toEqual(
    input.map(({ key }) => (key % 2 ? (key * (key + 1)) / 2 : key)),
  );
  expect(budget.peakBytes).toBeGreaterThan(3204);
  expect(budget.usedBytes).toBe(0);
  const invalid = input.map((row, index) =>
    index === input.length - 1 ? { ...row, before: "invalid" } : row,
  );
  expect(() => applyWindowsToRows(invalid, { total: expr }, { memoryBudget: budget })).toThrow();
  expect(budget.usedBytes).toBe(0);
});

it.each([
  "asc",
  "desc",
] as const)("matches an independent dynamic-frame oracle at peer-index capacity (%s)", (direction) => {
  const groups = 40;
  const indexBytes = (groups + 1) * 4;
  const sign = direction === "asc" ? 1 : -1;
  const partition: WindowRow[] = Array.from({ length: groups * 3 }, (_, index) => {
    const group = Math.floor(index / 3);
    const key = direction === "asc" ? group : groups - group - 1;
    return {
      originalIndex: index,
      row: { key, id: index, before: index % 2 ? 0 : index % 13, after: index % 3 ? 2 : 50 },
      orderKey: [key],
      partitionKey: [],
    };
  });
  for (const capacity of [indexBytes - 1, indexBytes]) {
    const budget = new MemoryBudget(capacity);
    const peers = new WindowPeers(partition, budget);
    try {
      // Force repeated traversal before checking both indexed and fallback paths.
      for (let visit = 0; visit < 30; visit++) peers.group(visit % 2 ? 0 : groups - 1, "start");
      expect(budget.usedBytes).toBe(capacity === indexBytes ? indexBytes : 0);
      for (const mode of ["groups", "range"] as const) {
        for (const exclude of ["no-others", "current-row", "group", "ties"] as const) {
          const expr = expression(mode);
          expr.over.orderBy = [{ expr: col("key"), direction }];
          expr.over.frame = {
            mode,
            exclude,
            start: { kind: "preceding", offset: col("before") },
            end: { kind: "following", offset: col("after") },
          };
          for (let visit = 0; visit < partition.length; visit++) {
            const index = visit % 2 ? Math.floor(visit / 2) : partition.length - 1 - visit / 2;
            const current = partition[index]?.row;
            if (current === undefined) throw Error("Missing oracle row");
            const position = mode === "groups" ? Math.floor(index / 3) : Number(current.key) * sign;
            const low = position - Number(current.before);
            const high = position + Number(current.after);
            const expected = partition
              .filter((candidate, candidateIndex) => {
                const value =
                  mode === "groups"
                    ? Math.floor(candidateIndex / 3)
                    : Number(candidate.row.key) * sign;
                if (value < low || value > high) return false;
                if (exclude === "current-row") return candidateIndex !== index;
                if (exclude === "group") return candidate.row.key !== current.key;
                if (exclude === "ties")
                  return candidate.row.key !== current.key || candidateIndex === index;
                return true;
              })
              .map((candidate) => candidate.row);
            expect(frameRows(partition, index, expr, peers)).toEqual(expected);
          }
        }
      }
      expect(budget.peakBytes).toBeLessThanOrEqual(capacity);
    } finally {
      peers.dispose();
    }
    expect(budget.usedBytes).toBe(0);
  }
});
