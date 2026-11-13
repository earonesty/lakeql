import { describe, expect, it } from "vitest";
import { col, lit } from "./expr.js";
import { intervalValue } from "./interval.js";
import { timestampFromEpoch } from "./timestamp.js";
import type { WindowExpr } from "./window.js";
import { frameSpan } from "./window-frame.js";
import { partitionRows, sortWindowRows } from "./window-utils.js";

function expression(direction: "asc" | "desc"): WindowExpr {
  return {
    fn: { aggregate: "sum" },
    args: [col("key")],
    over: {
      partitionBy: [],
      orderBy: [{ expr: col("key"), direction }],
      frame: {
        mode: "range",
        start: { kind: "preceding", offset: col("before") },
        end: { kind: "following", offset: col("after") },
        exclude: "no-others",
      },
    },
  };
}

describe("RANGE endpoint search", () => {
  it("handles empty intervals, unbounded endpoints, and typed invalid input", () => {
    const expr = expression("asc");
    const partition = partitionRows(
      [
        { key: 1, before: 0, after: 0 },
        { key: 3, before: 0, after: 0 },
      ],
      expr,
    );
    expr.over.frame = {
      mode: "range",
      start: { kind: "following", offset: lit(10) },
      end: { kind: "following", offset: lit(20) },
      exclude: "no-others",
    };
    expect(frameSpan(partition, 0, expr)).toEqual({ start: 2, end: 2 });
    expr.over.frame = {
      ...expr.over.frame,
      start: { kind: "unbounded-preceding" },
      end: { kind: "following", offset: lit(20) },
    };
    expect(frameSpan(partition, 0, expr)).toEqual({ start: 0, end: 2 });
    expr.over.frame = {
      ...expr.over.frame,
      start: { kind: "preceding", offset: lit(20) },
      end: { kind: "unbounded-following" },
    };
    expect(frameSpan(partition, 1, expr)).toEqual({ start: 0, end: 2 });
    expr.over.frame.start = { kind: "preceding", offset: lit(-1) };
    expect(() => frameSpan(partition, 0, expr)).toThrow("non-negative finite");
    const invalid = partitionRows([{ key: null, before: 1, after: 1 }], expression("asc"));
    expect(() => frameSpan(invalid, 0, expression("asc"))).toThrow("non-null numeric or timestamp");
  });

  it.each([
    "asc",
    "desc",
  ] as const)("matches a linear oracle with ties and nonmonotone offsets (%s)", (direction) => {
    const expr = expression(direction);
    const partition = partitionRows(
      Array.from({ length: 300 }, (_, i) => ({
        key: (i * 17) % 71,
        before: i % 2 ? 0 : i % 20,
        after: i % 3 ? i % 10 : 1000,
      })),
      expr,
    );
    sortWindowRows(partition, expr.over.orderBy);
    const sign = direction === "asc" ? 1 : -1;
    for (let index = 0; index < partition.length; index += 1) {
      const row = partition[index]?.row;
      const current = Number(row?.key) * sign;
      const start = current - Number(row?.before);
      const end = current + Number(row?.after);
      const keys = partition.map((item) => Number(item.row.key) * sign);
      let first = keys.findIndex((key) => key >= start);
      if (first === -1) first = keys.length;
      let last = first;
      while (last < keys.length && Number(keys[last]) <= end) last += 1;
      expect(frameSpan(partition, index, expr)).toEqual({ start: first, end: last });
    }
  });

  it.each([
    "asc",
    "desc",
  ] as const)("finds timestamp interval boundaries including duplicate timestamps (%s)", (direction) => {
    const expr = expression(direction);
    expr.over.frame = {
      mode: "range",
      start: { kind: "preceding", offset: lit(intervalValue("1 day")) },
      end: { kind: "following", offset: lit(intervalValue("1 day")) },
      exclude: "no-others",
    };
    const day = 86_400_000n;
    const partition = partitionRows(
      [0n, 1n, 1n, 2n, 5n].map((n) => ({ key: timestampFromEpoch(n * day, "millis") })),
      expr,
    );
    sortWindowRows(partition, expr.over.orderBy);
    expect(partition.map((_row, index) => frameSpan(partition, index, expr))).toEqual(
      direction === "asc"
        ? [
            { start: 0, end: 3 },
            { start: 0, end: 4 },
            { start: 0, end: 4 },
            { start: 1, end: 4 },
            { start: 4, end: 5 },
          ]
        : [
            { start: 0, end: 1 },
            { start: 1, end: 4 },
            { start: 1, end: 5 },
            { start: 1, end: 5 },
            { start: 2, end: 5 },
          ],
    );
  });

  it("bounds order-key reads logarithmically without allocating a partition index", () => {
    const expr = expression("asc");
    let reads = 0;
    const partition = Array.from({ length: 10_000 }, (_, index) => ({
      row: { key: index, before: 1, after: 1 },
      originalIndex: index,
      partitionKey: [],
      get orderKey() {
        reads += 1;
        return [index];
      },
    }));
    for (let index = 0; index < partition.length; index += 1) {
      expect(frameSpan(partition, index, expr)).toEqual({
        start: Math.max(0, index - 1),
        end: Math.min(partition.length, index + 2),
      });
    }
    expect(reads).toBeLessThan(300_000);
  });
});
