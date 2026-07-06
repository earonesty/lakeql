import { describe, expect, it } from "vitest";
import { col, gt, lit } from "./expr.js";
import { timestampFromEpoch } from "./timestamp.js";
import type { WindowExpr } from "./window.js";
import { collectWindowColumns, windowExpressions, windowReadColumns } from "./window-query.js";
import {
  compareWindowValues,
  filterRow,
  isAggregateWindow,
  normalizedWindowName,
  partitionRows,
  partitionSpans,
  requireExactWindowAggregate,
  samePartition,
  samePeer,
  sortWindowRows,
  stableKey,
} from "./window-utils.js";

describe("window utilities", () => {
  const expr: WindowExpr = {
    fn: { aggregate: "sum" },
    args: [col("amount")],
    filter: gt("amount", lit(10)),
    over: {
      partitionBy: [col("region")],
      orderBy: [{ expr: col("amount"), direction: "desc", nulls: "last" }],
      frame: {
        mode: "range",
        start: { kind: "preceding", offset: col("range_start") },
        end: { kind: "following", offset: col("range_end") },
        exclude: "no-others",
      },
    },
  };

  it("normalizes names, classifies aggregate windows, and enforces exact aggregate contracts", () => {
    expect(normalizedWindowName("row_number")).toBe("row_number");
    expect(normalizedWindowName({ aggregate: "sum" })).toBe("sum");
    expect(isAggregateWindow({ aggregate: "sum" })).toBe(true);
    expect(isAggregateWindow("rank")).toBe(false);
    expect(requireExactWindowAggregate("count")).toBeUndefined();
    expect(() => requireExactWindowAggregate("approx_count_distinct")).toThrow(
      "window results are exact",
    );
  });

  it("sorts partition rows and detects partitions and peers", () => {
    const rows = [
      { id: 1, region: "west", amount: 10 },
      { id: 2, region: "east", amount: 20 },
      { id: 3, region: "east", amount: 20 },
      { id: 4, region: "west", amount: null },
    ];
    const windowRows = partitionRows(rows, expr);
    sortWindowRows(windowRows, expr.over.orderBy);

    expect(windowRows.map((row) => row.row.id)).toEqual([2, 3, 1, 4]);
    expect(partitionSpans(windowRows)).toEqual([
      { start: 0, end: 2 },
      { start: 2, end: 4 },
    ]);
    const first = windowRows[0];
    const second = windowRows[1];
    const third = windowRows[2];
    expect(first).toBeDefined();
    expect(second).toBeDefined();
    expect(third).toBeDefined();
    if (first === undefined || second === undefined || third === undefined) return;
    expect(samePartition(first, second)).toBe(true);
    expect(samePeer(first, second)).toBe(true);
    expect(samePeer(first, third)).toBe(false);
  });

  it("compares window values across null, scalar, timestamp, and error branches", () => {
    const term = { expr: col("x") };
    expect(compareWindowValues(null, undefined, term)).toBe(0);
    expect(compareWindowValues(null, 1, term)).toBe(1);
    expect(compareWindowValues(null, 1, { ...term, nulls: "first" })).toBe(-1);
    expect(compareWindowValues(2, 1, { ...term, direction: "desc" })).toBe(-1);
    expect(compareWindowValues("a", "b", term)).toBe(-1);
    expect(compareWindowValues(true, false, term)).toBe(1);
    expect(
      compareWindowValues(timestampFromEpoch(1n, "millis"), timestampFromEpoch(2n, "millis"), term),
    ).toBe(-1);
    expect(() => compareWindowValues(timestampFromEpoch(1n, "millis"), 1, term)).toThrow(
      "timestamp values must have matching types",
    );
    expect(() => compareWindowValues(1, "1", term)).toThrow("values must have matching types");
    expect(() => compareWindowValues({}, {}, term)).toThrow("values must be scalar");
  });

  it("collects window read expressions without reading synthetic aliases", () => {
    expect(filterRow(expr, { amount: 11 })).toBe(true);
    expect(filterRow(expr, { amount: 5 })).toBe(false);
    expect(stableKey([1, BigInt(2), null])).toBe("[1,2,null]");

    expect(
      windowReadColumns({
        select: ["region", "running"],
        orderBy: [{ column: "running" }, { column: "store_id" }],
        projections: {
          doubled: { kind: "arithmetic", op: "mul", left: col("amount"), right: lit(2) },
        },
        windows: { running: expr },
        where: gt("amount", lit(0)),
        qualify: gt("running", lit(10)),
      }),
    ).toEqual(["amount", "range_end", "range_start", "region", "store_id"]);

    expect(windowExpressions({ running: expr })).toHaveLength(6);
    const columns = new Set<string>();
    collectWindowColumns(expr, columns);
    expect([...columns].sort()).toEqual(["amount", "range_end", "range_start", "region"]);
    expect(
      windowReadColumns({
        windows: {
          running: { fn: "row_number", args: [], over: { partitionBy: [], orderBy: [] } },
        },
      }),
    ).toBeUndefined();
  });
});
