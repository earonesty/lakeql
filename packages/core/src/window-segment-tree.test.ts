import { describe, expect, it } from "vitest";
import { col, lit } from "./expr.js";
import { intervalValue } from "./interval.js";
import { timestampFromEpoch } from "./timestamp.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import { aggregateWindowValue } from "./window-aggregate.js";
import { frameRows } from "./window-frame.js";
import { segmentTreeAggregateValues } from "./window-segment-tree.js";
import { partitionRows, partitionSpans, sortWindowRows } from "./window-utils.js";

describe("window segment tree aggregates", () => {
  it("matches exact frame recomputation for mergeable aggregate windows", () => {
    const rows: Row[] = [
      { id: 1, account: "a", amount: 5 },
      { id: 2, account: "a", amount: 7 },
      { id: 3, account: "a", amount: null },
      { id: 4, account: "a", amount: 13 },
      { id: 5, account: "a", amount: 17 },
    ];
    const exprs: WindowExpr[] = [
      window({ aggregate: "count" }, []),
      window({ aggregate: "sum" }),
      window({ aggregate: "avg" }),
      window({ aggregate: "min" }),
      window({ aggregate: "max" }),
      window({ aggregate: "var_samp" }),
      window({ aggregate: "var_pop" }),
      window({ aggregate: "stddev_samp" }),
      window({ aggregate: "stddev_pop" }),
    ];
    const sorted = partitionRows(rows, exprs[0] as WindowExpr);
    sortWindowRows(sorted, exprs[0]?.over.orderBy ?? []);
    const partition = sorted.slice(
      partitionSpans(sorted)[0]?.start ?? 0,
      partitionSpans(sorted)[0]?.end ?? 0,
    );

    for (const expr of exprs) {
      expect(segmentTreeAggregateValues(partition, expr)).toEqual(
        partition.map((_row, index) =>
          aggregateWindowValue(frameRows(partition, index, expr), expr),
        ),
      );
    }
  });

  it("defers unsupported aggregate shapes to the exact evaluator", () => {
    const rows: Row[] = [
      { id: 1, account: "a", amount: 5 },
      { id: 2, account: "a", amount: 7 },
    ];
    const sorted = partitionRows(rows, window({ aggregate: "sum" }));
    sortWindowRows(sorted, [{ expr: col("id") }]);
    expect(
      segmentTreeAggregateValues(sorted, { ...window({ aggregate: "sum" }), distinct: true }),
    ).toBeUndefined();
    expect(
      segmentTreeAggregateValues(sorted, {
        ...window({ aggregate: "sum" }),
        over: {
          partitionBy: [col("account")],
          orderBy: [{ expr: col("id") }],
          frame: {
            mode: "rows",
            start: { kind: "unbounded-preceding" },
            end: { kind: "current-row" },
            exclude: "current-row",
          },
        },
      }),
    ).toBeUndefined();
    expect(segmentTreeAggregateValues(sorted, window({ aggregate: "median" }))).toBeUndefined();
    expect(
      segmentTreeAggregateValues(sorted, { ...window("row_number"), args: [] }),
    ).toBeUndefined();
  });

  it("uses stable variance state for large-magnitude values", () => {
    const rows = [
      { id: 1, account: "a", amount: 1_000_000_001 },
      { id: 2, account: "a", amount: 1_000_000_002 },
      { id: 3, account: "a", amount: 1_000_000_003 },
    ];
    const expr = window({ aggregate: "stddev_samp" });
    const sorted = partitionRows(rows, expr);
    sortWindowRows(sorted, expr.over.orderBy);
    const values = segmentTreeAggregateValues(sorted, expr);
    expect(values?.[2]).toBeCloseTo(1);
    expect(values).toEqual(
      sorted.map((_row, index) => aggregateWindowValue(frameRows(sorted, index, expr), expr)),
    );
  });

  it("handles filters, empty frames, timestamp ordering, and typed errors", () => {
    const rows: Row[] = [
      { id: 1, account: "a", amount: 5, keep: false, ts: timestampFromEpoch(2n, "millis") },
      { id: 2, account: "a", amount: 7, keep: true, ts: timestampFromEpoch(1n, "millis") },
      { id: 3, account: "a", amount: 11, keep: false, ts: timestampFromEpoch(3n, "millis") },
    ];
    const sorted = partitionRows(rows, window({ aggregate: "sum" }));
    sortWindowRows(sorted, [{ expr: col("id") }]);

    expect(
      segmentTreeAggregateValues(sorted, {
        ...window({ aggregate: "sum" }),
        filter: col("keep"),
      }),
    ).toEqual([null, 7, 7]);

    expect(
      segmentTreeAggregateValues(sorted, window({ aggregate: "min" }, [col("ts")]))?.map((value) =>
        String(value),
      ),
    ).toEqual(["1970-01-01T00:00:00.002Z", "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.001Z"]);

    const badRows = partitionRows(
      [{ id: 1, account: "a", amount: "bad" }],
      window({ aggregate: "sum" }),
    );
    sortWindowRows(badRows, [{ expr: col("id") }]);
    expect(() => segmentTreeAggregateValues(badRows, window({ aggregate: "sum" }))).toThrow(
      "sum window aggregate requires numeric input",
    );

    const mixedMinRows = partitionRows(
      [
        { id: 1, account: "a", amount: timestampFromEpoch(1n, "millis") },
        { id: 2, account: "a", amount: 2 },
      ],
      window({ aggregate: "min" }),
    );
    sortWindowRows(mixedMinRows, [{ expr: col("id") }]);
    expect(() => segmentTreeAggregateValues(mixedMinRows, window({ aggregate: "min" }))).toThrow(
      "aggregate timestamp values must have matching types",
    );

    const mixedScalarRows = partitionRows(
      [
        { id: 1, account: "a", amount: 1 },
        { id: 2, account: "a", amount: "x" },
      ],
      window({ aggregate: "min" }),
    );
    sortWindowRows(mixedScalarRows, [{ expr: col("id") }]);
    expect(() => segmentTreeAggregateValues(mixedScalarRows, window({ aggregate: "min" }))).toThrow(
      "aggregate values must have matching types",
    );

    const stringRows = partitionRows(
      [
        { id: 1, account: "a", amount: "b" },
        { id: 2, account: "a", amount: "a" },
      ],
      window({ aggregate: "max" }),
    );
    sortWindowRows(stringRows, [{ expr: col("id") }]);
    expect(segmentTreeAggregateValues(stringRows, window({ aggregate: "max" }))).toEqual([
      "b",
      "b",
    ]);

    const intervalMinRows = partitionRows(
      [
        { id: 1, account: "a", amount: intervalValue("1 day") },
        { id: 2, account: "a", amount: intervalValue("2 days") },
      ],
      window({ aggregate: "min" }),
    );
    sortWindowRows(intervalMinRows, [{ expr: col("id") }]);
    expect(() => segmentTreeAggregateValues(intervalMinRows, window({ aggregate: "min" }))).toThrow(
      "aggregate values must be scalar",
    );
  });
});

function window(fn: WindowExpr["fn"], args = [col("amount")]): WindowExpr {
  return {
    fn,
    args,
    over: {
      partitionBy: [col("account")],
      orderBy: [{ expr: col("id") }],
      frame: {
        mode: "rows",
        start: { kind: "preceding", offset: lit(2) },
        end: { kind: "current-row" },
        exclude: "no-others",
      },
    },
  };
}
