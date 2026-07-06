import { describe, expect, it } from "vitest";
import { col, type Expr, gt, lit } from "./expr.js";
import { createInMemoryLake } from "./in-memory.js";
import { intervalValue } from "./interval.js";
import type { AggregateOp } from "./query.js";
import { timestampFromEpoch } from "./timestamp.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import { aggregateWindowValue } from "./window-aggregate.js";
import { applyWindowsToRows } from "./window-backend.js";

const rows = [
  { order_id: 1, customer_id: "a", ts: 10, amount: 5 },
  { order_id: 2, customer_id: "a", ts: 20, amount: 7 },
  { order_id: 3, customer_id: "b", ts: 10, amount: 11 },
  { order_id: 4, customer_id: "a", ts: 20, amount: 13 },
  { order_id: 5, customer_id: "b", ts: 30, amount: null },
];

function overCustomerTs(fn: WindowExpr["fn"], args: WindowExpr["args"] = []): WindowExpr {
  return {
    fn,
    args,
    over: {
      partitionBy: [col("customer_id")],
      orderBy: [{ expr: col("ts") }, { expr: col("order_id") }],
    },
  };
}

describe("window backend", () => {
  it("evaluates exact aggregate window states across aggregate families", () => {
    const aggregateRows: Row[] = [
      { amount: 1, label: "a" },
      { amount: 2, label: "b" },
      { amount: 2, label: "b" },
      { amount: null, label: null },
    ];
    const aggregate = (fn: { aggregate: AggregateOp }, args: Expr[] = [col("amount")]) =>
      aggregateWindowValue(aggregateRows, {
        fn,
        args,
        over: { partitionBy: [], orderBy: [] },
      });

    expect(aggregate({ aggregate: "count" })).toBe(3);
    expect(aggregate({ aggregate: "count_distinct" })).toBe(2);
    expect(
      aggregateWindowValue(aggregateRows, {
        fn: { aggregate: "count" },
        args: [col("amount")],
        distinct: true,
        over: { partitionBy: [], orderBy: [] },
      }),
    ).toBe(2);
    expect(aggregate({ aggregate: "avg" })).toBe(5 / 3);
    expect(aggregate({ aggregate: "var_pop" })).toBeCloseTo(2 / 9);
    expect(aggregate({ aggregate: "var_samp" })).toBeCloseTo(1 / 3);
    expect(aggregate({ aggregate: "stddev_pop" })).toBeCloseTo(Math.sqrt(2 / 9));
    expect(aggregate({ aggregate: "stddev_samp" })).toBeCloseTo(Math.sqrt(1 / 3));
    expect(aggregate({ aggregate: "median" })).toBe(2);
    expect(aggregate({ aggregate: "quantile" }, [col("amount"), lit(0.75)])).toBe(2);
    expect(aggregate({ aggregate: "mode" }, [col("label")])).toBe("b");
    expect(aggregate({ aggregate: "first" }, [col("label")])).toBe("a");
    expect(aggregate({ aggregate: "last" }, [col("label")])).toBe("b");
    expect(aggregate({ aggregate: "any" }, [col("label")])).toBe("a");
    expect(aggregate({ aggregate: "min" }, [col("label")])).toBe("a");
    expect(aggregate({ aggregate: "max" }, [col("label")])).toBe("b");
  });

  it("throws typed errors for invalid aggregate window states", () => {
    expect(() =>
      aggregateWindowValue(rows, {
        fn: "row_number",
        args: [],
        over: { partitionBy: [], orderBy: [] },
      }),
    ).toThrow("Window expression is not an aggregate");
    expect(() =>
      aggregateWindowValue(rows, {
        fn: { aggregate: "approx_count_distinct" },
        args: [col("customer_id")],
        over: { partitionBy: [], orderBy: [] },
      }),
    ).toThrow("window results are exact");
    expect(() =>
      aggregateWindowValue(rows, {
        fn: { aggregate: "quantile" },
        args: [col("amount"), lit(2)],
        over: { partitionBy: [], orderBy: [] },
      }),
    ).toThrow("percentile must be between 0 and 1");
    expect(() =>
      aggregateWindowValue(rows, {
        fn: { aggregate: "sum" },
        args: [col("customer_id")],
        over: { partitionBy: [], orderBy: [] },
      }),
    ).toThrow("sum window aggregate requires numeric input");
  });

  it("evaluates ranking and offset windows through the public query builder", async () => {
    const lake = createInMemoryLake({ "orders.rows": rows });
    const result = await lake
      .path("orders.rows")
      .window("rn", overCustomerTs("row_number"))
      .window("prev_amount", overCustomerTs("lag", [col("amount")]))
      .select(["order_id", "customer_id", "rn", "prev_amount"])
      .orderBy([{ column: "order_id" }])
      .toArray();

    expect(result).toEqual([
      { order_id: 1, customer_id: "a", rn: 1, prev_amount: null },
      { order_id: 2, customer_id: "a", rn: 2, prev_amount: 5 },
      { order_id: 3, customer_id: "b", rn: 1, prev_amount: null },
      { order_id: 4, customer_id: "a", rn: 3, prev_amount: 7 },
      { order_id: 5, customer_id: "b", rn: 2, prev_amount: 11 },
    ]);
  });

  it("computes cumulative exact aggregate windows and qualify filters", async () => {
    const lake = createInMemoryLake({ "orders.rows": rows });
    const runningTotal: WindowExpr = {
      fn: { aggregate: "sum" },
      args: [col("amount")],
      over: {
        partitionBy: [col("customer_id")],
        orderBy: [{ expr: col("ts") }, { expr: col("order_id") }],
      },
    };
    const result = await lake
      .path("orders.rows")
      .window("running_total", runningTotal)
      .qualify(gt("running_total", lit(10)))
      .select(["order_id", "customer_id", "running_total"])
      .orderBy([{ column: "order_id" }])
      .toArray();

    expect(result).toEqual([
      { order_id: 2, customer_id: "a", running_total: 12 },
      { order_id: 3, customer_id: "b", running_total: 11 },
      { order_id: 4, customer_id: "a", running_total: 25 },
      { order_id: 5, customer_id: "b", running_total: 11 },
    ]);
  });

  it("applies aggregate window FILTER predicates inside each frame", () => {
    const runningPaid: WindowExpr = {
      fn: { aggregate: "sum" },
      args: [col("amount")],
      filter: { kind: "compare", op: "eq", left: col("status"), right: lit("paid") },
      over: {
        partitionBy: [col("customer_id")],
        orderBy: [{ expr: col("ts") }, { expr: col("order_id") }],
      },
    };
    const out = applyWindowsToRows(
      [
        { order_id: 1, customer_id: "a", ts: 10, amount: 5, status: "paid" },
        { order_id: 2, customer_id: "a", ts: 20, amount: 7, status: "void" },
        { order_id: 3, customer_id: "a", ts: 30, amount: 13, status: "paid" },
        { order_id: 4, customer_id: "b", ts: 10, amount: 11, status: "void" },
      ],
      { running_paid: runningPaid },
    ).sort((left, right) => Number(left.order_id) - Number(right.order_id));

    expect(out.map(({ order_id, running_paid }) => ({ order_id, running_paid }))).toEqual([
      { order_id: 1, running_paid: 5 },
      { order_id: 2, running_paid: 5 },
      { order_id: 3, running_paid: 18 },
      { order_id: 4, running_paid: null },
    ]);
  });

  it("produces peer-aware rank, dense_rank, percent_rank, and cume_dist", () => {
    const rankedRows = rows.map(({ order_id, customer_id, ts }) => ({ order_id, customer_id, ts }));
    const overPeers: WindowExpr = {
      fn: "rank",
      args: [],
      over: { partitionBy: [col("customer_id")], orderBy: [{ expr: col("ts") }] },
    };

    const out = applyWindowsToRows(rankedRows, {
      rank: overPeers,
      dense: { ...overPeers, fn: "dense_rank" },
      percent: { ...overPeers, fn: "percent_rank" },
      cume: { ...overPeers, fn: "cume_dist" },
    }).sort((left, right) => Number(left.order_id) - Number(right.order_id));

    expect(out).toEqual([
      { order_id: 1, customer_id: "a", ts: 10, rank: 1, dense: 1, percent: 0, cume: 1 / 3 },
      { order_id: 2, customer_id: "a", ts: 20, rank: 2, dense: 2, percent: 0.5, cume: 1 },
      { order_id: 3, customer_id: "b", ts: 10, rank: 1, dense: 1, percent: 0, cume: 0.5 },
      { order_id: 4, customer_id: "a", ts: 20, rank: 2, dense: 2, percent: 0.5, cume: 1 },
      { order_id: 5, customer_id: "b", ts: 30, rank: 2, dense: 2, percent: 1, cume: 1 },
    ]);
  });

  it("shares prefix-compatible physical sorts without changing stable peer order", () => {
    const peerRows = [
      { id: 1, account: "a", ts: 10, tie: 20 },
      { id: 2, account: "a", ts: 10, tie: 10 },
      { id: 3, account: "a", ts: 20, tie: 30 },
    ];
    const out = applyWindowsToRows(peerRows, {
      prefix_rn: {
        fn: "row_number",
        args: [],
        over: { partitionBy: [col("account")], orderBy: [{ expr: col("ts") }] },
      },
      full_rn: {
        fn: "row_number",
        args: [],
        over: {
          partitionBy: [col("account")],
          orderBy: [{ expr: col("ts") }, { expr: col("tie") }],
        },
      },
      prefix_rank: {
        fn: "rank",
        args: [],
        over: { partitionBy: [col("account")], orderBy: [{ expr: col("ts") }] },
      },
    }).sort((left, right) => Number(left.id) - Number(right.id));

    expect(
      out.map(({ id, prefix_rn, full_rn, prefix_rank }) => ({
        id,
        prefix_rn,
        full_rn,
        prefix_rank,
      })),
    ).toEqual([
      { id: 1, prefix_rn: 1, full_rn: 2, prefix_rank: 1 },
      { id: 2, prefix_rn: 2, full_rn: 1, prefix_rank: 1 },
      { id: 3, prefix_rn: 3, full_rn: 3, prefix_rank: 3 },
    ]);
  });

  it("uses standard default peer frames for value and aggregate functions", () => {
    const peerRows = [
      { id: 1, account: "a", ts: 10, amount: 5 },
      { id: 2, account: "a", ts: 20, amount: 7 },
      { id: 3, account: "a", ts: 20, amount: 13 },
    ];
    const overPeers = {
      partitionBy: [col("account")],
      orderBy: [{ expr: col("ts") }],
    };

    const out = applyWindowsToRows(peerRows, {
      last_amount: { fn: "last_value", args: [col("amount")], over: overPeers },
      running_total: { fn: { aggregate: "sum" }, args: [col("amount")], over: overPeers },
    }).sort((left, right) => Number(left.id) - Number(right.id));

    expect(out).toEqual([
      { id: 1, account: "a", ts: 10, amount: 5, last_amount: 5, running_total: 5 },
      { id: 2, account: "a", ts: 20, amount: 7, last_amount: 13, running_total: 25 },
      { id: 3, account: "a", ts: 20, amount: 13, last_amount: 13, running_total: 25 },
    ]);
  });

  it("evaluates exact ROWS frames for holistic aggregates and value functions", () => {
    const framed: WindowExpr = {
      ...overCustomerTs({ aggregate: "median" }, [col("amount")]),
      over: {
        partitionBy: [col("customer_id")],
        orderBy: [{ expr: col("ts") }, { expr: col("order_id") }],
        frame: {
          mode: "rows",
          start: { kind: "preceding", offset: lit(1) },
          end: { kind: "current-row" },
          exclude: "no-others",
        },
      },
    };
    const firstInFrame: WindowExpr = { ...framed, fn: "first_value", args: [col("amount")] };

    const out = applyWindowsToRows(rows, { median: framed, first_in_frame: firstInFrame }).sort(
      (left, right) => Number(left.order_id) - Number(right.order_id),
    );

    expect(
      out.map(({ order_id, median, first_in_frame }) => ({ order_id, median, first_in_frame })),
    ).toEqual([
      { order_id: 1, median: 5, first_in_frame: 5 },
      { order_id: 2, median: 6, first_in_frame: 5 },
      { order_id: 3, median: 11, first_in_frame: 11 },
      { order_id: 4, median: 10, first_in_frame: 7 },
      { order_id: 5, median: 11, first_in_frame: 11 },
    ]);
  });

  it("evaluates numeric RANGE offset frames in sort-order space", () => {
    const rangeRows = [
      { id: 1, account: "a", score: 10, amount: 5 },
      { id: 2, account: "a", score: 12, amount: 7 },
      { id: 3, account: "a", score: 18, amount: 11 },
      { id: 4, account: "a", score: 22, amount: 13 },
    ];
    const frame = {
      mode: "range" as const,
      start: { kind: "preceding" as const, offset: lit(5) },
      end: { kind: "following" as const, offset: lit(2) },
      exclude: "no-others" as const,
    };
    const out = applyWindowsToRows(rangeRows, {
      around_score: {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: { partitionBy: [col("account")], orderBy: [{ expr: col("score") }], frame },
      },
      desc_score: {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: {
          partitionBy: [col("account")],
          orderBy: [{ expr: col("score"), direction: "desc" }],
          frame,
        },
      },
    }).sort((left, right) => Number(left.id) - Number(right.id));

    expect(
      out.map(({ id, around_score, desc_score }) => ({ id, around_score, desc_score })),
    ).toEqual([
      { id: 1, around_score: 12, desc_score: 12 },
      { id: 2, around_score: 12, desc_score: 12 },
      { id: 3, around_score: 11, desc_score: 24 },
      { id: 4, around_score: 24, desc_score: 13 },
    ]);
  });

  it("evaluates timestamp RANGE offset frames with interval bounds", () => {
    const day = 86_400_000n;
    const rangeRows = [
      { id: 1, account: "a", ts: timestampFromEpoch(0n, "millis"), amount: 5 },
      { id: 2, account: "a", ts: timestampFromEpoch(day, "millis"), amount: 7 },
      { id: 3, account: "a", ts: timestampFromEpoch(3n * day, "millis"), amount: 11 },
      { id: 4, account: "a", ts: timestampFromEpoch(4n * day, "millis"), amount: 13 },
    ];
    const frame = {
      mode: "range" as const,
      start: { kind: "preceding" as const, offset: lit(intervalValue("1 day")) },
      end: { kind: "current-row" as const },
      exclude: "no-others" as const,
    };
    const out = applyWindowsToRows(rangeRows, {
      asc_ts: {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: { partitionBy: [col("account")], orderBy: [{ expr: col("ts") }], frame },
      },
      desc_ts: {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: {
          partitionBy: [col("account")],
          orderBy: [{ expr: col("ts"), direction: "desc" }],
          frame,
        },
      },
    }).sort((left, right) => Number(left.id) - Number(right.id));

    expect(out.map(({ id, asc_ts, desc_ts }) => ({ id, asc_ts, desc_ts }))).toEqual([
      { id: 1, asc_ts: 5, desc_ts: 12 },
      { id: 2, asc_ts: 12, desc_ts: 7 },
      { id: 3, asc_ts: 11, desc_ts: 24 },
      { id: 4, asc_ts: 24, desc_ts: 13 },
    ]);
  });

  it("supports IGNORE NULLS for offset and frame value functions", () => {
    const nullableRows = [
      { id: 1, account: "a", ts: 10, amount: null },
      { id: 2, account: "a", ts: 20, amount: 7 },
      { id: 3, account: "a", ts: 30, amount: null },
      { id: 4, account: "a", ts: 40, amount: 13 },
    ];
    const over = {
      partitionBy: [col("account")],
      orderBy: [{ expr: col("ts") }],
    };
    const out = applyWindowsToRows(nullableRows, {
      prev_amount: { fn: "lag", args: [col("amount")], over, ignoreNulls: true },
      first_amount: { fn: "first_value", args: [col("amount")], over, ignoreNulls: true },
      last_amount: { fn: "last_value", args: [col("amount")], over, ignoreNulls: true },
    }).sort((left, right) => Number(left.id) - Number(right.id));

    expect(
      out.map(({ id, prev_amount, first_amount, last_amount }) => ({
        id,
        prev_amount,
        first_amount,
        last_amount,
      })),
    ).toEqual([
      { id: 1, prev_amount: null, first_amount: null, last_amount: null },
      { id: 2, prev_amount: null, first_amount: 7, last_amount: 7 },
      { id: 3, prev_amount: 7, first_amount: 7, last_amount: 7 },
      { id: 4, prev_amount: 7, first_amount: 7, last_amount: 13 },
    ]);
  });

  it("covers value-function boundary behavior", () => {
    const out = applyWindowsToRows(
      [
        { id: 1, account: "a", amount: 5 },
        { id: 2, account: "a", amount: null },
        { id: 3, account: "a", amount: 13 },
        { id: 4, account: "b", amount: 17 },
      ],
      {
        bucket: {
          fn: "ntile",
          args: [lit(2)],
          over: { partitionBy: [col("account")], orderBy: [{ expr: col("id") }] },
        },
        next_amount: {
          fn: "lead",
          args: [col("amount"), lit(2), lit(-1)],
          over: { partitionBy: [col("account")], orderBy: [{ expr: col("id") }] },
        },
        singleton_percent: {
          fn: "percent_rank",
          args: [],
          over: { partitionBy: [col("id")], orderBy: [{ expr: col("amount") }] },
        },
        third_value: {
          fn: "nth_value",
          args: [col("amount"), lit(3)],
          over: {
            partitionBy: [col("account")],
            orderBy: [{ expr: col("id") }],
            frame: {
              mode: "rows",
              start: { kind: "current-row" },
              end: { kind: "following", offset: lit(1) },
              exclude: "no-others",
            },
          },
        },
      },
    ).sort((left, right) => Number(left.id) - Number(right.id));

    expect(
      out.map(({ id, bucket, next_amount, singleton_percent, third_value }) => ({
        id,
        bucket,
        next_amount,
        singleton_percent,
        third_value,
      })),
    ).toEqual([
      { id: 1, bucket: 1, next_amount: 13, singleton_percent: 0, third_value: null },
      { id: 2, bucket: 1, next_amount: -1, singleton_percent: 0, third_value: null },
      { id: 3, bucket: 2, next_amount: -1, singleton_percent: 0, third_value: null },
      { id: 4, bucket: 1, next_amount: -1, singleton_percent: 0, third_value: null },
    ]);
  });

  it("uses whole-partition frames when no ORDER BY is present", () => {
    const out = applyWindowsToRows(
      [
        { id: 1, account: "a", amount: 5 },
        { id: 2, account: "a", amount: 7 },
        { id: 3, account: "b", amount: 11 },
      ],
      {
        partition_total: {
          fn: { aggregate: "sum" },
          args: [col("amount")],
          over: { partitionBy: [col("account")], orderBy: [] },
        },
        partition_last: {
          fn: "last_value",
          args: [col("amount")],
          over: { partitionBy: [col("account")], orderBy: [] },
        },
      },
    ).sort((left, right) => Number(left.id) - Number(right.id));

    expect(
      out.map(({ id, partition_total, partition_last }) => ({
        id,
        partition_total,
        partition_last,
      })),
    ).toEqual([
      { id: 1, partition_total: 12, partition_last: 7 },
      { id: 2, partition_total: 12, partition_last: 7 },
      { id: 3, partition_total: 11, partition_last: 11 },
    ]);
  });

  it("evaluates exclusion modes and unbounded whole-partition frames", () => {
    const peerRows = [
      { id: 1, account: "a", bucket: 1, amount: 5 },
      { id: 2, account: "a", bucket: 1, amount: 7 },
      { id: 3, account: "a", bucket: 2, amount: 11 },
    ];
    const base = {
      partitionBy: [col("account")],
      orderBy: [{ expr: col("bucket") }],
      frame: {
        mode: "groups" as const,
        start: { kind: "unbounded-preceding" as const },
        end: { kind: "unbounded-following" as const },
        exclude: "group" as const,
      },
    };
    const out = applyWindowsToRows(peerRows, {
      without_current: {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: { ...base, frame: { ...base.frame, exclude: "current-row" } },
      },
      without_group: { fn: { aggregate: "sum" }, args: [col("amount")], over: base },
      without_ties: {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: { ...base, frame: { ...base.frame, exclude: "ties" } },
      },
    }).sort((left, right) => Number(left.id) - Number(right.id));

    expect(
      out.map(({ id, without_current, without_group, without_ties }) => ({
        id,
        without_current,
        without_group,
        without_ties,
      })),
    ).toEqual([
      { id: 1, without_current: 18, without_group: 11, without_ties: 16 },
      { id: 2, without_current: 16, without_group: 11, without_ties: 18 },
      { id: 3, without_current: 12, without_group: 12, without_ties: 23 },
    ]);
  });

  it("throws typed errors for invalid window arguments and frame shapes", () => {
    expect(() =>
      applyWindowsToRows(rows, {
        bad_ntile: overCustomerTs("ntile", [lit(0)]),
      }),
    ).toThrow("ntile requires a positive integer bucket count");

    expect(() =>
      applyWindowsToRows(rows, {
        bad_nth: overCustomerTs("nth_value", [col("amount"), lit(0)]),
      }),
    ).toThrow("nth_value requires a positive integer position");

    expect(() =>
      applyWindowsToRows(rows, {
        bad_lag: overCustomerTs("lag", [col("amount"), { kind: "column", name: "ts" }]),
      }),
    ).toThrow("lag argument must be numeric literal");

    expect(() =>
      applyWindowsToRows(rows, {
        negative_lag: overCustomerTs("lag", [col("amount"), lit(-1)]),
      }),
    ).toThrow("lag/lead offset must be a non-negative integer");

    expect(() =>
      applyWindowsToRows(rows, {
        unsupported: overCustomerTs("made_up" as never),
      }),
    ).toThrow("Unsupported window function made_up");

    expect(() =>
      applyWindowsToRows(rows, {
        bad_range: {
          fn: { aggregate: "sum" },
          args: [col("amount")],
          over: {
            partitionBy: [col("customer_id")],
            orderBy: [{ expr: col("ts") }, { expr: col("order_id") }],
            frame: {
              mode: "range",
              start: { kind: "preceding", offset: lit(1) },
              end: { kind: "current-row" },
              exclude: "no-others",
            },
          },
        },
      }),
    ).toThrow("RANGE offset frames require exactly one ORDER BY expression");

    expect(() =>
      applyWindowsToRows([{ id: 1, amount: 1, label: "a" }], {
        bad_range_type: {
          fn: { aggregate: "sum" },
          args: [col("amount")],
          over: {
            partitionBy: [],
            orderBy: [{ expr: col("label") }],
            frame: {
              mode: "range",
              start: { kind: "preceding", offset: lit(1) },
              end: { kind: "current-row" },
              exclude: "no-others",
            },
          },
        },
      }),
    ).toThrow("RANGE offset frames require a non-null numeric or timestamp ORDER BY value");

    expect(() =>
      applyWindowsToRows(rows, {
        missing_value: {
          fn: "first_value",
          args: [],
          over: { partitionBy: [col("customer_id")], orderBy: [{ expr: col("ts") }] },
        },
      }),
    ).toThrow("first_value requires a value argument");

    expect(() =>
      applyWindowsToRows(rows, {
        bad_rows_offset: {
          fn: { aggregate: "sum" },
          args: [col("amount")],
          over: {
            partitionBy: [col("customer_id")],
            orderBy: [{ expr: col("ts") }],
            frame: {
              mode: "rows",
              start: { kind: "preceding", offset: lit(1.5) },
              end: { kind: "current-row" },
              exclude: "no-others",
            },
          },
        },
      }),
    ).toThrow("Window frame offset must be a non-negative integer");

    expect(() =>
      applyWindowsToRows(rows, {
        missing_rows_offset: {
          fn: { aggregate: "sum" },
          args: [col("amount")],
          over: {
            partitionBy: [col("customer_id")],
            orderBy: [{ expr: col("ts") }],
            frame: {
              mode: "rows",
              start: { kind: "preceding" },
              end: { kind: "current-row" },
              exclude: "no-others",
            },
          },
        } as WindowExpr,
      }),
    ).toThrow("Window frame offset is required");

    expect(() =>
      applyWindowsToRows(rows, {
        bad_range_offset: {
          fn: { aggregate: "sum" },
          args: [col("amount")],
          over: {
            partitionBy: [col("customer_id")],
            orderBy: [{ expr: col("ts") }],
            frame: {
              mode: "range",
              start: { kind: "preceding", offset: lit(Number.NaN) },
              end: { kind: "current-row" },
              exclude: "no-others",
            },
          },
        },
      }),
    ).toThrow("RANGE frame offset must be a non-negative finite number");

    expect(() =>
      applyWindowsToRows(rows, {
        missing_numeric_range_offset: {
          fn: { aggregate: "sum" },
          args: [col("amount")],
          over: {
            partitionBy: [col("customer_id")],
            orderBy: [{ expr: col("ts") }],
            frame: {
              mode: "range",
              start: { kind: "preceding" },
              end: { kind: "following", offset: lit(1) },
              exclude: "no-others",
            },
          },
        } as WindowExpr,
      }),
    ).toThrow("Window frame offset is required");

    expect(() =>
      applyWindowsToRows([{ id: 1, amount: 1, ts: timestampFromEpoch(0n, "millis") }], {
        bad_timestamp_range_offset: {
          fn: { aggregate: "sum" },
          args: [col("amount")],
          over: {
            partitionBy: [],
            orderBy: [{ expr: col("ts") }],
            frame: {
              mode: "range",
              start: { kind: "preceding", offset: lit(1) },
              end: { kind: "current-row" },
              exclude: "no-others",
            },
          },
        },
      }),
    ).toThrow("Timestamp RANGE frame offset must be a non-negative interval");
  });

  it("fails with typed errors for budgets", async () => {
    const lake = createInMemoryLake({ "orders.rows": rows }, { budget: { maxBufferedRows: 2 } });
    await expect(
      lake.path("orders.rows").window("rn", overCustomerTs("row_number")).toArray(),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    const skewedPartition = createInMemoryLake(
      { "orders.rows": rows },
      { budget: { maxBufferedRows: 10, maxWindowPartitionRows: 2 } },
    );
    await expect(
      skewedPartition.path("orders.rows").window("rn", overCustomerTs("row_number")).toArray(),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { actual: 3, limit: 2, metric: "window partition rows" },
    });

    const memoryLimited = createInMemoryLake(
      { "orders.rows": rows },
      { budget: { maxBufferedRows: 10, maxMemoryBytes: 1 } },
    );
    await expect(
      memoryLimited.path("orders.rows").window("rn", overCustomerTs("row_number")).toArray(),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "operator memory bytes" },
    });
  });
});
