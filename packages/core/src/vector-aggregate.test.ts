import { describe, expect, it } from "vitest";
import { batchFromColumns, predicateSelection } from "./batch.js";
import { col, fn, gt, lit, mul } from "./expr.js";
import { timestampFromEpoch } from "./timestamp.js";
import {
  createVectorAggregateStates,
  finalizeVectorAggregateStates,
  mergeVectorAggregateStateSnapshots,
  mergeVectorAggregateStates,
  restoreVectorAggregateStates,
  snapshotVectorAggregateStates,
  updateVectorAggregateStates,
  vectorAggregateBatch,
} from "./vector-aggregate.js";

describe("vector aggregate kernels", () => {
  it("aggregates selected column batches with current global aggregate semantics", () => {
    const batch = batchFromColumns({
      id: [1, 2, 3, 4],
      amount: [10, null, 30, 40],
      label: ["a", "b", "a", null],
      flag: [true, false, true, null],
    });
    const selection = predicateSelection(batch, gt("id", 1));
    const spec = {
      rows: { op: "count" },
      amountRows: { op: "count", column: "amount" },
      total: { op: "sum", column: "amount" },
      average: { op: "avg", column: "amount" },
      variance: { op: "var_samp", column: "amount" },
      stddev: { op: "stddev_samp", column: "amount" },
      popVariance: { op: "var_pop", column: "amount" },
      popStddev: { op: "stddev_pop", column: "amount" },
      medianAmount: { op: "median", column: "amount" },
      amountP25: { op: "quantile", column: "amount", quantile: 0.25 },
      minId: { op: "min", column: "id" },
      maxLabel: { op: "max", column: "label" },
      labels: { op: "count_distinct", column: "label" },
      modeLabel: { op: "mode", column: "label" },
      firstLabel: { op: "first", column: "label" },
      lastLabel: { op: "last", column: "label" },
      anyLabel: { op: "any", column: "label" },
    } as const;

    expect(vectorAggregateBatch(spec, batch, selection)).toEqual({
      rows: 3,
      amountRows: 2,
      total: 70,
      average: 35,
      variance: 50,
      stddev: Math.sqrt(50),
      popVariance: 25,
      popStddev: 5,
      medianAmount: 35,
      amountP25: 32.5,
      minId: 2,
      maxLabel: "b",
      labels: 2,
      modeLabel: "b",
      firstLabel: "b",
      lastLabel: null,
      anyLabel: "b",
    });
  });

  it("returns the first value that reaches the winning mode frequency", () => {
    expect(
      vectorAggregateBatch(
        {
          numericMode: { op: "mode", column: "amount" },
          stringMode: { op: "mode", column: "label" },
          allNullMode: { op: "mode", column: "missing" },
        },
        batchFromColumns({
          amount: [1, 2, 2, 3, 3],
          label: ["b", "a", "a", "b", null],
          missing: [null, null, null, null, null],
        }),
      ),
    ).toEqual({
      numericMode: 2,
      stringMode: "b",
      allNullMode: null,
    });
  });

  it("aggregates timestamp min and max with snapshot round trips", () => {
    const batch = batchFromColumns({
      loaded_at: [
        timestampFromEpoch(1_700_000_000_000_999n, "micros"),
        timestampFromEpoch(1_700_000_000_000_001n, "micros"),
        null,
      ],
    });
    const spec = {
      firstLoaded: { op: "min", column: "loaded_at" },
      lastLoaded: { op: "max", column: "loaded_at" },
    } as const;
    const states = createVectorAggregateStates(spec);
    updateVectorAggregateStates(states, spec, batch);
    const restored = restoreVectorAggregateStates(snapshotVectorAggregateStates(states));

    expect(finalizeVectorAggregateStates(restored)).toEqual({
      firstLoaded: timestampFromEpoch(1_700_000_000_000_001n, "micros"),
      lastLoaded: timestampFromEpoch(1_700_000_000_000_999n, "micros"),
    });
  });

  it("returns DuckDB-compatible exact medians for numbers and strings", () => {
    expect(
      vectorAggregateBatch(
        {
          oddNumber: { op: "median", column: "odd" },
          evenNumber: { op: "median", column: "even" },
          nullSkipped: { op: "median", column: "nullable" },
          allNull: { op: "median", column: "missing" },
          stringMedian: { op: "median", column: "label" },
        },
        batchFromColumns({
          odd: [1, 2, 3, null],
          even: [1, 2, 3, 4],
          nullable: [null, 1, 3, null],
          missing: [null, null, null, null],
          label: ["b", "a", "c", "d"],
        }),
      ),
    ).toEqual({
      oddNumber: 2,
      evenNumber: 2.5,
      nullSkipped: 2,
      allNull: null,
      stringMedian: "b",
    });
  });

  it("returns DuckDB-compatible continuous quantiles for numeric inputs", () => {
    expect(
      vectorAggregateBatch(
        {
          p0: { op: "quantile", column: "amount", quantile: 0 },
          p25: { op: "quantile", column: "amount", quantile: 0.25 },
          p50: { op: "quantile", column: "amount", quantile: 0.5 },
          p75: { op: "quantile", column: "amount", quantile: 0.75 },
          p100: { op: "quantile", column: "amount", quantile: 1 },
          nullSkipped: { op: "quantile", column: "nullable", quantile: 0.5 },
          allNull: { op: "quantile", column: "missing", quantile: 0.5 },
        },
        batchFromColumns({
          amount: [1, 2, 3, 4],
          nullable: [null, 1, 3, null],
          missing: [null, null, null, null],
        }),
      ),
    ).toEqual({
      p0: 1,
      p25: 1.75,
      p50: 2.5,
      p75: 3.25,
      p100: 4,
      nullSkipped: 2,
      allNull: null,
    });
  });

  it("merges partial aggregate states without materializing rows", () => {
    const spec = {
      rows: { op: "count" },
      total: { op: "sum", column: "amount" },
      average: { op: "avg", column: "amount" },
      variance: { op: "var_samp", column: "amount" },
      stddev: { op: "stddev_samp", column: "amount" },
      median: { op: "median", column: "amount" },
      p75: { op: "quantile", column: "amount", quantile: 0.75 },
      maxId: { op: "max", column: "id" },
    } as const;
    const left = batchFromColumns({ id: [1, 2], amount: [10, null] });
    const right = batchFromColumns({ id: [3, 4], amount: [30, 40] });
    const merged = createVectorAggregateStates(spec);
    for (const batch of [left, right]) {
      const partial = createVectorAggregateStates(spec);
      updateVectorAggregateStates(partial, spec, batch);
      mergeVectorAggregateStates(merged, partial);
    }

    expect(finalizeVectorAggregateStates(merged)).toEqual({
      rows: 4,
      total: 80,
      average: 80 / 3,
      variance: 700 / 3,
      stddev: Math.sqrt(700 / 3),
      median: 30,
      p75: 35,
      maxId: 4,
    });
  });

  it("returns DuckDB-compatible nulls for empty and single-row sample statistics", () => {
    expect(
      vectorAggregateBatch(
        {
          sampleVariance: { op: "var_samp", column: "amount" },
          sampleStddev: { op: "stddev_samp", column: "amount" },
          popVariance: { op: "var_pop", column: "amount" },
          popStddev: { op: "stddev_pop", column: "amount" },
        },
        batchFromColumns({ amount: [10] }),
      ),
    ).toEqual({
      sampleVariance: null,
      sampleStddev: null,
      popVariance: 0,
      popStddev: 0,
    });
  });

  it("aggregates arithmetic expression inputs over selected column batches", () => {
    const batch = batchFromColumns({
      id: [1, 2, 3, 4],
      amount: [10, null, 30, 40],
    });
    const selection = predicateSelection(batch, gt("id", 1));
    const spec = {
      doubledTotal: { op: "sum", expr: mul({ kind: "column", name: "amount" }, 2) },
      doubledMax: { op: "max", expr: mul({ kind: "column", name: "amount" }, 2) },
      doubledRows: { op: "count", expr: mul({ kind: "column", name: "amount" }, 2) },
    } as const;

    expect(vectorAggregateBatch(spec, batch, selection)).toEqual({
      doubledTotal: 140,
      doubledMax: 80,
      doubledRows: 2,
    });
  });

  it("aggregates searched CASE expression inputs over selected column batches", () => {
    const batch = batchFromColumns({
      id: [1, 2, 3, 4],
      amount: [10, null, 30, 40],
    });
    const selection = predicateSelection(batch, gt("id", 1));
    const spec = {
      positiveAverage: {
        op: "avg",
        expr: {
          kind: "case",
          whens: [{ when: gt("amount", 20), value: col("amount") }],
          else: lit(0),
        },
      },
    } as const;

    expect(vectorAggregateBatch(spec, batch, selection)).toEqual({
      positiveAverage: (0 + 30 + 40) / 3,
    });
  });

  it("aggregates coalesce and nullif expression inputs over selected column batches", () => {
    const batch = batchFromColumns({
      id: [1, 2, 3, 4],
      amount: [10, null, 30, 40],
      fallback: [1, 20, null, 40],
      label: [" Keep ", "skip", null, "keep"],
      numericText: ["10", "20", "bad", null],
    });
    const selection = predicateSelection(batch, gt("id", 1));
    const spec = {
      totalWithFallback: { op: "sum", expr: fn("coalesce", col("amount"), col("fallback"), 0) },
      nonSkipLabels: { op: "count", expr: fn("nullif", col("label"), "skip") },
      nonDuplicateAmounts: { op: "count", expr: fn("nullif", col("amount"), col("fallback")) },
      minNormalizedLabel: { op: "min", expr: fn("lower", fn("trim", col("label"))) },
      maxReplacedLabel: { op: "max", expr: fn("replace", fn("trim", col("label")), "e", "E") },
      textTotal: { op: "sum", expr: fn("cast", col("numericText"), "number") },
      roundedTextTotal: { op: "sum", expr: fn("round", fn("cast", col("numericText"), "number")) },
    } as const;

    expect(vectorAggregateBatch(spec, batch, selection)).toEqual({
      totalWithFallback: 90,
      nonSkipLabels: 1,
      nonDuplicateAmounts: 1,
      minNormalizedLabel: "keep",
      maxReplacedLabel: "skip",
      textTotal: 20,
      roundedTextTotal: 20,
    });
  });

  it("enforces distinct aggregate row budgets during vector updates", () => {
    const spec = {
      labels: { op: "count_distinct", column: "label" },
      modeLabel: { op: "mode", column: "label" },
    } as const;
    const batch = batchFromColumns({ label: ["a", "b"] });
    const states = createVectorAggregateStates(spec, { budget: { maxBufferedRows: 1 } });

    expect(() =>
      updateVectorAggregateStates(states, spec, batch, undefined, {
        budget: { maxBufferedRows: 1 },
      }),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: expect.objectContaining({ metric: "buffered rows", limit: 1 }),
      }),
    );
  });

  it("enforces mode aggregate row budgets during vector updates", () => {
    const spec = {
      labelMode: { op: "mode", column: "label" },
    } as const;
    const states = createVectorAggregateStates(spec, { budget: { maxBufferedRows: 1 } });

    expect(() =>
      updateVectorAggregateStates(
        states,
        spec,
        batchFromColumns({ label: ["a", "b", "b"] }),
        undefined,
        { budget: { maxBufferedRows: 1 } },
      ),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: expect.objectContaining({ metric: "buffered rows", limit: 1 }),
      }),
    );
  });

  it("enforces median aggregate row budgets during vector updates", () => {
    const spec = {
      amountMedian: { op: "median", column: "amount" },
    } as const;
    const states = createVectorAggregateStates(spec, { budget: { maxBufferedRows: 1 } });

    expect(() =>
      updateVectorAggregateStates(states, spec, batchFromColumns({ amount: [1, 2] }), undefined, {
        budget: { maxBufferedRows: 1 },
      }),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: expect.objectContaining({ metric: "buffered rows", limit: 1 }),
      }),
    );
  });

  it("enforces quantile aggregate row budgets during vector updates", () => {
    const spec = {
      amountP50: { op: "quantile", column: "amount", quantile: 0.5 },
    } as const;
    const states = createVectorAggregateStates(spec, { budget: { maxBufferedRows: 1 } });

    expect(() =>
      updateVectorAggregateStates(states, spec, batchFromColumns({ amount: [1, 2] }), undefined, {
        budget: { maxBufferedRows: 1 },
      }),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: expect.objectContaining({ metric: "buffered rows", limit: 1 }),
      }),
    );
  });

  it("enforces distinct aggregate memory budgets during vector fan-in merges", () => {
    const spec = {
      labels: { op: "count_distinct", column: "label" },
    } as const;
    const left = createVectorAggregateStates(spec);
    const right = createVectorAggregateStates(spec);
    updateVectorAggregateStates(left, spec, batchFromColumns({ label: ["alpha"] }));
    updateVectorAggregateStates(right, spec, batchFromColumns({ label: ["beta"] }));

    expect(() =>
      mergeVectorAggregateStates(left, right, {
        budget: { maxMemoryBytes: 1 },
      }),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: expect.objectContaining({ metric: "operator memory bytes", limit: 1 }),
      }),
    );
  });

  it("round-trips partial aggregate states through JSON for deployment fan-in", () => {
    const spec = {
      rows: { op: "count" },
      total: { op: "sum", column: "amount" },
      average: { op: "avg", column: "amount" },
      variance: { op: "var_samp", column: "amount" },
      popVariance: { op: "var_pop", column: "amount" },
      stddev: { op: "stddev_samp", column: "amount" },
      popStddev: { op: "stddev_pop", column: "amount" },
      median: { op: "median", column: "amount" },
      p75: { op: "quantile", column: "amount", quantile: 0.75 },
      minAmount: { op: "min", column: "amount" },
      maxBig: { op: "max", column: "big" },
      labels: { op: "count_distinct", column: "label" },
      approximateLabels: { op: "approx_count_distinct", column: "label" },
      modeLabel: { op: "mode", column: "label" },
      firstLabel: { op: "first", column: "label" },
      lastLabel: { op: "last", column: "label" },
      anyLabel: { op: "any", column: "label" },
    } as const;
    const left = createVectorAggregateStates(spec);
    const right = createVectorAggregateStates(spec);
    updateVectorAggregateStates(
      left,
      spec,
      batchFromColumns({ amount: [1, 2], big: [10n, 20n], label: ["a", "b"] }),
    );
    updateVectorAggregateStates(
      right,
      spec,
      batchFromColumns({ amount: [3], big: [30n], label: ["b"] }),
    );

    const serializedLeft = JSON.parse(JSON.stringify(snapshotVectorAggregateStates(left)));
    const serializedRight = JSON.parse(JSON.stringify(snapshotVectorAggregateStates(right)));
    const merged = restoreVectorAggregateStates(serializedLeft);
    mergeVectorAggregateStates(merged, restoreVectorAggregateStates(serializedRight));

    expect(finalizeVectorAggregateStates(merged)).toEqual({
      rows: 3,
      total: 6,
      average: 2,
      variance: 1,
      popVariance: 2 / 3,
      stddev: 1,
      popStddev: Math.sqrt(2 / 3),
      median: 2,
      p75: 2.5,
      minAmount: 1,
      maxBig: 30n,
      labels: 2,
      approximateLabels: 2,
      modeLabel: "b",
      firstLabel: "a",
      lastLabel: "b",
      anyLabel: "a",
    });
  });

  it("merges portable distinct snapshots without restoring partial sets", () => {
    const spec = {
      rows: { op: "count" },
      labels: { op: "count_distinct", column: "label" },
    } as const;
    const left = createVectorAggregateStates(spec);
    const right = createVectorAggregateStates(spec);
    updateVectorAggregateStates(left, spec, batchFromColumns({ label: ["a", "b", "b"] }));
    updateVectorAggregateStates(right, spec, batchFromColumns({ label: ["b", "c"] }));

    const merged = createVectorAggregateStates(spec);
    mergeVectorAggregateStateSnapshots(
      merged,
      JSON.parse(JSON.stringify(snapshotVectorAggregateStates(left))),
    );
    mergeVectorAggregateStateSnapshots(
      merged,
      JSON.parse(JSON.stringify(snapshotVectorAggregateStates(right))),
    );

    expect(finalizeVectorAggregateStates(merged)).toEqual({
      rows: 5,
      labels: 3,
    });
  });

  it("keeps sorted distinct fan-in exact after a later batch update", () => {
    const spec = {
      labels: { op: "count_distinct", column: "label" },
    } as const;
    const firstLabels = Array.from({ length: 1200 }, (_, index) => `v${index}`);
    const secondLabels = Array.from({ length: 1200 }, (_, index) => `v${index + 600}`);
    const first = createVectorAggregateStates(spec);
    const second = createVectorAggregateStates(spec);
    updateVectorAggregateStates(first, spec, batchFromColumns({ label: firstLabels }));
    updateVectorAggregateStates(second, spec, batchFromColumns({ label: secondLabels }));

    const merged = createVectorAggregateStates(spec);
    mergeVectorAggregateStateSnapshots(
      merged,
      JSON.parse(JSON.stringify(snapshotVectorAggregateStates(first))),
    );
    mergeVectorAggregateStateSnapshots(
      merged,
      JSON.parse(JSON.stringify(snapshotVectorAggregateStates(second))),
    );
    updateVectorAggregateStates(merged, spec, batchFromColumns({ label: ["v1800", "v0"] }));

    const snapshot = snapshotVectorAggregateStates(merged).labels;
    expect(snapshot.op).toBe("count_distinct");
    expect(snapshot.values).toHaveLength(1801);
    expect(snapshot.values[0]).toBe("string:v0");
    expect(snapshot.values[snapshot.values.length - 1]).toBe("string:v999");
    expect(finalizeVectorAggregateStates(merged)).toEqual({ labels: 1801 });
  });

  it("keeps large direct UTF-8 distinct batches exact", () => {
    const spec = {
      labels: { op: "count_distinct", column: "label" },
    } as const;
    const labels = Array.from({ length: 2400 }, (_, index) => `v${index % 1600}`);

    expect(vectorAggregateBatch(spec, batchFromColumns({ label: labels }))).toEqual({
      labels: 1600,
    });
  });
});
