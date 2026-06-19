import { describe, expect, it } from "vitest";
import type { BatchExprValues } from "./batch.js";
import { batchFromColumns } from "./batch.js";
import {
  batchCallExprValues,
  batchCallPredicateMask,
  vectorCallExprSupported,
} from "./batch-call.js";
import { col, fn, lit } from "./expr.js";

function values(items: unknown[]): BatchExprValues {
  return {
    rowCount: items.length,
    valueAt(index) {
      return items[index] as ReturnType<BatchExprValues["valueAt"]>;
    },
  };
}

function literal(rowCount: number, value: ReturnType<BatchExprValues["valueAt"]>): BatchExprValues {
  return { rowCount, literal: value, valueAt: () => value };
}

function collect(call: BatchExprValues): unknown[] {
  return Array.from({ length: call.rowCount }, (_, index) => call.valueAt(index));
}

function mask(value: Uint8Array | undefined): number[] {
  if (value === undefined) throw new Error("missing mask");
  return [...value];
}

describe("batch call vector kernels", () => {
  it("evaluates scalar string, numeric, regex, cast, and null-combining calls", () => {
    const text = values([" Alpha ", "beta", null]);
    const number = values([1.25, -2.75, null]);
    const compareEq = (left: unknown, right: unknown) => left === right;

    expect(collect(batchCallExprValues(3, "lower", [text], compareEq))).toEqual([
      " alpha ",
      "beta",
      null,
    ]);
    expect(collect(batchCallExprValues(3, "upper", [text], compareEq))).toEqual([
      " ALPHA ",
      "BETA",
      null,
    ]);
    expect(collect(batchCallExprValues(3, "trim", [text], compareEq))).toEqual([
      "Alpha",
      "beta",
      null,
    ]);
    expect(
      collect(batchCallExprValues(3, "substr", [text, literal(3, 1), literal(3, 3)], compareEq)),
    ).toEqual(["Alp", "eta", null]);
    expect(
      collect(
        batchCallExprValues(3, "replace", [text, literal(3, "a"), literal(3, "x")], compareEq),
      ),
    ).toEqual([" Alphx ", "betx", null]);
    expect(
      collect(
        batchCallExprValues(
          3,
          "regexp_matches",
          [text, literal(3, "a"), literal(3, "i")],
          compareEq,
        ),
      ),
    ).toEqual([true, true, null]);
    expect(
      collect(
        batchCallExprValues(
          3,
          "regexp_replace",
          [text, literal(3, "[ae]"), literal(3, "_"), literal(3, "g")],
          compareEq,
        ),
      ),
    ).toEqual([" Alph_ ", "b_t_", null]);
    expect(collect(batchCallExprValues(3, "round", [number, literal(3, 1)], compareEq))).toEqual([
      1.3,
      -2.7,
      null,
    ]);
    expect(collect(batchCallExprValues(3, "floor", [number], compareEq))).toEqual([1, -3, null]);
    expect(collect(batchCallExprValues(3, "ceil", [number], compareEq))).toEqual([2, -2, null]);
    expect(collect(batchCallExprValues(3, "abs", [number], compareEq))).toEqual([1.25, 2.75, null]);
    expect(
      collect(
        batchCallExprValues(
          3,
          "cast",
          [values(["1", "nope", null]), literal(3, "number")],
          compareEq,
        ),
      ),
    ).toEqual([1, null, null]);
    expect(
      collect(batchCallExprValues(3, "coalesce", [values([null, "x", null]), text], compareEq)),
    ).toEqual([" Alpha ", "x", null]);
    expect(
      collect(batchCallExprValues(3, "nullif", [text, values([" Alpha ", "x", null])], compareEq)),
    ).toEqual([null, "beta", null]);
  });

  it("reports type and arity failures from scalar call kernels", () => {
    const compareEq = (left: unknown, right: unknown) => left === right;

    expect(() => batchCallExprValues(1, "lower", [values([1])], compareEq).valueAt(0)).toThrow(
      /expects string/u,
    );
    expect(() =>
      batchCallExprValues(
        1,
        "substr",
        [values(["abc"]), values(["x"]), values([1])],
        compareEq,
      ).valueAt(0),
    ).toThrow(/start and length/u);
    expect(() => batchCallExprValues(1, "round", [], compareEq)).toThrow(/round/u);
    expect(() =>
      batchCallExprValues(1, "cast", [values(["1"]), literal(1, "date")], compareEq).valueAt(0),
    ).toThrow(/Unsupported cast/u);
    expect(() => batchCallExprValues(1, "missing", [], compareEq)).toThrow(
      /does not support call expressions/u,
    );
  });

  it("checks vector call support shapes", () => {
    expect(vectorCallExprSupported(fn("st_bbox", lit(0), lit(1), lit(2), lit(3)))).toBe(true);
    expect(vectorCallExprSupported(fn("st_bbox", lit(0), lit("x"), lit(2), lit(3)))).toBe(false);
    expect(vectorCallExprSupported(fn("h3_in", col("cell"), lit('["a"]')))).toBe(true);
    expect(vectorCallExprSupported(fn("h3_in", lit("cell"), lit('["a"]')))).toBe(false);
    expect(vectorCallExprSupported(fn("h3_within", col("cell"), lit("origin"), lit(1)))).toBe(true);
    expect(vectorCallExprSupported(fn("h3_within", col("cell"), lit("origin"), lit(-1)))).toBe(
      false,
    );
    expect(
      vectorCallExprSupported(
        fn("st_intersects", col("geom"), fn("st_bbox", lit(0), lit(0), lit(1), lit(1))),
      ),
    ).toBe(true);
    expect(vectorCallExprSupported(fn("unknown", col("x")))).toBe(false);
  });

  it("evaluates H3 membership masks over utf8, dictionary, null, and invalid vectors", () => {
    const batch = batchFromColumns({ cell: ["a", "b", null, "c"] });
    const vector = batch.columns.cell;
    if (vector === undefined) throw new Error("missing vector");

    expect(
      mask(
        batchCallPredicateMask("h3_in", [
          { rowCount: 4, vector, valueAt: () => null },
          literal(4, '["a","c"]'),
        ]),
      ),
    ).toEqual([1, 0, 2, 1]);
    expect(
      mask(
        batchCallPredicateMask("h3_in", [
          { rowCount: 4, vector, valueAt: () => null },
          literal(4, null),
        ]),
      ),
    ).toEqual([2, 2, 2, 2]);
    expect(() =>
      batchCallPredicateMask("h3_in", [
        { rowCount: 4, vector, valueAt: () => null },
        literal(4, '{"bad":true}'),
      ]),
    ).toThrow(/JSON string array/u);

    const dictionary = batchFromColumns({ value: ["a", "b", "c"] }).columns.value;
    if (dictionary === undefined) throw new Error("missing dictionary");
    expect(
      mask(
        batchCallPredicateMask("h3_in", [
          {
            rowCount: 4,
            vector: { type: "dict", indices: new Uint32Array([0, 1, 2, 0]), dictionary },
            valueAt: () => null,
          },
          literal(4, '["b"]'),
        ]),
      ),
    ).toEqual([0, 1, 0, 0]);
    expect(batchCallPredicateMask("not_vectorized", [])).toBeUndefined();
  });
});
