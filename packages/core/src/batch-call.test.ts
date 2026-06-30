import { describe, expect, it } from "vitest";
import type { BatchExprValues } from "./batch.js";
import { batchFromColumns } from "./batch.js";
import {
  batchCallExprValues,
  batchCallPredicateMask,
  vectorCallExprSupported,
} from "./batch-call.js";
import { loadGeoBackend } from "./evaluator.js";
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
      collect(
        batchCallExprValues(3, "cast", [values([1, false, null]), literal(3, "string")], compareEq),
      ),
    ).toEqual(["1", "false", null]);
    expect(
      collect(
        batchCallExprValues(3, "cast", [values([0, "x", null]), literal(3, "boolean")], compareEq),
      ),
    ).toEqual([false, true, null]);
    expect(
      collect(
        batchCallExprValues(
          3,
          "st_bbox",
          [literal(3, -1), literal(3, -2), literal(3, 1), literal(3, 2)],
          compareEq,
        ),
      ),
    ).toEqual([
      '{"type":"BBox","minx":-1,"miny":-2,"maxx":1,"maxy":2}',
      '{"type":"BBox","minx":-1,"miny":-2,"maxx":1,"maxy":2}',
      '{"type":"BBox","minx":-1,"miny":-2,"maxx":1,"maxy":2}',
    ]);
    expect(
      collect(
        batchCallExprValues(3, "st_point", [literal(3, -118.24), literal(3, 34.05)], compareEq),
      ),
    ).toEqual([
      '{"type":"Point","coordinates":[-118.24,34.05]}',
      '{"type":"Point","coordinates":[-118.24,34.05]}',
      '{"type":"Point","coordinates":[-118.24,34.05]}',
    ]);
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
    expect(() =>
      batchCallExprValues(1, "cast", [values(["1"]), literal(1, 1)], compareEq).valueAt(0),
    ).toThrow(/string type name/u);
    expect(() => batchCallExprValues(1, "regexp_matches", [values(["a"])], compareEq)).toThrow(
      /2 or 3/u,
    );
    expect(() => batchCallExprValues(1, "regexp_replace", [values(["a"])], compareEq)).toThrow(
      /3 or 4/u,
    );
    expect(() =>
      batchCallExprValues(1, "regexp_matches", [values(["a"]), values([1])], compareEq).valueAt(0),
    ).toThrow(/must be strings/u);
    expect(() =>
      batchCallExprValues(
        1,
        "regexp_replace",
        [values(["a"]), values(["a"]), values([1])],
        compareEq,
      ).valueAt(0),
    ).toThrow(/must be strings/u);
    expect(() => batchCallExprValues(1, "floor", [values(["1"])], compareEq).valueAt(0)).toThrow(
      /expects number/u,
    );
    expect(() =>
      batchCallExprValues(1, "round", [values([1]), values(["bad"])], compareEq).valueAt(0),
    ).toThrow(/must be numbers/u);
    expect(() =>
      batchCallExprValues(
        1,
        "st_bbox",
        [literal(1, "x"), literal(1, 0), literal(1, 1), literal(1, 3)],
        compareEq,
      ),
    ).toThrow(/literal bounds/u);
    expect(() =>
      batchCallExprValues(
        1,
        "st_bbox",
        [literal(1, 2), literal(1, 0), literal(1, 1), literal(1, 3)],
        compareEq,
      ),
    ).toThrow(/bounds/u);
    expect(() =>
      batchCallExprValues(
        1,
        "st_bbox",
        [literal(1, Number.POSITIVE_INFINITY), literal(1, 0), literal(1, 1), literal(1, 3)],
        compareEq,
      ),
    ).toThrow(/finite/u);
    expect(() =>
      batchCallExprValues(1, "st_point", [literal(1, "x"), literal(1, 0)], compareEq),
    ).toThrow(/literal lon\/lat/u);
    expect(() =>
      batchCallExprValues(1, "st_point", [literal(1, Number.NaN), literal(1, 0)], compareEq),
    ).toThrow(/finite/u);
    expect(() => batchCallExprValues(1, "missing", [], compareEq)).toThrow(
      /does not support call expressions/u,
    );
  });

  it("checks vector call support shapes", () => {
    for (const name of [
      "lower",
      "upper",
      "trim",
      "substr",
      "replace",
      "regexp_matches",
      "regexp_replace",
      "cast",
      "round",
      "floor",
      "ceil",
      "abs",
      "coalesce",
      "nullif",
    ]) {
      expect(vectorCallExprSupported(fn(name, col("value")))).toBe(true);
    }
    expect(vectorCallExprSupported(fn("st_bbox", lit(0), lit(1), lit(2), lit(3)))).toBe(true);
    expect(vectorCallExprSupported(fn("st_bbox", lit(0), lit("x"), lit(2), lit(3)))).toBe(false);
    expect(vectorCallExprSupported(fn("st_point", lit(0), lit(1)))).toBe(true);
    expect(vectorCallExprSupported(fn("st_point", lit(0), lit("x")))).toBe(false);
    expect(vectorCallExprSupported(fn("h3_in", col("cell"), lit('["a"]')))).toBe(true);
    expect(vectorCallExprSupported(fn("h3_in", col("cell")))).toBe(false);
    expect(vectorCallExprSupported(fn("h3_in", lit("cell"), lit('["a"]')))).toBe(false);
    expect(vectorCallExprSupported(fn("h3_within", col("cell"), lit("origin"), lit(1)))).toBe(true);
    expect(vectorCallExprSupported(fn("h3_within", col("cell"), lit("origin")))).toBe(false);
    expect(vectorCallExprSupported(fn("h3_within", col("cell"), lit("origin"), lit(-1)))).toBe(
      false,
    );
    expect(
      vectorCallExprSupported(
        fn("st_intersects", col("geom"), fn("st_bbox", lit(0), lit(0), lit(1), lit(1))),
      ),
    ).toBe(true);
    expect(
      vectorCallExprSupported(fn("st_intersects", fn("st_point", lit(0), lit(0)), col("geom"))),
    ).toBe(true);
    expect(vectorCallExprSupported(fn("st_intersects", col("geom")))).toBe(false);
    expect(vectorCallExprSupported(fn("st_intersects", col("geom"), col("other")))).toBe(false);
    expect(
      vectorCallExprSupported(
        fn("st_dwithin", col("geom"), fn("st_point", lit(0), lit(0)), lit(1)),
      ),
    ).toBe(true);
    expect(
      vectorCallExprSupported(
        fn("st_dwithin", fn("st_point", lit(0), lit(0)), col("geom"), lit(1)),
      ),
    ).toBe(true);
    expect(
      vectorCallExprSupported(
        fn("st_dwithin", col("geom"), fn("st_point", lit(0), lit(0)), lit(-1)),
      ),
    ).toBe(false);
    expect(
      vectorCallExprSupported(
        fn(
          "st_dwithin",
          col("geom"),
          fn("st_point", lit(0), lit(0)),
          lit(Number.POSITIVE_INFINITY),
        ),
      ),
    ).toBe(false);
    expect(
      vectorCallExprSupported(fn("st_contains", col("geom"), fn("st_point", lit(0), lit(0)))),
    ).toBe(true);
    expect(
      vectorCallExprSupported(fn("st_contains", fn("st_point", lit(0), lit(0)), col("geom"))),
    ).toBe(true);
    expect(vectorCallExprSupported(fn("st_contains", col("geom"), lit(wkbPoint(0, 0))))).toBe(true);
    expect(
      vectorCallExprSupported(
        fn("st_within", col("geom"), fn("st_bbox", lit(0), lit(0), lit(1), lit(1))),
      ),
    ).toBe(true);
    expect(vectorCallExprSupported(fn("st_within", col("geom")))).toBe(false);
    expect(vectorCallExprSupported(fn("st_within", col("geom"), col("other")))).toBe(false);
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

  it("evaluates vectorized H3 radius and spatial intersection masks", async () => {
    await loadGeoBackend();
    const h3 = batchFromColumns({
      cell: ["8829a1d757fffff", "8829a1d74bfffff", null],
    }).columns.cell;
    if (h3 === undefined) throw new Error("missing h3 vector");

    expect(
      mask(
        batchCallPredicateMask("h3_within", [
          { rowCount: 3, vector: h3, valueAt: () => null },
          literal(3, "8829a1d757fffff"),
          literal(3, 0),
        ]),
      ),
    ).toEqual([1, 0, 2]);
    expect(
      mask(
        batchCallPredicateMask("h3_within", [
          { rowCount: 3, vector: h3, valueAt: () => null },
          literal(3, null),
          literal(3, 0),
        ]),
      ),
    ).toEqual([2, 2, 2]);
    expect(() =>
      batchCallPredicateMask("h3_within", [
        { rowCount: 3, vector: h3, valueAt: () => null },
        literal(3, "not-a-cell"),
        literal(3, 0),
      ]),
    ).toThrow(/origin is invalid/u);

    const geom = batchFromColumns({
      geom: [
        JSON.stringify({ type: "Point", coordinates: [0, 0] }),
        JSON.stringify({ type: "Point", coordinates: [10, 10] }),
        null,
      ],
    }).columns.geom;
    if (geom === undefined) throw new Error("missing geometry vector");
    const bbox = batchCallExprValues(
      3,
      "st_bbox",
      [literal(3, -1), literal(3, -1), literal(3, 1), literal(3, 1)],
      (left, right) => left === right,
    );

    expect(
      mask(
        batchCallPredicateMask("st_intersects", [
          { rowCount: 3, vector: geom, valueAt: () => null },
          bbox,
        ]),
      ),
    ).toEqual([1, 0, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_intersects", [
          bbox,
          { rowCount: 3, vector: geom, valueAt: () => null },
        ]),
      ),
    ).toEqual([1, 0, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_intersects", [
          { rowCount: 3, vector: geom, valueAt: () => null },
          literal(3, null),
        ]),
      ),
    ).toEqual([2, 2, 2]);

    const point = literal(3, JSON.stringify({ type: "Point", coordinates: [0, 0] }));
    expect(
      mask(
        batchCallPredicateMask("st_dwithin", [
          { rowCount: 3, vector: geom, valueAt: () => null },
          point,
          literal(3, 1),
        ]),
      ),
    ).toEqual([1, 0, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_dwithin", [
          point,
          { rowCount: 3, vector: geom, valueAt: () => null },
          literal(3, 1),
        ]),
      ),
    ).toEqual([1, 0, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_dwithin", [
          { rowCount: 3, vector: geom, valueAt: () => null },
          point,
          literal(3, null),
        ]),
      ),
    ).toEqual([2, 2, 2]);

    const polygon = literal(
      3,
      JSON.stringify({
        type: "Polygon",
        coordinates: [
          [
            [-1, -1],
            [1, -1],
            [1, 1],
            [-1, 1],
            [-1, -1],
          ],
        ],
      }),
    );
    const binary = batchFromColumns({
      geom: [wkbPoint(0, 0), wkbPoint(10, 10), null],
    }).columns.geom;
    if (binary === undefined) throw new Error("missing binary geometry vector");
    expect(
      mask(
        batchCallPredicateMask("st_within", [
          { rowCount: 3, vector: binary, valueAt: () => null },
          polygon,
        ]),
      ),
    ).toEqual([1, 0, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_contains", [
          polygon,
          { rowCount: 3, vector: binary, valueAt: () => null },
        ]),
      ),
    ).toEqual([1, 0, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_contains", [
          { rowCount: 3, vector: binary, valueAt: () => null },
          literal(3, wkbPoint(0, 0)),
        ]),
      ),
    ).toEqual([1, 0, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_within", [
          polygon,
          { rowCount: 3, vector: binary, valueAt: () => null },
        ]),
      ),
    ).toEqual([0, 0, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_within", [
          { rowCount: 3, vector: { type: "null", length: 3 }, valueAt: () => null },
          polygon,
        ]),
      ),
    ).toEqual([2, 2, 2]);

    const binaryDictionary = {
      type: "dict" as const,
      indices: new Uint32Array([0, 1, 0]),
      dictionary: binary,
      valid: new Uint8Array([1, 0, 1]),
    };
    expect(
      mask(
        batchCallPredicateMask("st_within", [
          { rowCount: 3, vector: binaryDictionary, valueAt: () => null },
          polygon,
        ]),
      ),
    ).toEqual([1, 2, 1]);
    expect(() =>
      batchCallPredicateMask("st_contains", [
        { rowCount: 3, vector: numericVector(), valueAt: () => null },
        polygon,
      ]),
    ).toThrow(/geometry values/u);
    expect(() =>
      batchCallPredicateMask("h3_in", [
        { rowCount: 3, vector: numericVector(), valueAt: () => null },
        literal(3, "[]"),
      ]),
    ).toThrow(/requires strings/u);
    expect(
      batchCallPredicateMask("h3_within", [{ rowCount: 3, vector: h3, valueAt: () => null }]),
    ).toBeUndefined();
    expect(() =>
      batchCallPredicateMask("h3_within", [
        { rowCount: 3, vector: h3, valueAt: () => null },
        literal(3, "8829a1d757fffff"),
        literal(3, -1),
      ]),
    ).toThrow(/non-negative integer/u);
    expect(
      batchCallPredicateMask("st_intersects", [
        { rowCount: 3, vector: geom, valueAt: () => null },
        values([1]),
      ]),
    ).toBeUndefined();
    expect(
      batchCallPredicateMask("st_dwithin", [
        { rowCount: 3, vector: geom, valueAt: () => null },
        point,
        literal(3, "x"),
      ]),
    ).toBeUndefined();
    expect(() =>
      batchCallPredicateMask("st_dwithin", [
        { rowCount: 3, vector: geom, valueAt: () => null },
        point,
        literal(3, -1),
      ]),
    ).toThrow(/non-negative distance/u);
    expect(
      mask(
        batchCallPredicateMask("st_intersects", [
          literal(3, null),
          { rowCount: 3, vector: geom, valueAt: () => null },
        ]),
      ),
    ).toEqual([2, 2, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_contains", [
          { rowCount: 3, vector: binary, valueAt: () => null },
          literal(3, null),
        ]),
      ),
    ).toEqual([2, 2, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_contains", [
          literal(3, null),
          { rowCount: 3, vector: binary, valueAt: () => null },
        ]),
      ),
    ).toEqual([2, 2, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_within", [
          literal(3, null),
          { rowCount: 3, vector: binary, valueAt: () => null },
        ]),
      ),
    ).toEqual([2, 2, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_dwithin", [
          literal(3, null),
          { rowCount: 3, vector: binary, valueAt: () => null },
          literal(3, 1),
        ]),
      ),
    ).toEqual([2, 2, 2]);
    expect(
      mask(
        batchCallPredicateMask("st_within", [
          {
            rowCount: 3,
            vector: {
              type: "binary",
              values: [wkbPoint(0, 0), wkbPoint(10, 10), wkbPoint(0, 0)],
            },
            valueAt: () => null,
          },
          polygon,
        ]),
      ),
    ).toEqual([1, 0, 1]);
    expect(
      mask(
        batchCallPredicateMask("st_within", [
          {
            rowCount: 3,
            vector: {
              type: "dict",
              indices: new Uint32Array([0, 5, 1]),
              dictionary: binary,
            },
            valueAt: () => null,
          },
          polygon,
        ]),
      ),
    ).toEqual([1, 2, 0]);
    const utf8Dictionary = {
      type: "dict" as const,
      indices: new Uint32Array([0, 1, 0]),
      dictionary: geom,
      valid: new Uint8Array([1, 0, 1]),
    };
    expect(
      mask(batchCallPredicateMask("st_intersects", [utf8DictionaryMask(utf8Dictionary), bbox])),
    ).toEqual([1, 2, 1]);
    expect(() =>
      batchCallPredicateMask("st_within", [
        {
          rowCount: 3,
          vector: {
            type: "dict",
            indices: new Uint32Array([0, 1, 0]),
            dictionary: numericVector(),
          },
          valueAt: () => null,
        },
        polygon,
      ]),
    ).toThrow(/geometry values/u);
  });
});

function wkbPoint(x: number, y: number): Uint8Array {
  const bytes = new Uint8Array(1 + 4 + 16);
  const view = new DataView(bytes.buffer);
  view.setUint8(0, 1);
  view.setUint32(1, 1, true);
  view.setFloat64(5, x, true);
  view.setFloat64(13, y, true);
  return bytes;
}

function numericVector() {
  const vector = batchFromColumns({ n: [1, 2, 3] }).columns.n;
  if (vector === undefined) throw new Error("missing numeric vector");
  return vector;
}

function utf8DictionaryMask(vector: NonNullable<BatchExprValues["vector"]>): BatchExprValues {
  return { rowCount: 3, vector, valueAt: () => null };
}
