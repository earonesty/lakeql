import { describe, expect, it } from "vitest";
import {
  batchExprValues,
  batchFromColumns,
  batchFromVectors,
  materializeBatchRows,
  materializeSelectedBatchRows,
  predicateSelection,
  selectedRowCount,
  tryPredicateSelection,
  vectorValue,
} from "./batch.js";
import { LakeqlError } from "./errors.js";
import { evaluate, loadGeoBackend, matches } from "./evaluator.js";
import {
  add,
  and,
  between,
  col,
  div,
  eq,
  fn,
  gt,
  gte,
  isIn,
  isNotNull,
  isNull,
  like,
  lit,
  lt,
  lte,
  mod,
  mul,
  ne,
  not,
  notIn,
  or,
  sub,
} from "./expr.js";
import { timestampFromEpoch } from "./timestamp.js";

describe("column batches", () => {
  it("infers vector types, validity masks, and materializes rows at the boundary", () => {
    const batch = batchFromColumns({
      id: [1, 2, null],
      name: ["a", null, "c"],
      flag: [true, false, true],
      big: [1n, 2n, 3n],
    });

    expect(batch.rowCount).toBe(3);
    expect(batch.columns.id?.type).toBe("f64");
    expect(batch.columns.name?.type).toBe("utf8");
    expect(batch.columns.flag?.type).toBe("bool");
    expect(batch.columns.big?.type).toBe("i64");
    expect(batch.columns.name?.valid).toBeInstanceOf(Uint8Array);
    const nameVector = batch.columns.name;
    expect(nameVector).toBeDefined();
    if (nameVector === undefined) throw new Error("missing name vector");
    expect(vectorValue(nameVector, 1)).toBeNull();
    expect(materializeBatchRows(batch)).toEqual([
      { id: 1, name: "a", flag: true, big: 1n },
      { id: 2, name: null, flag: false, big: 2n },
      { id: null, name: "c", flag: true, big: 3n },
    ]);
  });

  it("rejects ragged columns and mixed primitive types", () => {
    expect(() => batchFromColumns({ a: [1], b: [1, 2] })).toThrowError(LakeqlError);
    expect(() => batchFromColumns({ a: [1, "two"] })).toThrowError(LakeqlError);
  });

  it("represents nested values as recursive vectors and materializes them at the boundary", () => {
    const batch = batchFromColumns({
      id: [1, 2, 3],
      tags: [["late", "weather"], null, []],
      route: [
        { origin: "JFK", dest: "LAX", legs: [1, 2] },
        { origin: "SFO", dest: "SEA", legs: [1] },
        null,
      ],
      attrs: [
        new Map<string, unknown>([
          ["carrier", "AA"],
          ["status", "active"],
        ]),
        null,
        new Map<string, unknown>([["carrier", "DL"]]),
      ],
    });

    expect(batch.columns.tags?.type).toBe("list");
    expect(batch.columns.route?.type).toBe("struct");
    expect(batch.columns.attrs?.type).toBe("map");
    expect(materializeBatchRows(batch)).toEqual([
      {
        id: 1,
        tags: ["late", "weather"],
        route: { dest: "LAX", legs: [1, 2], origin: "JFK" },
        attrs: { carrier: "AA", status: "active" },
      },
      {
        id: 2,
        tags: null,
        route: { dest: "SEA", legs: [1], origin: "SFO" },
        attrs: null,
      },
      {
        id: 3,
        tags: [],
        route: null,
        attrs: { carrier: "DL" },
      },
    ]);
  });

  it("requires scalar values for vector predicates", () => {
    const batch = batchFromColumns({
      id: [1],
      tags: [["late"]],
    });

    expect(() => predicateSelection(batch, isNotNull("tags"))).not.toThrow();
    expect(() => predicateSelection(batch, eq("tags", "late"))).toThrowError(
      expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
    );
  });

  it("counts selected rows without materializing row objects", () => {
    const batch = batchFromColumns({ id: [1, 2, 3] });
    expect(selectedRowCount(batch.rowCount)).toBe(3);
    expect(selectedRowCount(batch.rowCount, new Uint8Array([1, 0, 1]))).toBe(2);
  });

  it("evaluates dictionary vectors with scalar semantics", () => {
    const batch = batchFromVectors({
      amount: {
        type: "dict",
        indices: new Uint32Array([0, 2, 1, 2]),
        dictionary: batchFromColumns({ value: [5, 10, 20] }).columns.value ?? {
          type: "null",
          length: 0,
        },
      },
    });

    expect(materializeBatchRows(batch)).toEqual([
      { amount: 5 },
      { amount: 20 },
      { amount: 10 },
      { amount: 20 },
    ]);
    expect([...predicateSelection(batch, gt("amount", 10))]).toEqual([0, 1, 0, 1]);
  });

  it("evaluates timestamp vectors with precision-preserving predicate semantics", () => {
    const batch = batchFromColumns({
      id: [1, 2, 3],
      loaded_at: [
        timestampFromEpoch(1_700_000_000_000_001n, "micros"),
        timestampFromEpoch(1_700_000_000_000_999n, "micros"),
        null,
      ],
    });

    expect([...predicateSelection(batch, gt("loaded_at", "2023-11-14T22:13:20.000500Z"))]).toEqual([
      0, 1, 2,
    ]);
    expect(JSON.stringify(materializeBatchRows(batch)[0])).toContain("2023-11-14T22:13:20.000001Z");
  });

  it("evaluates vector predicate selections with SQL null semantics", () => {
    const batch = batchFromColumns({
      id: [1, 2, 3, 4],
      amount: [5, 10, null, 20],
      label: ["a", "b", null, "d"],
      flag: [true, false, null, true],
    });
    const rows = materializeBatchRows(batch);
    const predicates = [
      gt("amount", 9),
      and(gt("amount", 9), isNotNull("label")),
      or(eq("label", "b"), isNull("amount")),
      not(eq("flag", true)),
      between("amount", 5, 10),
      isIn("label", ["a", lit(null)]),
      gt(add(col("amount"), 1), 10),
    ];

    for (const predicate of predicates) {
      const selection = predicateSelection(batch, predicate);
      expect(materializeSelectedBatchRows(batch, selection)).toEqual(
        rows.filter((row) => matches(predicate, row)),
      );
    }
  });

  it("covers scalar vector values and comparisons across supported primitive shapes", () => {
    const dictionary = batchFromColumns({ value: ["a", "b"] }).columns.value;
    if (dictionary === undefined) throw new Error("missing dictionary vector");
    const batch = batchFromVectors({
      f64: { type: "f64", values: new Float64Array([1, 2, 3]), valid: new Uint8Array([1, 0, 1]) },
      i64: { type: "i64", values: new BigInt64Array([1n, 2n, 3n]) },
      bool: { type: "bool", values: new Uint8Array([1, 0, 1]) },
      utf8: { type: "utf8", values: ["a", "b", "c"] },
      dict: { type: "dict", indices: new Uint32Array([0, 1, 0]), dictionary },
      nulls: { type: "null", length: 3 },
      loaded_at: {
        type: "timestamp",
        values: new BigInt64Array([1_700_000_000_000n, 1_700_000_000_500n, 1_700_000_001_000n]),
        unit: "millis",
        isAdjustedToUTC: true,
      },
    });

    expect(vectorValue(batch.columns.f64 ?? { type: "null", length: 0 }, 1)).toBeNull();
    expect(vectorValue(batch.columns.i64 ?? { type: "null", length: 0 }, 2)).toBe(3n);
    expect(vectorValue(batch.columns.bool ?? { type: "null", length: 0 }, 1)).toBe(false);
    expect(vectorValue(batch.columns.utf8 ?? { type: "null", length: 0 }, 2)).toBe("c");
    expect(vectorValue(batch.columns.dict ?? { type: "null", length: 0 }, 1)).toBe("b");
    expect(vectorValue(batch.columns.nulls ?? { type: "null", length: 0 }, 0)).toBeNull();
    expect(() => vectorValue(batch.columns.f64 ?? { type: "null", length: 0 }, -1)).toThrowError(
      LakeqlError,
    );

    const predicates = [
      [eq("f64", 3), [2]],
      [ne("utf8", "b"), [0, 2]],
      [lt("i64", 3), [0, 1]],
      [lte("i64", 2n), [0, 1]],
      [gte("i64", 2), [1, 2]],
      [gt(lit(2), col("f64")), [0]],
      [eq("dict", "a"), [0, 2]],
      [notIn("utf8", ["a", lit(null)]), []],
      [gt("loaded_at", "2023-11-14T22:13:20.500Z"), [2]],
    ] as const;

    for (const [predicate, expectedRows] of predicates) {
      expect(materializeSelectedBatchRows(batch, predicateSelection(batch, predicate))).toEqual(
        expectedRows.map((index) => materializeBatchRows(batch)[index]),
      );
    }
  });

  it("evaluates every arithmetic vector expression operator and preserves null semantics", () => {
    const batch = batchFromColumns({ left: [10, null, 7], right: [3, 2, 4] });
    const rows = materializeBatchRows(batch);
    const expressions = [
      add(col("left"), col("right")),
      sub(col("left"), col("right")),
      mul(col("left"), col("right")),
      div(col("left"), col("right")),
      mod(col("left"), col("right")),
    ];

    for (const expr of expressions) {
      const values = batchExprValues(batch, expr);
      expect(rows.map((_row, index) => values.valueAt(index))).toEqual(
        rows.map((row) => evaluate(expr, row)),
      );
    }
    expect(() =>
      batchExprValues(batchFromColumns({ value: ["x"] }), add(col("value"), 1)).valueAt(0),
    ).toThrowError(LakeqlError);
  });

  it("evaluates searched CASE expressions with SQL null predicate semantics", () => {
    const batch = batchFromColumns({
      amount: [5, null, 15, 30],
      label: ["small", "missing", "medium", "large"],
    });
    const expr = {
      kind: "case",
      whens: [
        { when: gt("amount", 20), value: col("label") },
        { when: gt("amount", 10), value: lit("mid") },
      ],
      else: lit("low"),
    } as const;
    const values = batchExprValues(batch, expr);
    const rows = materializeBatchRows(batch);

    expect(rows.map((_row, index) => values.valueAt(index))).toEqual(
      rows.map((row) => {
        if (matches(gt("amount", 20), row)) return row.label;
        if (matches(gt("amount", 10), row)) return "mid";
        return "low";
      }),
    );
  });

  it("evaluates vector coalesce and nullif calls with row evaluator semantics", () => {
    const batch = batchFromColumns({
      amount: [5, null, 15, 30],
      fallback: [10, 20, null, 40],
      label: ["small", "skip", null, "large"],
    });
    const rows = materializeBatchRows(batch);
    const expressions = [
      fn("coalesce", col("amount"), col("fallback"), lit(0)),
      fn("coalesce", lit(null), col("label"), lit("missing")),
      fn("nullif", col("label"), lit("skip")),
      fn("nullif", col("amount"), col("fallback")),
      fn("nullif", lit(null), lit("x")),
      fn("lower", col("label")),
      fn("upper", col("label")),
      fn("trim", fn("coalesce", col("label"), lit(" missing "))),
      fn("substr", col("label"), lit(1), lit(3)),
      fn("substr", col("label"), lit(null), lit(3)),
      fn("replace", col("label"), lit("s"), lit("S")),
      fn("replace", col("label"), lit(null), lit("S")),
      fn("regexp_matches", col("label"), lit("a")),
      fn("regexp_matches", col("label"), lit("^S"), lit("i")),
      fn("regexp_replace", col("label"), lit("[ae]"), lit("X")),
      fn("regexp_replace", col("label"), lit("[ae]"), lit("X"), lit("g")),
      fn("regexp_replace", col("label"), lit("(s)(m)"), lit("\\2\\1")),
      fn("cast", col("amount"), lit("string")),
      fn("cast", col("label"), lit("number")),
      fn("cast", col("amount"), lit("boolean")),
      fn("cast", lit(null), lit("number")),
      fn("round", col("amount")),
      fn("round", col("amount"), lit(1)),
      fn("round", col("amount"), lit(null)),
      fn("floor", col("amount")),
      fn("ceil", col("amount")),
      fn("abs", fn("nullif", col("amount"), lit(15))),
    ];

    for (const expr of expressions) {
      const values = batchExprValues(batch, expr);
      expect(rows.map((_row, index) => values.valueAt(index))).toEqual(
        rows.map((row) => evaluate(expr, row)),
      );
    }
  });

  it("returns undefined for unsupported vector predicates", () => {
    const batch = batchFromColumns({ name: ["alpha", "beta"] });
    expect(tryPredicateSelection(batch, like("name", "a%"))).toBeUndefined();
  });

  it("evaluates vector regexp_matches predicates", () => {
    const batch = batchFromColumns({ label: ["alpha", "Beta", "gamma", null] });
    const predicate = fn("regexp_matches", col("label"), lit("^a|ta$"), lit("i"));
    const rows = materializeBatchRows(batch);
    const selection = predicateSelection(batch, predicate);

    expect(materializeSelectedBatchRows(batch, selection)).toEqual(
      rows.filter((row) => matches(predicate, row)),
    );
  });

  it("evaluates geo and h3 predicate kernels over large string and dictionary vectors", async () => {
    await loadGeoBackend();
    const rows = 20_000;
    const downtown = JSON.stringify({ type: "Point", coordinates: [-118.25, 34.05] });
    const valley = JSON.stringify({ type: "Point", coordinates: [-118.45, 34.2] });
    const outside = JSON.stringify({ type: "Point", coordinates: [-119.1, 35.1] });
    const downtownWkt = "POINT(-118.25 34.05)";
    const losAngelesBox = JSON.stringify({
      type: "Polygon",
      coordinates: [
        [
          [-118.5, 34],
          [-118, 34],
          [-118, 34.3],
          [-118.5, 34.3],
          [-118.5, 34],
        ],
      ],
    });
    const h3A = "8829a1d757fffff";
    const h3B = "8829a1d74bfffff";
    const h3C = "8829a1d753fffff";
    const geomValues = new Array<string>(rows);
    const h3Values = new Array<string>(rows);
    for (let index = 0; index < rows; index += 1) {
      geomValues[index] = index % 3 === 0 ? downtown : index % 3 === 1 ? valley : outside;
      h3Values[index] = index % 3 === 0 ? h3A : index % 3 === 1 ? h3B : h3C;
    }
    const batch = batchFromColumns({ geom: geomValues, h3_8: h3Values });
    const wktBatch = batchFromColumns({ geom: [downtownWkt, "POINT(-119.1 35.1)"] });

    const intersects = predicateSelection(
      batch,
      fn("st_intersects", col("geom"), fn("st_bbox", lit(-118.5), lit(34), lit(-118), lit(34.3))),
    );
    const intersectsConstantGeometry = predicateSelection(
      batch,
      fn("st_intersects", lit(losAngelesBox), col("geom")),
    );
    const h3In = predicateSelection(
      batch,
      fn("h3_in", col("h3_8"), lit(JSON.stringify([h3A, h3B]))),
    );
    const h3Within = predicateSelection(batch, fn("h3_within", col("h3_8"), lit(h3A), lit(0)));
    const composed = predicateSelection(
      batch,
      or(
        fn("h3_within", col("h3_8"), lit(h3A), lit(0)),
        not(
          fn(
            "st_intersects",
            col("geom"),
            fn("st_bbox", lit(-118.5), lit(34), lit(-118), lit(34.3)),
          ),
        ),
      ),
    );
    const nullConstant = predicateSelection(batch, fn("h3_in", col("h3_8"), lit(null)));

    for (let index = 0; index < rows; index += 1) {
      expect(intersects[index]).toBe(index % 3 === 2 ? 0 : 1);
      expect(intersectsConstantGeometry[index]).toBe(index % 3 === 2 ? 0 : 1);
      expect(h3In[index]).toBe(index % 3 === 2 ? 0 : 1);
      expect(h3Within[index]).toBe(index % 3 === 0 ? 1 : 0);
      expect(composed[index]).toBe(index % 3 === 1 ? 0 : 1);
      expect(nullConstant[index]).toBe(2);
    }

    const dictionaryBatch = batchFromVectors({
      geom: {
        type: "dict",
        indices: Uint32Array.from([0, 1, 2, 0, 2, 1]),
        dictionary: batchFromColumns({ value: [downtown, valley, outside] }).columns.value ?? {
          type: "null",
          length: 0,
        },
      },
      h3_8: {
        type: "dict",
        indices: Uint32Array.from([0, 1, 2, 0, 2, 1]),
        dictionary: batchFromColumns({ value: [h3A, h3B, h3C] }).columns.value ?? {
          type: "null",
          length: 0,
        },
      },
    });

    expect([
      ...predicateSelection(
        dictionaryBatch,
        fn("st_intersects", col("geom"), fn("st_bbox", lit(-118.5), lit(34), lit(-118), lit(34.3))),
      ),
    ]).toEqual([1, 1, 0, 1, 0, 1]);
    expect([
      ...predicateSelection(dictionaryBatch, fn("h3_in", col("h3_8"), lit(JSON.stringify([h3A])))),
    ]).toEqual([1, 0, 0, 1, 0, 0]);
    expect([
      ...predicateSelection(dictionaryBatch, fn("h3_within", col("h3_8"), lit(h3B), lit(0))),
    ]).toEqual([0, 1, 0, 0, 0, 1]);
    expect([
      ...predicateSelection(
        wktBatch,
        fn("st_intersects", col("geom"), fn("st_bbox", lit(-118.5), lit(34), lit(-118), lit(34.3))),
      ),
    ]).toEqual([1, 0]);
  });
});
