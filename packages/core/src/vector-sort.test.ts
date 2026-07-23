import { describe, expect, it } from "vitest";
import {
  batchFromColumns,
  batchFromVectors,
  materializeBatchRows,
  predicateSelection,
} from "./batch.js";
import { gte } from "./expr.js";
import { timestampFromEpoch } from "./timestamp.js";
import {
  concatBatches,
  gatherBatch,
  vectorOrderByBatch,
  vectorSortIndices,
  vectorTopKBatch,
  vectorTopKIndices,
} from "./vector-sort.js";

describe("vector sort kernels", () => {
  it("gathers and concatenates type-preserving numeric vectors", () => {
    const left = batchFromVectors({
      f32: { type: "f32", values: Float32Array.of(1.5, 2.5) },
      i32: { type: "i32", values: Int32Array.of(-2, 3) },
      u32: { type: "u32", values: Uint32Array.of(1, 4_000_000_000) },
      u8: { type: "u8", values: Uint8Array.of(7, 255) },
    });
    const gathered = gatherBatch(left, [1, 0]);
    expect(gathered.columns.f32).toMatchObject({ type: "f32", values: Float32Array.of(2.5, 1.5) });
    expect(gathered.columns.i32).toMatchObject({ type: "i32", values: Int32Array.of(3, -2) });
    expect(gathered.columns.u32).toMatchObject({
      type: "u32",
      values: Uint32Array.of(4_000_000_000, 1),
    });
    expect(gathered.columns.u8).toMatchObject({ type: "u8", values: Uint8Array.of(255, 7) });

    const concatenated = concatBatches([left, gathered]);
    expect(concatenated.columns.f32?.type).toBe("f32");
    expect(concatenated.columns.i32?.type).toBe("i32");
    expect(concatenated.columns.u32?.type).toBe("u32");
    expect(concatenated.columns.u8?.type).toBe("u8");
    expect(materializeBatchRows(concatenated)).toHaveLength(4);

    const mixedTimestampUnits = concatBatches([
      batchFromVectors({
        at: {
          type: "timestamp",
          values: BigInt64Array.of(1_000n),
          unit: "millis",
          isAdjustedToUTC: true,
        },
      }),
      batchFromVectors({
        at: {
          type: "timestamp",
          values: BigInt64Array.of(1_000_000n),
          unit: "micros",
          isAdjustedToUTC: true,
        },
      }),
    ]);
    expect(materializeBatchRows(mixedTimestampUnits)).toEqual([
      { at: timestampFromEpoch(1_000n, "millis", true) },
      { at: timestampFromEpoch(1_000n, "millis", true) },
    ]);
  });

  it("orders column batches with row-sort null defaults and stable ties", () => {
    const batch = batchFromColumns({
      id: [1, 2, 3, 4, 5],
      amount: [20, null, 10, 20, null],
      label: ["b", "n1", "a", "a", "n2"],
    });

    expect(
      materializeBatchRows(vectorOrderByBatch(batch, [{ column: "amount", direction: "asc" }])),
    ).toEqual([
      { id: 3, amount: 10, label: "a" },
      { id: 1, amount: 20, label: "b" },
      { id: 4, amount: 20, label: "a" },
      { id: 2, amount: null, label: "n1" },
      { id: 5, amount: null, label: "n2" },
    ]);

    expect(
      materializeBatchRows(vectorOrderByBatch(batch, [{ column: "amount", direction: "desc" }])),
    ).toEqual([
      { id: 2, amount: null, label: "n1" },
      { id: 5, amount: null, label: "n2" },
      { id: 1, amount: 20, label: "b" },
      { id: 4, amount: 20, label: "a" },
      { id: 3, amount: 10, label: "a" },
    ]);
  });

  it("supports explicit null ordering, secondary keys, and selected rows", () => {
    const batch = batchFromColumns({
      id: [1, 2, 3, 4, 5],
      amount: [20, null, 10, 20, null],
      label: ["b", "n1", "a", "a", "n2"],
    });
    const selection = predicateSelection(batch, gte("id", 2));

    expect(
      materializeBatchRows(
        vectorOrderByBatch(
          batch,
          [
            { column: "amount", direction: "asc", nulls: "first" },
            { column: "label", direction: "desc" },
          ],
          selection,
        ),
      ),
    ).toEqual([
      { id: 5, amount: null, label: "n2" },
      { id: 2, amount: null, label: "n1" },
      { id: 3, amount: 10, label: "a" },
      { id: 4, amount: 20, label: "a" },
    ]);
  });

  it("keeps top-k bounded by offset plus limit before final ordering", () => {
    const batch = batchFromColumns({
      id: [1, 2, 3, 4, 5, 6],
      score: [30, 10, 50, 20, 40, 60],
    });

    expect(vectorTopKIndices(batch, [{ column: "score", direction: "desc" }], 3)).toHaveLength(3);
    expect(
      materializeBatchRows(
        vectorTopKBatch(batch, [{ column: "score", direction: "desc" }], {
          offset: 1,
          limit: 2,
        }),
      ),
    ).toEqual([
      { id: 3, score: 50 },
      { id: 5, score: 40 },
    ]);
    expect(vectorTopKIndices(batch, [{ column: "score" }], 0)).toEqual([]);
    expect(
      materializeBatchRows(vectorTopKBatch(batch, [{ column: "score" }], { limit: 0 })),
    ).toEqual([]);
  });

  it("orders timestamp vectors by epoch precision", () => {
    const batch = batchFromColumns({
      id: [1, 2, 3],
      loaded_at: [
        timestampFromEpoch(1_700_000_000_000_999n, "micros"),
        timestampFromEpoch(1_700_000_000_000_001n, "micros"),
        timestampFromEpoch(1_700_000_000_000_500n, "micros"),
      ],
    });

    expect(
      materializeBatchRows(
        vectorOrderByBatch(batch, [{ column: "loaded_at", direction: "asc" }]),
      ).map((row) => row.id),
    ).toEqual([2, 3, 1]);
    expect(
      materializeBatchRows(
        vectorTopKBatch(batch, [{ column: "loaded_at", direction: "desc" }], { limit: 1 }),
      ).map((row) => row.id),
    ).toEqual([1]);
  });

  it("gathers typed columns and validity masks without changing vector layout", () => {
    const first = new Uint8Array([1]);
    const batch = batchFromColumns({
      id: [1, 2, 3],
      big: [10n, 20n, 30n],
      flag: [true, null, false],
      label: ["a", null, "c"],
      payload: [first, null, new Uint8Array([3])],
    });
    const gathered = gatherBatch(batch, [2, 1]);

    expect(gathered.columns.id?.type).toBe("f64");
    expect(gathered.columns.big?.type).toBe("i64");
    expect(gathered.columns.flag?.type).toBe("bool");
    expect(gathered.columns.label?.type).toBe("utf8");
    expect(gathered.columns.payload?.type).toBe("binary");
    expect(materializeBatchRows(gathered)).toEqual([
      { id: 3, big: 30n, flag: false, label: "c", payload: new Uint8Array([3]) },
      { id: 2, big: 20n, flag: null, label: null, payload: null },
    ]);
  });

  it("gathers and orders dictionary vectors without expanding them", () => {
    const dictionary = batchFromColumns({ value: ["low", "mid", "high"] }).columns.value;
    if (dictionary === undefined) throw new Error("missing dictionary");
    const batch = batchFromVectors({
      label: {
        type: "dict",
        indices: new Uint32Array([1, 2, 0]),
        dictionary,
      },
      score: batchFromColumns({ score: [2, 3, 1] }).columns.score ?? { type: "null", length: 0 },
    });

    const ordered = vectorOrderByBatch(batch, [{ column: "score", direction: "desc" }]);
    expect(ordered.columns.label?.type).toBe("dict");
    expect(materializeBatchRows(ordered)).toEqual([
      { label: "high", score: 3 },
      { label: "mid", score: 2 },
      { label: "low", score: 1 },
    ]);
  });

  it("preserves nested payload vectors while ordering by scalar columns", () => {
    const batch = batchFromColumns({
      score: [2, 9, 4],
      events: [[{ code: "b" }], [{ code: "winner" }], []],
      metadata: [{ carrier: "AA" }, { carrier: "DL" }, null],
    });

    expect(
      materializeBatchRows(
        vectorTopKBatch(batch, [{ column: "score", direction: "desc" }], { limit: 2 }),
      ),
    ).toEqual([
      { score: 9, events: [{ code: "winner" }], metadata: { carrier: "DL" } },
      { score: 4, events: [], metadata: null },
    ]);
  });

  it("concatenates compatible typed batches with validity masks", () => {
    const left = batchFromColumns({
      id: [1, 2],
      flag: [true, null],
      label: ["a", null],
      payload: [new Uint8Array([1]), null],
    });
    const right = batchFromColumns({
      id: [3],
      flag: [false],
      label: ["c"],
      payload: [new Uint8Array([3])],
    });

    const combined = concatBatches([left, right]);

    expect(combined.columns.id?.type).toBe("f64");
    expect(combined.columns.flag?.type).toBe("bool");
    expect(combined.columns.label?.type).toBe("utf8");
    expect(combined.columns.payload?.type).toBe("binary");
    expect(materializeBatchRows(combined)).toEqual([
      { id: 1, flag: true, label: "a", payload: new Uint8Array([1]) },
      { id: 2, flag: null, label: null, payload: null },
      { id: 3, flag: false, label: "c", payload: new Uint8Array([3]) },
    ]);
    expect(() => vectorOrderByBatch(combined, [{ column: "payload" }])).toThrowError(
      /orderBy values must be scalar/u,
    );
  });

  it("concatenates null, timestamp, dictionary, and nested vectors with stable materialization", () => {
    const dictionaryA = batchFromColumns({ value: ["a", "b"] }).columns.value;
    const dictionaryB = batchFromColumns({ value: ["b", "c"] }).columns.value;
    if (dictionaryA === undefined || dictionaryB === undefined)
      throw new Error("missing dictionary");
    const left = batchFromVectors({
      empty: { type: "null", length: 2 },
      loaded_at: {
        type: "timestamp",
        values: new BigInt64Array([1_700_000_000_001n, 1_700_000_000_002n]),
        unit: "millis",
        isAdjustedToUTC: true,
      },
      label: { type: "dict", indices: new Uint32Array([0, 1]), dictionary: dictionaryA },
      tags: batchFromColumns({ tags: [["a"], null] }).columns.tags ?? { type: "null", length: 2 },
      attrs: batchFromColumns({ attrs: [{ id: 1 }, null] }).columns.attrs ?? {
        type: "null",
        length: 2,
      },
      lookup: batchFromColumns({ lookup: [new Map([["k", "v"]]), null] }).columns.lookup ?? {
        type: "null",
        length: 2,
      },
    });
    const right = batchFromVectors({
      empty: { type: "null", length: 1 },
      loaded_at: {
        type: "timestamp",
        values: new BigInt64Array([1_700_000_000_003_000n]),
        unit: "micros",
        isAdjustedToUTC: true,
      },
      label: { type: "dict", indices: new Uint32Array([1]), dictionary: dictionaryB },
      tags: batchFromColumns({ tags: [["b", "c"]] }).columns.tags ?? { type: "null", length: 1 },
      attrs: batchFromColumns({ attrs: [{ id: 2 }] }).columns.attrs ?? { type: "null", length: 1 },
      lookup: batchFromColumns({ lookup: [new Map([["k", "w"]])] }).columns.lookup ?? {
        type: "null",
        length: 1,
      },
    });

    const combined = concatBatches([left, right]);

    expect(combined.columns.empty?.type).toBe("null");
    expect(combined.columns.loaded_at?.type).toBe("timestamp");
    expect(combined.columns.label?.type).toBe("utf8");
    expect(combined.columns.tags?.type).toBe("list");
    expect(combined.columns.attrs?.type).toBe("struct");
    expect(combined.columns.lookup?.type).toBe("struct");
    expect(materializeBatchRows(combined)).toEqual([
      {
        empty: null,
        loaded_at: timestampFromEpoch(1_700_000_000_001n, "millis"),
        label: "a",
        tags: ["a"],
        attrs: { id: 1 },
        lookup: { k: "v" },
      },
      {
        empty: null,
        loaded_at: timestampFromEpoch(1_700_000_000_002n, "millis"),
        label: "b",
        tags: null,
        attrs: null,
        lookup: null,
      },
      {
        empty: null,
        loaded_at: timestampFromEpoch(1_700_000_000_003n, "millis"),
        label: "c",
        tags: ["b", "c"],
        attrs: { id: 2 },
        lookup: { k: "w" },
      },
    ]);
  });

  it("rejects invalid ordering requests with typed errors", () => {
    const batch = batchFromColumns({ id: [1], amount: [10] });

    expect(() => vectorSortIndices(batch, [])).toThrowError(
      expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
    );
    expect(() => vectorSortIndices(batch, [{ column: "missing" }])).toThrowError(
      expect.objectContaining({ code: "LAKEQL_UNKNOWN_COLUMN" }),
    );
    expect(() => vectorSortIndices(batch, [{ column: "" }])).toThrowError(
      expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
    );
    expect(() =>
      vectorSortIndices(batch, [{ column: "amount", direction: "sideways" as never }]),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }));
    expect(() =>
      vectorSortIndices(batch, [{ column: "amount", nulls: "middle" as never }]),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }));
    expect(() => vectorTopKBatch(batch, [{ column: "amount" }], { limit: -1 })).toThrowError(
      expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
    );
    expect(() =>
      vectorTopKBatch(batch, [{ column: "amount" }], { offset: -1, limit: 1 }),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }));
    expect(() => vectorTopKIndices(batch, [{ column: "amount" }], 1.5)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
    );
    expect(() => concatBatches([batch, batchFromColumns({ id: [2], other: [3] })])).toThrowError(
      expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
    );
    expect(() =>
      concatBatches([batchFromColumns({ id: [1] }), batchFromColumns({ id: ["x"] })]),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }));
    expect(() =>
      vectorSortIndices(batchFromColumns({ nested: [[1], [2]] }), [{ column: "nested" }]),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }));
  });
});
