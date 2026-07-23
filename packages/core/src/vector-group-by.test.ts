import { describe, expect, it } from "vitest";
import {
  batchFromColumns,
  batchFromVectors,
  materializeBatchRows,
  predicateSelection,
} from "./batch.js";
import { col, gt, mul } from "./expr.js";
import {
  createVectorGroupByState,
  enforceVectorGroupByBudget,
  finalizeVectorGroupByBatch,
  finalizeVectorGroupByRows,
  getOrCreateVectorGroup,
  getOrCreateVectorGroupByValues,
  mergeVectorGroupByStates,
  restoreVectorGroupByState,
  snapshotVectorGroupByState,
  updateVectorGroupAggregateValue,
  updateVectorGroupByState,
  vectorGroupByBatch,
} from "./vector-group-by.js";

describe("vector group-by kernels", () => {
  it("groups and reduces type-preserving numeric vectors", () => {
    const batch = batchFromVectors({
      bucket: { type: "u32", values: Uint32Array.of(2, 1, 2, 1) },
      amount: { type: "f32", values: Float32Array.of(1.5, 2.5, 3.5, 4.5) },
    });

    expect(
      materializeBatchRows(
        vectorGroupByBatch(
          ["bucket"],
          {
            total: { op: "sum", column: "amount" },
            average: { op: "avg", column: "amount" },
          },
          batch,
        ),
      ),
    ).toEqual([
      { bucket: 2, total: 5, average: 2.5 },
      { bucket: 1, total: 7, average: 3.5 },
    ]);

    const composite = batchFromVectors({
      floatKey: { type: "f32", values: Float32Array.of(1, 1, 2) },
      signedKey: { type: "i32", values: Int32Array.of(-1, -1, 1) },
      byteKey: { type: "u8", values: Uint8Array.of(7, 7, 8) },
    });
    expect(
      materializeBatchRows(
        vectorGroupByBatch(
          ["floatKey", "signedKey", "byteKey"],
          { rows: { op: "count" } },
          composite,
        ),
      ),
    ).toEqual([
      { floatKey: 1, signedKey: -1, byteKey: 7, rows: 2 },
      { floatKey: 2, signedKey: 1, byteKey: 8, rows: 1 },
    ]);

    const dictionaryEncoded = batchFromVectors({
      floatKey: {
        type: "dict",
        indices: Uint32Array.of(0, 0, 1),
        dictionary: { type: "f32", values: Float32Array.of(1.5, 2.5) },
      },
      byteKey: {
        type: "dict",
        indices: Uint32Array.of(0, 0, 1),
        dictionary: { type: "u8", values: Uint8Array.of(7, 8) },
      },
    });
    expect(
      materializeBatchRows(
        vectorGroupByBatch(["floatKey", "byteKey"], { rows: { op: "count" } }, dictionaryEncoded),
      ),
    ).toEqual([
      { floatKey: 1.5, byteKey: 7, rows: 2 },
      { floatKey: 2.5, byteKey: 8, rows: 1 },
    ]);
    expect(() =>
      vectorGroupByBatch(["missing"], { rows: { op: "count" } }, dictionaryEncoded),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_UNKNOWN_COLUMN" }));
  });

  it("groups selected batches with aggregate states without materializing input rows", () => {
    const batch = batchFromColumns({
      region: ["east", "west", "east", null, "west"],
      amount: [10, 20, null, 40, 5],
      id: [1, 2, 3, 4, 5],
    });
    const selection = predicateSelection(batch, gt("id", 1));
    const result = materializeBatchRows(
      vectorGroupByBatch(
        ["region"],
        {
          rows: { op: "count" },
          amountRows: { op: "count", column: "amount" },
          total: { op: "sum", column: "amount" },
          average: { op: "avg", column: "amount" },
          medianAmount: { op: "median", column: "amount" },
          amountP75: { op: "quantile", column: "amount", quantile: 0.75 },
          modeAmount: { op: "mode", column: "amount" },
          minId: { op: "min", column: "id" },
          maxId: { op: "max", column: "id" },
        },
        batch,
        selection,
      ),
    );

    expect(result).toEqual([
      {
        region: "west",
        rows: 2,
        amountRows: 2,
        total: 25,
        average: 12.5,
        medianAmount: 12.5,
        amountP75: 16.25,
        modeAmount: 20,
        minId: 2,
        maxId: 5,
      },
      {
        region: "east",
        rows: 1,
        amountRows: 0,
        total: 0,
        average: null,
        medianAmount: null,
        amountP75: null,
        modeAmount: null,
        minId: 3,
        maxId: 3,
      },
      {
        region: null,
        rows: 1,
        amountRows: 1,
        total: 40,
        average: 40,
        medianAmount: 40,
        amountP75: 40,
        modeAmount: 40,
        minId: 4,
        maxId: 4,
      },
    ]);
  });

  it("groups dictionary vectors through the aggregate state machinery", () => {
    const dictionary = batchFromColumns({ value: ["east", "west"] }).columns.value;
    const amount = batchFromColumns({ amount: [10, 20, 30, 40, 50] }).columns.amount;
    const id = batchFromColumns({ id: [1, 2, 3, 4, 5] }).columns.id;
    if (dictionary === undefined || amount === undefined || id === undefined) {
      throw new Error("missing test vectors");
    }
    const batch = batchFromVectors({
      region: {
        type: "dict",
        indices: new Uint32Array([0, 1, 0, 1, 1]),
        dictionary,
      },
      amount,
      id,
    });

    expect(
      materializeBatchRows(
        vectorGroupByBatch(
          ["region"],
          {
            rows: { op: "count" },
            total: { op: "sum", column: "amount" },
            average: { op: "avg", column: "amount" },
            medianAmount: { op: "median", column: "amount" },
            modeAmount: { op: "mode", column: "amount" },
            distinctIds: { op: "count_distinct", column: "id" },
          },
          batch,
        ),
      ),
    ).toEqual([
      {
        region: "east",
        rows: 2,
        total: 40,
        average: 20,
        medianAmount: 20,
        modeAmount: 10,
        distinctIds: 2,
      },
      {
        region: "west",
        rows: 3,
        total: 110,
        average: 36.666666666666664,
        medianAmount: 40,
        modeAmount: 20,
        distinctIds: 3,
      },
    ]);
  });

  it("groups nullable dictionary keys and global aggregate states through encoded paths", () => {
    const dictionary = batchFromColumns({ value: ["east", "west"] }).columns.value;
    const amount = batchFromColumns({ amount: [10, 20, 30, 40] }).columns.amount;
    if (dictionary === undefined || amount === undefined) throw new Error("missing test vectors");
    const batch = batchFromVectors({
      region: {
        type: "dict",
        indices: new Uint32Array([0, 1, 0, 1]),
        dictionary,
        valid: new Uint8Array([1, 0, 1, 1]),
      },
      amount,
    });

    expect(
      materializeBatchRows(
        vectorGroupByBatch(
          ["region"],
          {
            rows: { op: "count" },
            total: { op: "sum", column: "amount" },
          },
          batch,
        ),
      ),
    ).toEqual([
      { region: "east", rows: 2, total: 40 },
      { region: null, rows: 1, total: 20 },
      { region: "west", rows: 1, total: 40 },
    ]);

    expect(
      materializeBatchRows(
        vectorGroupByBatch([], { total: { op: "sum", column: "amount" } }, batch),
      ),
    ).toEqual([{ total: 100 }]);
  });

  it("uses the encoded group loop for scalar and composite vector keys", () => {
    const single = materializeBatchRows(
      vectorGroupByBatch(
        ["bucket"],
        { rows: { op: "count" }, total: { op: "sum", column: "amount" } },
        batchFromColumns({
          bucket: [2, 1, 2, 1, null],
          amount: [10, 20, 30, 40, 50],
        }),
      ),
    );
    expect(single).toEqual([
      { bucket: 2, rows: 2, total: 40 },
      { bucket: 1, rows: 2, total: 60 },
      { bucket: null, rows: 1, total: 50 },
    ]);

    const composite = materializeBatchRows(
      vectorGroupByBatch(
        ["bucket", "flag"],
        { rows: { op: "count" }, total: { op: "sum", column: "amount" } },
        batchFromColumns({
          bucket: [2, 2, 2, 1],
          flag: [true, true, false, true],
          amount: [10, 30, 5, 20],
        }),
      ),
    );
    expect(composite).toEqual([
      { bucket: 2, flag: true, rows: 2, total: 40 },
      { bucket: 2, flag: false, rows: 1, total: 5 },
      { bucket: 1, flag: true, rows: 1, total: 20 },
    ]);
  });

  it("keeps nulls together and distinguishes composite key primitive types", () => {
    const batch = batchFromColumns({
      key: ["1", "1", null, null],
      flag: [true, false, true, true],
      amount: [1, 2, 3, 4],
    });
    const state = createVectorGroupByState(["key", "flag"], {
      rows: { op: "count" },
      total: { op: "sum", column: "amount" },
    });

    updateVectorGroupByState(state, batch);

    expect(finalizeVectorGroupByRows(state)).toEqual([
      { key: "1", flag: true, rows: 1, total: 1 },
      { key: "1", flag: false, rows: 1, total: 2 },
      { key: null, flag: true, rows: 2, total: 7 },
    ]);
  });

  it("uses collision-resistant composite key encoding", () => {
    const batch = batchFromColumns({
      left: ["a|boolean:true", "a"],
      right: [false, true],
      amount: [1, 2],
    });

    expect(
      materializeBatchRows(
        vectorGroupByBatch(
          ["left", "right"],
          {
            total: { op: "sum", column: "amount" },
          },
          batch,
        ),
      ),
    ).toEqual([
      { left: "a|boolean:true", right: false, total: 1 },
      { left: "a", right: true, total: 2 },
    ]);
  });

  it("groups sparse selected numeric keys through vector key readers", () => {
    const batch = batchFromColumns({
      id: [1, 2, 3, 4, 5, 6],
      account: [10n, 10n, 20n, 20n, 30n, 30n],
      amount: [5, 7, 11, 13, 17, 19],
    });
    const selection = predicateSelection(batch, gt("id", 3));

    expect(
      materializeBatchRows(
        vectorGroupByBatch(
          ["account"],
          {
            rows: { op: "count" },
            total: { op: "sum", column: "amount" },
          },
          batch,
          selection,
        ),
      ),
    ).toEqual([
      { account: 20n, rows: 1, total: 13 },
      { account: 30n, rows: 2, total: 36 },
    ]);
  });

  it("keeps composite numeric vector keys collision-resistant", () => {
    const batch = batchFromColumns({
      left: [1, 11],
      right: [23, 3],
      amount: [10, 20],
    });

    expect(
      materializeBatchRows(
        vectorGroupByBatch(["left", "right"], { total: { op: "sum", column: "amount" } }, batch),
      ),
    ).toEqual([
      { left: 1, right: 23, total: 10 },
      { left: 11, right: 3, total: 20 },
    ]);
  });

  it("rejects nested group keys with the scalar-vector capability boundary made explicit", () => {
    const batch = batchFromColumns({
      tags: [["a", "b"], ["a", "b"], ["c"], null],
      attrs: [{ level: 1 }, { level: 1 }, { level: 2 }, { level: 1 }],
      lookup: [
        new Map<string, unknown>([["k", "v"]]),
        new Map<string, unknown>([["k", "v"]]),
        new Map<string, unknown>([["k", "w"]]),
        null,
      ],
      amount: [10, 5, 7, 3],
    });

    expect(() =>
      vectorGroupByBatch(
        ["tags", "attrs", "lookup"],
        {
          rows: { op: "count" },
          total: { op: "sum", column: "amount" },
        },
        batch,
      ),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }));
  });

  it("supports aggregate expression inputs and distinct budgets", () => {
    const batch = batchFromColumns({
      region: ["east", "east", "west", "west"],
      amount: [10, 20, 30, 40],
      label: ["a", "b", "a", "a"],
    });
    const result = materializeBatchRows(
      vectorGroupByBatch(
        ["region"],
        {
          doubledMax: { op: "max", expr: mul(col("amount"), 2) },
          labels: { op: "count_distinct", column: "label" },
        },
        batch,
      ),
    );

    expect(result).toEqual([
      { region: "east", doubledMax: 40, labels: 2 },
      { region: "west", doubledMax: 80, labels: 1 },
    ]);
  });

  it("finalizes an incremental group-by state as a batch", () => {
    const state = createVectorGroupByState(["region"], {
      rows: { op: "count" },
      total: { op: "sum", column: "amount" },
    });

    updateVectorGroupByState(state, batchFromColumns({ region: ["east"], amount: [10] }));
    updateVectorGroupByState(state, batchFromColumns({ region: ["east", "west"], amount: [5, 7] }));

    expect(materializeBatchRows(finalizeVectorGroupByBatch(state))).toEqual([
      { region: "east", rows: 2, total: 15 },
      { region: "west", rows: 1, total: 7 },
    ]);
  });

  it("round-trips grouped partials through JSON and merges them", () => {
    const spec = {
      rows: { op: "count" },
      maxAmount: { op: "max", column: "amount" },
    } as const;
    const left = createVectorGroupByState(["account"], spec);
    const right = createVectorGroupByState(["account"], spec);
    updateVectorGroupByState(left, batchFromColumns({ account: [10n, 20n], amount: [1, 2] }));
    updateVectorGroupByState(right, batchFromColumns({ account: [10n], amount: [5] }));

    const transported = JSON.parse(JSON.stringify(snapshotVectorGroupByState(right)));
    mergeVectorGroupByStates(left, restoreVectorGroupByState(["account"], spec, transported));

    expect(finalizeVectorGroupByRows(left)).toEqual([
      { account: 10n, rows: 2, maxAmount: 5 },
      { account: 20n, rows: 1, maxAmount: 2 },
    ]);
  });

  it("enforces group and memory budgets", () => {
    const batch = batchFromColumns({ region: ["east", "west"], amount: [1, 2] });

    expect(() =>
      vectorGroupByBatch(["region"], { rows: { op: "count" } }, batch, undefined, {
        maxGroups: 1,
      }),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_GROUP_LIMIT_EXCEEDED",
        details: { limit: 1, actual: 2 },
      }),
    );

    expect(() =>
      vectorGroupByBatch(["region"], { total: { op: "sum", column: "amount" } }, batch, undefined, {
        budget: { maxMemoryBytes: 1 },
      }),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: expect.objectContaining({ metric: "operator memory bytes", limit: 1 }),
      }),
    );
  });

  it("rejects binary group keys and aggregate values without coercion", () => {
    const batch = batchFromColumns({
      id: [1, 2],
      payload: [new Uint8Array([1]), new Uint8Array([2])],
    });

    expect(() => vectorGroupByBatch(["payload"], { rows: { op: "count" } }, batch)).toThrowError(
      /Binary values are not group keys/u,
    );
    expect(() =>
      vectorGroupByBatch([], { payloads: { op: "count", column: "payload" } }, batch),
    ).toThrowError(/Binary values are not aggregate values/u);
    expect(() =>
      vectorGroupByBatch([], { firstPayload: { op: "first", column: "payload" } }, batch),
    ).toThrowError(/Binary values are not aggregate values/u);
  });

  it("exposes group update APIs with explicit missing-state and memory errors", () => {
    const spec = {
      rows: { op: "count" },
      total: { op: "sum", column: "amount" },
      labels: { op: "count_distinct", column: "label" },
    } as const;
    const state = createVectorGroupByState(["region"], spec);
    const batch = batchFromColumns({
      region: ["east", "west"],
      amount: [10, 20],
      label: ["alpha", "beta"],
    });

    const group = getOrCreateVectorGroup(state, batch, 0);
    updateVectorGroupAggregateValue(group, "rows", true);
    updateVectorGroupAggregateValue(group, "total", 5);
    updateVectorGroupAggregateValue(group, "labels", "alpha");

    expect(finalizeVectorGroupByRows(state)).toEqual([
      { region: "east", rows: 1, total: 5, labels: 1 },
    ]);
    expect(getOrCreateVectorGroup(state, batch, 0)).toBe(group);
    expect(() => getOrCreateVectorGroup(state, batch, 1, { maxGroups: 1 })).toThrowError(
      expect.objectContaining({ code: "LAKEQL_GROUP_LIMIT_EXCEEDED" }),
    );
    expect(() => updateVectorGroupAggregateValue(group, "missing", true)).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_TYPE_ERROR",
        details: { alias: "missing" },
      }),
    );
    expect(() => enforceVectorGroupByBudget(state, { maxMemoryBytes: 1 })).toThrowError(
      expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
    );
  });

  it("enforces group limits while restoring and merging external partials", () => {
    const spec = { rows: { op: "count" } } as const;
    const source = createVectorGroupByState(["region"], spec);
    const east = ["east"];
    getOrCreateVectorGroupByValues(source, east);
    east[0] = "changed";
    getOrCreateVectorGroupByValues(source, ["west"]);
    const snapshot = snapshotVectorGroupByState(source);
    expect(snapshot.groups[0]?.keyValues).toEqual(["east"]);
    expect(() =>
      restoreVectorGroupByState(["region"], spec, snapshot, { maxGroups: 1 }),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_GROUP_LIMIT_EXCEEDED",
        details: { limit: 1, actual: 2 },
      }),
    );

    const target = createVectorGroupByState(["region"], spec);
    getOrCreateVectorGroupByValues(target, ["north"]);
    expect(() => mergeVectorGroupByStates(target, source, { maxGroups: 1 })).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_GROUP_LIMIT_EXCEEDED",
        details: { limit: 1, actual: 2 },
      }),
    );
  });
});
