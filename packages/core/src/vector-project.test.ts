import { describe, expect, it } from "vitest";
import { batchFromColumns, materializeBatchRows } from "./batch.js";
import { col, fn, gt, lit, mul } from "./expr.js";
import { vectorProjectBatch } from "./vector-project.js";

describe("vector projection kernels", () => {
  it("projects selected columns and expression vectors without materializing input rows", () => {
    const batch = batchFromColumns({
      store_id: [1, 2, 3],
      amount: [10, null, 30],
      label: [" Keep ", "skip", null],
    });

    const projected = vectorProjectBatch(batch, ["store_id"], {
      doubled: mul(col("amount"), 2),
      bucket: {
        kind: "case",
        whens: [{ when: gt("amount", 20), value: lit("large") }],
        else: lit("small"),
      },
      normalized: fn("lower", fn("trim", col("label"))),
      nonSkip: fn("nullif", col("label"), "skip"),
    });

    expect(projected.columns.store_id).toBe(batch.columns.store_id);
    expect(materializeBatchRows(projected)).toEqual([
      { store_id: 1, doubled: 20, bucket: "small", normalized: "keep", nonSkip: " Keep " },
      { store_id: 2, doubled: null, bucket: "small", normalized: "skip", nonSkip: null },
      { store_id: 3, doubled: 60, bucket: "large", normalized: null, nonSkip: null },
    ]);
  });

  it("defaults to all input columns when select is omitted", () => {
    const batch = batchFromColumns({ id: [1], label: ["a"] });

    expect(materializeBatchRows(vectorProjectBatch(batch))).toEqual([{ id: 1, label: "a" }]);
  });

  it("expands wildcard selections before adding expression vectors", () => {
    const batch = batchFromColumns({ id: [1], amount: [10] });

    expect(
      materializeBatchRows(vectorProjectBatch(batch, ["*"], { doubled: mul(col("amount"), 2) })),
    ).toEqual([{ id: 1, amount: 10, doubled: 20 }]);
  });

  it("rejects unknown selected columns with typed errors", () => {
    const batch = batchFromColumns({ id: [1] });

    expect(() => vectorProjectBatch(batch, ["missing"])).toThrowError(
      expect.objectContaining({ code: "LAKEQL_UNKNOWN_COLUMN" }),
    );
  });
});
