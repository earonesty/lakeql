import { describe, expect, it } from "vitest";
import {
  batchFromVectors,
  materializeBatchRows,
  materializeSelectedBatchRows,
  type PackedByteVector,
  predicateSelection,
  scalarVectorValue,
  vectorValue,
} from "./batch.js";
import { col, eq, lit } from "./expr.js";
import { vectorGroupByBatch } from "./vector-group-by.js";
import { retainedBatchBytes } from "./vector-retained-bytes.js";
import { concatBatches, gatherBatch } from "./vector-sort.js";

describe("packed byte vectors", () => {
  function fixture(): PackedByteVector {
    return {
      type: "utf8-view",
      data: new TextEncoder().encode("skipé猫"),
      ranges: Uint32Array.of(0, 4, 4, 2, 6, 0, 6, 3, 9, 0),
      rowOffset: 1,
      length: 4,
      valid: Uint8Array.of(1, 1, 1, 0),
    };
  }

  it("reads Unicode, empty strings, nulls and offset rows through generic access", () => {
    const vector = fixture();
    for (const read of [vectorValue, scalarVectorValue]) {
      expect([0, 1, 2, 3].map((index) => read(vector, index))).toEqual(["é", "", "猫", null]);
    }
    const batch = batchFromVectors({ text: vector });
    expect(
      materializeSelectedBatchRows(batch, predicateSelection(batch, eq(col("text"), lit("猫")))),
    ).toEqual([{ text: "猫" }]);
  });

  it("gathers repeated and null rows without copying the payload", () => {
    const vector = fixture();
    const gathered = gatherBatch(batchFromVectors({ text: vector }), [3, 2, 2, 0, 1]);
    expect(gathered.columns.text).toMatchObject({ type: "dict", dictionary: vector });
    expect(materializeBatchRows(gathered)).toEqual([
      { text: null },
      { text: "猫" },
      { text: "猫" },
      { text: "é" },
      { text: "" },
    ]);
    expect(materializeBatchRows(concatBatches([gathered, gathered]))).toHaveLength(10);
    expect(
      materializeBatchRows(vectorGroupByBatch(["text"], { rows: { op: "count" } }, gathered)),
    ).toEqual([
      { text: null, rows: 1 },
      { text: "猫", rows: 2 },
      { text: "é", rows: 1 },
      { text: "", rows: 1 },
    ]);
  });

  it("reads fixed-width binary values as input aliases", () => {
    const data = Uint8Array.of(0, 255, 128, 7, 9, 10);
    const vector: PackedByteVector = {
      type: "binary-view",
      data,
      fixedLength: 2,
      rowOffset: 1,
      length: 2,
    };
    const value = vectorValue(vector, 0) as Uint8Array;
    expect(value).toEqual(Uint8Array.of(128, 7));
    expect(value.buffer).toBe(data.buffer);
    expect(vectorValue(vector, 1)).toEqual(Uint8Array.of(9, 10));
  });

  it("charges whole shared backing buffers once across sliced columns", () => {
    const backing = new ArrayBuffer(128);
    const vector: PackedByteVector = {
      type: "binary-view",
      data: new Uint8Array(backing, 0, 8),
      ranges: new Uint32Array(backing, 16, 2),
      valid: new Uint8Array(backing, 32, 1),
      length: 1,
    };
    expect(retainedBatchBytes(batchFromVectors({ a: vector, b: { ...vector } }))).toBe(128);
  });
});
