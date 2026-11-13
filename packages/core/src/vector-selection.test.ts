import { expect, it } from "vitest";
import type { Vector } from "./batch.js";
import {
  selectCollectionChild,
  selectStructField,
  selectVector,
  type VectorSelection,
} from "./vector-selection.js";

function indices(selection: VectorSelection): number[] {
  const output: number[] = [];
  selection.forEach((index, row) => {
    expect(row).toBe(output.length);
    output.push(index);
  });
  expect(output.length).toBe(selection.length);
  return output;
}

it("replays repeated list selections without materializing child payload", () => {
  const values: string[] = [];
  values.length = 4;
  Object.defineProperty(values, 0, {
    get() {
      throw new Error("payload read");
    },
  });
  const leaf: Vector = {
    type: "list",
    offsets: Int32Array.of(0, 2, 4, 99),
    child: { type: "utf8", values },
  };
  const source: Vector = {
    type: "dict",
    indices: Uint32Array.of(1, 0, 1, 2),
    valid: Uint8Array.of(1, 1, 1, 0),
    dictionary: leaf,
  };
  const parent = selectVector(source);
  expect(indices(parent)).toEqual([1, 0, 1, -1]);
  const child = selectCollectionChild(parent, "child");
  expect(parent.mayHaveNull).toBe(true);
  expect(child.mayHaveNull).toBe(false);
  for (let run = 0; run < 2; run++) expect(indices(child)).toEqual([2, 3, 0, 1, 2, 3]);
});

it("propagates parent nulls and dictionary nulls through struct fields", () => {
  const parent = selectVector({
    type: "struct",
    length: 3,
    valid: Uint8Array.of(1, 0, 1),
    fields: {
      x: {
        type: "dict",
        indices: Uint32Array.of(1, 99, 0),
        dictionary: { type: "i32", values: Int32Array.of(10, 20), valid: Uint8Array.of(0, 1) },
      },
    },
  });
  expect(selectStructField(parent, "x").mayHaveNull).toBe(true);
  expect(indices(selectStructField(parent, "x"))).toEqual([1, -1, -1]);
  expect(() => selectStructField(parent, "toString")).toThrow(/Missing/);
});

it("validates selected map bounds and applies child masks independently", () => {
  const source: Vector = {
    type: "map",
    offsets: Int32Array.of(0, 2),
    keys: { type: "i32", values: Int32Array.of(1, 2), valid: Uint8Array.of(1, 0) },
    values: { type: "null", length: 2 },
  };
  const parent = selectVector(source);
  expect(indices(selectCollectionChild(parent, "keys"))).toEqual([0, -1]);
  expect(indices(selectCollectionChild(parent, "values"))).toEqual([-1, -1]);
  source.offsets[1] = 3;
  expect(() => selectCollectionChild(selectVector(source), "keys")).toThrow(/offsets/);
});

it("shares one collection-length pass across map child projections", () => {
  const parent = selectVector({
    type: "map",
    offsets: Int32Array.of(0, 2, 3),
    keys: { type: "i32", values: Int32Array.of(1, 2, 3) },
    values: { type: "i32", values: Int32Array.of(4, 5, 6) },
  });
  let passes = 0;
  const visit = parent.forEach;
  parent.forEach = (callback) => {
    passes++;
    visit(callback);
  };
  expect(parent.collectionLength()).toBe(3);
  expect(selectCollectionChild(parent, "keys").length).toBe(3);
  expect(selectCollectionChild(parent, "values").length).toBe(3);
  expect(passes).toBe(1);
});
