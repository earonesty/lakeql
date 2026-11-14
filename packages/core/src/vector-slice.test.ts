import { expect, it } from "vitest";
import { type Vector, vectorFromValues, vectorLength, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { retainedVectorBytes } from "./vector-retained-bytes.js";
import { sliceBatch, sliceVector } from "./vector-slice.js";

it("retains list children and absolute offsets through repeated nullable slices", () => {
  const input = vectorFromValues([[1, 2], null, [], [3, null, 5], [6]]);
  if (input.type !== "list") throw new Error("expected list");
  const sliced = sliceVector(sliceVector(input, 1, 5), 1, 3);
  if (sliced.type !== "list") throw new Error("expected list slice");
  expect(sliced.child).toBe(input.child);
  expect(sliced.offsets.buffer).toBe(input.offsets.buffer);
  expect([vectorValue(sliced, 0), vectorValue(sliced, 1)]).toEqual([[], [3, null, 5]]);
  expect(retainedVectorBytes(sliced)).toBe(retainedVectorBytes(input));
  expect(vectorValue(sliceVector(input, 1, 2), 0)).toBeNull();
  expect(vectorLength(sliceVector(input, 3, 3))).toBe(0);
});

it("slices maps without materializing keys or values", () => {
  const input: Vector = {
    type: "map",
    offsets: Int32Array.of(0, 1, 1, 3),
    keys: { type: "utf8", values: ["a", "b", "c"] },
    values: { type: "i32", values: Int32Array.of(1, 2, 3) },
    valid: Uint8Array.of(1, 0, 1),
  };
  const sliced = sliceVector(input, 1, 3);
  if (sliced.type !== "map") throw new Error("expected map");
  expect(sliced.keys).toBe(input.keys);
  expect(sliced.values).toBe(input.values);
  expect(sliced.offsets.buffer).toBe(input.offsets.buffer);
  expect(vectorValue(sliced, 0)).toBeNull();
  expect(vectorValue(sliced, 1)).toEqual({ b: 2, c: 3 });
});

it("recursively slices struct fields while retaining nested list payload", () => {
  const child = vectorFromValues([[1], [2, 3], [], [4]]);
  const input: Vector = {
    type: "struct",
    length: 4,
    fields: { items: child, id: { type: "i32", values: Int32Array.of(0, 1, 2, 3) } },
    valid: Uint8Array.of(1, 1, 0, 1),
  };
  const sliced = sliceVector(input, 1, 4);
  expect([0, 1, 2].map((index) => vectorValue(sliced, index))).toEqual([
    { items: [2, 3], id: 1 },
    null,
    { items: [4], id: 3 },
  ]);
  if (sliced.type !== "struct" || sliced.fields.items?.type !== "list" || child.type !== "list")
    throw new Error("expected nested shape");
  expect(sliced.fields.items.child).toBe(child.child);
});

it("preserves packed row offsets and replaces validity with its slice", () => {
  const input: Vector = {
    type: "utf8-view",
    data: new TextEncoder().encode("abc"),
    fixedLength: 1,
    length: 3,
    valid: Uint8Array.of(0, 1, 1),
  };
  const sliced = sliceVector(sliceVector(input, 1, 3), 1, 2);
  expect(vectorValue(sliced, 0)).toBe("c");
  expect(sliced).toMatchObject({ rowOffset: 2, length: 1, valid: Uint8Array.of(1) });
  expect(sliceVector(input, 0, 3)).toBe(input);
});

it.each([
  [-1, 0],
  [0, 4],
  [2, 1],
  [0.5, 1],
  [0, Infinity],
  [NaN, 1],
])("rejects invalid slice [%s,%s)", (start, end) => {
  expect(() => sliceVector({ type: "null", length: 3 }, start, end)).toThrow(/out of bounds/);
});

it("slices batches without gather indices, including empty projections", () => {
  const values = Int32Array.of(1, 2, 3, 4);
  const batch = { rowCount: 4, columns: { id: { type: "i32" as const, values } } };
  const sliced = sliceBatch(batch, 1, 3);
  expect(sliced.rowCount).toBe(2);
  expect(sliced.columns.id).toMatchObject({ type: "i32", values: Int32Array.of(2, 3) });
  if (sliced.columns.id?.type !== "i32") throw new Error("missing numeric vector");
  expect(sliced.columns.id.values.buffer).toBe(values.buffer);
  expect(sliceBatch(batch, 0, 4)).toBe(batch);
  expect(sliceBatch({ rowCount: 4, columns: {} }, 1, 3)).toEqual({ rowCount: 2, columns: {} });
  expect(() => sliceBatch({ rowCount: 4, columns: {} }, 0, 5)).toThrow(/out of bounds/);
});

it.each([
  vectorFromValues([1, 2, 3]),
  vectorFromValues([true, null, false]),
  { type: "utf8", values: ["a", "b", "c"] } as Vector,
  { type: "binary", values: [Uint8Array.of(1), Uint8Array.of(2), Uint8Array.of(3)] } as Vector,
  vectorFromValues([[1], null, [2, 3]]),
  vectorFromValues([{ a: 1 }, { a: 2 }, { a: 3 }]),
  { type: "utf8-view", data: new TextEncoder().encode("abc"), fixedLength: 1, length: 3 } as Vector,
  {
    type: "timestamp96",
    low: BigUint64Array.of(1n, 2n, 3n),
    high: Int32Array.of(0, 0, 0),
    unit: "ns",
    isAdjustedToUTC: true,
  } as Vector,
])("admits slice metadata at its exact cap for $type", (input) => {
  const run = (cap: number) => {
    const budget = new MemoryBudget(cap);
    const scope = new MemoryScope(budget);
    try {
      const sliced = sliceVector(input, 1, 3, scope);
      expect(sliced).toEqual(sliceVector(input, 1, 3));
      expect(sliceVector(input, 0, 3, scope)).toBe(input);
      return budget.peakBytes;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  };
  const peak = run(16384);
  expect(peak).toBeGreaterThan(0);
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrow(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
});
