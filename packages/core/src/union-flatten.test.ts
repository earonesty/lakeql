import { expect, it } from "vitest";
import { type Vector, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { planUnionConcat } from "./union-concat.js";
import { flattenUnionConcatInputs } from "./union-flatten.js";
import { sliceVector } from "./vector-slice.js";

it("flattens concat histories in row order while keeping selections opaque", () => {
  const producer = new MemoryScope(new MemoryBudget(65536));
  const leaves: Vector[] = Array.from({ length: 8 }, (_, value) => ({
    type: "f64",
    values: Float64Array.of(value),
  }));
  const first = planUnionConcat(leaves.slice(0, 4)).execute(producer);
  const second = planUnionConcat([first, ...leaves.slice(4)]).execute(producer);
  const slice = sliceVector(second, 2, 5);
  const dictionary: Vector = { type: "dict", indices: Uint32Array.of(7, 0), dictionary: second };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const flat = flattenUnionConcatInputs([second, slice, dictionary], scope);
  expect(flat).toEqual([...leaves, slice, dictionary]);
  expect(scope.budget.usedBytes).toBe(64 + flat.length * 16);
  const result = planUnionConcat(flat).execute(scope);
  expect(Array.from({ length: 13 }, (_, row) => vectorValue(result, row))).toEqual([
    0, 1, 2, 3, 4, 5, 6, 7, 2, 3, 4, 7, 0,
  ]);
  scope.dispose();
  producer.dispose();
});

it("bounds deep traversal and releases all flatten scratch", () => {
  const producer = new MemoryScope(new MemoryBudget(4 * 1024 * 1024));
  const leaf: Vector = { type: "null", length: 0 };
  let history = leaf;
  for (let i = 0; i < 1200; i++) history = planUnionConcat([history, leaf]).execute(producer);
  const scope = new MemoryScope(new MemoryBudget(4 * 1024 * 1024));
  const flat = flattenUnionConcatInputs([history], scope);
  expect(flat).toHaveLength(1201);
  expect(flat.every((value) => value === leaf)).toBe(true);
  expect(scope.budget.usedBytes).toBe(64 + 1201 * 16);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(flattenUnionConcatInputs([history], exact)).toHaveLength(1201);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => flattenUnionConcatInputs([history], tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  producer.dispose();
});

it("flattens a wide concat using only output slots and traversal frames", () => {
  const producer = new MemoryScope(new MemoryBudget(65536));
  const leaves: Vector[] = Array.from({ length: 256 }, (_, value) => ({
    type: "f64",
    values: Float64Array.of(value),
  }));
  const root = planUnionConcat(leaves).execute(producer);
  const outputBytes = 64 + 256 * 16;
  const scope = new MemoryScope(new MemoryBudget(outputBytes + 2 * 256));
  const flat = flattenUnionConcatInputs([root], scope);
  expect(flat).toEqual(leaves);
  expect(scope.budget.peakBytes).toBe(outputBytes + 2 * 256);
  expect(scope.budget.usedBytes).toBe(outputBytes);
  scope.dispose();
  producer.dispose();
});
