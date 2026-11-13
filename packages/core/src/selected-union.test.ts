import { expect, it } from "vitest";
import { type Vector, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { compactSelectedUnion } from "./selected-union.js";
import { planUnionConcat } from "./union-concat.js";

it("compacts reordered repeated selections across more than 256 physical domains", () => {
  const producer = new MemoryScope(new MemoryBudget(1024 * 1024));
  const leaves: Vector[] = Array.from({ length: 600 }, (_, i) => ({
    type: "f64",
    values: Float64Array.of(i),
  }));
  const concat = planUnionConcat(leaves).execute(producer);
  const indices = Uint32Array.from({ length: 700 }, (_, i) => (699 - i) % 600);
  const source: Vector = { type: "dict", indices, dictionary: concat };
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  const compact = compactSelectedUnion(source, scope);
  expect(Array.from(indices, (_, row) => vectorValue(compact, row))).toEqual([...indices]);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  compactSelectedUnion(source, exact);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => compactSelectedUnion(source, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  producer.dispose();
});

it("preserves nulls and packed payload aliases without decoding", () => {
  const payload = new TextEncoder().encode("snow");
  const leaf: Vector = { type: "utf8-view", data: payload, fixedLength: 4, length: 1 };
  const source: Vector = {
    type: "dict",
    indices: Uint32Array.of(0, 0, 0),
    valid: Uint8Array.of(1, 0, 1),
    dictionary: leaf,
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const result = compactSelectedUnion(source, scope);
  expect([0, 1, 2].map((row) => vectorValue(result, row))).toEqual(["snow", null, "snow"]);
  if (result.type !== "union") throw Error("Missing union");
  expect(result.children).toContain(leaf);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rolls back after invalid late routing and supports empty input", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const bad: Vector = {
    type: "dict",
    indices: Uint32Array.of(0, 0, 3),
    dictionary: { type: "null", length: 1 },
  };
  expect(() => compactSelectedUnion(bad, scope)).toThrow(/Invalid/);
  expect(scope.budget.usedBytes).toBe(0);
  expect(compactSelectedUnion({ type: "null", length: 0 }, scope)).toEqual({
    type: "null",
    length: 0,
  });
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("marks selected routing as owned but never as contiguous concat order", async () => {
  const { isOwnedUnionRoutingAllocation } = await import("./union-routing-ownership.js");
  const { isUnionConcatRoutingAllocation } = await import("./union-concat.js");
  const { flattenUnionConcatInputs } = await import("./union-flatten.js");
  const scope = new MemoryScope(new MemoryBudget(65536));
  const result = compactSelectedUnion(
    {
      type: "dict",
      indices: Uint32Array.of(1, 0, 1),
      dictionary: { type: "f64", values: Float64Array.of(7, 9) },
    },
    scope,
  );
  expect(isOwnedUnionRoutingAllocation(result)).toBe(true);
  expect(isUnionConcatRoutingAllocation(result)).toBe(false);
  expect(flattenUnionConcatInputs([result], scope)).toEqual([result]);
  expect([0, 1, 2].map((row) => vectorValue(result, row))).toEqual([9, 7, 9]);
  scope.dispose();
});
