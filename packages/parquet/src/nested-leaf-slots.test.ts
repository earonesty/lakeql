import { getSchemaPath } from "hyparquet/src/schema.js";
import { MemoryBudget, MemoryScope, type Vector, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { nestedLeafSlots } from "./nested-leaf-slots.js";

const path = getSchemaPath(
  [
    { name: "root", num_children: 1 },
    { name: "items", repetition_type: "REPEATED", num_children: 1 },
    { name: "value", repetition_type: "OPTIONAL", type: "INT32" },
  ],
  ["items", "value"],
);
const levels = (defs: number[]) => ({
  definition: Uint8Array.from(defs),
  repetition: undefined,
  rowsStarted: 1,
  presentCount: defs.filter((d) => d === 2).length,
  valueOffset: 0,
});

it("expands leaf nulls without inserting slots for absent repeated parents", () => {
  const input: Vector = { type: "i32", values: Int32Array.of(7, 9) };
  const scope = new MemoryScope(new MemoryBudget(4096));
  const vector = nestedLeafSlots(levels([0, 1, 2, 0, 2, 1]), path, input, scope);
  expect([0, 1, 2, 3].map((i) => vectorValue(vector, i))).toEqual([null, 7, 9, null]);
  if (vector.type !== "dict") throw Error("Expected slot routing");
  expect(vector.dictionary).toBe(input);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  nestedLeafSlots(levels([0, 1, 2, 0, 2, 1]), path, input, exact);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => nestedLeafSlots(levels([0, 1, 2, 0, 2, 1]), path, input, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("handles all-null children and preserves identity when no null expansion is needed", () => {
  const empty: Vector = { type: "i32", values: new Int32Array() };
  const scope = new MemoryScope(new MemoryBudget(4096));
  const nulls = nestedLeafSlots(levels([0, 1, 1]), path, empty, scope);
  expect([vectorValue(nulls, 0), vectorValue(nulls, 1)]).toEqual([null, null]);
  scope.dispose();
  const zero = new MemoryScope(new MemoryBudget(0));
  expect(nestedLeafSlots(levels([0, 0]), path, empty, zero)).toBe(empty);
  const input: Vector = { type: "i32", values: Int32Array.of(7) };
  expect(nestedLeafSlots(levels([0, 2]), path, input, zero)).toBe(input);
  expect(() => nestedLeafSlots(levels([2]), path, empty, zero)).toThrow(/Invalid/);
  expect(zero.budget.peakBytes).toBe(0);
  zero.dispose();
});
