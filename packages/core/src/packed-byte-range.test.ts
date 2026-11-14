import { expect, it } from "vitest";
import { type PackedByteVector, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { packedConcatPayloadBytes } from "./packed-gather.js";
import { planSelectedConcat } from "./selected-concat.js";
import { selectVector } from "./vector-selection.js";
import { gatherOwnedBatch } from "./vector-sort.js";

const invalid: PackedByteVector[] = [
  { type: "binary-view", data: Uint8Array.of(1), ranges: Uint32Array.of(1, 1), length: 1 },
  { type: "binary-view", data: new Uint8Array(8).subarray(2, 3), fixedLength: 2, length: 1 },
  ...[-1, 0.5, Infinity, NaN, Number.MAX_SAFE_INTEGER].map(
    (rowOffset): PackedByteVector => ({
      type: "utf8-view",
      data: Uint8Array.of(65),
      fixedLength: 1,
      rowOffset,
      length: 1,
    }),
  ),
  ...[-1, 0.5, Infinity, NaN].map(
    (fixedLength): PackedByteVector => ({
      type: "binary-view",
      data: Uint8Array.of(1),
      fixedLength,
      length: 1,
    }),
  ),
];

it.each(
  invalid.map((vector, index) => ({ vector, index })),
)("rejects malformed packed extent $index before owned gather allocation", ({ vector }) => {
  expect(() => packedConcatPayloadBytes(vector)).toThrow(/range/);
  expect(() => planSelectedConcat([selectVector(vector)])).toThrow(/range/);
  const budget = new MemoryBudget(4096);
  const scope = new MemoryScope(budget);
  const input = scope.retainAllocations({ rowCount: 1, columns: { value: vector } });
  expect(() => gatherOwnedBatch(input, [0])).toThrow(/range/);
  expect(budget.peakBytes).toBe(0);
  input.release();
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("ignores hidden invalid ranges for nulls while retaining empty values at the payload end", () => {
  const vector: PackedByteVector = {
    type: "binary-view",
    data: Uint8Array.of(99, 1, 2).subarray(1),
    ranges: Uint32Array.of(999, 999, 0, 2, 2, 0),
    valid: Uint8Array.of(0, 1, 1),
    length: 3,
  };
  expect(packedConcatPayloadBytes(vector)).toBe(2);
  const plan = planSelectedConcat([selectVector(vector)]);
  if (plan === undefined) throw new Error("Missing plan");
  const scope = new MemoryScope(new MemoryBudget(plan.bytes));
  scope.admit(plan.bytes, "selected payload");
  const output = plan.execute(scope);
  expect([0, 1, 2].map((row) => vectorValue(output, row))).toEqual([
    null,
    Uint8Array.of(1, 2),
    new Uint8Array(),
  ]);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
