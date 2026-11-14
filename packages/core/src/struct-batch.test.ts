import { expect, it } from "vitest";
import type { Vector } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { batchFromStructVector } from "./struct-batch.js";

it("borrows struct fields with admitted descriptors, including zero-column row counts", () => {
  const fields = { a: { type: "i32", values: Int32Array.of(1, 2) } as Vector };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const batch = batchFromStructVector(
    { type: "struct", fields, length: 2, valid: Uint8Array.of(1, 1) },
    scope,
  );
  expect(batch.columns).toBe(fields);
  expect(batch.rowCount).toBe(2);
  const charge = scope.budget.usedBytes;
  const tight = new MemoryScope(new MemoryBudget(charge - 1));
  expect(() => batchFromStructVector({ type: "struct", fields, length: 2 }, tight)).toThrow(
    /budget/,
  );
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  expect(batchFromStructVector({ type: "struct", fields: {}, length: 7 }, scope).rowCount).toBe(7);
  scope.dispose();
});

it("rejects parent nulls and malformed shapes before allocation", () => {
  const field: Vector = { type: "null", length: 2 };
  const scope = new MemoryScope(new MemoryBudget(0));
  for (const vector of [
    { type: "struct", fields: { a: field }, length: 2, valid: Uint8Array.of(1, 0) },
    { type: "struct", fields: { a: field }, length: 1 },
    { type: "struct", fields: {}, length: 2, valid: Uint8Array.of(1) },
    field,
  ] as Vector[])
    expect(() => batchFromStructVector(vector, scope)).toThrow(/all-present/);
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});
