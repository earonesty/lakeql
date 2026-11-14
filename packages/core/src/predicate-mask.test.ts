import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { allocatePredicateMask } from "./predicate-mask.js";

it.each([0, 1, 4096])("admits backing and view at the exact capacity for %s rows", (length) => {
  const pool = new MemoryBudget(c.view + length);
  const scope = new MemoryScope(pool);
  const mask = allocatePredicateMask(length, scope);
  expect(mask).toEqual(new Uint8Array(length));
  expect(pool.usedBytes).toBe(c.view + length);
  const retained = scope.retainAllocation(mask);
  scope.dispose();
  expect(pool.usedBytes).toBe(c.view + length);
  retained.release();
  expect(pool.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(c.view + length - 1));
  expect(() => allocatePredicateMask(length, tight)).toThrow(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it.each([
  -1,
  0.5,
  NaN,
  Infinity,
  Number.MAX_SAFE_INTEGER,
])("rejects invalid mask extent %s before allocation", (length) => {
  const pool = new MemoryBudget(8192);
  const scope = new MemoryScope(pool);
  expect(() => allocatePredicateMask(length, scope)).toThrow(
    expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
  );
  expect(pool.peakBytes).toBe(0);
  scope.dispose();
});
