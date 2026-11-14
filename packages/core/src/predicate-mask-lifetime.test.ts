import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { batchFromColumns, materializeBatchRows, predicateSelection } from "./batch.js";
import { and, col, not, or } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

const left = [true, true, true, false, false, false, null, null, null];
const right = [true, false, null, true, false, null, true, false, null];
const batch = batchFromColumns({ left, right });

it.each([2, 100])("bounds wide logical masks at two row buffers for %s operands", (count) => {
  for (const op of ["and", "or"] as const) {
    const pool = new MemoryBudget(2 * c.view + 18);
    const scope = new MemoryScope(pool);
    const operands = [col("left"), ...Array.from({ length: count - 1 }, () => col("right"))];
    const result = predicateSelection(
      batch,
      op === "and" ? and(...operands) : or(...operands),
      scope,
    );
    expect(result).toEqual(
      Uint8Array.from(op === "and" ? [1, 0, 2, 0, 0, 0, 2, 0, 2] : [1, 1, 1, 1, 0, 2, 1, 2, 2]),
    );
    expect(pool.peakBytes).toBe(2 * c.view + 18);
    expect(pool.usedBytes).toBe(c.view + 9);
    const owner = scope.retainAllocation(result);
    scope.dispose();
    expect(pool.usedBytes).toBe(c.view + 9);
    owner.release();
    expect(pool.usedBytes).toBe(0);
  }
  expect(materializeBatchRows(batch)).toEqual(
    left.map((value, index) => ({ left: value, right: right[index] })),
  );
});

it("reuses NOT backing without mutating source columns", () => {
  const pool = new MemoryBudget(c.view + 9);
  const scope = new MemoryScope(pool);
  expect(predicateSelection(batch, not(col("left")), scope)).toEqual(
    Uint8Array.from([0, 0, 0, 1, 1, 1, 2, 2, 2]),
  );
  expect(pool.peakBytes).toBe(c.view + 9);
  expect(materializeBatchRows(batch).map((row) => row.left)).toEqual(left);
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("cleans up on one-byte-short admission and later operand failure", () => {
  for (const cap of [17, 18]) {
    const pool = new MemoryBudget(2 * c.view + cap);
    const scope = new MemoryScope(pool);
    const expr = cap === 17 ? and(col("left"), col("right")) : and(col("left"), col("missing"));
    expect(() => predicateSelection(batch, expr, scope)).toThrow();
    expect(pool.usedBytes).toBe(0);
    scope.dispose();
  }
});
