import { expect, it, vi } from "vitest";
import { batchFromColumns, predicateSelection } from "./batch.js";
import { decimalFromParts } from "./decimal-value.js";
import { evaluate } from "./evaluator.js";
import { between, col, eq, gt, gte, isIn, lit, lt, lte, ne } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("agrees across row and vector decimal comparisons with nulls and different scales", () => {
  const values = [
    decimalFromParts(-101n, 2),
    decimalFromParts(10n, 1),
    decimalFromParts(100n, 2),
    decimalFromParts(101n, 2),
    null,
  ];
  const batch = batchFromColumns({ value: values });
  for (const make of [eq, ne, lt, lte, gt, gte]) {
    const expr = make(col("value"), lit(decimalFromParts(1n, 0)));
    const scope = new MemoryScope(new MemoryBudget(65536));
    expect(Array.from(predicateSelection(batch, expr, scope))).toEqual(
      values.map((value) => {
        const result = evaluate(expr, { value });
        return result === null ? 2 : result ? 1 : 0;
      }),
    );
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
  for (const expr of [
    isIn(col("value"), [decimalFromParts(1n, 0), null]),
    between(col("value"), decimalFromParts(1n, 0), decimalFromParts(1010n, 3)),
  ]) {
    expect(Array.from(predicateSelection(batch, expr))).toEqual(
      values.map((value) => {
        const result = evaluate(expr, { value });
        return result === null ? 2 : result ? 1 : 0;
      }),
    );
  }
});

it("bounds comparison workspace independently of scale and row count", () => {
  for (const scale of [3, 1000, Number.MAX_SAFE_INTEGER]) {
    const left = decimalFromParts(123n, scale);
    const right = decimalFromParts(1230n, scale - 1);
    const expr = lt(col("value"), lit(right));
    let workspace: number | undefined;
    for (const count of [1, 10000]) {
      const batch = batchFromColumns({ value: Array(count).fill(left) });
      const scope = new MemoryScope(new MemoryBudget(65536));
      const children = vi.spyOn(MemoryScope.prototype, "childScope");
      expect(predicateSelection(batch, expr, scope).every((value) => value === 1)).toBe(true);
      const scratch = scope.budget.peakBytes - scope.budget.usedBytes;
      workspace ??= scratch;
      expect(scratch).toBe(workspace);
      expect(children).not.toHaveBeenCalled();
      children.mockRestore();
      const peak = scope.budget.peakBytes;
      scope.dispose();
      const exact = new MemoryScope(new MemoryBudget(peak));
      expect(predicateSelection(batch, expr, exact).length).toBe(count);
      exact.dispose();
      const tight = new MemoryScope(new MemoryBudget(peak - 1));
      expect(() => predicateSelection(batch, expr, tight)).toThrow(/budget/i);
      tight.dispose();
      expect(tight.budget.usedBytes).toBe(0);
    }
  }
});

it("rejects implicit decimal coercion in both evaluators", () => {
  const value = decimalFromParts(1n, 0);
  const batch = batchFromColumns({ value: [value] });
  for (const other of [1, 1n, "1", true]) {
    const expr = eq(col("value"), lit(other));
    expect(() => evaluate(expr, { value })).toThrow(/decimal operands/);
    expect(() => predicateSelection(batch, expr)).toThrow(/decimal operands/);
  }
});
