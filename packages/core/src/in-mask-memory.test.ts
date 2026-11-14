import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { batchFromColumns, predicateSelection } from "./batch.js";
import { col, isIn, not, notIn } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([false, true])("matches bounded linear IN fallback, with NULL=%s", (withNull) => {
  const input = Array.from({ length: 1000 }, (_, index) =>
    index === 0
      ? null
      : index === 1
        ? NaN
        : index === 2
          ? Infinity
          : index === 3
            ? -0
            : (index % 200) - 10,
  );
  const candidates = Array.from({ length: 128 }, (_, index) => index % 100);
  const literals = withNull ? [...candidates, null] : candidates;
  const batch = batchFromColumns({ value: input });
  for (const variant of ["in", "not-in", "not-wrapper"]) {
    const negated = variant !== "in";
    const expr =
      variant === "not-in"
        ? notIn("value", literals)
        : negated
          ? not(isIn("value", literals))
          : isIn("value", literals);
    const run = (cap: number) => {
      const budget = new MemoryBudget(cap);
      const scope = new MemoryScope(budget);
      try {
        const mask = predicateSelection(batch, expr, scope);
        expect(budget.usedBytes).toBe(c.view + input.length);
        return { mask: mask.slice(), peak: budget.peakBytes };
      } finally {
        scope.dispose();
        expect(budget.usedBytes).toBe(0);
      }
    };
    const linear = run(c.view + input.length);
    const indexed = run(c.view + input.length + candidates.length * 8);
    expect(indexed.mask).toEqual(linear.mask);
    expect(indexed.peak).toBe(c.view + input.length + candidates.length * 8);
    expect(run(indexed.peak - 1).peak).toBe(c.view + input.length);
    expect(indexed.mask[0]).toBe(2);
    expect(indexed.mask[3]).toBe(negated ? 0 : 1);
    const tooSmall = new MemoryScope(new MemoryBudget(c.view + input.length - 1));
    expect(() => predicateSelection(batch, expr, tooSmall)).toThrow();
    expect(tooSmall.budget.usedBytes).toBe(0);
    tooSmall.dispose();
  }
});

// Non-finite literals and row-varying candidates must keep scalar semantics.
it.each([NaN, Infinity, -Infinity])("falls back for non-finite literal %s", (literal) => {
  const batch = batchFromColumns({ value: Array.from({ length: 1000 }, (_, i) => i) });
  const scope = new MemoryScope(new MemoryBudget(10000));
  const result = predicateSelection(batch, isIn("value", [literal, ...Array(128).fill(-1)]), scope);
  expect([...result]).toEqual(Array(1000).fill(Number(Number.isNaN(literal))));
  expect(scope.budget.peakBytes).toBe(c.view + 1000);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("evaluates row-varying candidates without building a numeric literal lookup", () => {
  const batch = batchFromColumns({ value: [1, 2, null], other: [1, 3, 4] });
  const scope = new MemoryScope(new MemoryBudget(10000));
  const result = predicateSelection(
    batch,
    isIn("value", [...Array(128).fill(-1), col("other")]),
    scope,
  );
  expect([...result]).toEqual([1, 0, 2]);
  expect(scope.budget.peakBytes).toBe(c.view + 3);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
