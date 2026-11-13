import { expect, it } from "vitest";
import { batchFromColumns, batchFromVectors, predicateSelection } from "./batch.js";
import { and, between, col, type Expr, eq, fn, gt, isIn, isNull, lit, not, or } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

const expressions: (Expr | undefined)[] = [
  undefined,
  gt("n", 2),
  between("n", 1, 4),
  isIn("n", [1, 3]),
  isNull("n"),
  not(or(eq("n", 1), and(gt("n", 2), isNull("s")))),
  fn("h3_in", col("s"), lit('["a","c"]')),
  { kind: "case", whens: [{ when: gt("n", 2), value: lit(true) }], else: lit(false) },
];

it.each(expressions)("admits all masks for predicate %j before allocation", (expr) => {
  const batch = batchFromColumns({ n: [1, null, 3, 4], s: ["a", "b", null, "c"] });
  const expected = predicateSelection(batch, expr);
  function run(maxBytes: number) {
    const budget = new MemoryBudget(maxBytes);
    const scope = new MemoryScope(budget);
    try {
      expect(predicateSelection(batch, expr, scope)).toEqual(expected);
      expect(budget.usedBytes).toBe(batch.rowCount);
      return budget.peakBytes;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  }
  const peak = run(65536);
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrow();
});

it("charges dictionary and expanded masks, preserving nullable dictionary results", () => {
  const dictionary = batchFromColumns({ n: [1, null, 3] }).columns.n;
  if (!dictionary) throw new Error("missing dictionary");
  const batch = batchFromVectors({
    n: { type: "dict", dictionary, indices: Uint32Array.of(2, 1, 0, 2) },
  });
  const budget = new MemoryBudget(7);
  const scope = new MemoryScope(budget);
  expect(Array.from(predicateSelection(batch, gt("n", 1), scope))).toEqual([1, 2, 0, 1]);
  expect(budget.peakBytes).toBe(7);
  expect(budget.usedBytes).toBe(4);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("releases failed predicate workspace while leaving the caller scope usable", () => {
  const batch = batchFromColumns({ n: [1, 2, 3, 4] });
  const budget = new MemoryBudget(7);
  const scope = new MemoryScope(budget);
  expect(() => predicateSelection(batch, between("n", 1, 3), scope)).toThrow();
  expect(scope.disposed).toBe(false);
  expect(budget.usedBytes).toBe(0);
  expect(() => predicateSelection(batch, lit(3), scope)).toThrow();
  expect(budget.usedBytes).toBe(0);
  expect(Array.from(predicateSelection(batch, gt("n", 2), scope))).toEqual([0, 0, 1, 1]);
  expect(budget.usedBytes).toBe(4);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("evaluates BETWEEN at two-mask capacity", () => {
  const batch = batchFromColumns({ n: [1, 2, 3, 4] });
  const budget = new MemoryBudget(8);
  const scope = new MemoryScope(budget);
  expect(Array.from(predicateSelection(batch, between("n", 1, 3), scope))).toEqual([1, 1, 1, 0]);
  expect(budget.peakBytes).toBe(8);
  expect(budget.usedBytes).toBe(4);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});
