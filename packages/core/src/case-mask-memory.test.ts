import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { batchExprValues, batchFromColumns } from "./batch.js";
import { col, type Expr, lit } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { vectorProjectBatch } from "./vector-project.js";

const batch = batchFromColumns({ a: [true, null, false, true], b: [true, true, null, false] });
const expression = (branches: number): Expr => ({
  kind: "case",
  whens: Array.from({ length: branches }, (_, index) => ({
    when: index === 0 ? col("a") : index === branches - 1 ? col("b") : lit(false),
    value: lit(index),
  })),
  else: lit(-1),
});

it.each([
  1, 2, 100, 255, 256,
])("uses compact first-match choices for %s CASE branches", (branches) => {
  const width = branches <= 255 ? 1 : 2;
  const peak =
    (branches === 1 ? c.view : 2 * c.view) + batch.rowCount * (branches === 1 ? 1 : width + 1);
  const budget = new MemoryBudget(peak);
  const scope = new MemoryScope(budget);
  const values = batchExprValues(batch, expression(branches), scope);
  expect([0, 1, 2, 3].map((index) => values.valueAt(index))).toEqual([
    0,
    branches === 1 ? -1 : branches - 1,
    -1,
    0,
  ]);
  expect(budget.peakBytes).toBe(peak);
  expect(budget.usedBytes).toBe(c.view + batch.rowCount * width);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => batchExprValues(batch, expression(branches), tight)).toThrow();
  tight.dispose();
  expect(tight.budget.usedBytes).toBe(0);
});

it("disposes failed CASE workspace without disposing its projection caller", () => {
  const scope = new MemoryScope(new MemoryBudget(2 * c.view + 7));
  expect(() => vectorProjectBatch(batch, [], { choice: expression(100) }, scope)).toThrow();
  expect(scope.disposed).toBe(false);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("returns NULL when no branch matches and ELSE is absent", () => {
  const expr: Expr = { kind: "case", whens: [{ when: lit(null), value: lit(10) }] };
  const scope = new MemoryScope(new MemoryBudget(2 * c.view + 8));
  const values = batchExprValues(batch, expr, scope);
  expect([0, 1, 2, 3].map((index) => values.valueAt(index))).toEqual([null, null, null, null]);
  scope.dispose();
});

it("requires no choice buffer for an empty CASE branch list", () => {
  const scope = new MemoryScope(new MemoryBudget(0));
  const values = batchExprValues(batch, { kind: "case", whens: [], else: lit(42) }, scope);
  expect(values.valueAt(0)).toBe(42);
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});
