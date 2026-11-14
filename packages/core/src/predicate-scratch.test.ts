import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, batchExprValues, predicateSelection } from "./batch.js";
import type { Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

const upper: Expr = { kind: "call", fn: "upper", args: [{ kind: "column", name: "text" }] };
const literal: Expr = { kind: "literal", value: "SS" };
const expressions: Expr[] = [
  { kind: "compare", op: "eq", left: upper, right: literal },
  {
    kind: "in",
    target: literal,
    values: [upper, { kind: "literal", value: null }],
    negated: false,
  },
  { kind: "null-check", target: upper, negated: true },
  { kind: "between", target: upper, low: literal, high: literal },
  {
    kind: "case",
    whens: [
      {
        when: { kind: "compare", op: "eq", left: upper, right: literal },
        value: { kind: "literal", value: true },
      },
    ],
    else: { kind: "literal", value: false },
  },
];
function fixture(rows: number): Batch {
  return {
    rowCount: rows,
    columns: {
      text: {
        type: "utf8-view",
        data: new TextEncoder().encode("ß".repeat(rows)),
        fixedLength: 2,
        length: rows,
      },
    },
  };
}
it.each(expressions)("bounds decoded predicate scratch for $kind", (expression) => {
  for (const rows of [1, 100]) {
    const batch = fixture(rows);
    const scope = new MemoryScope(new MemoryBudget(65536));
    expect([...predicateSelection(batch, expression, scope)]).toEqual(Array(rows).fill(1));
    const peak = scope.budget.peakBytes;
    expect(peak).toBeGreaterThan(rows);
    expect(scope.budget.usedBytes).toBe(c.view + rows);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
    const exact = new MemoryScope(new MemoryBudget(peak));
    expect([...predicateSelection(batch, expression, exact)]).toEqual(Array(rows).fill(1));
    exact.dispose();
    const tight = new MemoryScope(new MemoryBudget(peak - 1));
    expect(() => predicateSelection(batch, expression, tight)).toThrow(/budget/i);
    expect(tight.budget.usedBytes).toBe(0);
    tight.dispose();
  }
  const small = new MemoryScope(new MemoryBudget(65536));
  const large = new MemoryScope(new MemoryBudget(65536));
  predicateSelection(fixture(1), expression, small);
  predicateSelection(fixture(100), expression, large);
  // Masks may overlap for BETWEEN; decoded string scratch must not accumulate.
  expect(large.budget.peakBytes - small.budget.peakBytes).toBeLessThanOrEqual(2 * 99);
  small.dispose();
  large.dispose();
});

it("keeps IN candidate evaluation eager and releases scratch on a later error", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const expression: Expr = {
    kind: "in",
    target: literal,
    negated: false,
    values: [
      literal,
      { kind: "arithmetic", op: "add", left: upper, right: { kind: "literal", value: 1 } },
    ],
  };
  expect(() => predicateSelection(fixture(10), expression, scope)).toThrow(/numeric/);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

// Candidate expressions were historically evaluated before any comparisons.
it.each([null, "SS"])("preserves eager IN errors for target %s", (target) => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const expression: Expr = {
    kind: "in",
    target: { kind: "literal", value: target },
    negated: false,
    values: [
      { kind: "literal", value: true },
      { kind: "arithmetic", op: "add", left: upper, right: { kind: "literal", value: 1 } },
    ],
  };
  expect(() => predicateSelection(fixture(1), expression, scope)).toThrow(/numeric/);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it.each([20, 256])("releases nested CASE predicate workspaces across %s WHENs", (branches) => {
  const rows = 100;
  const when: Expr = {
    kind: "case",
    whens: [{ when: { kind: "literal", value: false }, value: { kind: "literal", value: true } }],
    else: { kind: "literal", value: false },
  };
  const expression: Expr = {
    kind: "case",
    whens: Array.from({ length: branches }, () => ({ when, value: literal })),
    else: upper,
  };
  const width = branches <= 255 ? 1 : 2;
  const scope = new MemoryScope(new MemoryBudget(3 * c.view + rows * (width + 2)));
  const values = batchExprValues(fixture(rows), expression, scope);
  // Only the outer branch choices survive compilation, regardless of branch count.
  expect(scope.budget.usedBytes).toBe(c.view + rows * width);
  expect(scope.budget.peakBytes).toBe(3 * c.view + rows * (width + 2));
  expect(values.valueAt(0)).toBe("SS");
  expect(values.valueAt(rows - 1)).toBe("SS");
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
