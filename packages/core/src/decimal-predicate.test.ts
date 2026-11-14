import { expect, it, vi } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, batchFromColumns, predicateSelection } from "./batch.js";
import { compareDecimalOperands } from "./decimal-comparison.js";
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
      expect(predicateSelection(batch, expr, tight).length).toBe(count);
      tight.dispose();
      expect(tight.budget.usedBytes).toBe(0);
      const minimum = count + scalarComparisonPeak(left, right);
      const insufficient = new MemoryScope(new MemoryBudget(minimum - 1));
      expect(() => predicateSelection(batch, expr, insufficient)).toThrow(/budget/i);
      expect(insufficient.budget.usedBytes).toBe(0);
      insufficient.dispose();
    }
  }
});

it("compares mixed numeric union batches using exact represented values", () => {
  const values = [decimalFromParts(1n, 1), 0.1, 0n, 0.5, decimalFromParts(5n, 1), null];
  const batch = batchFromColumns({ value: values });
  const expr = gt(col("value"), decimalFromParts(1n, 1));
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(Array.from(predicateSelection(batch, expr, scope))).toEqual([0, 1, 0, 1, 1, 2]);
  expect(values.map((value) => evaluate(expr, { value }))).toEqual([
    false,
    true,
    false,
    true,
    true,
    null,
  ]);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const exactIntegers = batchFromColumns({ value: [decimalFromParts(9007199254740993n, 0)] });
  expect(Array.from(predicateSelection(exactIntegers, gt("value", 9007199254740992)))).toEqual([1]);
});

it("rejects nonnumeric decimal coercion in both evaluators", () => {
  const value = decimalFromParts(1n, 0);
  const batch = batchFromColumns({ value: [value] });
  for (const other of ["1", true, NaN, Infinity]) {
    const expr = eq(col("value"), lit(other));
    expect(() => evaluate(expr, { value })).toThrow(/finite numeric/);
    expect(() => predicateSelection(batch, expr)).toThrow(/finite numeric/);
  }
});

it("prepares numeric literals once for direct and dictionary decimal vectors", () => {
  const decimal = decimalFromParts(1n, 1000);
  for (const dictionary of [false, true]) {
    const source = batchFromColumns({ value: Array(10000).fill(decimal) });
    const vector = source.columns.value;
    if (vector === undefined) throw new Error("missing test vector");
    const batch: Batch = dictionary
      ? {
          rowCount: 10000,
          columns: { value: { type: "dict", dictionary: vector, indices: new Uint32Array(10000) } },
        }
      : source;
    const scope = new MemoryScope(new MemoryBudget(65536));
    const reader = vi.spyOn(DataView.prototype, "setFloat64");
    try {
      expect(
        predicateSelection(batch, lt("value", Number.MIN_VALUE), scope).every(
          (value) => value === 1,
        ),
      ).toBe(true);
      expect(reader).toHaveBeenCalledTimes(1);
    } finally {
      reader.mockRestore();
      scope.dispose();
    }
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("preserves all-null, empty, reversed and nullable literal comparison semantics", () => {
  const decimal = decimalFromParts(1n, 1);
  for (const values of [[], [decimal, decimal]]) {
    const batch: Batch = {
      rowCount: values.length,
      columns: {
        value: { type: "decimal", values, valid: new Uint8Array(values.length) },
      },
    };
    expect(Array.from(predicateSelection(batch, eq("value", "invalid")))).toEqual(
      values.map(() => 2),
    );
  }
  const values = [null, decimal, null, decimalFromParts(5n, 1)];
  const batch = batchFromColumns({ value: values });
  for (const make of [eq, ne, lt, lte, gt, gte]) {
    for (const expr of [make("value", 0.1), make(lit(0.1), col("value"))]) {
      expect(Array.from(predicateSelection(batch, expr))).toEqual(
        values.map((value) => {
          const expected = evaluate(expr, { value });
          return expected === null ? 2 : expected ? 1 : 0;
        }),
      );
    }
  }
});

it("admits one prepared literal with the output and rolls back callback failures", () => {
  const batch = batchFromColumns({ value: Array(1000).fill(decimalFromParts(1n, 1000)) });
  const expr = lt("value", Number.MIN_VALUE);
  const scope = new MemoryScope(new MemoryBudget(65536));
  predicateSelection(batch, expr, scope);
  const peak = scope.budget.peakBytes;
  expect(scope.budget.usedBytes).toBe(c.view + 1000);
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(predicateSelection(batch, expr, exact).length).toBe(1000);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(predicateSelection(batch, expr, tight).every((value) => value === 1)).toBe(true);
  expect(tight.budget.usedBytes).toBe(c.view + 1000);
  tight.dispose();
  const minimum =
    c.view + 1000 + scalarComparisonPeak(decimalFromParts(1n, 1000), Number.MIN_VALUE);
  const fallback = new MemoryScope(new MemoryBudget(minimum));
  expect(predicateSelection(batch, expr, fallback).length).toBe(1000);
  fallback.dispose();
  const insufficient = new MemoryScope(new MemoryBudget(minimum - 1));
  expect(() => predicateSelection(batch, expr, insufficient)).toThrow(/budget/i);
  expect(insufficient.budget.usedBytes).toBe(0);
  insufficient.dispose();
  const invalid: Batch = {
    rowCount: 1,
    columns: { value: { type: "decimal", values: new Array(1) } },
  };
  const rejected = new MemoryScope(new MemoryBudget(65536));
  expect(() => predicateSelection(invalid, expr, rejected)).toThrow(/decimal vector/);
  expect(rejected.budget.usedBytes).toBe(0);
  rejected.dispose();
});

function scalarComparisonPeak(left: unknown, right: unknown): number {
  const scope = new MemoryScope(new MemoryBudget(65536));
  compareDecimalOperands(left, right, scope);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  return peak;
}

it("retries once after late preparation exhaustion without releasing sibling reservations", () => {
  const literal = decimalFromParts(1n, 1);
  const values = [
    decimalFromParts(1n, 3),
    decimalFromParts(10n ** 1000n, 1000),
    null,
    decimalFromParts(-1n, 0),
  ];
  const batch = batchFromColumns({ value: values });
  const peak = Math.max(
    ...values
      .filter((value) => value !== null)
      .map((value) => scalarComparisonPeak(value, literal)),
  );
  const pool = new MemoryBudget(65536);
  const siblingBytes = pool.maxBytes - (c.view + values.length + peak);
  const sibling = pool.reserve(siblingBytes, "retained sibling output");
  const scope = new MemoryScope(pool);
  const reserve = vi.spyOn(MemoryScope.prototype, "reserve");
  try {
    expect(Array.from(predicateSelection(batch, lt("value", literal), scope))).toEqual([
      1, 0, 2, 1,
    ]);
    expect(
      reserve.mock.calls.filter(([, purpose]) => purpose === "prepared decimal comparison state"),
    ).toHaveLength(1);
    expect(pool.usedBytes).toBe(siblingBytes + c.view + values.length);
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
  } finally {
    reserve.mockRestore();
    scope.dispose();
  }
  expect(pool.usedBytes).toBe(siblingBytes);
  sibling.release();
  expect(pool.usedBytes).toBe(0);
});

it("keeps same-scale comparison usable with only the output-mask budget", () => {
  const batch = batchFromColumns({
    value: [decimalFromParts(1n, 2), null, decimalFromParts(3n, 2)],
  });
  const scope = new MemoryScope(new MemoryBudget(c.view + 3));
  expect(
    Array.from(predicateSelection(batch, lt("value", decimalFromParts(2n, 2)), scope)),
  ).toEqual([1, 2, 0]);
  expect(scope.budget.usedBytes).toBe(c.view + 3);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
