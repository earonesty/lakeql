import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, predicateSelection } from "./batch.js";
import type { Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  -Number.MAX_SAFE_INTEGER,
  -1,
  0,
  1,
  Number.MAX_SAFE_INTEGER,
  1n,
])("bounds bigint fast comparison scratch for literal %s", (literal) => {
  const numbers = [
    -(2n ** 63n),
    -9007199254740991n,
    -1n,
    0n,
    1n,
    9007199254740991n,
    2n ** 63n - 1n,
  ];
  const batch: Batch = {
    rowCount: numbers.length,
    columns: { value: { type: "i64", values: BigInt64Array.from(numbers) } },
  };
  const expression: Expr = {
    kind: "compare",
    op: "gte",
    left: { kind: "column", name: "value" },
    right: { kind: "literal", value: literal },
  };
  const scope = new MemoryScope(new MemoryBudget(numbers.length + c.bigint));
  expect([...predicateSelection(batch, expression, scope)]).toEqual(
    numbers.map((value) => Number(value >= literal)),
  );
  expect(scope.budget.peakBytes).toBe(numbers.length + c.bigint);
  expect(scope.budget.usedBytes).toBe(numbers.length);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(numbers.length + c.bigint - 1));
  expect(() => predicateSelection(batch, expression, tight)).toThrow(/budget/i);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("needs no bigint scalar workspace for an empty vector", () => {
  const batch: Batch = {
    rowCount: 0,
    columns: { value: { type: "u64", values: new BigUint64Array() } },
  };
  const scope = new MemoryScope(new MemoryBudget(0));
  expect(
    predicateSelection(
      batch,
      {
        kind: "compare",
        op: "eq",
        left: { kind: "column", name: "value" },
        right: { kind: "literal", value: 0 },
      },
      scope,
    ),
  ).toHaveLength(0);
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});
