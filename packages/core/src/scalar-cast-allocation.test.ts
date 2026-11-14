import { expect, it } from "vitest";
import { batchExprValues } from "./batch.js";
import type { Scalar } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { scalarStringWithAdmission } from "./scalar-cast-allocation.js";
import { TimestampValue } from "./timestamp.js";

it("admits scalar string casts before conversion and retains native results", () => {
  const values: Scalar[] = [
    null,
    true,
    false,
    NaN,
    Infinity,
    -Infinity,
    Number.MIN_VALUE,
    -0,
    0n,
    -(10n ** 1000n),
    new Uint8Array([0, 12, 255]),
    new Uint8Array(),
    new TimestampValue(-123456789n, "nanos", false),
    new TimestampValue(10n ** 100n, "nanos", true),
    { kind: "interval", months: 1, days: 2, nanoseconds: "3" },
  ];
  for (const value of values) {
    const scope = new MemoryScope(new MemoryBudget(65536));
    expect(scalarStringWithAdmission(value, scope)).toBe(String(value));
    const bytes = scope.budget.usedBytes;
    expect(bytes).toBeGreaterThan(0);
    scope.dispose();
    const exact = new MemoryScope(new MemoryBudget(bytes));
    expect(scalarStringWithAdmission(value, exact)).toBe(String(value));
    exact.dispose();
    const rejected = new MemoryScope(new MemoryBudget(bytes - 1));
    expect(() => scalarStringWithAdmission(value, rejected)).toThrow(/budget/i);
    expect(rejected.budget.usedBytes).toBe(0);
    rejected.dispose();
  }
});

it("borrows existing strings and propagates cast admission through expressions", () => {
  const empty = new MemoryScope(new MemoryBudget(0));
  expect(scalarStringWithAdmission("é😀", empty)).toBe("é😀");
  expect(empty.budget.usedBytes).toBe(0);
  const values = batchExprValues(
    { rowCount: 1, columns: {} },
    {
      kind: "call",
      fn: "cast",
      args: [
        { kind: "literal", value: 123n },
        { kind: "literal", value: "string" },
      ],
    },
  );
  expect(() => values.valueAt(0, empty)).toThrow(/budget/i);
  empty.dispose();
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(values.valueAt(0, scope)).toBe("123");
  expect(scope.budget.usedBytes).toBeGreaterThan(0);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
