import { expect, it } from "vitest";
import { type IntervalValue, intervalToString } from "./interval.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("admits interval formatting before conversion, including canonicalization", () => {
  const values: IntervalValue[] = [
    { kind: "interval", months: 0, days: 0, nanoseconds: "0" },
    { kind: "interval", months: -2, days: 3, nanoseconds: "000123" },
    { kind: "interval", months: 1e100, days: -1e100, nanoseconds: `-${"9".repeat(4096)}` },
  ];
  for (const value of values) {
    const expected = intervalToString(value);
    const scope = new MemoryScope(new MemoryBudget(65536));
    expect(intervalToString(value, scope)).toBe(expected);
    const used = scope.budget.usedBytes;
    expect(used).toBeGreaterThan(expected.length * 2);
    scope.dispose();
    const exact = new MemoryScope(new MemoryBudget(used));
    expect(intervalToString(value, exact)).toBe(expected);
    exact.dispose();
    const tight = new MemoryScope(new MemoryBudget(used - 1));
    expect(() => intervalToString(value, tight)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
    );
    expect(tight.budget.usedBytes).toBe(0);
    tight.dispose();
  }
});
it("releases formatting admission if parsing the nanoseconds fails", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(() =>
    intervalToString({ kind: "interval", months: 0, days: 0, nanoseconds: "invalid" }, scope),
  ).toThrow();
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
