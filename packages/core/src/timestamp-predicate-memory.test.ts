import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, predicateSelection } from "./batch.js";
import type { CompareOp, Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { type TimestampUnit, TimestampValue, timestampValueFromIso } from "./timestamp.js";

it.each([
  "millis",
  "micros",
  "nanos",
] as const)("compares timestamp literals exactly for %s columns", (unit: TimestampUnit) => {
  const scale = unit === "millis" ? 1000000n : unit === "micros" ? 1000n : 1n;
  const epochs = [-2n, -1n, 0n, 1n, 2n];
  const batch: Batch = {
    rowCount: epochs.length,
    columns: {
      t: { type: "timestamp", values: BigInt64Array.from(epochs), unit, isAdjustedToUTC: true },
    },
  };
  for (const nanos of [-scale - 1n, -1n, 0n, 1n, scale + 1n]) {
    for (const op of ["eq", "ne", "lt", "lte", "gt", "gte"] as CompareOp[]) {
      const expression: Expr = {
        kind: "compare",
        op,
        left: { kind: "column", name: "t" },
        right: { kind: "literal", value: new TimestampValue(nanos, "nanos") },
      };
      const run = (cap: number) => {
        const scope = new MemoryScope(new MemoryBudget(cap));
        try {
          const expected = epochs.map((epoch) => {
            const value = epoch * scale;
            return Number(
              op === "eq"
                ? value === nanos
                : op === "ne"
                  ? value !== nanos
                  : op === "lt"
                    ? value < nanos
                    : op === "lte"
                      ? value <= nanos
                      : op === "gt"
                        ? value > nanos
                        : value >= nanos,
            );
          });
          expect([...predicateSelection(batch, expression, scope)]).toEqual(expected);
          expect(scope.budget.usedBytes).toBe(c.view + epochs.length);
          return scope.budget.peakBytes;
        } finally {
          scope.dispose();
          expect(scope.budget.usedBytes).toBe(0);
        }
      };
      const peak = run(65536);
      expect(run(peak)).toBe(peak);
      expect(() => run(peak - 1)).toThrow(/budget/i);
    }
  }
});

it.each([
  "1969-12-31T23:59:59.999999999Z",
  "1970-01-01T00:00:00.000000001Z",
])("keeps ISO literal fractions for %s", (literal) => {
  const batch: Batch = {
    rowCount: 2,
    columns: {
      t: {
        type: "timestamp",
        values: BigInt64Array.of(0n, 0n),
        valid: new Uint8Array([1, 0]),
        unit: "millis",
        isAdjustedToUTC: true,
      },
    },
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect([
    ...predicateSelection(
      batch,
      {
        kind: "compare",
        op: "eq",
        left: { kind: "literal", value: literal },
        right: { kind: "column", name: "t" },
      },
      scope,
    ),
  ]).toEqual([0, 2]);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("admits ISO parser workspace and retains only its result", () => {
  const iso = "1969-12-31T23:59:59.999999999Z";
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(timestampValueFromIso(iso, scope)?.epochNanoseconds).toBe(-1n);
  const peak = scope.budget.peakBytes;
  expect(scope.budget.usedBytes).toBeGreaterThan(0);
  expect(scope.budget.usedBytes).toBeLessThan(peak);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(timestampValueFromIso(iso, exact)?.epochNanoseconds).toBe(-1n);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => timestampValueFromIso(iso, tight)).toThrow(/budget/i);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it.each([
  "invalid",
  "x".repeat(30),
  "2020-99-99T00:00:00Z",
  "x".repeat(100000),
])("releases invalid ISO parsing workspace", (text) => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(timestampValueFromIso(text, scope)).toBeUndefined();
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it.each([
  "compare",
  "in",
  "nullif",
])("admits %s parsing when literal reads allocate nothing", (kind) => {
  const batch: Batch = { rowCount: 100, columns: {} };
  const expression: Expr = {
    kind: "compare",
    op: "eq",
    left: { kind: "literal", value: new TimestampValue(1n, "nanos") },
    right: { kind: "literal", value: "1970-01-01T00:00:00.000000001Z" },
  };
  const selected: Expr =
    kind === "in"
      ? { kind: "in", target: expression.left, values: [expression.right], negated: false }
      : kind === "nullif"
        ? {
            kind: "null-check",
            target: { kind: "call", fn: "nullif", args: [expression.left, expression.right] },
            negated: false,
          }
        : expression;
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect([...predicateSelection(batch, selected, scope)]).toEqual(Array(100).fill(1));
  expect(scope.budget.usedBytes).toBe(c.view + 100);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => predicateSelection(batch, selected, tight)).toThrow(/budget/i);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});
