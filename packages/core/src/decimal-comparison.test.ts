import { expect, it } from "vitest";
import { compareDecimalOperands } from "./decimal-comparison.js";
import {
  compareDecimalValues,
  decimalFromParts,
  withPreparedDecimalComparison,
} from "./decimal-value.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("compares exact decimals with represented binary64 values without decimal rounding", () => {
  for (const [coefficient, scale, number, order] of [
    [1n, 1, 0.1, -1],
    [-1n, 1, -0.1, 1],
    [5n, 1, 0.5, 0],
    [5n ** 1074n, 1074, Number.MIN_VALUE, 0],
    [-(5n ** 1074n), 1074, -Number.MIN_VALUE, 0],
    [0n, 1000, -0, 0],
    [9007199254740993n, 0, 9007199254740992, 1],
    [BigInt(Number.MAX_VALUE), 0, Number.MAX_VALUE, 0],
    [1n, Number.MAX_SAFE_INTEGER, Number.MIN_VALUE, -1],
    [-1n, Number.MAX_SAFE_INTEGER, -Number.MIN_VALUE, 1],
  ] as const) {
    const decimal = decimalFromParts(coefficient, scale);
    expect(compareDecimalOperands(decimal, number)).toBe(order);
    expect(compareDecimalOperands(number, decimal)).toBe(order === 0 ? 0 : -order);
  }
});

it("agrees with an independent rational oracle across binary64 exponents", () => {
  const view = new DataView(new ArrayBuffer(8));
  let state = 0x947abcf1;
  const random = () => (state = (Math.imul(state, 1664525) + 1013904223) >>> 0);
  for (let i = 0; i < 2000; i++) {
    const high = random();
    const low = random();
    view.setUint32(0, high);
    view.setUint32(4, low);
    const number = view.getFloat64(0);
    if (!Number.isFinite(number)) continue;
    const exponentBits = (high >>> 20) & 2047;
    const exponent = exponentBits === 0 ? -1074 : exponentBits - 1075;
    let numerator = BigInt(high & 0xfffff) * 4294967296n + BigInt(low);
    if (exponentBits !== 0) numerator += 4503599627370496n;
    if (high >>> 31) numerator = -numerator;
    const coefficient = BigInt(random() | 0);
    const scale = random() % 21;
    // Cross multiplication in the independent binary-rational domain. Never
    // use decimal conversion or the implementation's power-of-five expansion.
    const left = coefficient * (exponent < 0 ? 1n << BigInt(-exponent) : 1n);
    const right = numerator * (exponent > 0 ? 1n << BigInt(exponent) : 1n) * 10n ** BigInt(scale);
    expect(compareDecimalOperands(decimalFromParts(coefficient, scale), number)).toBe(
      left < right ? -1 : left > right ? 1 : 0,
    );
  }
});

it("admits numeric conversion before allocation and releases all comparison workspace", () => {
  for (const value of [0, 1, 0.1, Number.MIN_VALUE, Number.MAX_VALUE, 1234567890123456789n]) {
    const decimal = decimalFromParts(123n, 2);
    const scope = new MemoryScope(new MemoryBudget(65536));
    const expected = compareDecimalOperands(decimal, value, scope);
    const peak = scope.budget.peakBytes;
    expect(scope.budget.usedBytes).toBe(0);
    scope.dispose();
    const exact = new MemoryScope(new MemoryBudget(peak));
    expect(compareDecimalOperands(decimal, value, exact)).toBe(expected);
    expect(exact.budget.usedBytes).toBe(0);
    exact.dispose();
    const tight = new MemoryScope(new MemoryBudget(peak - 1));
    expect(() => compareDecimalOperands(decimal, value, tight)).toThrow(/budget/i);
    expect(tight.budget.usedBytes).toBe(0);
    tight.dispose();
  }
});

it("rejects nonnumeric and nonfinite operands without conversion hooks", () => {
  const decimal = decimalFromParts(1n, 0);
  for (const value of [NaN, Infinity, -Infinity, "1", true, {}, null, undefined]) {
    expect(() => compareDecimalOperands(decimal, value)).toThrow(/finite numeric/);
    expect(() => compareDecimalOperands(value, decimal)).toThrow(/finite numeric/);
  }
});

it("reuses prepared digit storage while preserving sign, scale and equality ordering", () => {
  for (const coefficient of [-1230n, 1230n]) {
    const literal = decimalFromParts(coefficient, 3);
    const scope = new MemoryScope(new MemoryBudget(65536));
    withPreparedDecimalComparison(literal, scope, (compare) => {
      let retained: number | undefined;
      for (const scale of [0, 2, 3, 1000, Number.MAX_SAFE_INTEGER]) {
        for (const coefficient of [-123n, -1230n, 0n, 123n, 1230n]) {
          const value = decimalFromParts(coefficient, scale);
          expect(compare(value)).toBe(compareDecimalValues(value, literal));
        }
        retained ??= scope.budget.usedBytes;
        expect(scope.budget.usedBytes).toBe(retained);
      }
      return 0;
    });
    expect(scope.budget.usedBytes).toBe(0);
    expect(() =>
      withPreparedDecimalComparison(literal, scope, (compare) => {
        compare(decimalFromParts(coefficient, 2));
        throw new Error("consumer failure");
      }),
    ).toThrow("consumer failure");
    expect(scope.budget.usedBytes).toBe(0);
    scope.dispose();
  }
});

it("admits repeated comparisons directly against a query budget sharing a bounded pool", () => {
  const value = decimalFromParts(1n, 1);
  const probe = new MemoryBudget(65536);
  expect(compareDecimalOperands(value, 0.1, probe)).toBe(-1);
  const extent = probe.peakBytes;
  const pool = new MemoryBudget(extent + 37);
  const peer = pool.child(37).reserve(37, "concurrent retained output");
  const query = pool.child(extent);
  for (let index = 0; index < 1000; index++) {
    expect(compareDecimalOperands(value, 0.1, query)).toBe(-1);
    expect(query.usedBytes).toBe(0);
    expect(pool.usedBytes).toBe(37);
  }
  expect(query.peakBytes).toBe(extent);
  expect(pool.peakBytes).toBe(extent + 37);
  const tight = pool.child(extent - 1);
  expect(() => compareDecimalOperands(value, 0.1, tight)).toThrow(/budget/i);
  expect(tight.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(37);
  peer.release();
  expect(pool.usedBytes).toBe(0);
});

it("keeps prepared comparison storage admitted through callbacks without an owning scope", () => {
  const right = decimalFromParts(1230n, 3);
  const left = decimalFromParts(123n, 2);
  const pool = new MemoryBudget(65536);
  const query = pool.child(65536);
  let retained = 0;
  withPreparedDecimalComparison(right, query, (compare) => {
    expect(compare(left)).toBe(0);
    retained = query.usedBytes;
    expect(retained).toBeGreaterThan(0);
    for (let index = 0; index < 1000; index++) {
      expect(compare(left)).toBe(0);
      expect(query.usedBytes).toBe(retained);
    }
    return 0;
  });
  const peak = query.peakBytes;
  expect(pool.usedBytes).toBe(0);
  for (const cap of [peak - 1, peak]) {
    const budget = new MemoryBudget(cap);
    const run = () => withPreparedDecimalComparison(right, budget, (compare) => compare(left));
    if (cap < peak) expect(run).toThrow(/budget/i);
    else expect(run()).toBe(0);
    expect(budget.usedBytes).toBe(0);
  }
  expect(() =>
    withPreparedDecimalComparison(right, query, (compare) => {
      compare(left);
      throw new Error("comparison consumer failed");
    }),
  ).toThrow("comparison consumer failed");
  expect(query.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(0);
});
