import { expect, it } from "vitest";
import { compareDecimalOperands } from "./decimal-comparison.js";
import { decimalFromParts } from "./decimal-value.js";
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
