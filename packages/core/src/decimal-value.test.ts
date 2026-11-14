import { expect, it } from "vitest";
import {
  compareDecimalValues,
  decimalFormatAllocationBytes,
  decimalFromParts,
  decimalToString,
} from "./decimal-value.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  [123456789987654321n, 9, "123456789.987654321"],
  [-9876543210123456789n, 9, "-9876543210.123456789"],
  [12300n, 4, "1.23"],
  [12300n, 2, "123"],
  [1000n, 0, "1000"],
  [1n, 6, "0.000001"],
  [1n, 7, "1e-7"],
  [10n ** 20n, 0, "100000000000000000000"],
  [10n ** 21n, 0, "1e+21"],
  [0n, Number.MAX_SAFE_INTEGER, "0"],
  [1n, Number.MAX_SAFE_INTEGER, "1e-9007199254740991"],
  [-100n, Number.MAX_SAFE_INTEGER, "-1e-9007199254740989"],
] as const)("formats exact coefficient %s at scale %s", (coefficient, scale, expected) => {
  const value = decimalFromParts(coefficient, scale);
  expect(String(value)).toBe(expected);
  expect(JSON.stringify(value)).toBe(JSON.stringify(expected));
  const bound = decimalFormatAllocationBytes(value);
  const scope = new MemoryScope(new MemoryBudget(bound));
  expect(decimalToString(value, scope)).toBe(expected);
  expect(scope.budget.usedBytes).toBe(bound);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(bound - 1));
  expect(() => decimalToString(value, tight)).toThrow(/budget/i);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("compares with an independent integer-alignment oracle over bounded random scales", () => {
  let seed = 17;
  const random = () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0);
  const scope = new MemoryScope(new MemoryBudget(4096));
  for (let index = 0; index < 5000; index++) {
    const a = BigInt(random()) * ((random() >>> 16) & 1 ? -1n : 1n);
    const b = BigInt(random()) * ((random() >>> 16) & 1 ? -1n : 1n);
    const sa = random() % 32;
    const sb = random() % 32;
    const x = a * 10n ** BigInt(sb);
    const y = b * 10n ** BigInt(sa);
    const expected = x < y ? -1 : x > y ? 1 : 0;
    expect(compareDecimalValues(decimalFromParts(a, sa), decimalFromParts(b, sb), scope)).toBe(
      expected,
    );
    expect(scope.budget.usedBytes).toBe(0);
  }
  scope.dispose();
});

it("compares equivalent coefficients and huge scale gaps without expanded workspace", () => {
  const scope = new MemoryScope(new MemoryBudget(512));
  const cases = [
    [12n, 1, 1200n, 3, 0],
    [-12n, 1, -1200n, 3, 0],
    [0n, 0, 0n, Number.MAX_SAFE_INTEGER, 0],
    [1n, 0, 1n, Number.MAX_SAFE_INTEGER, 1],
    [-1n, 0, -1n, Number.MAX_SAFE_INTEGER, -1],
    [1n, Number.MAX_SAFE_INTEGER, 10n, Number.MAX_SAFE_INTEGER, -1],
    [123n, 2, 12301n, 4, -1],
    [-123n, 2, -12301n, 4, 1],
    [0n, 0, -1n, Number.MAX_SAFE_INTEGER, 1],
  ] as const;
  for (const [a, sa, b, sb, expected] of cases)
    expect(compareDecimalValues(decimalFromParts(a, sa), decimalFromParts(b, sb), scope)).toBe(
      expected,
    );
  expect(scope.budget.peakBytes).toBeLessThan(512);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("admits scalar backing and rejects implicit numeric coercion", () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  const value = decimalFromParts(123n, 2, scope);
  const bytes = scope.budget.usedBytes;
  expect(bytes).toBeGreaterThan(0);
  expect(Object.isFrozen(value)).toBe(true);
  expect(() => Number(value)).toThrow(/explicit numeric/);
  expect(() => +value).toThrow(/explicit numeric/);
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(bytes));
  expect(String(decimalFromParts(123n, 2, exact))).toBe("1.23");
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => decimalFromParts(123n, 2, tight)).toThrow(/budget/i);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it.each([
  -1,
  0.5,
  Infinity,
  NaN,
  Number.MAX_SAFE_INTEGER + 1,
])("rejects invalid scale %s before admission", (scale) => {
  const scope = new MemoryScope(new MemoryBudget(0));
  expect(() => decimalFromParts(1n, scale, scope)).toThrow(/safe integer scale/);
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});

it("rejects comparison workspace before formatting and releases successful reservations", () => {
  const a = decimalFromParts(123n, 1);
  const b = decimalFromParts(12345n, 2);
  const probe = new MemoryScope(new MemoryBudget(4096));
  expect(compareDecimalValues(a, b, probe)).toBe(-1);
  const bytes = probe.budget.peakBytes;
  expect(probe.budget.usedBytes).toBe(0);
  probe.dispose();
  const exact = new MemoryScope(new MemoryBudget(bytes));
  expect(compareDecimalValues(a, b, exact)).toBe(-1);
  expect(exact.budget.usedBytes).toBe(0);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => compareDecimalValues(a, b, tight)).toThrow(/budget/i);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("preserves random exact coefficients and canonicalizes equivalent scales", () => {
  let seed = 91;
  const random = () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0);
  for (let index = 0; index < 1000; index++) {
    const coefficient = (BigInt(random()) << 64n) | (BigInt(random()) << 32n) | BigInt(random());
    const signed = (random() >>> 16) & 1 ? coefficient : -coefficient;
    const scale = random() % 100;
    const value = decimalFromParts(signed, scale);
    const text = String(value);
    const match = /^(-?)(\d+)(?:\.(\d+))?(?:e([+-]?\d+))?$/.exec(text);
    expect(match).not.toBeNull();
    if (!match) throw new Error("Invalid decimal text");
    const fraction = match[3] ?? "";
    const decoded = BigInt(match[1] + match[2] + fraction);
    const exponent = Number(match[4] ?? 0) - fraction.length;
    const common = Math.max(scale, -exponent);
    expect(decoded * 10n ** BigInt(common + exponent)).toBe(signed * 10n ** BigInt(common - scale));
    expect(String(decimalFromParts(signed * 1000n, scale + 3))).toBe(text);
  }
});
