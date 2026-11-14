import { expect, it } from "vitest";
import { decimalArithmetic, type ExactDecimalOperation } from "./decimal-arithmetic.js";
import { decimalFromParts } from "./decimal-value.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  ["add", 123456789987654321n, 9, 1n, 9, "123456789.987654322"],
  ["subtract", 123456789987654321n, 9, 1n, 9, "123456789.98765432"],
  ["add", -123n, 2, 124n, 2, "0.01"],
  ["subtract", 1n, 0, 1n, 3, "0.999"],
  ["multiply", -123n, 2, 20n, 1, "-2.46"],
  ["multiply", 9876543210123456789n, 9, 100n, 2, "9876543210.123456789"],
] as const)("computes %s with admitted exact storage", (operation, a, sa, b, sb, expected) => {
  const left = decimalFromParts(a, sa);
  const right = decimalFromParts(b, sb);
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  expect(String(decimalArithmetic(operation, left, right, scope))).toBe(expected);
  const peak = pool.peakBytes;
  expect(pool.usedBytes).toBeGreaterThan(0);
  expect(pool.usedBytes).toBeLessThan(peak);
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(String(decimalArithmetic(operation, left, right, exact))).toBe(expected);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => decimalArithmetic(operation, left, right, tight)).toThrow(/budget/i);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("rejects huge alignment before allocating powers or leaking reservations", () => {
  for (const scale of [1_000_000_000, Number.MAX_SAFE_INTEGER]) {
    const scope = new MemoryScope(new MemoryBudget(4096));
    const left = decimalFromParts(1n, scale);
    const right = decimalFromParts(1n, 0);
    for (const operation of ["add", "subtract"] as const) {
      expect(() => decimalArithmetic(operation, left, right, scope)).toThrowError(
        expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
      );
      expect(scope.budget.usedBytes).toBe(0);
      expect(scope.budget.peakBytes).toBe(0);
    }
    scope.dispose();
  }
});

it("does not expand zero operands and validates result scale before admission", () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  const tiny = decimalFromParts(1n, Number.MAX_SAFE_INTEGER);
  const zero = decimalFromParts(0n, 0);
  expect(String(decimalArithmetic("add", tiny, zero, scope))).toBe("1e-9007199254740991");
  expect(String(decimalArithmetic("add", zero, tiny, scope))).toBe("1e-9007199254740991");
  expect(String(decimalArithmetic("multiply", zero, tiny, scope))).toBe("0");
  expect(String(decimalArithmetic("subtract", zero, tiny, scope))).toBe("-1e-9007199254740991");
  expect(String(decimalArithmetic("add", tiny, tiny, scope))).toBe("2e-9007199254740991");
  const retained = scope.budget.usedBytes;
  expect(() => decimalArithmetic("multiply", tiny, tiny, scope)).toThrow(/result scale/);
  expect(scope.budget.usedBytes).toBe(retained);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("matches an independent aligned-integer oracle across operations", () => {
  let seed = 37;
  const random = () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0);
  for (let index = 0; index < 1000; index++) {
    const a = BigInt(random()) * ((random() >>> 16) & 1 ? 1n : -1n);
    const b = BigInt(random()) * ((random() >>> 16) & 1 ? 1n : -1n);
    const sa = random() % 20;
    const sb = random() % 20;
    for (const operation of ["add", "subtract", "multiply"] satisfies ExactDecimalOperation[]) {
      const scope = new MemoryScope(new MemoryBudget(65536));
      const result = decimalArithmetic(
        operation,
        decimalFromParts(a, sa),
        decimalFromParts(b, sb),
        scope,
      );
      const common = Math.max(sa, sb);
      const x = a * 10n ** BigInt(common - sa);
      const y = b * 10n ** BigInt(common - sb);
      expect(result.coefficient).toBe(
        operation === "multiply" ? a * b : operation === "add" ? x + y : x - y,
      );
      expect(result.scale).toBe(operation === "multiply" ? sa + sb : common);
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
    }
  }
});

it("keeps retained results charged while sibling arithmetic competes for the pool", () => {
  const a = decimalFromParts(123456789987654321n, 9);
  const b = decimalFromParts(5n, 2);
  const probe = new MemoryScope(new MemoryBudget(65536));
  decimalArithmetic("add", a, b, probe);
  const peak = probe.budget.peakBytes;
  probe.dispose();
  const pool = new MemoryBudget(peak);
  const first = new MemoryScope(pool.child());
  const second = new MemoryScope(pool.child());
  expect(String(decimalArithmetic("add", a, b, first))).toBe("123456790.037654321");
  const held = pool.usedBytes;
  expect(() => decimalArithmetic("add", a, b, second)).toThrow(/budget/i);
  expect(pool.usedBytes).toBe(held);
  expect(second.budget.usedBytes).toBe(0);
  first.dispose();
  expect(pool.usedBytes).toBe(0);
  const result = decimalArithmetic("add", a, b, second);
  expect(String(result)).toBe("123456790.037654321");
  second.dispose();
  expect(pool.usedBytes).toBe(0);
});
