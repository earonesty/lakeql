import { expect, it } from "vitest";
import { materializeBatchRows } from "./batch.js";
import { constantVector } from "./constant-vector.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  null,
  true,
  7,
  123n,
  "é😀",
])("represents constant scalar %s with dictionary storage", (value) => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  try {
    const vector = constantVector(value, 3, scope);
    expect(vector.type).toBe("dict");
    expect(materializeBatchRows({ rowCount: 3, columns: { value: vector } })).toEqual(
      Array.from({ length: 3 }, () => ({ value })),
    );
  } finally {
    scope.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});

it("admits one long payload instead of a payload per row", () => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const value = "é😀".repeat(1000);
  const vector = constantVector(value, 4096, scope);
  expect(pool.peakBytes).toBeLessThan(65536);
  expect(materializeBatchRows({ rowCount: 4096, columns: { value: vector } })[4095]).toEqual({
    value,
  });
  const peak = pool.peakBytes;
  scope.dispose();
  const exact = new MemoryBudget(peak);
  const exactScope = new MemoryScope(exact);
  constantVector(value, 4096, exactScope);
  exactScope.dispose();
  expect(exact.usedBytes).toBe(0);
  const short = new MemoryBudget(peak - 1);
  const shortScope = new MemoryScope(short);
  expect(() => constantVector(value, 4096, shortScope)).toThrow(/budget/i);
  expect(short.usedBytes).toBe(0);
  shortScope.dispose();
});

it("does not materialize an unused empty-vector payload", () => {
  const pool = new MemoryBudget(2048);
  const scope = new MemoryScope(pool);
  expect(
    materializeBatchRows({
      rowCount: 0,
      columns: { value: constantVector("x".repeat(100000), 0, scope) },
    }),
  ).toEqual([]);
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
});

it.each([
  -1,
  1.5,
  Infinity,
  NaN,
  0x100000001,
])("rejects invalid constant length %s before allocation", (rows) => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  expect(() => constantVector("value", rows, scope)).toThrow(/row count/i);
  expect(pool.usedBytes).toBe(0);
  scope.dispose();
});

it("owns mutable binary constant backing independently", () => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const bytes = new Uint8Array([1, 2]);
  const vector = constantVector(bytes, 2, scope);
  bytes.fill(9);
  expect(materializeBatchRows({ rowCount: 2, columns: { value: vector } })).toEqual([
    { value: new Uint8Array([1, 2]) },
    { value: new Uint8Array([1, 2]) },
  ]);
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
});
