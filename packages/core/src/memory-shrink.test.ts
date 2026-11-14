import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("shrinks exclusive child storage without releasing its live handle or changing peak", () => {
  const pool = new MemoryBudget(100);
  const sibling = pool.child(20).reserve(20, "sibling");
  const child = pool.child(80);
  const owner = child.reserve(80, "workspace");
  owner.shrink(17);
  expect(owner.bytes).toBe(17);
  expect(child.usedBytes).toBe(17);
  expect(pool.usedBytes).toBe(37);
  expect(pool.peakBytes).toBe(100);
  const alias = owner.retain();
  expect(alias.bytes).toBe(17);
  expect(() => owner.shrink(0)).toThrow(/exclusive/);
  alias.release();
  owner.shrink(0);
  expect(pool.usedBytes).toBe(20);
  owner.release();
  expect(() => owner.shrink(0)).toThrow(/released/);
  sibling.release();
  expect(pool.usedBytes).toBe(0);
});

it("rejects invalid or shared reductions atomically and composes with merge/partition", () => {
  const pool = new MemoryBudget(100);
  const child = pool.child(100);
  const owner = child.reserve(50, "state");
  const remote = owner.retainIn(pool.child(100));
  expect(() => owner.shrink(20)).toThrow(/exclusive/);
  remote.release();
  for (const bytes of [-1, NaN, Infinity, 0.5, 51]) {
    expect(() => owner.shrink(bytes)).toThrow();
    expect(owner.bytes).toBe(50);
    expect(pool.usedBytes).toBe(50);
  }
  owner.shrink(20);
  const combined = owner.merge(child.reserve(10, "more"));
  expect(combined.bytes).toBe(30);
  const parts = combined.partition([12, 18]);
  parts[0]?.shrink(2);
  expect(pool.usedBytes).toBe(20);
  for (const part of parts) part.release();
  expect(pool.usedBytes).toBe(0);
});

it("does not recycle a stage's consumed allocation allowance when storage shrinks", () => {
  const budget = new MemoryBudget(100);
  const scope = new MemoryScope(budget);
  scope.admit(100, "stage");
  const owner = scope.reserve(80, "workspace");
  owner.shrink(10);
  expect(budget.usedBytes).toBe(30);
  expect(() => scope.reserve(21, "beyond remaining stage allowance")).toThrow();
  const last = scope.reserve(20, "remaining allowance");
  scope.dispose();
  expect(budget.usedBytes).toBe(30);
  owner.release();
  last.release();
  expect(budget.usedBytes).toBe(0);
});
