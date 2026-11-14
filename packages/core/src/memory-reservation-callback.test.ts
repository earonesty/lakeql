import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";

it("keeps detached ownership callbacks attached to their original reservation", () => {
  const pool = new MemoryBudget(16);
  const first = pool.reserve(8, "first");
  const { retain, retainIn, partition, shrink, release } = first;
  expect(first.release).toBe(release);
  const alias = retain();
  alias.release();
  const sibling = pool.child(8);
  const transferred = retainIn(sibling);
  transferred.release();
  shrink(6);
  expect(first.bytes).toBe(6);
  const parts = partition([2, 4]);
  release();
  expect(pool.usedBytes).toBe(6);
  const left = parts[0];
  const right = parts[1];
  if (!left || !right) throw new Error("Missing partition");
  const { merge } = left;
  const combined = merge(right);
  const finish = combined.release;
  finish();
  finish();
  expect(pool.usedBytes).toBe(0);
  expect(() => retain()).toThrow(/released/);
});

it("keeps release and retain usable when callers freeze a handle", () => {
  const pool = new MemoryBudget(8);
  const handle = Object.freeze(pool.reserve(8, "frozen"));
  const release = handle.release;
  const retained = handle.retain();
  release();
  expect(pool.usedBytes).toBe(8);
  retained.release();
  expect(pool.usedBytes).toBe(0);
});

it("preserves writable callback overrides before a method is first read", () => {
  const pool = new MemoryBudget(8);
  const handle = pool.reserve(8, "overridden");
  const originalRetain = handle.retain;
  handle.retainIn = () => originalRetain();
  const alias = handle.retainIn(pool);
  handle.release();
  alias.release();
  expect(pool.usedBytes).toBe(0);
});
