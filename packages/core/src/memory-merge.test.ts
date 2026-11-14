import { expect, it, vi } from "vitest";
import { MemoryBudget } from "./memory-budget.js";

it("merges exclusive reservations without changing charges or invoking reclamation", () => {
  const root = new MemoryBudget(10);
  const child = root.child(10);
  const first = child.reserve(3, "first");
  const second = child.reserve(7, "second");
  const reclaim = vi.fn();
  const detach = root.registerReclaimer(reclaim);
  const combined = first.merge(second);
  expect(combined.bytes).toBe(10);
  expect(child.usedBytes).toBe(10);
  expect(root.usedBytes).toBe(10);
  expect(root.peakBytes).toBe(10);
  expect(reclaim).not.toHaveBeenCalled();
  expect(() => first.retain()).toThrow(/released/);
  expect(() => second.retain()).toThrow(/released/);
  first.release();
  second.release();
  expect(root.usedBytes).toBe(10);
  const [left, right] = combined.partition([4, 6]);
  if (left === undefined || right === undefined) throw new Error("missing partition");
  left.merge(right).release();
  combined.release();
  expect(child.usedBytes).toBe(0);
  expect(root.usedBytes).toBe(0);
  detach();
});

it.each(["first", "second"])("preserves both handles when %s has another holder", (side) => {
  const root = new MemoryBudget(20);
  const child = root.child(20);
  const sibling = root.child(20);
  const first = child.reserve(4, "first");
  const second = child.reserve(6, "second");
  const shared = (side === "first" ? first : second).retainIn(sibling);
  expect(() => first.merge(second)).toThrow(/exclusive/);
  expect(root.usedBytes).toBe(10);
  const pin = first.retain();
  pin.release();
  shared.release();
  first.merge(second).release();
  expect(root.usedBytes).toBe(0);
  expect(child.usedBytes).toBe(0);
  expect(sibling.usedBytes).toBe(0);
});

it("rejects self, cross-budget and released reservations without consuming live admission", () => {
  const root = new MemoryBudget(30);
  const child = root.child(30);
  const first = root.reserve(10, "first");
  const other = child.reserve(10, "other");
  expect(() => first.merge(first)).toThrow(/itself/);
  expect(() => first.merge(other)).toThrow(/one budget/);
  expect(root.usedBytes).toBe(20);
  const released = root.reserve(10, "released");
  released.release();
  expect(() => first.merge(released)).toThrow(/released/);
  first.retain().release();
  other.retain().release();
  first.release();
  other.release();
  expect(root.usedBytes).toBe(0);
});

it("transfers zero extents and repeated growth without accumulating charges", () => {
  const pool = new MemoryBudget(1000);
  let merged = pool.reserve(0, "empty");
  for (let i = 0; i < 1000; i++) {
    merged = merged.merge(pool.reserve(1, "growth"));
    expect(merged.bytes).toBe(i + 1);
    expect(pool.usedBytes).toBe(i + 1);
  }
  merged = merged.merge(pool.reserve(0, "empty tail"));
  expect(pool.peakBytes).toBe(1000);
  merged.release();
  expect(pool.usedBytes).toBe(0);
});
