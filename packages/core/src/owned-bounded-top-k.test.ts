import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { OwnedBoundedTopK } from "./owned-bounded-top-k.js";

it("owns stable winners and releases evicted payloads without growing with input count", () => {
  const pool = new MemoryBudget(65536);
  const parent = new MemoryScope(pool);
  const heap = new OwnedBoundedTopK<{ key: number; id: number }>(
    parent,
    3,
    (a, b) => a.key - b.key,
  );
  let plateau = 0;
  for (let id = 0; id < 100; id++) {
    const source = new MemoryScope(pool);
    const value = source.allocate(256, "test payload", () => ({ key: id % 5, id }));
    heap.offer(value);
    source.dispose();
    if (id === 10) plateau = pool.usedBytes;
    if (id > 10) expect(pool.usedBytes).toBe(plateau);
    expect(heap.size).toBeLessThanOrEqual(3);
  }
  const output = heap.finish();
  parent.dispose();
  expect(output.value).toEqual([
    { key: 0, id: 0 },
    { key: 0, id: 5 },
    { key: 0, id: 10 },
  ]);
  expect(pool.usedBytes).toBe(output.bytes);
  output.release();
  expect(pool.usedBytes).toBe(0);
});

it("closes partial heap mutations on comparison error without releasing caller ownership", () => {
  const pool = new MemoryBudget(65536);
  const parent = new MemoryScope(pool);
  const input = parent.allocate(256, "test payload", () => 1);
  const heap = new OwnedBoundedTopK<number>(parent, 2, () => {
    throw Error("comparison");
  });
  heap.offer(input);
  expect(() => heap.offer(input)).toThrow("comparison");
  expect(pool.usedBytes).toBe(256);
  expect(input.value).toBe(1);
  expect(() => heap.finish()).toThrow(/disposed/);
  parent.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("cleans up failed output admission while preserving an independently held input", () => {
  const pool = new MemoryBudget(65536);
  const parent = new MemoryScope(pool);
  const input = parent.allocate(256, "test payload", () => 7);
  const heap = new OwnedBoundedTopK<number>(parent, 1, (a, b) => a - b);
  heap.offer(input);
  const pressure = pool.reserve(pool.availableBytes, "sibling pressure");
  expect(() => heap.finish()).toThrow(/budget/i);
  pressure.release();
  expect(pool.usedBytes).toBe(256);
  expect(input.value).toBe(7);
  parent.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("preserves the source on cross-budget candidate admission failure", () => {
  const sourcePool = new MemoryBudget(65536);
  const targetPool = new MemoryBudget(4096);
  const source = new MemoryScope(sourcePool);
  const target = new MemoryScope(targetPool);
  const input = source.allocate(8192, "large candidate", () => ({ key: 1 }));
  const heap = new OwnedBoundedTopK<{ key: number }>(target, 1, (a, b) => a.key - b.key);
  expect(() => heap.offer(input)).toThrow(/budget/i);
  expect(targetPool.usedBytes).toBe(0);
  expect(sourcePool.usedBytes).toBe(8192);
  expect(input.value.key).toBe(1);
  target.dispose();
  source.dispose();
  expect(sourcePool.usedBytes).toBe(0);
});

it("does not retain candidates at zero capacity and independently owns empty output", () => {
  const pool = new MemoryBudget(4096);
  const scope = new MemoryScope(pool);
  const input = scope.allocate(256, "candidate", () => 1);
  const heap = new OwnedBoundedTopK<number>(scope, 0, (a, b) => a - b);
  const baseline = pool.usedBytes;
  expect(heap.offer(input)).toBe(false);
  expect(pool.usedBytes).toBe(baseline);
  const output = heap.finish();
  scope.dispose();
  expect(output.value).toEqual([]);
  expect(pool.usedBytes).toBe(output.bytes);
  output.release();
  expect(pool.usedBytes).toBe(0);
});

it("transfers only requested winner payloads and releases offset candidates", () => {
  const pool = new MemoryBudget(65536);
  const parent = new MemoryScope(pool);
  const heap = new OwnedBoundedTopK<number>(parent, 8, (a, b) => a - b);
  for (let value = 7; value >= 0; value--) {
    const source = new MemoryScope(pool);
    const input = source.allocate(1024, "candidate payload", () => value);
    heap.offer(input);
    source.dispose();
  }
  expect(() => heap.finish(-1, 2)).toThrow(/extent/);
  const output = heap.finish(5, 2);
  parent.dispose();
  expect(output.value).toEqual([5, 6]);
  expect(pool.usedBytes).toBe(output.bytes);
  expect(output.bytes).toBeLessThan(3 * 1024);
  output.release();
  expect(pool.usedBytes).toBe(0);
});
