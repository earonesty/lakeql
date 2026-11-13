import { describe, expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";

describe("allocation memory budget", () => {
  it("detaches reclaimers and keeps admission correct after a throwing reclaimer", () => {
    const pool = new MemoryBudget(10);
    const held = pool.reserve(10, "held");
    const detach = pool.registerReclaimer(() => {
      throw new Error("reclaim failed");
    });
    expect(() => pool.reserve(1, "next")).toThrow("reclaim failed");
    detach();
    detach();
    expect(pool.tryReserve(1, "next")).toBeUndefined();
    const remove = pool.registerReclaimer(() => held.release());
    const next = pool.reserve(10, "next");
    expect(pool.usedBytes).toBe(10);
    next.release();
    remove();
    expect(() => pool.tryReserve(-1, "invalid")).toThrow();
  });

  it("admits exact limits and rejects one byte over atomically across jobs", () => {
    const pool = new MemoryBudget(64 * 1024 * 1024);
    const first = pool.child();
    const second = pool.child();
    const a = first.reserve(32 * 1024 * 1024, "decode");
    const b = second.reserve(32 * 1024 * 1024, "range");
    expect(() => second.reserve(1, "copy")).toThrow("Allocation exceeds");
    expect(second.usedBytes).toBe(b.bytes);
    expect(pool.usedBytes).toBe(pool.maxBytes);
    a.release();
    expect(second.availableBytes).toBe(a.bytes);
    b.release();
    expect(pool.usedBytes).toBe(0);
    expect(pool.peakBytes).toBe(pool.maxBytes);
  });

  it("enforces a smaller query limit without consuming shared capacity", () => {
    const pool = new MemoryBudget(128);
    const query = pool.child(32);
    expect(() => query.reserve(33, "operator")).toThrow();
    expect(pool.usedBytes).toBe(0);
    expect(query.peakBytes).toBe(0);
    const lease = query.reserve(32, "operator");
    expect(query.availableBytes).toBe(0);
    lease.release();
  });

  it("keeps evicted cache allocations charged until every reader releases", () => {
    const pool = new MemoryBudget(100);
    const cached = pool.reserve(100, "cache");
    const reader = cached.retain();
    const anotherReader = reader.retain();
    cached.release();
    cached.release();
    expect(pool.usedBytes).toBe(100);
    expect(() => cached.retain()).toThrow("Cannot retain released");
    reader.release();
    expect(pool.usedBytes).toBe(100);
    anotherReader.release();
    expect(pool.usedBytes).toBe(0);
  });

  it("charges replacement overlap and releases reservations on allocation failure", () => {
    const pool = new MemoryBudget(100);
    const old = pool.reserve(60, "old buffer");
    expect(() => pool.reserve(60, "replacement")).toThrow();
    const temporary = pool.reserve(40, "temporary");
    try {
      throw new Error("decode failed");
    } catch {
      temporary.release();
    }
    expect(pool.usedBytes).toBe(60);
    old.release();
    expect(pool.usedBytes).toBe(0);
  });

  it("validates byte counts and supports zero-capacity pools", () => {
    for (const bytes of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => new MemoryBudget(bytes)).toThrow();
      expect(() => new MemoryBudget(100).reserve(bytes, "invalid")).toThrow();
    }
    const pool = new MemoryBudget(0);
    const zero = pool.reserve(0, "empty");
    zero.release();
    expect(pool.usedBytes).toBe(0);
    expect(() => pool.reserve(1, "nonempty")).toThrow();
  });
});

it("charges each reader's budget while counting shared ancestors once", () => {
  const pool = new MemoryBudget(8);
  const firstJob = pool.child(8);
  const secondJob = pool.child(8);
  const first = firstJob.reserve(8, "shared backing");
  const second = first.retainIn(secondJob);
  expect(firstJob.usedBytes).toBe(8);
  expect(secondJob.usedBytes).toBe(8);
  expect(pool.usedBytes).toBe(8);
  expect(pool.peakBytes).toBe(8);
  const duplicate = second.retain();
  expect(secondJob.usedBytes).toBe(8);
  first.release();
  expect(firstJob.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(8);
  second.release();
  expect(secondJob.usedBytes).toBe(8);
  duplicate.release();
  expect(secondJob.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(0);
});

it("promotes cache ownership to a parent without double charging and checks new reader caps", () => {
  const pool = new MemoryBudget(8);
  const producer = pool.child(8);
  const input = producer.reserve(8, "input");
  const cached = input.retainIn(pool);
  input.release();
  expect(producer.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(8);
  const small = pool.child(7);
  expect(() => cached.retainIn(small)).toThrow("Allocation exceeds memory budget");
  expect(small.usedBytes).toBe(0);
  expect(() => cached.retainIn(new MemoryBudget(8))).toThrow("shared root budget");
  cached.release();
  expect(pool.usedBytes).toBe(0);
  expect(() => cached.retainIn(producer)).toThrow("released memory");
});

it("rejects reader admission atomically when its existing allocations leave insufficient headroom", () => {
  const pool = new MemoryBudget(24);
  const branch = pool.child(24);
  const writer = branch.child(16);
  const reader = branch.child(16);
  const backing = writer.reserve(8, "backing");
  const existing = reader.reserve(9, "existing output");
  expect(() => backing.retainIn(reader)).toThrow("Allocation exceeds memory budget");
  expect(reader.usedBytes).toBe(9);
  expect(writer.usedBytes).toBe(8);
  expect(branch.usedBytes).toBe(17);
  expect(pool.usedBytes).toBe(17);
  existing.release();
  const pin = backing.retainIn(reader);
  expect(branch.usedBytes).toBe(8);
  backing.release();
  pin.release();
  expect(pool.usedBytes).toBe(0);
});

it("pins backing throughout reentrant target-budget reclamation", () => {
  const pool = new MemoryBudget(16);
  const sourceBudget = pool.child(8);
  const targetBudget = pool.child(8);
  const source = sourceBudget.reserve(8, "source backing");
  const existing = targetBudget.reserve(8, "old target output");
  let chargedDuringReclaim = 0;
  const detach = targetBudget.registerReclaimer(() => {
    // Cache eviction can remove the very handle currently being acquired.
    source.release();
    existing.release();
    chargedDuringReclaim = pool.usedBytes;
  });
  const target = source.retainIn(targetBudget);
  expect(chargedDuringReclaim).toBe(8);
  expect(sourceBudget.usedBytes).toBe(0);
  expect(targetBudget.usedBytes).toBe(8);
  expect(pool.usedBytes).toBe(8);
  target.release();
  detach();
  expect(pool.usedBytes).toBe(0);
});

it("preserves shared ancestry charges across nested branch retention and release", () => {
  const root = new MemoryBudget(16);
  const common = root.child(12);
  const left = common.child(8).child(8);
  const right = common.child(8).child(8);
  const allocation = left.reserve(8, "nested backing");
  const retained = allocation.retainIn(right);
  expect([root.usedBytes, common.usedBytes, left.usedBytes, right.usedBytes]).toEqual([8, 8, 8, 8]);
  expect(() => right.reserve(1, "child full")).toThrow(/budget/);
  allocation.release();
  expect([root.usedBytes, common.usedBytes, left.usedBytes, right.usedBytes]).toEqual([8, 8, 0, 8]);
  const next = left.reserve(4, "second backing");
  expect(root.usedBytes).toBe(12);
  expect(() => left.reserve(1, "shared ancestor full")).toThrow(/budget/);
  retained.release();
  expect(root.usedBytes).toBe(4);
  next.release();
  expect([root.usedBytes, common.usedBytes, left.usedBytes, right.usedBytes]).toEqual([0, 0, 0, 0]);
});

it("retains within a full budget without reclamation and keeps independent lifetimes", () => {
  const pool = new MemoryBudget(16);
  const job = pool.child(16);
  const original = job.reserve(16, "full allocation");
  let reclaimed = false;
  const unregister = job.registerReclaimer(() => {
    reclaimed = true;
  });
  const first = original.retainIn(job);
  const second = first.retainIn(job);
  original.release();
  first.release();
  expect(reclaimed).toBe(false);
  expect(pool.usedBytes).toBe(16);
  expect(job.usedBytes).toBe(16);
  second.release();
  expect(pool.usedBytes).toBe(0);
  expect(job.usedBytes).toBe(0);
  expect(() => second.retainIn(job)).toThrow(/released/);
  unregister();
});
