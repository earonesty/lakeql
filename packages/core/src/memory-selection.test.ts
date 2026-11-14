import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, retainSelectedAllocations } from "./memory-scope.js";

it("detaches selected leaf backing from obsolete composite routing owners", () => {
  const pool = new MemoryBudget(65536);
  const producer = new MemoryScope(pool);
  const payload = producer.allocate(100, "payload", () => new Uint8Array(100).fill(7)).value;
  const source = producer.retainAllocations({ payload });
  const intermediate = new MemoryScope(pool);
  intermediate.adopt(source.retain());
  intermediate.allocate(200, "obsolete routing", () => new Uint8Array(200));
  const joined = intermediate.retainAllocations({ payload });
  const output = new MemoryScope(pool);
  retainSelectedAllocations(joined, output, (value) => value === payload);
  joined.release();
  source.release();
  producer.dispose();
  intermediate.dispose();
  expect(pool.usedBytes).toBe(100);
  expect(payload[99]).toBe(7);
  output.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("rolls back partial selections when predicates or target admission fail", () => {
  const pool = new MemoryBudget(65536);
  const producer = new MemoryScope(pool.child());
  producer.allocate(100, "a", () => ({ a: 1 }));
  producer.allocate(100, "b", () => ({ b: 2 }));
  const source = producer.retainAllocations(undefined);
  const target = new MemoryScope(pool.child(400));
  expect(() => retainSelectedAllocations(source, target, () => true)).toThrow(/budget/);
  expect(target.budget.usedBytes).toBe(0);
  let visits = 0;
  expect(() =>
    retainSelectedAllocations(source, target, () => {
      if (++visits === 2) throw Error("predicate");
      return true;
    }),
  ).toThrow(/predicate/);
  expect(target.budget.usedBytes).toBe(0);
  expect(producer.budget.usedBytes).toBe(200);
  source.release();
  producer.dispose();
  target.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("rejects released input and preserves unknown external ownership conservatively", () => {
  const pool = new MemoryBudget(65536);
  const producer = new MemoryScope(pool);
  const source = producer.allocate(100, "external payload", () => new Uint8Array(100)).retain();
  // A transparent third-party handle intentionally hides internal provenance.
  const wrap = (owner: typeof source): typeof source => ({
    get value() {
      return owner.value;
    },
    bytes: owner.bytes,
    budget: owner.budget,
    retain: () => wrap(owner.retain()),
    retainIn: (budget) => wrap(owner.retainIn(budget)),
    release: () => owner.release(),
  });
  const external = wrap(source);
  const target = new MemoryScope(pool);
  retainSelectedAllocations(external, target, () => false);
  external.release();
  producer.dispose();
  expect(pool.usedBytes).toBe(100);
  expect(() => retainSelectedAllocations(source, target, () => true)).toThrow(/released/);
  target.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("releases the source guard when the destination is already disposed", () => {
  const pool = new MemoryBudget(100);
  const producer = new MemoryScope(pool);
  const source = producer.allocate(100, "payload", () => new Uint8Array(100)).retain();
  const target = new MemoryScope(pool);
  target.dispose();
  expect(() => retainSelectedAllocations(source, target, () => true)).toThrow();
  source.release();
  producer.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("guards source backing while target reclamation closes producer handles", () => {
  const pool = new MemoryBudget(2000);
  const producer = new MemoryScope(pool.child());
  producer.allocate(100, "payload", () => new Uint8Array(100));
  const source = producer.retainAllocations(undefined);
  const targetBudget = pool.child(1000);
  const target = new MemoryScope(targetBudget);
  const cached = target.allocate(1000, "cached", () => new Uint8Array(1000));
  let reclaimed = false;
  const unregister = targetBudget.registerReclaimer(() => {
    reclaimed = true;
    cached.release();
    source.release();
    producer.dispose();
    expect(pool.usedBytes).toBe(100); // The operation guard is still live.
  });
  retainSelectedAllocations(source, target, () => true);
  expect(reclaimed).toBe(true);
  expect(pool.usedBytes).toBe(100);
  unregister();
  target.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("traverses and releases a deep ownership history without recursive stack growth", () => {
  const pool = new MemoryBudget(16 * 1024 * 1024);
  const producer = new MemoryScope(pool);
  const payload = producer.allocate(100, "payload", () => new Uint8Array(100)).value;
  let source = producer.retainAllocations(undefined);
  producer.dispose();
  for (let depth = 0; depth < 12000; depth++) {
    const next = new MemoryScope(pool);
    next.adopt(source.retain());
    const nested = next.retainAllocations(undefined);
    source.release();
    next.dispose();
    source = nested;
  }
  const target = new MemoryScope(pool);
  retainSelectedAllocations(source, target, (value) => value === payload);
  source.release();
  expect(pool.usedBytes).toBe(100);
  expect(pool.peakBytes).toBeGreaterThan(12000 * 256);
  target.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("admits traversal workspace and rolls it back before evaluating a leaf", () => {
  const pool = new MemoryBudget(65536);
  const producer = new MemoryScope(pool);
  producer.allocate(100, "payload", () => new Uint8Array(100));
  const source = producer.retainAllocations(undefined);
  const target = new MemoryScope(pool.child(255));
  let called = false;
  expect(() =>
    retainSelectedAllocations(source, target, () => {
      called = true;
      return true;
    }),
  ).toThrow(/budget/);
  expect(called).toBe(false);
  expect(target.budget.usedBytes).toBe(0);
  source.release();
  producer.dispose();
  target.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("drains remaining composite releases after a third-party release throws", () => {
  const budget = new MemoryBudget(100);
  const scope = new MemoryScope(budget);
  const released: string[] = [];
  const external = (name: string): import("./memory-scope.js").OwnedMemory<unknown> => ({
    value: name,
    bytes: 0,
    budget,
    retain: () => external(name),
    retainIn: () => external(name),
    release() {
      released.push(name);
      if (name === "bad") throw Error("release failure");
    },
  });
  const output = scope.retainBacking(undefined, [external("bad"), external("good")]).retain();
  scope.dispose();
  expect(() => output.release()).toThrow(/release failure/);
  expect(released).toEqual(["bad", "good"]);
  const later = new MemoryScope(budget);
  later.allocate(100, "later", () => new Uint8Array(100));
  const owner = later.retainAllocations(undefined);
  later.dispose();
  owner.release();
  expect(budget.usedBytes).toBe(0);
});

it("processes a wide atomic backing set without per-leaf traversal frames", () => {
  const pool = new MemoryBudget(65536);
  const producer = new MemoryScope(pool.child());
  for (let i = 0; i < 1024; i++) producer.allocate(1, "byte", () => new Uint8Array(1));
  const input = producer.retainAllocations(undefined);
  const target = new MemoryScope(pool.child(256));
  let visits = 0;
  retainSelectedAllocations(input, target, () => {
    visits++;
    return false;
  });
  expect(visits).toBe(1024);
  expect(target.budget.peakBytes).toBe(256);
  expect(target.budget.usedBytes).toBe(0);
  target.dispose();
  input.release();
  producer.dispose();
  expect(pool.usedBytes).toBe(0);
});
