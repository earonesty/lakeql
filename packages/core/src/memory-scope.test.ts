import { describe, expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";

describe("execution memory ownership", () => {
  it("reserves real buffers before allocation and preserves retained backing ownership", () => {
    const pool = new MemoryBudget(64 * 1024 * 1024);
    const scope = new MemoryScope(pool.child());
    const first = scope.allocate(32 * 1024 * 1024, "input", () =>
      new Uint8Array(32 * 1024 * 1024).fill(7),
    );
    const second = scope.allocate(32 * 1024 * 1024, "output", () =>
      new Uint8Array(32 * 1024 * 1024).fill(9),
    );
    expect(first.value.at(-1)).toBe(7);
    expect(second.value.at(-1)).toBe(9);
    let allocated = false;
    expect(() =>
      scope.allocate(1, "overflow", () => {
        allocated = true;
        return new Uint8Array(1);
      }),
    ).toThrow();
    expect(allocated).toBe(false);
    const retained = first.retain();
    scope.dispose();
    scope.dispose();
    expect(pool.usedBytes).toBe(32 * 1024 * 1024);
    expect(retained.value.at(-1)).toBe(7);
    expect(() => first.retain()).toThrow("released memory ownership");
    retained.release();
    retained.release();
    expect(pool.usedBytes).toBe(0);
    expect(pool.peakBytes).toBe(pool.maxBytes);
  });

  it("keeps pending asynchronous allocations charged until they settle after disposal", async () => {
    const pool = new MemoryBudget(8);
    const scope = new MemoryScope(pool);
    let finish: ((value: Uint8Array) => void) | undefined;
    const read = scope.allocateAsync(
      8,
      "pending read",
      () =>
        new Promise<Uint8Array>((resolve) => {
          finish = resolve;
        }),
    );
    expect(pool.usedBytes).toBe(8);
    scope.dispose();
    expect(pool.usedBytes).toBe(8);
    expect(pool.tryReserve(1, "another query")).toBeUndefined();
    finish?.(new Uint8Array(8));
    await expect(read).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    expect(pool.usedBytes).toBe(0);
  });

  it("cleans up synchronous and asynchronous failures and rejects new work after disposal", async () => {
    const pool = new MemoryBudget(16);
    const scope = new MemoryScope(pool);
    expect(() =>
      scope.allocate(8, "failed allocation", () => {
        throw new Error("failed");
      }),
    ).toThrow("failed");
    expect(pool.usedBytes).toBe(0);
    await expect(
      scope.allocateAsync(8, "failed read", async () => {
        throw new Error("network");
      }),
    ).rejects.toThrow("network");
    expect(pool.usedBytes).toBe(0);
    const success = await scope.allocateAsync(8, "read", async () => new Uint8Array(8));
    const retained = success.retain();
    success.release();
    expect(pool.usedBytes).toBe(8);
    scope.dispose();
    expect(pool.usedBytes).toBe(8);
    retained.release();
    expect(() => retained.retain()).toThrow();
    expect(() => scope.allocate(0, "closed", () => null)).toThrow("disposed");
    await expect(scope.allocateAsync(0, "closed", async () => null)).rejects.toThrow("disposed");
    expect(pool.usedBytes).toBe(0);
  });

  it("does not leak if an allocator closes its own scope", () => {
    const pool = new MemoryBudget(8);
    const scope = new MemoryScope(pool);
    expect(() =>
      scope.allocate(8, "reentrant cleanup", () => {
        scope.dispose();
        return new Uint8Array(8);
      }),
    ).toThrow("disposed");
    expect(pool.usedBytes).toBe(0);
  });
});

it("adopts independent ownership only within the same budget", () => {
  const budget = new MemoryBudget(8);
  const source = new MemoryScope(budget);
  const owner = source.allocate(8, "source", () => new Uint8Array(8)).retain();
  source.dispose();
  const other = new MemoryScope(new MemoryBudget(8));
  expect(() => other.adopt(owner)).toThrow("same memory budget");
  expect(budget.usedBytes).toBe(8);
  const destination = new MemoryScope(budget);
  const adopted = destination.adopt(owner);
  destination.dispose();
  expect(() => adopted.retain()).toThrow();
  expect(budget.usedBytes).toBe(0);
});

it("retains output backing across producer disposal without double charging", () => {
  const budget = new MemoryBudget(12);
  const producer = new MemoryScope(budget);
  const consumer = new MemoryScope(budget);
  const data = producer.allocate(8, "page", () => new Uint8Array(8).fill(7));
  const dictionary = producer.allocate(4, "dictionary", () => new Uint8Array(4).fill(3));
  const output = consumer.retainBacking(
    { values: data.value.subarray(2, 6), dictionary: dictionary.value },
    [data, dictionary, data],
  );
  expect(output.bytes).toBe(12);
  producer.dispose();
  expect(budget.usedBytes).toBe(12);
  expect(output.value.values).toEqual(Uint8Array.of(7, 7, 7, 7));
  const cached = output.retain();
  consumer.dispose();
  expect(budget.usedBytes).toBe(12);
  expect(cached.value.dictionary[0]).toBe(3);
  cached.release();
  cached.release();
  expect(budget.usedBytes).toBe(0);
  expect(budget.peakBytes).toBe(12);
});

it("rolls back partial backing retention and rejects incompatible pools", () => {
  const budget = new MemoryBudget(8);
  const producer = new MemoryScope(budget);
  const consumer = new MemoryScope(budget);
  const first = producer.allocate(4, "first", () => new Uint8Array(4));
  const released = producer.allocate(4, "second", () => new Uint8Array(4));
  released.release();
  expect(() => consumer.retainBacking({}, [first, released])).toThrow("released");
  expect(() => new MemoryScope(new MemoryBudget(8)).retainBacking({}, [first])).toThrow(
    "same memory budget",
  );
  producer.dispose();
  expect(budget.usedBytes).toBe(0);
  consumer.dispose();
  expect(() => consumer.retainBacking({}, [])).toThrow("disposed");
});

it("exposes one disposal signal and aborts signals first accessed after disposal", () => {
  const scope = new MemoryScope(new MemoryBudget(0));
  const signal = scope.signal;
  expect(scope.signal).toBe(signal);
  expect(signal.aborted).toBe(false);
  scope.dispose();
  expect(signal.aborted).toBe(true);
  const closed = new MemoryScope(new MemoryBudget(0));
  closed.dispose();
  expect(closed.signal.aborted).toBe(true);
});

it("links external cancellation and rejects work under an already aborted signal", () => {
  const controller = new AbortController();
  const scope = new MemoryScope(new MemoryBudget(4), [controller.signal, controller.signal]);
  scope.allocate(4, "buffer", () => new Uint8Array(4));
  controller.abort();
  expect(scope.disposed).toBe(true);
  expect(scope.signal.aborted).toBe(true);
  expect(scope.budget.usedBytes).toBe(0);
  const closed = new MemoryScope(new MemoryBudget(4), [controller.signal]);
  expect(() => closed.allocate(4, "forbidden", () => new Uint8Array(4))).toThrow("disposed");
});

it("rolls back composite cross-job retention if later backing exceeds the reader cap", () => {
  const pool = new MemoryBudget(8);
  const producer = new MemoryScope(pool.child(8));
  const one = producer.allocate(4, "one", () => new Uint8Array(4));
  const two = producer.allocate(4, "two", () => new Uint8Array(4));
  const combined = producer.retainBacking([one.value, two.value], [one, two]);
  const reader = pool.child(7);
  expect(() => combined.retainIn(reader)).toThrow("Allocation exceeds memory budget");
  expect(reader.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(8);
  const cache = combined.retainIn(pool);
  producer.dispose();
  expect(producer.budget.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(8);
  cache.release();
  expect(pool.usedBytes).toBe(0);
});

it("pins all composite backing before a target reclaimer disposes the producer", () => {
  const pool = new MemoryBudget(16);
  const producer = new MemoryScope(pool.child(8));
  const first = producer.allocate(4, "first", () => Uint8Array.of(1, 2, 3, 4));
  const second = producer.allocate(4, "second", () => Uint8Array.of(5, 6, 7, 8));
  const output = producer.retainBacking([first.value, second.value], [first, second]);
  const reader = pool.child(8);
  const old = reader.reserve(8, "previous reader output");
  let during = 0;
  const detach = reader.registerReclaimer(() => {
    producer.dispose();
    old.release();
    during = pool.usedBytes;
  });
  const retained = output.retainIn(reader);
  expect(during).toBe(8);
  expect(retained.value[1]?.at(-1)).toBe(8);
  expect(producer.budget.usedBytes).toBe(0);
  expect(reader.usedBytes).toBe(8);
  expect(pool.usedBytes).toBe(8);
  retained.release();
  detach();
  expect(pool.usedBytes).toBe(0);
});

it("captures only completed live allocations and keeps aliased output charged after disposal", async () => {
  const budget = new MemoryBudget(16);
  const scope = new MemoryScope(budget);
  const input = scope.allocate(4, "input", () => Uint8Array.of(1, 2, 3, 4));
  const released = scope.allocate(2, "released", () => new Uint8Array(2));
  released.release();
  let finish: (value: Uint8Array) => void = () => {
    throw new Error("not started");
  };
  const pending = scope.allocateAsync(
    4,
    "pending",
    () =>
      new Promise<Uint8Array>((resolve) => {
        finish = resolve;
      }),
  );
  const output = scope.retainAllocations(input.value.subarray(1, 3));
  expect(output.bytes).toBe(4);
  finish(new Uint8Array(4));
  await pending;
  scope.dispose();
  expect(budget.usedBytes).toBe(4);
  expect([...output.value]).toEqual([2, 3]);
  output.release();
  expect(budget.usedBytes).toBe(0);
  expect(() => scope.retainAllocations(null)).toThrow(/disposed/);
});

it("retains only an exact allocation while rejecting unowned views", () => {
  const budget = new MemoryBudget(64);
  const scope = new MemoryScope(budget);
  const first = scope.allocate(8, "first", () => new Uint8Array(8));
  scope.allocate(16, "unrelated", () => new Uint8Array(16));
  const value = first.value;
  const retained = scope.retainAllocation(value);
  expect(() => scope.retainAllocation(value.subarray(0, 4))).toThrow();
  scope.releaseAllocation(value);
  expect(() => scope.retainAllocation(value)).toThrow();
  expect(() => scope.releaseAllocation(value)).toThrow();
  scope.dispose();
  expect(budget.usedBytes).toBe(8);
  expect(retained.value).toBe(value);
  expect(() => first.value).toThrow(/released/);
  retained.release();
  expect(budget.usedBytes).toBe(0);
  expect(() => scope.retainAllocation(value)).toThrow();
});

it("conditionally releases exact scratch without releasing borrowed views or retained pins", () => {
  const budget = new MemoryBudget(4);
  const scope = new MemoryScope(budget);
  const owner = scope.allocate(4, "scratch", () => new Uint8Array(4));
  const value = owner.value;
  const retained = owner.retain();
  expect(scope.tryReleaseAllocation(value.subarray(0))).toBe(false);
  expect(scope.tryReleaseAllocation(new Uint8Array(4))).toBe(false);
  expect(budget.usedBytes).toBe(4);
  expect(scope.tryReleaseAllocation(value)).toBe(true);
  expect(scope.tryReleaseAllocation(value)).toBe(false);
  expect(budget.usedBytes).toBe(4);
  retained.release();
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
  expect(() => scope.tryReleaseAllocation(value)).toThrow();
});

it("indexes duplicate exact values in insertion order across out-of-order releases", () => {
  const scope = new MemoryScope(new MemoryBudget(20));
  const value = {};
  const first = scope.allocate(1, "first", () => value);
  const middle = scope.allocate(2, "middle", () => value);
  const last = scope.allocate(3, "last", () => value);
  const pin = scope.retainAllocation(value);
  expect(pin.bytes).toBe(1);
  pin.release();
  middle.release();
  expect(scope.tryReleaseAllocation(value)).toBe(true);
  expect(scope.budget.usedBytes).toBe(3);
  const remaining = scope.retainAllocation(value);
  expect(remaining.bytes).toBe(3);
  remaining.release();
  first.release();
  last.release();
  expect(scope.tryReleaseAllocation(value)).toBe(false);
  const replacement = scope.allocate(4, "replacement", () => value);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  replacement.release();
});

it("preserves strict-equality scalar lookups and cleans up unmatchable NaN", () => {
  const scope = new MemoryScope(new MemoryBudget(4));
  scope.allocate(1, "NaN", () => NaN);
  scope.allocate(1, "zero", () => -0);
  scope.allocate(1, "undefined", () => undefined);
  expect(scope.tryReleaseAllocation(NaN)).toBe(false);
  expect(() => scope.retainAllocation(NaN)).toThrow();
  expect(scope.tryReleaseAllocation(0)).toBe(true);
  expect(scope.tryReleaseAllocation(undefined)).toBe(true);
  expect(scope.budget.usedBytes).toBe(1);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("finds exact owners without inspecting unrelated allocation values", () => {
  const budget = new MemoryBudget(1000);
  const source = new MemoryScope(budget);
  const target = new MemoryScope(budget);
  const values = Array.from({ length: 1000 }, () => ({}));
  let inspections = 0;
  for (const value of values) {
    const owner = source.allocate(1, "indexed", () => value).retain();
    target.adopt({
      ...owner,
      get value() {
        inspections++;
        return owner.value;
      },
    });
  }
  inspections = 0;
  const pin = target.retainAllocation(values[999]);
  pin.release();
  expect(inspections).toBe(0);
  expect(target.tryReleaseAllocation(values[999])).toBe(true);
  expect(inspections).toBeLessThanOrEqual(2);
  source.dispose();
  target.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("keeps index links out of the public ownership handle's enumerable fields", () => {
  const scope = new MemoryScope(new MemoryBudget(1));
  const owner = scope.allocate(1, "shape", () => new Uint8Array(1));
  expect(Object.keys(owner).sort()).toEqual([
    "budget",
    "bytes",
    "release",
    "retain",
    "retainIn",
    "value",
  ]);
  const copy = { ...owner };
  expect("previous" in copy).toBe(false);
  expect("next" in copy).toBe(false);
  scope.dispose();
});

it("shares composite backing pins across same-budget retains and releases the final handle", () => {
  const budget = new MemoryBudget(100);
  const scope = new MemoryScope(budget);
  let retains = 0;
  const counted = (owner: OwnedMemory<Uint8Array>): OwnedMemory<Uint8Array> => ({
    ...owner,
    retain() {
      retains++;
      return counted(owner.retain());
    },
    retainIn(target) {
      retains++;
      return counted(owner.retainIn(target));
    },
  });
  const backing = Array.from({ length: 100 }, () =>
    counted(scope.allocate(1, "composite backing", () => new Uint8Array(1))),
  );
  const composite = scope.retainBacking({}, backing);
  expect(retains).toBe(100);
  const copies = Array.from({ length: 100 }, (_, i) =>
    i % 2 === 0 ? composite.retain() : composite.retainIn(budget),
  );
  expect(retains).toBe(100);
  scope.dispose();
  expect(budget.usedBytes).toBe(100);
  for (const copy of copies.slice(1)) copy.release();
  expect(budget.usedBytes).toBe(100);
  const last = copies[0];
  expect(last).toBeDefined();
  last?.release();
  last?.release();
  expect(budget.usedBytes).toBe(0);
  expect(() => last?.retain()).toThrow(/released/);
});

it("reuses unchanged snapshots and invalidates them when ownership changes", () => {
  const budget = new MemoryBudget(4);
  const scope = new MemoryScope(budget);
  let captures = 0;
  const source = new MemoryScope(budget);
  const owner = source.allocate(1, "first", () => new Uint8Array(1)).retain();
  const adopted = scope.adopt({
    ...owner,
    retain() {
      captures++;
      return owner.retain();
    },
  });
  source.dispose();
  const first = scope.retainAllocations("first");
  const second = scope.retainAllocations("second");
  expect(captures).toBe(1);
  expect(first.value).toBe("first");
  expect(second.value).toBe("second");
  const added = scope.allocate(2, "added", () => new Uint8Array(2));
  const third = scope.retainAllocations("third");
  expect(captures).toBe(2);
  expect([first.bytes, second.bytes, third.bytes]).toEqual([1, 1, 3]);
  adopted.release();
  const fourth = scope.retainAllocations("fourth");
  expect(fourth.bytes).toBe(2);
  added.release();
  const empty = scope.retainAllocations("empty");
  expect(empty.bytes).toBe(0);
  scope.dispose();
  expect(budget.usedBytes).toBe(3);
  first.release();
  second.release();
  third.release();
  expect(budget.usedBytes).toBe(2);
  fourth.release();
  empty.release();
  expect(budget.usedBytes).toBe(0);
});

it("guards attached scope trees without capturing detached cache lifetimes", () => {
  const budget = new MemoryBudget(32);
  const parent = new MemoryScope(budget);
  parent.allocate(1, "parent", () => new Uint8Array(1));
  const child = parent.childScope();
  child.allocate(2, "child", () => new Uint8Array(2));
  const stage = child.childStage(8, "nested stage");
  stage.allocate(4, "grandchild", () => new Uint8Array(4));
  const detached = parent.detachedScope();
  detached.allocate(8, "cache", () => new Uint8Array(8));
  const expired = parent.childScope();
  expired.allocate(1, "expired", () => new Uint8Array(1));
  expired.dispose();
  const guard = parent.retainTreeAllocations("tree");
  expect(guard.bytes).toBe(7);
  parent.dispose();
  expect(child.disposed).toBe(true);
  expect(stage.disposed).toBe(true);
  expect(detached.disposed).toBe(false);
  // Unconsumed nested admission is released, completed allocations are guarded.
  expect(budget.usedBytes).toBe(15);
  detached.dispose();
  expect(budget.usedBytes).toBe(7);
  guard.release();
  guard.release();
  expect(budget.usedBytes).toBe(0);
});

it("captures only allocations live at the tree snapshot boundary", () => {
  const budget = new MemoryBudget(8);
  const parent = new MemoryScope(budget);
  const child = parent.childScope();
  const first = child.allocate(2, "first", () => new Uint8Array(2));
  const before = parent.retainTreeAllocations(undefined);
  first.release();
  child.allocate(3, "second", () => new Uint8Array(3));
  const after = parent.retainTreeAllocations(undefined);
  expect(before.bytes).toBe(2);
  expect(after.bytes).toBe(3);
  parent.dispose();
  expect(budget.usedBytes).toBe(5);
  before.release();
  expect(budget.usedBytes).toBe(3);
  after.release();
  expect(budget.usedBytes).toBe(0);
});

it("replaces admitted input capacity with existing cache backing without double charging", () => {
  const root = new MemoryBudget(16);
  const cache = new MemoryScope(root);
  const stored = cache.allocate(8, "cache", () => new Uint8Array(8).fill(7));
  const job = root.child(8);
  const stage = new MemoryScope(job);
  stage.admit(8, "input stage");
  expect(root.usedBytes).toBe(16);
  const pin = stage.pinExisting(stored);
  expect(root.usedBytes).toBe(8);
  expect(job.usedBytes).toBe(8);
  expect(stage.availableAllocationBytes).toBe(0);
  stage.dispose();
  cache.dispose();
  expect(root.usedBytes).toBe(8);
  expect([...pin.value]).toEqual(Array(8).fill(7));
  pin.release();
  expect(root.usedBytes).toBe(0);
});

it("keeps separately budgeted input pins when remaining new-allocation credit is smaller", () => {
  const root = new MemoryBudget(32);
  const cache = new MemoryScope(root);
  const stored = cache.allocate(8, "cached input", () => new Uint8Array(8));
  const stage = new MemoryScope(root.child(16));
  stage.admit(4, "output only");
  const pin = stage.pinExisting(stored);
  expect(stage.availableAllocationBytes).toBe(4);
  expect(stage.budget.usedBytes).toBe(12);
  expect(root.usedBytes).toBe(12);
  const foreign = new MemoryScope(new MemoryBudget(8));
  const other = foreign.allocate(8, "foreign", () => new Uint8Array(8));
  expect(() => stage.pinExisting(other)).toThrow(/same allocation pool/);
  expect(stage.availableAllocationBytes).toBe(4);
  foreign.dispose();
  stage.dispose();
  pin.release();
  cache.dispose();
  expect(root.usedBytes).toBe(0);
});

it("invalidates released payload access independently across scoped and composite handles", () => {
  const scope = new MemoryScope(new MemoryBudget(8));
  const original = scope.allocate(8, "payload", () => new Uint8Array(8).fill(7));
  const retained = original.retain();
  const composite = scope.retainAllocations({ data: original.value });
  const sibling = composite.retain();
  original.release();
  expect(() => original.value).toThrow(/released/);
  expect(retained.value[0]).toBe(7);
  scope.dispose();
  expect(composite.value.data[0]).toBe(7);
  composite.release();
  expect(() => composite.value).toThrow(/released/);
  expect(sibling.value.data[0]).toBe(7);
  retained.release();
  expect(() => retained.value).toThrow(/released/);
  expect(scope.budget.usedBytes).toBe(8);
  sibling.release();
  sibling.release();
  expect(() => sibling.value).toThrow(/released/);
  expect(scope.budget.usedBytes).toBe(0);
});

it("disposes an adopted owner already released by its producer without reading its payload", () => {
  const producer = new MemoryScope(new MemoryBudget(8));
  const owner = producer.allocate(8, "payload", () => new Uint8Array(8)).retain();
  const consumer = new MemoryScope(producer.budget);
  consumer.adopt(owner);
  owner.release();
  expect(() => consumer.dispose()).not.toThrow();
  producer.dispose();
  expect(producer.budget.usedBytes).toBe(0);
});

it("preserves spread and detached methods on independently retained atomic handles", () => {
  const scope = new MemoryScope(new MemoryBudget(100));
  const original = scope.allocate(12, "atomic", () => new Uint8Array(12)).retain();
  const wrapper = { ...original };
  expect(Object.keys(wrapper).sort()).toEqual([
    "budget",
    "bytes",
    "release",
    "retain",
    "retainIn",
    "value",
  ]);
  const { retain, retainIn, release } = original;
  const first = retain();
  const second = retainIn(scope.budget);
  scope.dispose();
  release();
  release();
  expect(() => retain()).toThrow(/released/);
  expect(() => original.value).toThrow(/released/);
  expect(first.value).toBe(wrapper.value);
  first.release();
  expect(scope.budget.usedBytes).toBe(12);
  second.release();
  expect(scope.budget.usedBytes).toBe(0);
});
