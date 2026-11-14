import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";

function throwingOwner(scope: MemoryScope, failure: unknown): OwnedMemory<object> {
  const original = scope.allocate(8, "throwing owner", () => ({})).retain();
  return {
    value: original.value,
    bytes: original.bytes,
    budget: original.budget,
    retain: original.retain,
    retainIn: original.retainIn,
    release() {
      original.release();
      throw failure;
    },
  };
}

it("drains later owners and descendants after a child release throws", () => {
  const pool = new MemoryBudget(64);
  const parent = new MemoryScope(pool);
  const first = parent.childScope();
  const failure = new Error("release failure");
  first.adopt(throwingOwner(first, failure));
  const second = parent.childScope();
  second.allocate(8, "sibling payload", () => ({}));
  parent.allocate(8, "parent payload", () => ({}));
  expect(() => parent.dispose()).toThrow(failure);
  expect(first.disposed).toBe(true);
  expect(second.disposed).toBe(true);
  expect(pool.usedBytes).toBe(0);
  expect(() => parent.dispose()).not.toThrow();
});

it("drains all same-scope owners and preserves the first thrown value", () => {
  const scope = new MemoryScope(new MemoryBudget(64));
  scope.adopt(throwingOwner(scope, undefined));
  scope.adopt(throwingOwner(scope, new Error("second failure")));
  scope.allocate(8, "last payload", () => ({}));
  let caught = false;
  try {
    scope.dispose();
  } catch (error) {
    caught = true;
    expect(error).toBeUndefined();
  }
  expect(caught).toBe(true);
  expect(scope.budget.usedBytes).toBe(0);
});

it("keeps pending I/O charged through settlement after another owner fails cleanup", async () => {
  const scope = new MemoryScope(new MemoryBudget(64));
  let complete!: (value: Uint8Array) => void;
  const pending = scope.allocateAsync(
    16,
    "pending cloud read",
    () =>
      new Promise((resolve) => {
        complete = resolve;
      }),
  );
  const signal = scope.signal;
  scope.adopt(throwingOwner(scope, new Error("release failure")));
  scope.allocate(8, "later owner", () => ({}));
  expect(() => scope.dispose()).toThrow("release failure");
  expect(signal.aborted).toBe(true);
  expect(scope.budget.usedBytes).toBe(16);
  complete(new Uint8Array(16));
  await expect(pending).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(scope.budget.usedBytes).toBe(0);
});

it("unlinks a released owner and cleans its backing when snapshot invalidation also throws", () => {
  const scope = new MemoryScope(new MemoryBudget(64));
  const first = new Error("snapshot failure");
  const second = new Error("owner failure");
  let releases = 0;
  function wrap(input: OwnedMemory<object>, failure: Error): OwnedMemory<object> {
    return {
      value: input.value,
      bytes: input.bytes,
      budget: input.budget,
      retain() {
        return wrap(input.retain(), first);
      },
      retainIn(budget) {
        return wrap(input.retainIn(budget), first);
      },
      release() {
        releases++;
        input.release();
        throw failure;
      },
    };
  }
  const original = scope.allocate(8, "shared payload", () => ({}));
  const adopted = scope.adopt(wrap(original.retain(), second));
  scope.retainAllocations(undefined).release();
  expect(() => adopted.release()).toThrow(first);
  expect(releases).toBe(2);
  expect(() => adopted.value).toThrow(/released/);
  const survivor = scope.tryRetainAllocation(original.value);
  expect(survivor?.value).toBe(original.value);
  survivor?.release();
  expect(() => scope.dispose()).not.toThrow();
  expect(scope.budget.usedBytes).toBe(0);
});

it("releases a new allocation when invalidating cached backing fails", () => {
  const scope = new MemoryScope(new MemoryBudget(64));
  const failure = new Error("snapshot cleanup failure");
  function wrap(input: OwnedMemory<object>): OwnedMemory<object> {
    return {
      value: input.value,
      bytes: input.bytes,
      budget: input.budget,
      retain() {
        return wrap(input.retain());
      },
      retainIn(budget) {
        return wrap(input.retainIn(budget));
      },
      release() {
        input.release();
        throw failure;
      },
    };
  }
  scope.adopt(wrap(scope.allocate(8, "original", () => ({})).retain()));
  scope.retainAllocations(undefined).release();
  expect(() => scope.allocate(8, "new payload", () => ({}))).toThrow(failure);
  expect(scope.budget.usedBytes).toBe(8);
  expect(() => scope.dispose()).toThrow(failure);
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects registration if snapshot cleanup reentrantly closes its scope", () => {
  const scope = new MemoryScope(new MemoryBudget(64));
  function wrap(input: OwnedMemory<object>): OwnedMemory<object> {
    return {
      value: input.value,
      bytes: input.bytes,
      budget: input.budget,
      retain() {
        return wrap(input.retain());
      },
      retainIn(budget) {
        return wrap(input.retainIn(budget));
      },
      release() {
        input.release();
        scope.dispose();
      },
    };
  }
  scope.adopt(wrap(scope.allocate(8, "original", () => ({})).retain()));
  scope.retainAllocations(undefined).release();
  expect(() => scope.allocate(8, "new payload", () => ({}))).toThrow(/disposed/);
  expect(scope.disposed).toBe(true);
  expect(scope.budget.usedBytes).toBe(0);
});
