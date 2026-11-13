import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { MemoryStageScheduler } from "./memory-stage-scheduler.js";
import { memoryStore } from "./memory-store.js";
import { withObjectStoreReadControls } from "./store.js";

it("shares remaining admission after the initiating scope closes, including delayed scratch", async () => {
  const root = new MemoryBudget(12);
  const parent = new MemoryScope(root);
  parent.admit(12, "shared stage");
  const child = parent.childScope();
  const detached = child.detachedScope();
  parent.allocate(4, "caller output", () => new Uint8Array(4));
  expect(detached.availableAllocationBytes).toBe(8);
  parent.dispose();
  expect(child.disposed).toBe(true);
  expect(detached.disposed).toBe(false);
  expect(root.usedBytes).toBe(8);
  const scratch = detached.allocate(4, "late scratch", () => new Uint8Array(4));
  scratch.release();
  expect(detached.availableAllocationBytes).toBe(4);
  const final = detached.allocate(4, "late output", () => new Uint8Array(4));
  expect(() => detached.allocate(1, "overflow", () => new Uint8Array(1))).toThrow(/extent/);
  final.release();
  detached.dispose();
  expect(root.usedBytes).toBe(0);
});

it("transfers exclusive admission to a stage that survives its source scope", () => {
  const budget = new MemoryBudget(8);
  const source = new MemoryScope(budget);
  source.admit(8, "original");
  const target = new MemoryScope(budget);
  const reservation = source.reserve(6, "shared transport");
  target.admitReserved(reservation);
  reservation.release();
  source.dispose();
  expect(target.disposed).toBe(false);
  expect(target.hasAdmission).toBe(true);
  expect(budget.usedBytes).toBe(6);
  target.allocate(6, "transport", () => new Uint8Array(6));
  expect(() => target.allocate(1, "overflow", () => new Uint8Array(1))).toThrow(/extent/);
  target.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("preserves caller ownership when transfer validation fails", () => {
  const root = new MemoryBudget(16);
  const source = root.child();
  const admission = source.reserve(8, "transfer");
  const wrong = new MemoryScope(root.child());
  expect(() => wrong.admitReserved(admission)).toThrow(/same memory budget/);
  const target = new MemoryScope(source);
  const shared = admission.retain();
  expect(() => target.admitReserved(admission)).toThrow(/exclusive/);
  shared.release();
  target.admitReserved(admission);
  expect(root.usedBytes).toBe(8);
  const extra = source.reserve(1, "extra");
  expect(() => target.admitReserved(extra)).toThrow(/already/);
  extra.release();
  target.dispose();
  wrong.dispose();
  expect(root.usedBytes).toBe(0);
});

it("shares one admitted extent across nested children without replenishing released bytes", () => {
  const budget = new MemoryBudget(8);
  const parent = new MemoryScope(budget);
  parent.admit(8, "stage");
  const left = parent.childScope();
  const right = parent.childScope().childScope();
  left.allocate(4, "left", () => new Uint8Array(4));
  expect(right.availableAllocationBytes).toBe(4);
  left.dispose();
  expect(budget.usedBytes).toBe(4);
  right.allocate(4, "right", () => new Uint8Array(4));
  expect(() => right.allocate(1, "overflow", () => new Uint8Array(1))).toThrow(/extent/);
  expect(() => right.admit(1, "bypass")).toThrow(/parent/);
  parent.dispose();
  expect(right.disposed).toBe(true);
  expect(budget.usedBytes).toBe(0);
});

it("does not allocate after cache reclamation cancels only the borrowing child", () => {
  const budget = new MemoryBudget(4);
  const cache = budget.reserve(4, "cache");
  const parent = new MemoryScope(budget);
  const child = parent.childScope();
  const detach = budget.registerReclaimer(() => {
    child.dispose();
    cache.release();
  });
  let allocated = false;
  expect(() =>
    child.allocate(4, "read", () => {
      allocated = true;
      return new Uint8Array(4);
    }),
  ).toThrow(/disposed/);
  expect(allocated).toBe(false);
  expect(parent.disposed).toBe(false);
  expect(budget.usedBytes).toBe(0);
  detach();
  parent.dispose();
});

it.each([
  1, 2,
])("controlled concurrent reads consume exact stage admission with %s slots", async (slots) => {
  const store = memoryStore();
  await store.put("x", Uint8Array.of(1, 2, 3, 4));
  const controlled = withObjectStoreReadControls(store, { maxConcurrentReads: slots });
  const read = controlled.getRangeOwned?.bind(controlled);
  if (read === undefined) throw new Error("Missing owned range capability");
  const root = new MemoryBudget(8);
  const scheduler = new MemoryStageScheduler(root);
  const output = await scheduler.run({
    budget: root.child(),
    bytes: 8,
    purpose: "two reads",
    async execute(scope) {
      const owners = await Promise.all(
        [0, 1].map(() => read("x", { offset: 0, length: 4 }, scope)),
      );
      return owners.map((owner) => owner.value);
    },
  });
  expect(output.value.map((value) => [...value])).toEqual([
    [1, 2, 3, 4],
    [1, 2, 3, 4],
  ]);
  expect(root.peakBytes).toBe(8);
  expect(root.usedBytes).toBe(8);
  output.release();
  expect(root.usedBytes).toBe(0);
});

it("keeps borrowed child pending I/O charged until settlement after parent disposal", async () => {
  const budget = new MemoryBudget(4);
  const parent = new MemoryScope(budget);
  parent.admit(4, "stage");
  const child = parent.childScope();
  let settle!: (value: Uint8Array) => void;
  const pending = child.allocateAsync(
    4,
    "read",
    () =>
      new Promise<Uint8Array>((resolve) => {
        settle = resolve;
      }),
  );
  const rejected = expect(pending).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  parent.dispose();
  expect(budget.usedBytes).toBe(4);
  settle(new Uint8Array(4));
  await rejected;
  expect(budget.usedBytes).toBe(0);
});
