import { MemoryBudget, MemoryScope, type OwnedMemory } from "lakeql-core";
import { expect, it } from "vitest";
import type { NestedColumnRow } from "./nested-column-rows.js";
import { readAlignedNestedRows } from "./nested-row-alignment.js";

it("bounds advances, retains aligned rows, and closes all sources on early return", async () => {
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  let active = 0,
    peak = 0,
    closed = 0;
  const source = async function* (): AsyncGenerator<OwnedMemory<NestedColumnRow>> {
    try {
      for (let row = 0; row < 3; row++) {
        peak = Math.max(peak, ++active);
        await new Promise<void>((resolve) => setTimeout(resolve, 1));
        active--;
        const child = scope.childScope();
        const value = child.allocate(256, "test row", () => ({ row, fragments: [] })).value;
        const owner = child.retainAllocations(value);
        child.dispose();
        try {
          yield owner;
        } finally {
          owner.release();
        }
      }
    } finally {
      closed++;
    }
  };
  const iterator = readAlignedNestedRows([source(), source(), source()], scope, 2);
  const first = await iterator.next();
  if (first.done) throw Error("Missing row");
  const retained = first.value.retain();
  await iterator.return(undefined);
  scope.dispose();
  expect(retained.value.columns.map((column) => column.row)).toEqual([0, 0, 0]);
  expect(closed).toBe(3);
  expect(peak).toBe(2);
  expect(active).toBe(0);
  retained.release();
  expect(budget.usedBytes).toBe(0);
});

it.each([
  "count",
  "order",
  "error",
])("rejects %s mismatch and drains all source lifetimes", async (mode) => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  let closed = 0;
  const source = async function* (bad: boolean): AsyncGenerator<OwnedMemory<NestedColumnRow>> {
    try {
      if (bad && mode === "count") return;
      if (bad && mode === "error") throw Error("source failure");
      const child = scope.childScope();
      const owner = child.retainAllocations(
        child.allocate(256, "test row", () => ({
          row: bad ? 2 : 0,
          fragments: [],
        })).value,
      );
      child.dispose();
      try {
        yield owner;
      } finally {
        owner.release();
      }
    } finally {
      closed++;
    }
  };
  const iterator = readAlignedNestedRows([source(false), source(true), source(false)], scope, 3);
  await expect(iterator.next()).rejects.toThrow(
    mode === "error" ? /source failure/ : /Nested column/,
  );
  expect(closed).toBe(3);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects cross-budget row admission without leaking source pins", async () => {
  const pool = new MemoryBudget(65536);
  const input = new MemoryScope(pool.child(32768));
  const output = new MemoryScope(pool.child(2048));
  let closed = false;
  async function* source(): AsyncGenerator<OwnedMemory<NestedColumnRow>> {
    const owner = input.allocate(8192, "large source row", () => ({ row: 0, fragments: [] }));
    try {
      yield owner;
    } finally {
      owner.release();
      closed = true;
    }
  }
  await expect(readAlignedNestedRows([source()], output).next()).rejects.toThrow(/budget/);
  expect(closed).toBe(true);
  expect(input.budget.usedBytes).toBe(0);
  expect(output.budget.usedBytes).toBe(0);
  input.dispose();
  output.dispose();
});

it("keeps retained aligned output alive across separately capped stages in one pool", async () => {
  const pool = new MemoryBudget(65536);
  const input = new MemoryScope(pool.child(16384));
  const output = new MemoryScope(pool.child(16384));
  let closed = false;
  async function* source(): AsyncGenerator<OwnedMemory<NestedColumnRow>> {
    const owner = input.allocate(8192, "source row", () => ({ row: 0, fragments: [] }));
    try {
      yield owner;
    } finally {
      owner.release();
      closed = true;
    }
  }
  const iterator = readAlignedNestedRows([source()], output);
  const first = await iterator.next();
  if (first.done) throw Error("Missing row");
  const retained = first.value.retain();
  try {
    await iterator.return(undefined);
    input.dispose();
    output.dispose();
    expect(closed).toBe(true);
    expect(retained.value.columns[0]?.row).toBe(0);
    expect(pool.usedBytes).toBeGreaterThan(0);
    expect(pool.peakBytes).toBeLessThanOrEqual(65536);
  } finally {
    retained.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it("keeps async advance workspace charged until canceled work actually settles", async () => {
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  let started!: () => void;
  const ready = new Promise<void>((resolve) => {
    started = resolve;
  });
  let finish!: () => void;
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let closed = false;
  async function* source(): AsyncGenerator<OwnedMemory<NestedColumnRow>> {
    try {
      started();
      await pending;
      yield* [] as OwnedMemory<NestedColumnRow>[];
    } finally {
      closed = true;
    }
  }
  const iterator = readAlignedNestedRows([source()], scope);
  const next = iterator.next();
  const rejected = expect(next).rejects.toThrow(/disposed/);
  await ready;
  scope.dispose();
  expect(budget.usedBytes).toBeGreaterThan(0);
  expect(closed).toBe(false);
  finish();
  await rejected;
  expect(closed).toBe(true);
  expect(budget.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("aligns bounded row blocks and rejects unequal block sizes (mismatch=%s)", async (mismatch) => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  let closed = 0;
  const source = async function* (bad: boolean): AsyncGenerator<OwnedMemory<NestedColumnRow>> {
    let row = 0;
    try {
      for (const count of [2, bad ? 1 : 2, 1]) {
        const child = scope.childScope();
        const owner = child.allocate(256, "test block", () => ({
          row,
          rowCount: count,
          fragments: [],
        }));
        const pin = owner.retain();
        child.dispose();
        try {
          yield pin;
        } finally {
          pin.release();
        }
        row += count;
      }
    } finally {
      closed++;
    }
  };
  const consume = async () => {
    const blocks = [];
    for await (const owner of readAlignedNestedRows([source(false), source(mismatch)], scope, 2))
      blocks.push([owner.value.row, owner.value.rowCount ?? 1]);
    return blocks;
  };
  try {
    if (mismatch) await expect(consume()).rejects.toThrow(/row order mismatch/);
    else
      expect(await consume()).toEqual([
        [0, 2],
        [2, 2],
        [4, 1],
      ]);
    expect(closed).toBe(2);
    expect(scope.budget.usedBytes).toBe(0);
  } finally {
    scope.dispose();
  }
});
