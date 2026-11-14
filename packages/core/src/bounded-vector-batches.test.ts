import { expect, it } from "vitest";
import { batchFromColumns, type Vector, vectorValue } from "./batch.js";
import { boundedVectorBatches } from "./bounded-vector-batches.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

const batch = batchFromColumns({ id: [10, 11, 12, 13, 14], text: ["a", "b", "c", "d", "e"] });

it("keeps physical offsets and bounded sizes through a short final batch", () => {
  const budget = new MemoryBudget(8192);
  const observed = [];
  for (const input of boundedVectorBatches({ rowOffset: 20, batch }, 2, budget)) {
    expect(input.owner).toBeUndefined();
    expect(budget.usedBytes).toBeGreaterThan(0);
    observed.push({
      offset: input.rowOffset,
      count: input.batch.rowCount,
      first: vectorValue(input.batch.columns.id as Vector, 0),
    });
  }
  expect(observed).toEqual([
    { offset: 20, count: 2, first: 10 },
    { offset: 22, count: 2, first: 12 },
    { offset: 24, count: 1, first: 14 },
  ]);
  expect(budget.usedBytes).toBe(0);
});

it("retained slice owns both metadata and source backing after iterator and source close", () => {
  const budget = new MemoryBudget(8192);
  const source = new MemoryScope(budget);
  const owner = source.allocate(1000, "source backing", () => ({ rowOffset: 20, batch }));
  const stream = boundedVectorBatches({ ...owner.value, owner }, 2, budget);
  const first = stream.next();
  if (first.done || !first.value.owner) throw new Error("missing output owner");
  const retained = first.value.owner.retain();
  expect(retained.value).toEqual({ rowOffset: 20, batch: first.value.batch });
  stream.return();
  source.dispose();
  expect(budget.usedBytes).toBeGreaterThan(1000);
  expect(retained.value.batch.rowCount).toBe(2);
  retained.release();
  expect(budget.usedBytes).toBe(0);
});

it("admits at exact measured capacity and rejects below it without leaking a sibling", () => {
  const run = (cap: number) => {
    const budget = new MemoryBudget(cap);
    const sibling = budget.reserve(37, "sibling");
    const stream = boundedVectorBatches({ rowOffset: 0, batch }, 2, budget);
    try {
      for (const _ of stream) {
      }
    } finally {
      stream.return();
      expect(budget.usedBytes).toBe(37);
      sibling.release();
    }
    return budget.peakBytes;
  };
  const peak = run(8192);
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrow(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
});

it.each([false, true])("abort cleans slice metadata (before=%s)", (before) => {
  const budget = new MemoryBudget(8192);
  const controller = new AbortController();
  const stream = boundedVectorBatches({ rowOffset: 0, batch }, 2, budget, controller.signal);
  if (!before) expect(stream.next().done).toBe(false);
  controller.abort();
  expect(budget.usedBytes).toBe(0);
  expect(() => stream.next()).toThrow(expect.objectContaining({ code: "LAKEQL_ABORTED" }));
});

it("passes through complete and empty input without allocating", () => {
  const budget = new MemoryBudget(0);
  for (const value of [batch, { rowCount: 0, columns: {} }]) {
    const input = { rowOffset: 0, batch: value };
    expect([...boundedVectorBatches(input, 5, budget)]).toEqual([input]);
  }
  expect(budget.peakBytes).toBe(0);
});
