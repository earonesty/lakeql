import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { borrowOwnedRowBatches } from "./owned-row-batches.js";

function input(budget: MemoryBudget, rows = [{ id: 1 }, { id: 2 }, { id: 3 }]) {
  const scope = new MemoryScope(budget);
  const owner = scope.allocate(1000, "fixture rows", () => rows).retain();
  scope.dispose();
  return owner;
}

it("reuses a complete batch without increasing its live or peak charge", () => {
  const budget = new MemoryBudget(1000);
  const owner = input(budget);
  const rows = owner.value;
  const batches = borrowOwnedRowBatches(owner, 3);
  expect(batches.next().value).toBe(rows);
  expect(budget.usedBytes).toBe(1000);
  expect(budget.peakBytes).toBe(1000);
  expect(batches.next().done).toBe(true);
  expect(budget.usedBytes).toBe(0);
});

it("releases input and partial batch admission on insufficient directory capacity", () => {
  const budget = new MemoryBudget(1000);
  expect(() => borrowOwnedRowBatches(input(budget), 2).next()).toThrow(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
  expect(budget.usedBytes).toBe(0);
});

it.each([false, true])("cancels before or during borrowed output (before=%s)", (before) => {
  const budget = new MemoryBudget(4096);
  const abort = new AbortController();
  const owner = input(budget);
  if (before) abort.abort();
  const batches = borrowOwnedRowBatches(owner, 2, abort.signal);
  if (!before) {
    expect(batches.next().value).toEqual([{ id: 1 }, { id: 2 }]);
    expect(budget.usedBytes).toBeGreaterThan(1000);
    abort.abort();
    expect(budget.usedBytes).toBe(0);
  }
  expect(() => batches.next()).toThrow(expect.objectContaining({ code: "LAKEQL_ABORTED" }));
  expect(budget.usedBytes).toBe(0);
});

it("preserves an independent input owner on early consumer close", () => {
  const budget = new MemoryBudget(4096);
  const owner = input(budget);
  const independent = owner.retain();
  const batches = borrowOwnedRowBatches(owner, 2);
  batches.next();
  batches.return();
  expect(independent.value).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
  expect(budget.usedBytes).toBe(1000);
  independent.release();
  expect(budget.usedBytes).toBe(0);
});

it.each([
  0,
  -1,
  NaN,
  Infinity,
  0.5,
  Number.MAX_SAFE_INTEGER + 1,
])("rejects capacity %s and consumes its handle", (size) => {
  const budget = new MemoryBudget(4096);
  expect(() => borrowOwnedRowBatches(input(budget), size).next()).toThrow(
    expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
  );
  expect(budget.usedBytes).toBe(0);
});
