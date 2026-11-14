import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { collectOwnedRowSlice } from "./owned-row-slice.js";
import type { Row } from "./types.js";

async function* batches() {
  yield [];
  yield [{ id: 0 }, { id: 1 }];
  yield [];
  yield [{ id: 2 }, { id: 3 }, { id: 4 }];
}

it.each([
  [0, 0],
  [0, 2],
  [1, 3],
  [2, 3],
  [5, 2],
  [9, 2],
])("collects offset %s limit %s across empty and partial batches", async (offset, limit) => {
  const budget = new MemoryBudget(65536);
  const result = await collectOwnedRowSlice(batches(), budget, offset, limit);
  expect(result.rows.value).toEqual(
    [0, 1, 2, 3, 4].slice(offset, offset + limit).map((id) => ({ id })),
  );
  expect(result.hasMore).toBe(offset + limit < 5);
  expect(result.seen).toBe(Math.min(offset + limit, 5));
  result.rows.release();
  expect(budget.usedBytes).toBe(0);
});

it("detaches selected nested bytes before producer advance and closure", async () => {
  const budget = new MemoryBudget(65536);
  const row = { data: { bytes: new Uint8Array([1, 2]) } };
  let closed = false;
  async function* source() {
    try {
      yield [row];
      row.data.bytes.fill(9);
      // Lookahead is deliberately outside the detachable data contract.
      yield [{ invalid: () => {} }];
    } finally {
      row.data.bytes.fill(7);
      closed = true;
    }
  }
  const result = await collectOwnedRowSlice(source(), budget, 0, 1);
  expect(closed).toBe(true);
  expect(result.hasMore).toBe(true);
  expect(result.rows.value).toEqual([{ data: { bytes: new Uint8Array([1, 2]) } }]);
  expect(budget.usedBytes).toBeGreaterThan(0);
  result.rows.release();
  expect(budget.usedBytes).toBe(0);
});

it("admits exact peak, rejects one byte below, and preserves sibling reservations", async () => {
  const measure = new MemoryBudget(65536);
  const measured = await collectOwnedRowSlice(batches(), measure, 1, 4);
  const peak = measure.peakBytes;
  measured.rows.release();
  for (const cap of [peak - 1, peak]) {
    const budget = new MemoryBudget(cap + 37);
    const sibling = budget.reserve(37, "sibling");
    if (cap < peak)
      await expect(collectOwnedRowSlice(batches(), budget, 1, 4)).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
      });
    else {
      const result = await collectOwnedRowSlice(batches(), budget, 1, 4);
      expect(result.rows.value).toHaveLength(4);
      result.rows.release();
    }
    expect(budget.usedBytes).toBe(37);
    sibling.release();
    expect(budget.usedBytes).toBe(0);
  }
});

it("releases collected and partial payload before awaited producer cleanup", async () => {
  const budget = new MemoryBudget(4096);
  let entered!: () => void;
  let finish!: () => void;
  const closing = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  async function* source(): AsyncIterable<Row[]> {
    try {
      yield [{ id: 1 }];
      yield [{ text: "x".repeat(4096) }];
    } finally {
      entered();
      await gate;
    }
  }
  const rejected = expect(collectOwnedRowSlice(source(), budget, 0, 3)).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  await closing;
  try {
    expect(budget.usedBytes).toBe(0);
  } finally {
    finish();
  }
  await rejected;
});

it("releases retained rows on cancellation while waiting for another batch", async () => {
  const budget = new MemoryBudget(4096);
  const abort = new AbortController();
  async function* source() {
    yield [{ id: 1 }];
    expect(budget.usedBytes).toBeGreaterThan(0);
    abort.abort();
    expect(budget.usedBytes).toBe(0);
    yield [{ id: 2 }];
  }
  await expect(collectOwnedRowSlice(source(), budget, 0, 3, abort.signal)).rejects.toMatchObject({
    code: "LAKEQL_ABORTED",
  });
  expect(budget.usedBytes).toBe(0);
});
