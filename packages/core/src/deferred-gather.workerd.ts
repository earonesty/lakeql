import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { gatherDeferredColumns } from "./deferred-gather.js";
import { materializeOwnedBatchRows } from "./materialization-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("retains deferred Worker binary output across both producer disposals", async () => {
  const pool = new MemoryBudget(8192);
  const early = new MemoryScope(pool);
  const source = early.retainAllocations({
    rowOffset: 20,
    batch: batchFromColumns({ id: [1, 2, 3] }, early),
  });
  early.dispose();
  let closed = false;
  async function* read(start: number, end: number) {
    expect([start, end]).toEqual([22, 23]);
    const late = new MemoryScope(pool);
    try {
      const data = late.allocate(3, "fixture bytes", () => Uint8Array.of(1, 2, 3));
      yield late.retainAllocations({
        rowOffset: 22,
        batch: batchFromColumns({ payload: [data.value] }, late),
      });
    } finally {
      late.dispose();
      closed = true;
    }
  }
  const iterator = gatherDeferredColumns(source, Uint32Array.of(2), 3, read);
  const next = await iterator.next();
  if (next.done) throw new Error("Missing output");
  const rows = materializeOwnedBatchRows(next.value);
  next.value.release();
  await iterator.return(undefined);
  source.release();
  expect(closed).toBe(true);
  expect(rows.value).toEqual([{ id: 3, payload: Uint8Array.of(1, 2, 3) }]);
  expect(pool.usedBytes).toBeGreaterThan(0);
  rows.release();
  expect(pool.usedBytes).toBe(0);
});
