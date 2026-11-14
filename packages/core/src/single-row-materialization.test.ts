import { expect, it } from "vitest";
import { type Batch, batchFromColumns } from "./batch.js";
import { materializeOwnedBatchRow } from "./materialization-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  1, 1000,
])("materializes one packed candidate without visiting other rows (%s rows)", (count) => {
  const pool = new MemoryBudget(65536);
  const source = new MemoryScope(pool);
  const input = source.allocate(2 + count * 8, "packed fixture", (): Batch => {
    const ranges = new Uint32Array(count * 2);
    if (count > 1) ranges[1] = 99999; // An unselected malformed range must not be decoded.
    ranges[(count - 1) * 2 + 1] = 2;
    return {
      rowCount: count,
      columns: {
        text: { type: "utf8-view", data: new TextEncoder().encode("ok"), ranges, length: count },
      },
    };
  });
  const output = materializeOwnedBatchRow(input, count - 1);
  source.dispose();
  expect(output.value).toEqual({ text: "ok" });
  expect(pool.usedBytes).toBe(output.bytes);
  expect(output.bytes).toBeLessThan(1024);
  output.release();
  expect(pool.usedBytes).toBe(0);
});

it("pins binary aliases and validates single-row indices before allocation", () => {
  const pool = new MemoryBudget(65536);
  const source = new MemoryScope(pool);
  const batch = batchFromColumns({ value: [new Uint8Array([1, 2, 3])] }, source);
  const input = source.retainAllocations(batch);
  const before = pool.usedBytes;
  for (const index of [-1, 1, NaN, 0.5]) {
    expect(() => materializeOwnedBatchRow(input, index)).toThrow(/index/);
    expect(pool.usedBytes).toBe(before);
  }
  const output = materializeOwnedBatchRow(input, 0);
  input.release();
  source.dispose();
  expect(output.value).toEqual({ value: new Uint8Array([1, 2, 3]) });
  expect(pool.usedBytes).toBe(output.bytes);
  expect(pool.usedBytes).toBeGreaterThanOrEqual(before);
  output.release();
  expect(pool.usedBytes).toBe(0);
});
