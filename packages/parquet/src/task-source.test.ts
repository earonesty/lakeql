import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { taskMetadataOwned } from "./task-source.js";
import type { StoreAsyncBuffer } from "./types.js";

it.each([
  { owned: false, cancel: true },
  { owned: true, cancel: true },
  { owned: false, cancel: false },
  { owned: true, cancel: false },
])("releases task metadata when cancellation or expiry arrives during a read: %j", async ({
  owned,
  cancel,
}) => {
  const bytes = parquetWriteBuffer({
    columnData: [{ name: "id", type: "INT32", data: [1, 2, 3] }],
  });
  const controller = new AbortController();
  const budget = new MemoryBudget(128 * 1024);
  const scope = new MemoryScope(budget);
  let now = 0;
  let reads = 0;
  const afterRead = async () => {
    await Promise.resolve();
    reads++;
    if (cancel) controller.abort();
    else now = 20;
  };
  const file: StoreAsyncBuffer = {
    byteLength: bytes.byteLength,
    async slice(start, end) {
      const result = bytes.slice(start, end);
      await afterRead();
      return result;
    },
    ...(owned
      ? {
          async sliceOwned(start: number, end: number, readScope: MemoryScope) {
            const owner = readScope.allocate(64 + end - start, "fixture metadata read", () =>
              bytes.slice(start, end),
            );
            await afterRead();
            return owner;
          },
        }
      : {}),
  };
  await expect(
    taskMetadataOwned("actual.parquet", file, scope, {
      memoryBudget: budget,
      now: () => now,
      startedAt: 0,
      budget: { signal: controller.signal, maxElapsedMs: 10 },
    }),
  ).rejects.toMatchObject({ code: cancel ? "LAKEQL_ABORTED" : "LAKEQL_BUDGET_EXCEEDED" });
  expect(reads).toBeGreaterThan(0);
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});
