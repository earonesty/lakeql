import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, memoryStore } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { parquetBatchSize } from "./batch-size.js";
import { readParquetColumnBatchesFromFile } from "./column-batches.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import { readParquetVectorBatchesFromFile } from "./vector-batches.js";

it("rejects invalid batch sizes before direct, fallback, or owned reader I/O", async () => {
  const bytes = parquetWriteBuffer({
    columnData: [{ name: "id", type: "INT32", data: [1, 2, 3] }],
  });
  const store = memoryStore();
  await store.put("batch.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "batch.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  const slice = vi.spyOn(source, "slice");
  const ownedSlice = vi.spyOn(source, "sliceOwned");
  for (const batchSize of [0, -1, 0.5, Number.NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    const budget = new MemoryBudget(65536);
    const options = { columns: ["id"], batchSize };
    const readers = [
      readParquetColumnBatchesFromFile(source, metadata, options),
      readParquetVectorBatchesFromFile(source, metadata, options),
      readOwnedParquetVectorBatches(source, metadata, budget, options),
    ];
    for (const reader of readers) {
      await expect(reader[Symbol.asyncIterator]().next()).rejects.toMatchObject({
        code: "LAKEQL_VALIDATION_ERROR",
      });
    }
    expect(budget.peakBytes).toBe(0);
  }
  expect(slice).not.toHaveBeenCalled();
  expect(ownedSlice).not.toHaveBeenCalled();
});

it("preserves default and explicit safe row-count limits", () => {
  expect(parquetBatchSize(undefined)).toBe(4096);
  expect(parquetBatchSize(1)).toBe(1);
  expect(parquetBatchSize(Number.MAX_SAFE_INTEGER)).toBe(Number.MAX_SAFE_INTEGER);
});
