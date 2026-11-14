import { parquetWriteBuffer } from "hyparquet-writer";
import { type Batch, MemoryBudget, memoryStore, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { ParquetReadScope } from "./read-scope.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import { readParquetVectorBatchesFromFile } from "./vector-batches.js";

it("retains admitted Parquet backing after decode-stage disposal", async () => {
  const bytes = parquetWriteBuffer({
    columnData: [
      { name: "id", type: "INT32", data: [1, 2, 3, 4] },
      { name: "name", type: "STRING", data: ["é", "alpha", "😀", "alpha"] },
    ],
  });
  const store = memoryStore();
  await store.put("fixture.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "fixture.parquet");
  const budget = new MemoryBudget(1024 * 1024);
  const stage = new ParquetReadScope(source, budget);
  const metadata = await readParquetMetadataFromFile(stage.file);
  const batches: Batch[] = [];
  for await (const { batch } of readParquetVectorBatchesFromFile(stage.file, metadata, {
    columns: ["id", "name"],
    batchSize: 2,
    allocationScope: stage.allocations,
  })) {
    batches.push(batch);
  }
  const retained = stage.retainInputBacking(batches);
  const charged = budget.usedBytes;
  expect(charged).toBeGreaterThan(0);
  stage.dispose();
  expect(budget.usedBytes).toBe(charged);
  const rows: unknown[][] = [];
  for (const batch of retained.value) {
    const id = batch.columns.id;
    const name = batch.columns.name;
    if (!id || !name) throw new Error("missing fixture columns");
    for (let index = 0; index < batch.rowCount; index += 1)
      rows.push([vectorValue(id, index), vectorValue(name, index)]);
  }
  expect(rows).toEqual([
    [1, "é"],
    [2, "alpha"],
    [3, "😀"],
    [4, "alpha"],
  ]);
  retained.release();
  expect(budget.usedBytes).toBe(0);
});

it("rejects an unfunded fixture range before allocation and cleans up failed metadata reads", async () => {
  const store = memoryStore();
  await store.put("invalid.parquet", Uint8Array.of(1, 2, 3, 4, 5, 6, 7, 8));
  const source = await asyncBufferFromStore(store, "invalid.parquet");
  const stage = new ParquetReadScope(source, new MemoryBudget(0));
  await expect(stage.file.slice(0, 8)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(stage.allocations.budget.peakBytes).toBe(0);
  stage.dispose();
  const malformed = new ParquetReadScope(source, new MemoryBudget(8));
  await expect(readParquetMetadataFromFile(malformed.file)).rejects.toThrow();
  malformed.dispose();
  expect(malformed.allocations.budget.usedBytes).toBe(0);
});

it("does not use borrowed prefetch or range reads when ownership is unavailable", () => {
  expect(
    () =>
      new ParquetReadScope(
        {
          byteLength: 1,
          async slice() {
            throw new Error("untracked");
          },
        },
        new MemoryBudget(1),
      ),
  ).toThrow(/owned range/);
});
