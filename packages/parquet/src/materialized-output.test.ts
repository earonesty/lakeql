import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, MemoryScope, materializeOwnedBatchRows, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { readOwnedParquetColumnBatches } from "./column-batches.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it("owns materialized real Parquet output after its scan and input owners close", async () => {
  const data = ["猫", null, "", "tail"];
  const store = memoryStore();
  await store.put(
    "output.parquet",
    new Uint8Array(
      parquetWriteBuffer({
        rowGroupSize: [2, 2],
        columnData: [
          { name: "id", type: "INT32", data: [0, 1, 2, 3] },
          { name: "text", type: "STRING", data },
        ],
      }),
    ),
  );
  const file = await asyncBufferFromStore(store, "output.parquet");
  const metadata = await readParquetMetadataFromFile(file);
  const budget = new MemoryBudget(1024 * 1024);
  const outputs = [];
  try {
    for await (const owner of readOwnedParquetColumnBatches(file, metadata, budget, {
      batchSize: 1,
    })) {
      const scope = new MemoryScope(budget);
      try {
        scope.adopt(owner.retain());
        const input = scope.retainAllocations(owner.value.batch);
        try {
          outputs.push(materializeOwnedBatchRows(input));
        } finally {
          input.release();
        }
      } finally {
        scope.dispose();
        owner.release();
      }
    }
    expect(budget.usedBytes).toBeGreaterThan(0);
    expect(outputs.flatMap((owner) => owner.value)).toEqual(data.map((text, id) => ({ id, text })));
    expect(budget.peakBytes).toBeLessThanOrEqual(budget.maxBytes);
  } finally {
    for (const output of outputs) output.release();
  }
  expect(budget.usedBytes).toBe(0);
});
