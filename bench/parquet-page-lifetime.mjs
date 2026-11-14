// Decode-only measurement: transport buffers are borrowed from a real in-memory Parquet file.
import assert from "node:assert/strict";
import {
  MemoryBudget,
  MemoryScope,
  memoryStore,
  vectorValue,
} from "../packages/core/dist/index.js";
import { writeParquet } from "../packages/parquet/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/store-buffer.js";
import { readParquetVectorBatchesFromFile } from "../packages/parquet/dist/vector-batches.js";

for (const rowCount of [10000, 100000]) {
  const store = memoryStore();
  await writeParquet(store, "pages.parquet", {
    codec: "UNCOMPRESSED",
    pageSize: 1024,
    rowGroupSize: [rowCount],
    columnData: [
      {
        name: "id",
        type: "INT32",
        encoding: "PLAIN",
        data: Array.from({ length: rowCount }, (_, i) => i),
      },
      {
        name: "value",
        type: "DOUBLE",
        encoding: "PLAIN",
        data: Array.from({ length: rowCount }, (_, i) => i * 1.5),
      },
    ],
  });
  const file = await asyncBufferFromStore(store, "pages.parquet");
  const metadata = await readParquetMetadataFromFile(file);
  for (const lifetime of ["scope", "iteration"]) {
    const times = [];
    let peakBytes = 0;
    for (let repetition = 0; repetition < 8; repetition++) {
      const budget = new MemoryBudget(64 * 1024 * 1024);
      const scope = new MemoryScope(budget);
      const start = performance.now();
      let index = 0;
      try {
        for await (const { batch } of readParquetVectorBatchesFromFile(file, metadata, {
          columns: ["id", "value"],
          batchSize: 128,
          allocationScope: scope,
          pageAllocationLifetime: lifetime,
        })) {
          for (let row = 0; row < batch.rowCount; row++, index++) {
            assert.equal(vectorValue(batch.columns.id, row), index);
            assert.equal(vectorValue(batch.columns.value, row), index * 1.5);
          }
        }
        assert.equal(index, rowCount);
        if (lifetime === "iteration") assert.equal(budget.usedBytes, 0);
        peakBytes = budget.peakBytes;
      } finally {
        scope.dispose();
      }
      assert.equal(budget.usedBytes, 0);
      if (repetition >= 3) times.push(performance.now() - start);
    }
    times.sort((a, b) => a - b);
    console.log(
      JSON.stringify({
        rowCount,
        lifetime,
        peakBytes,
        medianMs: times[2],
        samples: 5,
        transport: "borrowed in-memory file",
        scope: "typed decode buffers; not whole heap",
      }),
    );
  }
}
