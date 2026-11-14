import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, MemoryScope, materializeBatchRows, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import { readParquetVectorBatchesFromFile } from "./vector-batches.js";

it.each([false, true])("decodes fixed binary as packed backing, nullable=%s", async (nullable) => {
  const values = nullable
    ? [null, Uint8Array.of(0, 255, 1), null, Uint8Array.of(128, 0, 2), null]
    : [Uint8Array.of(0, 255, 1), Uint8Array.of(128, 0, 2)];
  const bytes = parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    schema: [
      { name: "root", num_children: 1 },
      {
        name: "value",
        type: "FIXED_LEN_BYTE_ARRAY",
        type_length: 3,
        repetition_type: nullable ? "OPTIONAL" : "REQUIRED",
      },
    ],
    columnData: [{ name: "value", encoding: "PLAIN", data: values }],
  });
  const source = {
    byteLength: bytes.byteLength,
    slice: async (start: number, end?: number) => bytes.slice(start, end),
  };
  const metadata = await readParquetMetadataFromFile(source);
  const run = async (cap: number) => {
    const budget = new MemoryBudget(cap);
    const scope = new MemoryScope(budget);
    const rows = [];
    try {
      for await (const { batch } of readParquetVectorBatchesFromFile(source, metadata, {
        columns: ["value"],
        allocationScope: scope,
        batchSize: 2,
      })) {
        const vector = batch.columns.value;
        const packed = vector?.type === "dict" ? vector.dictionary : vector;
        expect(packed?.type).toBe("binary-view");
        if (packed?.type !== "binary-view") throw new Error("expected packed binary");
        expect(packed.ranges).toBeUndefined();
        expect(packed.fixedLength).toBe(3);
        rows.push(...materializeBatchRows(batch));
      }
      expect(rows).toEqual(values.map((value) => ({ value })));
      return budget.peakBytes;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  };
  const store = memoryStore();
  await store.put("fixed.parquet", new Uint8Array(bytes));
  const ownedSource = await asyncBufferFromStore(store, "fixed.parquet");
  const ownedBudget = new MemoryBudget(8192);
  const iterator = readOwnedParquetVectorBatches(ownedSource, metadata, ownedBudget, {
    columns: ["value"],
    batchSize: 2,
  })[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) throw new Error("missing owned batch");
  await iterator.return?.();
  expect(ownedBudget.usedBytes).toBeGreaterThan(0);
  expect(materializeBatchRows(first.value.value.batch)).toEqual(
    values.slice(0, 2).map((value) => ({ value })),
  );
  first.value.release();
  expect(ownedBudget.usedBytes).toBe(0);

  const peak = await run(1024);
  await run(peak);
  if (nullable) {
    expect(peak).toBeGreaterThan(0);
    await expect(run(peak - 1)).rejects.toThrow(/budget/i);
  } else {
    expect(peak).toBe(0); // Borrowed input backing; no offsets or value-copy allocation.
  }
});
