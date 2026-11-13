import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, materializeBatchRows, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it("bounds page-window input and decode backing across growing real row groups", async () => {
  const peaks: number[] = [];
  for (const length of [128, 2048]) {
    const values = Array.from({ length }, (_, i) => i);
    const bytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      pageSize: 64,
      rowGroupSize: [length],
      columnData: [{ name: "id", type: "INT32", encoding: "PLAIN", data: values }],
    });
    const store = memoryStore();
    await store.put("pages.parquet", new Uint8Array(bytes));
    const source = await asyncBufferFromStore(store, "pages.parquet");
    const metadata = await readParquetMetadataFromFile(source);
    async function run(maxBytes: number, canStopEarly = true) {
      const budget = new MemoryBudget(maxBytes);
      const rows = [];
      try {
        for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
          columns: ["id"],
          canStopEarly,
          batchSize: 8,
        })) {
          rows.push(...materializeBatchRows(owner.value.batch));
          owner.release();
        }
        expect(rows).toEqual(values.map((id) => ({ id })));
        return budget.peakBytes;
      } finally {
        expect(budget.usedBytes).toBe(0);
      }
    }
    const peak = await run(65536);
    peaks.push(peak);
    await run(peak);
    await run(peak, false);
    await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    const budget = new MemoryBudget(65536);
    const iterator = readOwnedParquetVectorBatches(source, metadata, budget, {
      columns: ["id"],
      canStopEarly: true,
      batchSize: 8,
    })[Symbol.asyncIterator]();
    const first = await iterator.next();
    if (first.done) throw new Error("missing first output");
    try {
      for (;;) {
        const next = await iterator.next();
        if (next.done) break;
        next.value.release();
      }
      expect(budget.usedBytes).toBeGreaterThan(0);
      expect(materializeBatchRows(first.value.value.batch)).toEqual(
        values.slice(0, 8).map((id) => ({ id })),
      );
    } finally {
      await iterator.return?.();
      first.value.release();
    }
    expect(budget.usedBytes).toBe(0);
  }
  expect(peaks[1]).toBe(peaks[0]);
  expect(peaks[0]).toBeLessThan(1024);
});
