import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, materializeBatchRows, memoryStore, SharedMemoryCache } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it.each([
  false,
  true,
])("retains cached packed pages across multi-column advancement (window=%s)", async (canStopEarly) => {
  const rows = Array.from({ length: 128 }, (_, id) => ({ id, label: `value-${id}-λ` }));
  const bytes = parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    pageSize: 64,
    rowGroupSize: [rows.length],
    schema: [
      { name: "root", num_children: 2 },
      { name: "id", type: "INT32", repetition_type: "REQUIRED" },
      { name: "label", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "REQUIRED" },
    ],
    columnData: [
      { name: "id", encoding: "PLAIN", data: rows.map((row) => row.id) },
      { name: "label", encoding: "PLAIN", data: rows.map((row) => row.label) },
    ],
  });
  const store = memoryStore();
  await store.put("pages.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "pages.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  const root = new MemoryBudget(1024 * 1024);
  const shared = new SharedMemoryCache({ maxBytes: root.maxBytes, memoryBudget: root });
  const cache = new DecodedColumnCache(shared, { maxBytes: root.maxBytes, policy: "latency" });
  const writes = vi.spyOn(cache, "setVectorOwned");
  const options = {
    columns: ["id", "label"],
    batchSize: 7,
    canStopEarly,
    decodedColumnCache: cache,
    decodedColumnCacheKey: "packed-pages",
  };
  const cold = root.child();
  const actual = [];
  for await (const owner of readOwnedParquetVectorBatches(source, metadata, cold, options)) {
    actual.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(actual).toEqual(rows);
  expect(writes.mock.calls.length).toBeGreaterThan(2);
  expect(cold.usedBytes).toBe(0);
  writes.mockClear();
  const warm = root.child();
  const iterator = readOwnedParquetVectorBatches(source, metadata, warm, options)[
    Symbol.asyncIterator
  ]();
  const first = await iterator.next();
  if (first.done) throw new Error("missing packed output");
  const firstRows = materializeBatchRows(first.value.value.batch);
  try {
    for (;;) {
      const next = await iterator.next();
      if (next.done) break;
      next.value.release();
    }
    expect(writes).not.toHaveBeenCalled();
    shared.deleteWhere(() => true);
    expect(warm.usedBytes).toBeGreaterThan(0);
    expect(root.usedBytes).toBe(warm.usedBytes);
    expect(materializeBatchRows(first.value.value.batch)).toEqual(firstRows);
    expect(firstRows).toEqual(rows.slice(0, firstRows.length));
  } finally {
    await iterator.return?.();
    first.value.release();
    shared.deleteWhere(() => true);
  }
  expect(root.usedBytes).toBe(0);
  expect(warm.usedBytes).toBe(0);
});
