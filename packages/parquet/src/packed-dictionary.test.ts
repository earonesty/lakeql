import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, materializeBatchRows, memoryStore, SharedMemoryCache } from "lakeql-core";
import { expect, it } from "vitest";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it.each([
  "text",
  "binary",
  "fixed",
] as const)("retains packed %s dictionaries across cache eviction", async (kind) => {
  const values = Array.from({ length: 256 }, (_, index) =>
    index % 7 === 0
      ? null
      : kind === "text"
        ? index % 2
          ? "λ😀"
          : ""
        : Uint8Array.of(index % 2, 255, 128),
  );
  const bytes = parquetWriteBuffer({
    codec: "SNAPPY",
    pageSize: 128,
    schema: [
      { name: "root", num_children: 1 },
      {
        name: "value",
        repetition_type: "OPTIONAL",
        type: kind === "fixed" ? "FIXED_LEN_BYTE_ARRAY" : "BYTE_ARRAY",
        ...(kind === "fixed" ? { type_length: 3 } : {}),
        ...(kind === "text" ? { converted_type: "UTF8" as const } : {}),
      },
    ],
    columnData: [{ name: "value", data: values }],
  });
  const store = memoryStore();
  await store.put("dict.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "dict.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  expect(metadata.row_groups[0]?.columns[0]?.meta_data?.dictionary_page_offset).toBeDefined();
  const runBounded = async (limit: number) => {
    const budget = new MemoryBudget(limit);
    const actual = [];
    try {
      for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
        columns: ["value"],
        batchSize: 13,
      })) {
        actual.push(...materializeBatchRows(owner.value.batch));
        owner.release();
      }
      expect(actual).toEqual(values.map((value) => ({ value })));
      return budget.peakBytes;
    } finally {
      expect(budget.usedBytes).toBe(0);
    }
  };
  const peak = await runBounded(1024 * 1024);
  await runBounded(peak);
  await expect(runBounded(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  const root = new MemoryBudget(1024 * 1024);
  const shared = new SharedMemoryCache({ maxBytes: root.maxBytes, memoryBudget: root });
  const cache = new DecodedColumnCache(shared, { maxBytes: root.maxBytes, policy: "latency" });
  const options = {
    columns: ["value"],
    batchSize: 13,
    decodedColumnCache: cache,
    decodedColumnCacheKey: "dict",
  };
  for (const warm of [false, true]) {
    const job = root.child();
    const actual = [];
    const iterator = readOwnedParquetVectorBatches(source, metadata, job, options)[
      Symbol.asyncIterator
    ]();
    const first = await iterator.next();
    if (first.done) throw new Error("missing dictionary batch");
    const vector = first.value.value.batch.columns.value;
    expect(vector?.type).toBe("dict");
    if (vector?.type !== "dict") throw new Error("missing dictionary vector");
    expect(vector.dictionary.type).toBe(kind === "text" ? "utf8-view" : "binary-view");
    actual.push(...materializeBatchRows(first.value.value.batch));
    for (let next = await iterator.next(); !next.done; next = await iterator.next()) {
      actual.push(...materializeBatchRows(next.value.value.batch));
      next.value.release();
    }
    expect(actual).toEqual(values.map((value) => ({ value })));
    if (warm) shared.deleteWhere(() => true);
    expect(materializeBatchRows(first.value.value.batch)).toEqual(actual.slice(0, 13));
    first.value.release();
    expect(job.usedBytes).toBe(0);
  }
  expect(root.usedBytes).toBe(0);
});
