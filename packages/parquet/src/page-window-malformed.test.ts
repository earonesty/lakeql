import { deserializeTCompactProtocol } from "hyparquet/src/thrift.js";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, materializeBatchRows, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it("rejects truncated page bodies instead of completing with missing rows", async () => {
  const bytes = parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    columnData: [{ name: "id", type: "INT32", encoding: "PLAIN", data: [1, 2, 3, 4] }],
  });
  const store = memoryStore();
  await store.put("truncated.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "truncated.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  const column = metadata.row_groups[0]?.columns[0]?.meta_data;
  if (column === undefined) throw new Error("missing column metadata");
  column.total_compressed_size -= 1n;
  const budget = new MemoryBudget(65536);
  let outputs = 0;
  await expect(async () => {
    for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
      columns: ["id"],
      canStopEarly: true,
    })) {
      outputs++;
      owner.release();
    }
  }).rejects.toMatchObject({ code: "LAKEQL_PARQUET_READ_ERROR" });
  expect(outputs).toBe(0);
  expect(budget.usedBytes).toBe(0);
});

it("retries a header larger than the first probe when the chunk is smaller than the next probe", async () => {
  const bytes = new Uint8Array(
    parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      columnData: [{ name: "id", type: "INT32", encoding: "PLAIN", data: [1, 2, 3, 4] }],
    }),
  );
  const store = memoryStore();
  await store.put("header.parquet", bytes);
  const original = await asyncBufferFromStore(store, "header.parquet");
  const metadata = await readParquetMetadataFromFile(original);
  const column = metadata.row_groups[0]?.columns[0]?.meta_data;
  if (column === undefined) throw new Error("missing column metadata");
  const start = Number(column.data_page_offset);
  const reader = { view: new DataView(bytes.buffer, bytes.byteOffset + start), offset: 0 };
  deserializeTCompactProtocol(reader);
  const stop = start + reader.offset - 1;
  expect(bytes[stop]).toBe(0);
  // Unknown compact-protocol binary field 100, length 300: legal forward-compatible metadata.
  const extension = new Uint8Array(305);
  extension.set([0x08, 0xc8, 0x01, 0xac, 0x02]);
  const expanded = new Uint8Array(bytes.length + extension.length);
  expanded.set(bytes.subarray(0, stop));
  expanded.set(extension, stop);
  expanded.set(bytes.subarray(stop), stop + extension.length);
  column.total_compressed_size += BigInt(extension.length);
  expect(Number(column.total_compressed_size)).toBeGreaterThan(256);
  expect(Number(column.total_compressed_size)).toBeLessThan(1024);
  await store.put("expanded.parquet", expanded);
  const source = await asyncBufferFromStore(store, "expanded.parquet");
  const budget = new MemoryBudget(4096);
  const rows = [];
  for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
    columns: ["id"],
    canStopEarly: true,
  })) {
    rows.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(rows).toEqual([1, 2, 3, 4].map((id) => ({ id })));
  expect(budget.usedBytes).toBe(0);
});
