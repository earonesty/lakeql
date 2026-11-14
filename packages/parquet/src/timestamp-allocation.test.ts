import { parquetWriteBuffer } from "hyparquet-writer";
import {
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  memoryStore,
  timestampFromEpoch,
} from "lakeql-core";
import { expect, it, vi } from "vitest";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import { readParquetVectorBatchesFromFile } from "./vector-batches.js";

it.each([false, true])("admits converted timestamp backing (nullable=%s)", async (nullable) => {
  const values = nullable
    ? [null, 1700000000000001n, null, -1n, 0n, null]
    : [1700000000000001n, 1700000000000002n, -1n, 0n];
  const bytes = parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    schema: [
      { name: "root", num_children: 1 },
      {
        name: "ts",
        type: "INT64",
        converted_type: "TIMESTAMP_MICROS",
        repetition_type: nullable ? "OPTIONAL" : "REQUIRED",
      },
    ],
    columnData: [{ name: "ts", encoding: "PLAIN", data: values }],
  });
  const store = memoryStore();
  await store.put("timestamp.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "timestamp.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  async function run(cap: number) {
    const budget = new MemoryBudget(cap);
    const scope = new MemoryScope(budget);
    const backing = new Set<ArrayBufferLike>();
    const allocate = scope.allocate.bind(scope);
    vi.spyOn(scope, "allocate").mockImplementation(
      <T>(bytes: number, purpose: string, factory: () => T) => {
        const owner = allocate(bytes, purpose, factory);
        if (ArrayBuffer.isView(owner.value)) backing.add(owner.value.buffer);
        return owner;
      },
    );
    const rows = [];
    try {
      for await (const { batch } of readParquetVectorBatchesFromFile(source, metadata, {
        columns: ["ts"],
        allocationScope: scope,
      })) {
        const vector = batch.columns.ts;
        expect(vector?.type).toBe("timestamp");
        if (vector?.type !== "timestamp") throw new Error("missing timestamp vector");
        expect(budget.usedBytes).toBeGreaterThanOrEqual(vector.values.byteLength);
        expect(backing.has(vector.values.buffer)).toBe(true);
        rows.push(...materializeBatchRows(batch));
      }
      expect(rows).toEqual(
        values.map((value) => ({
          ts: value === null ? null : timestampFromEpoch(value, "micros"),
        })),
      );
      return budget.peakBytes;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  }
  const peak = await run(65536);
  expect(peak).toBeGreaterThanOrEqual(values.length * 8);
  await run(peak);
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});
