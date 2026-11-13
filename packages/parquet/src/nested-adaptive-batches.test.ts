import { parquetMetadata } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, materializeBatchRows, memoryStore } from "lakeql-core";
import { expect, it, vi } from "vitest";
import * as fragmentWindows from "./nested-fragment-window.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";

async function fixture() {
  const records = Array.from({ length: 8192 }, (_, i) =>
    i % 11 === 0
      ? null
      : {
          score: i % 7 === 0 ? null : i,
          payload: `${i}:`.padEnd(16, "x"),
        },
  );
  const bytes = parquetWriteBuffer({
    schema: [
      { name: "root", num_children: 1 },
      { name: "record", num_children: 2, repetition_type: "OPTIONAL" },
      { name: "score", type: "INT32", repetition_type: "OPTIONAL" },
      { name: "payload", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "REQUIRED" },
    ],
    columnData: [{ name: "record", data: records }],
  });
  const store = memoryStore();
  await store.put("adaptive", new Uint8Array(bytes));
  return {
    records,
    metadata: parquetMetadata(bytes),
    file: await asyncBufferFromStore(store, "adaptive"),
  };
}

it("adapts assembly to available bytes without rereading or duplicating rows", async () => {
  const { records, metadata, file } = await fixture();
  const reads = vi.spyOn(file, "sliceOwned");
  const indexes = vi.spyOn(fragmentWindows, "indexNestedFragments");
  const counts: number[] = [];
  const batchCounts: number[] = [];
  for (const limit of [1024 * 1024, 512 * 1024]) {
    reads.mockClear();
    indexes.mockClear();
    const budget = new MemoryBudget(limit);
    let offset = 0,
      batches = 0;
    for await (const owner of readOwnedParquetVectorBatches(file, metadata, budget, {
      batchSize: 8192,
    })) {
      try {
        expect(owner.value.rowOffset).toBe(offset);
        const rows = materializeBatchRows(owner.value.batch);
        expect(rows).toEqual(
          records.slice(offset, offset + rows.length).map((record) => ({ record })),
        );
        expect(rows.length).toBeGreaterThan(0);
        expect(rows.length).toBeLessThanOrEqual(8192);
        offset += rows.length;
        batches++;
      } finally {
        owner.release();
      }
    }
    expect(offset).toBe(records.length);
    batchCounts.push(batches);
    if (limit === 1024 * 1024) expect(indexes).not.toHaveBeenCalled();
    else {
      expect(indexes.mock.calls.length).toBeGreaterThan(0);
      expect(indexes.mock.calls.length).toBeLessThanOrEqual(metadata.row_groups.length * 2);
    }
    expect(budget.usedBytes).toBe(0);
    expect(budget.peakBytes).toBeLessThanOrEqual(limit);
    counts.push(reads.mock.calls.length);
  }
  expect(counts[1]).toBe(counts[0]);
  expect(batchCounts[1]).toBeGreaterThan(batchCounts[0] ?? Infinity);
  indexes.mockRestore();
});

it("keeps an adaptive output alive after early close and releases all backing afterwards", async () => {
  const { records, metadata, file } = await fixture();
  const budget = new MemoryBudget(512 * 1024);
  const iterator = readOwnedParquetVectorBatches(file, metadata, budget, { batchSize: 8192 });
  const first = await iterator.next();
  if (first.done) throw Error("Missing adaptive output");
  try {
    await iterator.return(undefined);
    const rows = materializeBatchRows(first.value.value.batch);
    expect(rows.length).toBeLessThan(records.length);
    expect(rows).toEqual(records.slice(0, rows.length).map((record) => ({ record })));
    expect(budget.usedBytes).toBeGreaterThan(0);
  } finally {
    first.value.release();
    await iterator.return(undefined);
  }
  expect(budget.usedBytes).toBe(0);
});
