import { parquetMetadata } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import {
  gte,
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  memoryStore,
  type QueryStats,
} from "lakeql-core";
import { expect, it } from "vitest";
import { readNestedRowGroupBatches } from "./nested-row-group-read.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it.each([
  undefined,
  2,
  3,
])("preserves global offsets and nullable structs across groups (prune=%s)", async (prune) => {
  const pruning = prune !== undefined;
  const records = [null, { score: null }, { score: 7 }, { score: -2 }, { score: 0 }, { score: 9 }];
  const bytes = parquetWriteBuffer({
    schema: [
      { name: "root", num_children: 2 },
      { name: "id", type: "INT32", repetition_type: "REQUIRED" },
      { name: "record", num_children: 1, repetition_type: "OPTIONAL" },
      { name: "score", type: "INT32", repetition_type: "OPTIONAL" },
    ],
    rowGroupSize: [2, 2, 2],
    columnData: [
      { name: "id", data: [0, 1, 2, 3, 4, 5] },
      { name: "record", data: records },
    ],
  });
  const metadata = parquetMetadata(bytes);
  const store = memoryStore();
  await store.put("nested.parquet", new Uint8Array(bytes));
  const file = await asyncBufferFromStore(store, "nested.parquet");
  const scope = new MemoryScope(new MemoryBudget(65536));
  const stats: QueryStats = {
    queryId: "nested-groups",
    elapsedMs: 0,
    manifestsRead: 0,
    manifestsSkipped: 0,
    filesPlanned: 0,
    filesRead: 0,
    filesSkipped: 0,
    rowGroupsRead: 0,
    rowGroupsSkipped: 0,
    columnsRead: [],
    bytesRequested: 0,
    rangeRequests: 0,
    rowsDecoded: 0,
    rowsMatched: 0,
    rowsReturned: 0,
    cacheHits: 0,
    cacheMisses: 0,
  };
  const rows: unknown[] = [],
    offsets: number[] = [];
  try {
    for await (const owner of readNestedRowGroupBatches(file, metadata, scope, {
      columns: ["record", "id"],
      batchSize: 2,
      rowStart: 1,
      rowEnd: 5,
      stats,
      ...(prune === undefined ? {} : { where: gte("id", prune) }),
    })) {
      try {
        offsets.push(owner.value.rowOffset);
        rows.push(...materializeBatchRows(owner.value.batch));
        expect(owner.value.residualPredicateSatisfied).toBe(
          !(prune === 3 && owner.value.rowOffset === 2),
        );
      } finally {
        owner.release();
      }
    }
    expect(offsets).toEqual(pruning ? [2, 4] : [1, 2, 4]);
    const start = pruning ? 2 : 1;
    expect(rows).toEqual(records.slice(start, 5).map((record, i) => ({ record, id: i + start })));
    expect(stats.rowGroupsRead).toBe(pruning ? 2 : 3);
    expect(stats.rowGroupsSkipped).toBe(pruning ? 1 : 0);
    expect(stats.rowsDecoded).toBe(pruning ? 3 : 4);
    expect(stats.columnsRead).toEqual(["id", "record"]);
    expect(stats.rowsMatched).toBe(0);
    expect(scope.budget.usedBytes).toBe(0);
  } finally {
    scope.dispose();
  }
});

it("validates windows and concurrency before schema allocation or transport", async () => {
  const bytes = parquetWriteBuffer({ columnData: [{ name: "id", data: [1, 2, 3] }] });
  const metadata = parquetMetadata(bytes);
  const store = memoryStore();
  await store.put("windows", new Uint8Array(bytes));
  const original = await asyncBufferFromStore(store, "windows");
  let reads = 0;
  const file = {
    ...original,
    sliceOwned: async () => {
      reads++;
      throw Error("Unexpected I/O");
    },
  };
  for (const options of [
    { rowStart: -1 },
    { rowStart: NaN },
    { rowStart: 0.5 },
    { rowEnd: Infinity },
    { rowEnd: -1 },
    { rowStart: 2, rowEnd: 1 },
    { rowStart: 0, rowEnd: 0, maxConcurrentReads: 0 },
    { rowStart: 0, rowEnd: 0, maxConcurrentPageReads: 0 },
  ]) {
    const scope = new MemoryScope(new MemoryBudget(0));
    try {
      await expect(
        readNestedRowGroupBatches(file, metadata, scope, { batchSize: 2, ...options }).next(),
      ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
      expect(scope.budget.usedBytes).toBe(0);
    } finally {
      scope.dispose();
    }
  }
  for (const options of [{ rowStart: 1, rowEnd: 1 }, { rowStart: 4 }]) {
    const scope = new MemoryScope(new MemoryBudget(0));
    try {
      expect(
        (
          await readNestedRowGroupBatches(file, metadata, scope, {
            batchSize: 2,
            ...options,
          }).next()
        ).done,
      ).toBe(true);
      expect(scope.budget.peakBytes).toBe(0);
    } finally {
      scope.dispose();
    }
  }
  expect(reads).toBe(0);
});

it("reads a large single block within a budget bounded independently of row-directory capacity", async () => {
  const count = 8192;
  const records = Array.from({ length: count }, (_, score) => ({ score }));
  const bytes = parquetWriteBuffer({
    rowGroupSize: count,
    schema: [
      { name: "root", num_children: 1 },
      { name: "record", num_children: 1, repetition_type: "REQUIRED" },
      { name: "score", type: "INT32", repetition_type: "REQUIRED" },
    ],
    columnData: [{ name: "record", data: records }],
  });
  const metadata = parquetMetadata(bytes);
  const store = memoryStore();
  await store.put("block", new Uint8Array(bytes));
  const file = await asyncBufferFromStore(store, "block");
  const budget = new MemoryBudget(256 * 1024);
  const scope = new MemoryScope(budget);
  let seen = 0;
  try {
    for await (const owner of readNestedRowGroupBatches(file, metadata, scope, {
      batchSize: count,
    })) {
      try {
        expect(owner.value.batch.rowCount).toBe(count);
        expect(materializeBatchRows(owner.value.batch)).toEqual(
          records.map((record) => ({ record })),
        );
        seen += owner.value.batch.rowCount;
      } finally {
        owner.release();
      }
    }
    expect(seen).toBe(count);
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
  expect(budget.peakBytes).toBeLessThanOrEqual(256 * 1024);
});
