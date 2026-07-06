import { readFileSync } from "node:fs";
import type { RowGroup } from "hyparquet";
import {
  advanceTaskCheckpoint,
  and,
  between,
  col,
  createOutputManifest,
  createOutputManifestFromCheckpoints,
  createTaskManifest,
  createVectorAggregateStates,
  eq,
  finalizeVectorAggregateStates,
  fn,
  gt,
  gte,
  isIn,
  isNull,
  LakeqlError,
  like,
  lit,
  lt,
  lte,
  materializeBatchRows,
  memoryCache,
  memoryCheckpointAdapter,
  memoryStore,
  mergeVectorAggregateStates,
  mul,
  ne,
  not,
  notIn,
  or,
  restoreVectorAggregateStates,
  SharedMemoryCache,
  stableStringify,
  timestampFromEpoch,
} from "lakeql-core";
import {
  fixturePath,
  GEO,
  GROUPBY,
  H3,
  HIVE,
  ICEBERG,
  MANIFESTS,
  SALES,
  STATS,
  TYPES,
  WIDE,
  WRITE,
} from "lakeql-fixtures";
import { beforeAll, describe, expect, it } from "vitest";
import { lakeqlParquetCompressors } from "./compressors.js";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import {
  aggregateParquetGroupTasks,
  aggregateParquetTask,
  aggregateParquetTasks,
  createParquetLake,
  createParquetTableAs,
  type ParquetMetadata,
  partitionedParquetOutputEntries,
  planParquetTaskWorkUnits,
  planRowGroups,
  planRowGroupsFromMetadata,
  readIcebergParquetDeletes,
  readParquetColumnBatches,
  readParquetMetadata,
  readParquetObjects,
  rejectUnsupportedParquetSchema,
  rowGroupMayMatch,
  rowGroupMustMatch,
  scanParquetTaskBatches,
  scanParquetTaskColumnBatches,
  writeParquet,
  writePartitionedParquet,
  writePartitionedParquetTask,
} from "./index.js";
import {
  columnChunkRanges,
  countingObjectStore,
  delayedHeadObjectStore,
  delayedPathHeadObjectStore,
  rangeGuardObjectStore,
  testQueryStats,
} from "./test-helpers.js";

const store = memoryStore();

describe("lakeqlParquetCompressors", () => {
  it("exposes ZSTD decompression as a Parquet adapter capability", () => {
    const compressedHello = new Uint8Array([
      40, 181, 47, 253, 32, 5, 41, 0, 0, 104, 101, 108, 108, 111,
    ]);

    expect(new TextDecoder().decode(lakeqlParquetCompressors.ZSTD(compressedHello, 5))).toBe(
      "hello",
    );
  });
});

function rowGroupWithStats(
  column: string,
  minValue?: string | number | bigint,
  maxValue?: string | number | bigint,
): RowGroup {
  return rowGroupWithStatsEntries([[column, minValue, maxValue]]);
}

function rowGroupWithStatsEntries(
  entries: [
    column: string,
    minValue?: string | number | bigint,
    maxValue?: string | number | bigint,
  ][],
): RowGroup {
  return rowGroupFromStatsEntries(entries, "modern");
}

function rowGroupWithLegacyStats(
  column: string,
  minValue?: string | number | bigint,
  maxValue?: string | number | bigint,
): RowGroup {
  return rowGroupFromStatsEntries([[column, minValue, maxValue]], "legacy");
}

function rowGroupFromStatsEntries(
  entries: [
    column: string,
    minValue?: string | number | bigint,
    maxValue?: string | number | bigint,
  ][],
  mode: "modern" | "legacy",
): RowGroup {
  const columns: RowGroup["columns"] = [];
  for (const [column, minValue, maxValue] of entries) {
    const statistics: NonNullable<
      NonNullable<NonNullable<RowGroup["columns"][number]["meta_data"]>["statistics"]>
    > = {};
    const legacyStatistics = statistics as typeof statistics & { min?: unknown; max?: unknown };
    if (mode === "modern") {
      if (minValue !== undefined) statistics.min_value = minValue;
      if (maxValue !== undefined) statistics.max_value = maxValue;
    } else {
      if (minValue !== undefined) legacyStatistics.min = minValue;
      if (maxValue !== undefined) legacyStatistics.max = maxValue;
    }
    columns.push({
      file_offset: 0n,
      meta_data: {
        type: typeof minValue === "string" ? "BYTE_ARRAY" : "INT32",
        encodings: [],
        path_in_schema: [column],
        codec: "SNAPPY",
        num_values: 1n,
        total_uncompressed_size: 0n,
        total_compressed_size: 0n,
        data_page_offset: 0n,
        statistics: { ...statistics, null_count: 0n },
      },
    });
  }
  return {
    columns,
    total_byte_size: 0n,
    num_rows: 1n,
  };
}

function metadataWithSchema(schema: unknown[]): ParquetMetadata {
  return {
    version: 1,
    schema,
    num_rows: 0n,
    row_groups: [],
    metadata_length: 0,
  } as unknown as ParquetMetadata;
}

beforeAll(async () => {
  await store.put(`data/${SALES.file}`, readFileSync(fixturePath(SALES.file)));
  await store.put(`data/copy-${SALES.file}`, readFileSync(fixturePath(SALES.file)));
  await store.put(`data/${TYPES.file}`, readFileSync(fixturePath(TYPES.file)));
  await store.put(`data/${WIDE.file}`, readFileSync(fixturePath(WIDE.file)));
  await store.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
  await store.put(`data/${GROUPBY.file}`, readFileSync(fixturePath(GROUPBY.file)));
  await store.put(`data/${GEO.file}`, readFileSync(fixturePath(GEO.file)));
  await store.put(`data/${H3.file}`, readFileSync(fixturePath(H3.file)));
  await store.put(
    `data/${ICEBERG.equalityDeleteFile}`,
    readFileSync(fixturePath(ICEBERG.equalityDeleteFile)),
  );
  await store.put(
    `data/${ICEBERG.positionDeleteFile}`,
    readFileSync(fixturePath(ICEBERG.positionDeleteFile)),
  );
  for (const file of HIVE.files) {
    await store.put(`data/${file}`, readFileSync(fixturePath(file)));
  }
});

describe("readParquetObjects", () => {
  it("reads all rows from the sales fixture", async () => {
    const rows = await readParquetObjects(store, `data/${SALES.file}`);
    expect(rows).toHaveLength(SALES.rows);
    expect(rows[0]).toMatchObject({ store_id: "store-000", region: "west" });
  });

  it("projects columns", async () => {
    const rows = await readParquetObjects(store, `data/${SALES.file}`, {
      columns: ["region", "amount"],
    });
    expect(Object.keys(rows[0] as object).sort()).toEqual(["amount", "region"]);
  });

  it("respects rowStart/rowEnd", async () => {
    const rows = await readParquetObjects(store, `data/${SALES.file}`, {
      rowStart: 10,
      rowEnd: 15,
    });
    expect(rows).toHaveLength(5);
  });

  it("decodes the full type matrix, including int64 past MAX_SAFE_INTEGER", async () => {
    const rows = await readParquetObjects(store, `data/${TYPES.file}`);
    expect(rows).toHaveLength(TYPES.rows);
    const first = rows[0] as Record<string, unknown>;
    expect(first.id).toBe(0);
    expect(first.big).toBe(9007199254740991n);
    expect(first.flag).toBe(true);
    expect(first.name).toBeNull();
    const last = rows[TYPES.rows - 1] as Record<string, unknown>;
    expect(last.big).toBe(9007199254740991n + BigInt(TYPES.rows - 1));
  });

  it("fails loudly with LAKEQL_OBJECT_NOT_FOUND on a missing object", async () => {
    await expect(readParquetObjects(store, "data/nope.parquet")).rejects.toMatchObject({
      code: "LAKEQL_OBJECT_NOT_FOUND",
    });
  });

  it("wraps decode failures in LAKEQL_PARQUET_READ_ERROR", async () => {
    await store.put("data/garbage.parquet", new TextEncoder().encode("not parquet bytes"));
    await expect(readParquetObjects(store, "data/garbage.parquet")).rejects.toThrowError(
      LakeqlError,
    );
    await expect(readParquetObjects(store, "data/garbage.parquet")).rejects.toMatchObject({
      code: "LAKEQL_PARQUET_READ_ERROR",
    });
  });

  it("respects rowStart and rowEnd inside row groups", async () => {
    const outStore = memoryStore();
    await writeParquet(outStore, "data/range-window.parquet", {
      rowGroupSize: [3, 3],
      columnData: [{ name: "id", data: [0, 1, 2, 3, 4, 5], type: "INT32" }],
    });

    await expect(
      readParquetObjects(outStore, "data/range-window.parquet", {
        rowStart: 2,
        rowEnd: 5,
        batchSize: 2,
      }),
    ).resolves.toEqual([{ id: 2 }, { id: 3 }, { id: 4 }]);
  });
});

describe("readParquetColumnBatches", () => {
  it("reads typed column batches without materializing rows first", async () => {
    const expected = await readParquetObjects(store, `data/${SALES.file}`, {
      columns: ["store_id", "amount"],
      rowStart: 0,
      rowEnd: 2,
    });
    const batches = [];
    for await (const batch of readParquetColumnBatches(store, `data/${SALES.file}`, {
      columns: ["store_id", "amount"],
      batchSize: 7,
      rowStart: 0,
      rowEnd: 15,
    })) {
      batches.push(batch);
    }

    expect(batches.map((batch) => batch.batch.rowCount)).toEqual([7, 7, 1]);
    expect(batches[0]?.batch.columns.store_id?.type).toBe("utf8");
    expect(batches[0]?.batch.columns.amount?.type).toBe("f64");
    expect(
      materializeBatchRows(batches[0]?.batch ?? { rowCount: 0, columns: {} }).slice(0, 2),
    ).toEqual(expected);
  });

  it("reads top-level columns by default and reuses decoded column batches", async () => {
    const outStore = memoryStore();
    await writeParquet(outStore, "data/column-cache.parquet", {
      rowGroupSize: [3],
      columnData: [
        { name: "id", data: [1, 2, 3], type: "INT32" },
        { name: "label", data: ["a", "b", "c"], type: "STRING" },
      ],
    });
    const decodedColumnCache = new DecodedColumnCache(
      new SharedMemoryCache({ maxBytes: 1024 * 1024 }),
      { maxBytes: 1024 * 1024, policy: "latency" },
    );
    const coldStats = testQueryStats();
    const coldBatches = [];
    for await (const batch of readParquetColumnBatches(outStore, "data/column-cache.parquet", {
      batchSize: 2,
      stats: coldStats,
      decodedColumnCache,
      decodedColumnCacheKey: "data/column-cache.parquet",
    })) {
      coldBatches.push(batch);
    }

    expect(coldBatches.map((batch) => [batch.rowOffset, batch.batch.rowCount])).toEqual([
      [0, 2],
      [2, 1],
    ]);
    expect(materializeBatchRows(coldBatches[0]?.batch ?? { rowCount: 0, columns: {} })).toEqual([
      { id: 1, label: "a" },
      { id: 2, label: "b" },
    ]);
    expect(coldStats.columnsRead).toEqual(["id", "label"]);
    expect(coldStats.cacheMisses).toBeGreaterThan(0);

    const warmStats = testQueryStats();
    for await (const _ of readParquetColumnBatches(outStore, "data/column-cache.parquet", {
      batchSize: 2,
      stats: warmStats,
      decodedColumnCache,
      decodedColumnCacheKey: "data/column-cache.parquet",
    })) {
      // drain the warm read
    }
    expect(warmStats.cacheHits).toBeGreaterThan(0);
  });

  it("preserves null validity masks in column vectors", async () => {
    const expected = await readParquetObjects(store, `data/${TYPES.file}`, {
      columns: ["name", "flag", "big"],
      rowStart: 0,
      rowEnd: 3,
    });
    const batches = [];
    for await (const batch of readParquetColumnBatches(store, `data/${TYPES.file}`, {
      columns: ["name", "flag", "big"],
      rowStart: 0,
      rowEnd: 3,
    })) {
      batches.push(batch.batch);
    }

    const batch = batches[0];
    expect(batch?.columns.name?.type).toBe("utf8");
    expect(batch?.columns.name?.valid).toBeInstanceOf(Uint8Array);
    expect(materializeBatchRows(batch ?? { rowCount: 0, columns: {} })).toEqual(expected);
  });

  it("records batch-level row-group, column, and decoded-row metrics", async () => {
    const stats = testQueryStats();
    const batches = [];
    for await (const batch of readParquetColumnBatches(store, `data/${STATS.file}`, {
      columns: ["metric"],
      where: gte("metric", 100),
      batchSize: STATS.rowGroupSize,
      stats,
    })) {
      batches.push(batch);
    }

    expect(batches.map((batch) => batch.batch.rowCount)).toEqual([
      STATS.rowGroupSize,
      STATS.rowGroupSize,
    ]);
    expect(stats.columnsRead).toEqual(["metric"]);
    expect(stats.rowGroupsRead).toBe(2);
    expect(stats.rowGroupsSkipped).toBe(1);
    expect(stats.rowsDecoded).toBe(STATS.rowGroupSize * 2);
  });

  it("reads direct vector batches across scalar parquet leaf types and row windows", async () => {
    const outStore = memoryStore();
    await writeParquet(outStore, "data/vector-leaves.parquet", {
      rowGroupSize: [3, 3],
      columnData: [
        { name: "flag", data: [true, false, null, true, false, true], type: "BOOLEAN" },
        { name: "i32", data: [1, 2, 3, 4, 5, 6], type: "INT32" },
        { name: "i64", data: [1n, 2n, 3n, 4n, 5n, 6n], type: "INT64" },
        { name: "f32", data: [1.5, 2.5, 3.5, 4.5, 5.5, 6.5], type: "FLOAT" },
        { name: "f64", data: [10, 20, 30, 40, 50, 60], type: "DOUBLE" },
        { name: "name", data: ["a", "b", null, "d", "e", "f"], type: "STRING" },
      ],
    });

    const batches = [];
    for await (const batch of readParquetColumnBatches(outStore, "data/vector-leaves.parquet", {
      columns: ["flag", "i32", "i64", "f32", "f64", "name"],
      rowStart: 1,
      rowEnd: 5,
      batchSize: 2,
    })) {
      batches.push(batch);
    }

    expect(batches.map((batch) => [batch.rowOffset, batch.batch.rowCount])).toEqual([
      [1, 2],
      [3, 2],
    ]);
    expect(materializeBatchRows(batches[0]?.batch ?? { rowCount: 0, columns: {} })).toEqual([
      { flag: false, i32: 2, i64: 2n, f32: 2.5, f64: 20, name: "b" },
      { flag: null, i32: 3, i64: 3n, f32: 3.5, f64: 30, name: null },
    ]);
    expect(materializeBatchRows(batches[1]?.batch ?? { rowCount: 0, columns: {} })).toEqual([
      { flag: true, i32: 4, i64: 4n, f32: 4.5, f64: 40, name: "d" },
      { flag: false, i32: 5, i64: 5n, f32: 5.5, f64: 50, name: "e" },
    ]);
  });
});

describe("writeParquet", () => {
  it("writes Parquet bytes to an ObjectStore and round-trips through the reader", async () => {
    const outStore = memoryStore();
    const result = await writeParquet(outStore, "out/roundtrip.parquet", {
      rowGroupSize: [2],
      columnData: [
        { name: "id", data: [1, 2, 3], type: "INT32" },
        { name: "name", data: ["a", "b", "c"], type: "STRING" },
        { name: "score", data: [1.5, 2.5, 3.5], type: "DOUBLE" },
      ],
    });

    expect(result).toMatchObject({ path: "out/roundtrip.parquet", byteSize: expect.any(Number) });
    expect(result.byteSize).toBeGreaterThan(0);
    await expect(readParquetObjects(outStore, "out/roundtrip.parquet")).resolves.toEqual([
      { id: 1, name: "a", score: 1.5 },
      { id: 2, name: "b", score: 2.5 },
      { id: 3, name: "c", score: 3.5 },
    ]);
    await expect(outStore.head("out/roundtrip.parquet")).resolves.toMatchObject({
      contentType: "application/vnd.apache.parquet",
    });
  });

  it("wraps writer failures in LAKEQL_PARQUET_WRITE_ERROR", async () => {
    await expect(
      writeParquet(memoryStore(), "out/bad.parquet", {
        columnData: [
          { name: "id", data: [1, 2, 3], type: "INT32" },
          { name: "name", data: ["a"], type: "STRING" },
        ],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_PARQUET_WRITE_ERROR" });
  });

  it("enforces create-only output mode for direct writes", async () => {
    const outStore = memoryStore();
    await writeParquet(outStore, "out/create.parquet", {
      writeMode: "create",
      columnData: [{ name: "id", data: [1], type: "INT32" }],
    });

    await expect(
      writeParquet(outStore, "out/create.parquet", {
        writeMode: "create",
        columnData: [{ name: "id", data: [2], type: "INT32" }],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });

    await writeParquet(outStore, "out/create.parquet", {
      writeMode: "overwrite",
      columnData: [{ name: "id", data: [3], type: "INT32" }],
    });
    await expect(readParquetObjects(outStore, "out/create.parquet")).resolves.toEqual([{ id: 3 }]);
  });

  it("validates direct write mode", async () => {
    await expect(
      writeParquet(memoryStore(), "out/bad-mode.parquet", {
        writeMode: "append" as never,
        columnData: [{ name: "id", data: [1], type: "INT32" }],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  });

  it("matches the fixed write-golden fixture bytes", async () => {
    const outStore = memoryStore();
    await writeParquet(outStore, "out/write-golden.parquet", {
      rowGroupSize: [2],
      columnData: [
        { name: "id", data: [1, 2, 3], type: "INT32" },
        { name: "name", data: ["a", "b", "c"], type: "STRING" },
        { name: "score", data: [1.5, 2.5, 3.5], type: "DOUBLE" },
      ],
    });

    const actual = await outStore.get("out/write-golden.parquet");
    expect(actual).toEqual(new Uint8Array(readFileSync(fixturePath(WRITE.file))));
  });
});

describe("readIcebergParquetDeletes", () => {
  it("decodes Iceberg equality and position delete Parquet files", async () => {
    await expect(
      readIcebergParquetDeletes(store, {
        content: "equality-delete",
        path: `data/${ICEBERG.equalityDeleteFile}`,
      }),
    ).resolves.toEqual({
      equalityDeletes: [{ columns: ["country"], row: { country: "CA" } }],
    });

    await expect(
      readIcebergParquetDeletes(store, {
        content: "position-delete",
        path: `data/${ICEBERG.positionDeleteFile}`,
      }),
    ).resolves.toEqual({
      positionDeletes: [{ path: ICEBERG.dataFiles[0], position: 1 }],
    });
  });

  it("validates malformed Iceberg delete files with typed errors", async () => {
    const outStore = memoryStore();
    await writeParquet(outStore, "deletes/bad-position.parquet", {
      columnData: [
        { name: "file_path", data: [""], type: "STRING" },
        { name: "pos", data: [-1], type: "INT32" },
      ],
    });
    await writeParquet(outStore, "deletes/bad-equality.parquet", {
      columnData: [{ name: "_metadata", data: ["ignored"], type: "STRING" }],
    });

    await expect(
      readIcebergParquetDeletes(outStore, {
        content: "position-delete",
        path: "deletes/bad-position.parquet",
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      readIcebergParquetDeletes(outStore, {
        content: "equality-delete",
        path: "deletes/bad-equality.parquet",
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      readIcebergParquetDeletes(outStore, {
        content: "deletion-vector",
        path: "deletes/vector.dv",
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_UNSUPPORTED_DELETE_FILES" });
  });
});

describe("writePartitionedParquet", () => {
  it("writes deterministic hive-partitioned Parquet chunks", async () => {
    const outStore = memoryStore();
    const result = await writePartitionedParquet(outStore, "out/events", {
      rows: [
        { date: "2026-01-02", country: "CA", id: 3, amount: 30 },
        { date: "2026-01-01", country: "US", id: 1, amount: 10 },
        { date: "2026-01-01", country: "US", id: 2, amount: 20 },
      ],
      partitionBy: ["date", "country"],
      maxRowsPerFile: 1,
      jobId: "job",
    });

    expect(result.files.map((file) => file.path)).toEqual([
      "out/events/date=2026-01-01/country=US/part-job-00000.parquet",
      "out/events/date=2026-01-01/country=US/part-job-00001.parquet",
      "out/events/date=2026-01-02/country=CA/part-job-00002.parquet",
    ]);
    expect(result.files.map((file) => file.rowCount)).toEqual([1, 1, 1]);
    expect(result.files[0]).toMatchObject({
      byteSize: expect.any(Number),
      contentHash: expect.stringMatching(/^sha256:[0-9a-f]{64}$/u),
      partitionValues: { date: "2026-01-01", country: "US" },
    });
    expect(result.files[0]?.byteSize).toBeGreaterThan(0);

    await expect(
      readParquetObjects(outStore, "out/events/date=2026-01-01/country=US/part-job-00000.parquet"),
    ).resolves.toEqual([{ amount: 10, id: 1 }]);

    const lake = createParquetLake({ store: outStore });
    await expect(
      lake.hive("out/events/**/*.parquet").select(["id", "date", "country", "amount"]).toArray(),
    ).resolves.toEqual([
      { id: 1, date: "2026-01-01", country: "US", amount: 10 },
      { id: 2, date: "2026-01-01", country: "US", amount: 20 },
      { id: 3, date: "2026-01-02", country: "CA", amount: 30 },
    ]);
  });

  it("writes unpartitioned chunks under the prefix", async () => {
    const outStore = memoryStore();
    const result = await writePartitionedParquet(outStore, "out/plain/", {
      rows: [
        { id: 1, active: true },
        { id: 2, active: false },
        { id: 3, active: true },
      ],
      maxRowsPerFile: 2,
    });

    expect(result.files.map((file) => file.path)).toEqual([
      "out/plain/part-data-00000.parquet",
      "out/plain/part-data-00001.parquet",
    ]);
    await expect(readParquetObjects(outStore, result.files[0]?.path ?? "")).resolves.toEqual([
      { active: true, id: 1 },
      { active: false, id: 2 },
    ]);
    await expect(readParquetObjects(outStore, result.files[1]?.path ?? "")).resolves.toEqual([
      { active: true, id: 3 },
    ]);
  });

  it("includes task and idempotency components in deterministic output paths", async () => {
    const outStore = memoryStore();
    const result = await writePartitionedParquet(outStore, "out/tasks", {
      rows: [
        { date: "2026-01-01", id: 1 },
        { date: "2026-01-01", id: 2 },
      ],
      partitionBy: ["date"],
      maxRowsPerFile: 1,
      jobId: "job",
      taskId: "task/7",
      idempotencyKey: "attempt 1",
    });

    expect(result.files.map((file) => file.path)).toEqual([
      "out/tasks/date=2026-01-01/part-job-task%2F7-attempt%201-00000.parquet",
      "out/tasks/date=2026-01-01/part-job-task%2F7-attempt%201-00001.parquet",
    ]);
    await expect(readParquetObjects(outStore, result.files[0]?.path ?? "")).resolves.toEqual([
      { id: 1 },
    ]);

    const retried = await writePartitionedParquet(outStore, "out/tasks-retry", {
      rows: [
        { date: "2026-01-01", id: 1 },
        { date: "2026-01-01", id: 2 },
      ],
      partitionBy: ["date"],
      maxRowsPerFile: 1,
      jobId: "job",
      taskId: "task/7",
      idempotencyKey: "attempt 1",
    });
    expect(retried.files.map((file) => file.contentHash)).toEqual(
      result.files.map((file) => file.contentHash),
    );
  });

  it("enforces create-only output mode for partitioned writes", async () => {
    const outStore = memoryStore();
    const options = {
      rows: [{ id: 1 }, { id: 2 }],
      maxRowsPerFile: 1,
      jobId: "retry",
      writeMode: "create" as const,
    };
    const first = await writePartitionedParquet(outStore, "out/retry", options);

    await expect(writePartitionedParquet(outStore, "out/retry", options)).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });

    await writePartitionedParquet(outStore, "out/retry", {
      ...options,
      rows: [{ id: 3 }],
      writeMode: "overwrite",
    });
    await expect(readParquetObjects(outStore, first.files[0]?.path ?? "")).resolves.toEqual([
      { id: 3 },
    ]);
  });

  it("splits chunks that exceed maxBytesPerFile", async () => {
    const outStore = memoryStore();
    const rows = [
      { id: 1, payload: "a".repeat(64) },
      { id: 2, payload: "b".repeat(64) },
      { id: 3, payload: "c".repeat(64) },
    ];
    const twoRows = await writePartitionedParquet(outStore, "out/size-baseline-two", {
      rows: rows.slice(0, 2),
    });
    const threeRows = await writePartitionedParquet(outStore, "out/size-baseline-three", {
      rows,
    });
    const maxBytesPerFile = Math.floor(
      ((twoRows.files[0]?.byteSize ?? 0) + (threeRows.files[0]?.byteSize ?? 0)) / 2,
    );

    const result = await writePartitionedParquet(outStore, "out/size-limited", {
      rows,
      maxRowsPerFile: 3,
      maxBytesPerFile,
    });

    expect(result.files.map((file) => file.path)).toEqual([
      "out/size-limited/part-data-00000.parquet",
      "out/size-limited/part-data-00001.parquet",
    ]);
    expect(result.files.map((file) => file.rowCount)).toEqual([2, 1]);
    expect(result.files.every((file) => file.byteSize <= maxBytesPerFile)).toBe(true);
  });

  it("converts written files into output manifest entries", async () => {
    const outStore = memoryStore();
    const result = await writePartitionedParquet(outStore, "out/manifest", {
      rows: [
        { date: "2026-01-01", country: "US", id: 1 },
        { date: "2026-01-02", country: "CA", id: 2 },
      ],
      partitionBy: ["date", "country"],
      maxRowsPerFile: 1,
      jobId: "manifest",
    });

    const entries = partitionedParquetOutputEntries(result, {
      taskId: (_file, index) => `task-${index}`,
      iceberg: true,
    });
    const manifest = createOutputManifest({
      jobId: "job_manifest",
      planFingerprint: "fp_manifest",
      entries,
    });

    expect(manifest.entries).toEqual([
      {
        taskId: "task-0",
        outputPath: "out/manifest/date=2026-01-01/country=US/part-manifest-00000.parquet",
        partitionValues: { country: "US", date: "2026-01-01" },
        rowCount: 1,
        byteSize: result.files[0]?.byteSize,
        contentHash: result.files[0]?.contentHash,
        etag: result.files[0]?.etag,
        iceberg: {
          recordCount: 1,
          fileSizeInBytes: result.files[0]?.byteSize,
          partitionValues: { country: "US", date: "2026-01-01" },
        },
      },
      {
        taskId: "task-1",
        outputPath: "out/manifest/date=2026-01-02/country=CA/part-manifest-00001.parquet",
        partitionValues: { country: "CA", date: "2026-01-02" },
        rowCount: 1,
        byteSize: result.files[1]?.byteSize,
        contentHash: result.files[1]?.contentHash,
        etag: result.files[1]?.etag,
        iceberg: {
          recordCount: 1,
          fileSizeInBytes: result.files[1]?.byteSize,
          partitionValues: { country: "CA", date: "2026-01-02" },
        },
      },
    ]);
  });

  it("runs partitioned writes through task checkpoints", async () => {
    const outStore = memoryStore();
    const checkpoints = memoryCheckpointAdapter();
    const result = await writePartitionedParquetTask(outStore, "out/checkpointed", {
      checkpoints,
      taskId: "job_9-task-000001-a",
      idempotencyKey: "attempt-1",
      nowMs: 100,
      rows: [
        { country: "US", id: 1 },
        { country: "CA", id: 2 },
      ],
      partitionBy: ["country"],
      maxRowsPerFile: 1,
      jobId: "job_9",
      writeMode: "create",
      iceberg: true,
    });

    expect(result.entries.map((entry) => entry.outputPath)).toEqual([
      "out/checkpointed/country=CA/part-job_9-job_9-task-000001-a-attempt-1-00000.parquet",
      "out/checkpointed/country=US/part-job_9-job_9-task-000001-a-attempt-1-00001.parquet",
    ]);
    await expect(checkpoints.get("job_9-task-000001-a")).resolves.toMatchObject({
      state: "complete",
      outputs: [
        {
          taskId: "job_9-task-000001-a",
          iceberg: { recordCount: 1, partitionValues: { country: "CA" } },
        },
        {
          taskId: "job_9-task-000001-a",
          iceberg: { recordCount: 1, partitionValues: { country: "US" } },
        },
      ],
    });

    const replay = await writePartitionedParquetTask(outStore, "out/checkpointed", {
      checkpoints,
      taskId: "job_9-task-000001-a",
      idempotencyKey: "attempt-1",
      nowMs: 200,
      rows: [{ country: "US", id: 999 }],
      partitionBy: ["country"],
      jobId: "job_9",
      writeMode: "create",
    });
    expect(replay.entries).toEqual(result.entries);
    expect(replay.result.files.map((file) => file.path)).toEqual(
      result.result.files.map((file) => file.path),
    );
    const listedPaths: string[] = [];
    for await (const object of outStore.list("out/checkpointed/")) listedPaths.push(object.path);
    expect(listedPaths).toEqual(result.result.files.map((file) => file.path));
    await expect(
      createOutputManifestFromCheckpoints({
        jobId: "job_9",
        planFingerprint: "fp_job_9",
        checkpoints,
      }),
    ).resolves.toMatchObject({
      entries: result.entries,
    });
  });

  it("resumes partitioned write tasks from intermediate checkpoints", async () => {
    const runningStore = memoryStore();
    const runningCheckpoints = memoryCheckpointAdapter();
    await advanceTaskCheckpoint(runningCheckpoints, {
      taskId: "job_running-task-000001-a",
      nextState: "planned",
      idempotencyKey: "attempt-1",
      nowMs: 10,
    });
    await advanceTaskCheckpoint(runningCheckpoints, {
      taskId: "job_running-task-000001-a",
      nextState: "running",
      idempotencyKey: "attempt-1",
      nowMs: 11,
    });
    const running = await writePartitionedParquetTask(runningStore, "out/running", {
      checkpoints: runningCheckpoints,
      taskId: "job_running-task-000001-a",
      idempotencyKey: "attempt-1",
      nowMs: 20,
      rows: [{ country: "US", id: 1 }],
      partitionBy: ["country"],
      jobId: "job_running",
      writeMode: "create",
    });
    expect(running.entries).toHaveLength(1);
    await expect(runningCheckpoints.get("job_running-task-000001-a")).resolves.toMatchObject({
      state: "complete",
      outputs: [
        {
          outputPath:
            "out/running/country=US/part-job_running-job_running-task-000001-a-attempt-1-00000.parquet",
        },
      ],
    });

    const outputWrittenStore = memoryStore();
    const outputWrittenCheckpoints = memoryCheckpointAdapter();
    const prewritten = await writePartitionedParquet(outputWrittenStore, "out/output-written", {
      rows: [{ country: "CA", id: 2 }],
      partitionBy: ["country"],
      jobId: "job_output",
      taskId: "job_output-task-000001-a",
      idempotencyKey: "attempt-1",
      writeMode: "create",
    });
    const prewrittenEntries = partitionedParquetOutputEntries(prewritten, {
      taskId: "job_output-task-000001-a",
    });
    await advanceTaskCheckpoint(outputWrittenCheckpoints, {
      taskId: "job_output-task-000001-a",
      nextState: "planned",
      idempotencyKey: "attempt-1",
      nowMs: 30,
    });
    await advanceTaskCheckpoint(outputWrittenCheckpoints, {
      taskId: "job_output-task-000001-a",
      nextState: "running",
      idempotencyKey: "attempt-1",
      nowMs: 31,
    });
    await advanceTaskCheckpoint(outputWrittenCheckpoints, {
      taskId: "job_output-task-000001-a",
      nextState: "output-written",
      idempotencyKey: "attempt-1",
      nowMs: 32,
      outputs: prewrittenEntries,
    });
    const outputWritten = await writePartitionedParquetTask(
      outputWrittenStore,
      "out/output-written",
      {
        checkpoints: outputWrittenCheckpoints,
        taskId: "job_output-task-000001-a",
        idempotencyKey: "attempt-1",
        nowMs: 40,
        rows: [{ country: "CA", id: 999 }],
        partitionBy: ["country"],
        jobId: "job_output",
        writeMode: "create",
      },
    );
    expect(outputWritten.entries).toEqual(prewrittenEntries);
    await expect(
      readParquetObjects(outputWrittenStore, prewritten.files[0]?.path ?? ""),
    ).resolves.toEqual([{ id: 2 }]);

    const manifestCheckpoints = memoryCheckpointAdapter();
    await advanceTaskCheckpoint(manifestCheckpoints, {
      taskId: "job_manifest_resume-task-000001-a",
      nextState: "planned",
      idempotencyKey: "attempt-1",
      nowMs: 50,
    });
    await advanceTaskCheckpoint(manifestCheckpoints, {
      taskId: "job_manifest_resume-task-000001-a",
      nextState: "running",
      idempotencyKey: "attempt-1",
      nowMs: 51,
    });
    await advanceTaskCheckpoint(manifestCheckpoints, {
      taskId: "job_manifest_resume-task-000001-a",
      nextState: "output-written",
      idempotencyKey: "attempt-1",
      nowMs: 52,
      outputs: prewrittenEntries.map((entry) => ({
        ...entry,
        taskId: "job_manifest_resume-task-000001-a",
      })),
    });
    await advanceTaskCheckpoint(manifestCheckpoints, {
      taskId: "job_manifest_resume-task-000001-a",
      nextState: "manifest-recorded",
      idempotencyKey: "attempt-1",
      nowMs: 53,
    });
    await writePartitionedParquetTask(outputWrittenStore, "out/manifest-resume", {
      checkpoints: manifestCheckpoints,
      taskId: "job_manifest_resume-task-000001-a",
      idempotencyKey: "attempt-1",
      nowMs: 60,
      rows: [{ country: "US", id: 999 }],
      partitionBy: ["country"],
      jobId: "job_manifest_resume",
      writeMode: "create",
    });
    await expect(
      manifestCheckpoints.get("job_manifest_resume-task-000001-a"),
    ).resolves.toMatchObject({
      state: "complete",
      outputs: [{ taskId: "job_manifest_resume-task-000001-a" }],
    });
  });

  it("creates partitioned Parquet tables from query results through checkpoints", async () => {
    const ctasStore = memoryStore();
    await writePartitionedParquet(ctasStore, "src/events", {
      rows: [
        { country: "US", id: 1, amount: 10 },
        { country: "CA", id: 2, amount: 20 },
        { country: "US", id: 3, amount: 30 },
      ],
      partitionBy: ["country"],
      jobId: "source",
    });
    const lake = createParquetLake({ store: ctasStore });
    const checkpoints = memoryCheckpointAdapter();

    const created = await createParquetTableAs(ctasStore, "out/ctas", {
      query: lake.hive("src/events/**/*.parquet").select(["id", "amount", "country"]),
      checkpoints,
      jobId: "job_ctas",
      planFingerprint: "fp_ctas",
      taskId: "job_ctas-task-000000-a",
      idempotencyKey: "attempt-1",
      nowMs: 1000,
      partitionBy: ["country"],
      maxRowsPerFile: 2,
      writeMode: "create",
      iceberg: true,
    });

    expect(created.rowsRead).toBe(3);
    expect(created.manifest).toMatchObject({
      jobId: "job_ctas",
      planFingerprint: "fp_ctas",
      entries: [
        { taskId: "job_ctas-task-000000-a", partitionValues: { country: "CA" } },
        { taskId: "job_ctas-task-000000-a", partitionValues: { country: "US" } },
      ],
    });
    await expect(
      lake.hive("out/ctas/**/*.parquet").select(["id", "amount", "country"]).toArray(),
    ).resolves.toEqual([
      { id: 2, amount: 20, country: "CA" },
      { id: 1, amount: 10, country: "US" },
      { id: 3, amount: 30, country: "US" },
    ]);

    const replay = await createParquetTableAs(ctasStore, "out/ctas", {
      query: { toArray: async () => [{ country: "US", id: 999, amount: 999 }] },
      checkpoints,
      jobId: "job_ctas",
      planFingerprint: "fp_ctas",
      taskId: "job_ctas-task-000000-a",
      idempotencyKey: "attempt-1",
      partitionBy: ["country"],
      writeMode: "create",
    });
    expect(replay.entries).toEqual(created.entries);
    expect(replay.manifest).toEqual(created.manifest);
  });

  it("validates insert constraints before writing rows", async () => {
    const outStore = memoryStore();
    await writePartitionedParquet(outStore, "out/validated", {
      rows: [
        { id: 1, category: "a", score: 10 },
        { id: 2, category: "b", score: 20 },
      ],
      validation: {
        required: ["id", "category"],
        unique: [["id"]],
        ranges: { score: { min: 0, max: 100 } },
        enums: { category: ["a", "b"] },
      },
    });

    await expect(
      readParquetObjects(outStore, "out/validated/part-data-00000.parquet"),
    ).resolves.toHaveLength(2);
  });

  it("infers row column types and honors explicit type overrides", async () => {
    const outStore = memoryStore();
    const result = await writePartitionedParquet(outStore, "out/types", {
      rows: [
        {
          id: 1,
          active: true,
          big: 9007199254740993n,
          name: "a",
          score: 1.5,
          note: null,
          forced: 1,
        },
        {
          id: 2,
          active: false,
          big: 9007199254740994n,
          name: "b",
          score: 2.25,
          note: "present",
          forced: 2,
        },
      ],
      columnTypes: { forced: "DOUBLE" },
    });

    await expect(readParquetObjects(outStore, result.files[0]?.path ?? "")).resolves.toEqual([
      {
        id: 1,
        active: true,
        big: 9007199254740993n,
        name: "a",
        score: 1.5,
        note: null,
        forced: 1,
      },
      {
        id: 2,
        active: false,
        big: 9007199254740994n,
        name: "b",
        score: 2.25,
        note: "present",
        forced: 2,
      },
    ]);
  });

  it("validates row and option errors before writing", async () => {
    const outStore = memoryStore();

    await expect(
      writePartitionedParquet(outStore, "", { rows: [{ id: 1 }] }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/empty", { rows: [] }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/duplicate-partitions", {
        rows: [{ date: "2026-01-01", id: 1 }],
        partitionBy: ["date", "date"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/bad-limit", {
        rows: [{ id: 1 }],
        maxRowsPerFile: 0,
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/bad-byte-limit", {
        rows: [{ id: 1 }],
        maxBytesPerFile: 0,
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/bad-task", {
        rows: [{ id: 1 }],
        taskId: "",
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/bad-idempotency", {
        rows: [{ id: 1 }],
        idempotencyKey: " ",
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/bad-partition", {
        rows: [{ date: null, id: 1 }],
        partitionBy: ["date"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/mixed", { rows: [{ value: 1 }, { value: "x" }] }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/non-finite", { rows: [{ value: Number.NaN }] }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/object", { rows: [{ value: { nested: true } }] }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/all-null", { rows: [{ value: null }] }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/partition-only", {
        rows: [{ date: "2026-01-01" }],
        partitionBy: ["date"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/missing-required", {
        rows: [{ id: 1 }, { id: null }],
        validation: { required: ["id"] },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/duplicate", {
        rows: [{ id: 1 }, { id: 1 }],
        validation: { unique: [["id"]] },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/empty-unique", {
        rows: [{ id: 1 }],
        validation: { unique: [[]] },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/range", {
        rows: [{ score: 101 }],
        validation: { ranges: { score: { min: 0, max: 100 } } },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/range-type", {
        rows: [{ score: "101" }],
        validation: { ranges: { score: { min: 0, max: 100 } } },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/enum", {
        rows: [{ category: "c" }],
        validation: { enums: { category: ["a", "b"] } },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
  });
});

describe("createParquetLake", () => {
  it("queries projected rows from a path", async () => {
    const lake = createParquetLake({ store, queryId: () => "test-query" });
    const rows = await lake
      .path(`data/${SALES.file}`)
      .select(["store_id", "amount"])
      .where(eq("region", "west"))
      .limit(3)
      .toArray();

    expect(rows).toEqual([
      { store_id: "store-000", amount: 0 },
      { store_id: "store-004", amount: 148.04 },
      { store_id: "store-001", amount: 296.08 },
    ]);
  });

  it("expands globs over ObjectStore.list in deterministic order", async () => {
    const lake = createParquetLake({ store });
    const count = await lake.path("data/*sales.parquet").where(gt("amount", 900)).count();
    const single = await lake.path(`data/${SALES.file}`).where(gt("amount", 900)).count();
    expect(count).toBe(single * 2);
  });

  it("queries a prefix of loose Parquet files and prunes Hive partitions before scans", async () => {
    const prefixStore = memoryStore();
    await writeParquet(prefixStore, "events/year=2025/month=01/part-0.parquet", {
      columnData: [
        { name: "id", data: [1], type: "INT32" },
        { name: "amount", data: [10], type: "DOUBLE" },
      ],
    });
    await writeParquet(prefixStore, "events/year=2026/month=01/part-0.parquet", {
      columnData: [
        { name: "id", data: [2], type: "INT32" },
        { name: "amount", data: [20], type: "DOUBLE" },
      ],
    });

    const lake = createParquetLake({ store: prefixStore });
    const result = lake
      .hive("events/")
      .select(["id", "year", "month"])
      .where(eq("year", "2026"))
      .run();

    await expect(result.toArray()).resolves.toEqual([{ id: 2, year: "2026", month: "01" }]);
    expect(result.stats.filesSkipped).toBe(1);
    expect(result.stats.filesRead).toBe(1);
  });

  it("enforces file expansion budgets and empty-match errors while planning", async () => {
    const prefixStore = memoryStore();
    await writeParquet(prefixStore, "budget/a.parquet", {
      columnData: [{ name: "id", data: [1], type: "INT32" }],
    });
    await writeParquet(prefixStore, "budget/b.parquet", {
      columnData: [{ name: "id", data: [2], type: "INT32" }],
    });

    await expect(
      createParquetLake({ store: prefixStore, budget: { maxFiles: 1 } })
        .path("budget/*.parquet")
        .count(),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(
      createParquetLake({ store: prefixStore }).path("budget/missing/*.parquet").count(),
    ).rejects.toMatchObject({ code: "LAKEQL_NO_FILES_MATCHED" });
  });

  it("unifies compatible file schemas with nulls and rejects conflicting columns", async () => {
    const schemaStore = memoryStore();
    await writeParquet(schemaStore, "schemas/compatible/a.parquet", {
      columnData: [
        { name: "id", data: [1], type: "INT32" },
        { name: "extra", data: ["a"], type: "BYTE_ARRAY", converted_type: "UTF8" },
      ],
    });
    await writeParquet(schemaStore, "schemas/compatible/b.parquet", {
      columnData: [{ name: "id", data: [2], type: "INT32" }],
    });

    await expect(
      createParquetLake({ store: schemaStore }).path("schemas/compatible/*.parquet").toArray(),
    ).resolves.toEqual([
      { id: 1, extra: "a" },
      { id: 2, extra: null },
    ]);
    await expect(
      createParquetLake({ store: schemaStore })
        .path("schemas/compatible/*.parquet")
        .select(["extra"])
        .toArray(),
    ).resolves.toEqual([{ extra: "a" }, { extra: null }]);

    await writeParquet(schemaStore, "schemas/conflict/a.parquet", {
      columnData: [{ name: "id", data: [1], type: "INT32" }],
    });
    await writeParquet(schemaStore, "schemas/conflict/b.parquet", {
      columnData: [{ name: "id", data: ["2"], type: "BYTE_ARRAY", converted_type: "UTF8" }],
    });
    await expect(
      createParquetLake({ store: schemaStore }).path("schemas/conflict/*.parquet").toArray(),
    ).rejects.toMatchObject({ code: "LAKEQL_SCHEMA_CONFLICT" });
  });

  it("streams NDJSON and JSON with unsafe int64 mapped safely", async () => {
    const lake = createParquetLake({ store });
    const ndjson = await new Response(
      lake.path(`data/${TYPES.file}`).select(["id", "big"]).limit(1).streamNdjson(),
    ).text();
    expect(ndjson).toBe('{"id":0,"big":9007199254740991}\n');

    const json = await new Response(
      lake.path(`data/${TYPES.file}`).select(["id", "big"]).offset(2).limit(1).streamJson(),
    ).text();
    expect(json).toBe('[{"id":2,"big":"9007199254740993"}]');
  });

  it("accepts JSON query v1 and records stats", async () => {
    const lake = createParquetLake({ store, queryId: () => "json-query" });
    const result = lake.query({
      version: 1,
      from: `data/${SALES.file}`,
      select: ["region", "amount"],
      where: { eq: ["region", "east"] },
      limit: 2,
    });
    expect(await result.toArray()).toEqual([
      { region: "east", amount: 37.01 },
      { region: "east", amount: 185.05 },
    ]);
    expect(result.run().stats.queryId).toBe("json-query");
  });

  it("keeps emitted batches bounded by the caller batch size", async () => {
    const lake = createParquetLake({ store });
    const sizes: number[] = [];
    for await (const batch of lake.path(`data/${WIDE.file}`).batchSize(5).batches()) {
      sizes.push(batch.length);
      expect(batch.length).toBeLessThanOrEqual(5);
      expect(Object.keys(batch[0] as object)).toHaveLength(WIDE.columns);
    }
    expect(sizes).toEqual([5, 5, 5, 5, 4]);
  });

  it("enforces typed query budgets", async () => {
    const lake = createParquetLake({ store, budget: { maxRowsDecoded: 2 } });
    await expect(lake.path(`data/${SALES.file}`).toArray()).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
  });

  it("aggregates the groupby fixture and enforces maxGroups", async () => {
    const lake = createParquetLake({ store });
    await expect(
      lake
        .path(`data/${GROUPBY.file}`)
        .groupBy(["region"])
        .aggregate(
          {
            rows: { op: "count" },
            total: { op: "sum", column: "amount" },
            average: { op: "avg", column: "amount" },
            firstLabel: { op: "first", column: "label" },
            lastLabel: { op: "last", column: "label" },
          },
          { maxGroups: GROUPBY.groups },
        ),
    ).resolves.toEqual([
      { region: "west", rows: 2, total: 30, average: 15, firstLabel: "w1", lastLabel: "w2" },
      { region: "east", rows: 2, total: 20, average: 10, firstLabel: "e1", lastLabel: "e2" },
      { region: "north", rows: 2, total: 20, average: 10, firstLabel: "n1", lastLabel: "n2" },
      { region: "south", rows: 2, total: 10, average: 5, firstLabel: "s1", lastLabel: "s2" },
    ]);

    await expect(
      lake
        .path(`data/${GROUPBY.file}`)
        .groupBy(["region"])
        .aggregate(
          {
            rows: { op: "count" },
          },
          { maxGroups: GROUPBY.groups - 1 },
        ),
    ).rejects.toMatchObject({ code: "LAKEQL_GROUP_LIMIT_EXCEEDED" });
  });

  it("queries geo and h3 fixtures with function predicates", async () => {
    const lake = createParquetLake({ store });

    const geoResult = lake
      .path(`data/${GEO.file}`)
      .select(["id", "name"])
      .where(
        fn("st_intersects", col("geom"), fn("st_bbox", lit(-118.5), lit(34), lit(-118), lit(34.3))),
      )
      .run();
    await expect(geoResult.toArray()).resolves.toEqual([
      { id: 1, name: "downtown" },
      { id: 2, name: "valley" },
    ]);
    expect(geoResult.stats.rowGroupsRead).toBe(2);
    expect(geoResult.stats.rowGroupsSkipped).toBe(1);

    const h3InResult = lake
      .path(`data/${H3.file}`)
      .select(["id"])
      .where(fn("h3_in", col("h3_8"), lit(JSON.stringify(["8829a1d757fffff", "8829a1d74bfffff"]))))
      .run();
    await expect(h3InResult.toArray()).resolves.toEqual([{ id: 1 }, { id: 3 }]);
    expect(h3InResult.stats.rowGroupsRead).toBe(2);
    expect(h3InResult.stats.rowGroupsSkipped).toBe(2);

    const exactH3Result = lake
      .path(`data/${H3.file}`)
      .select(["id"])
      .where(fn("h3_within", col("h3_8"), lit("8829a1d757fffff"), lit(0)))
      .run();
    await expect(exactH3Result.toArray()).resolves.toEqual([{ id: 1 }]);
    expect(exactH3Result.stats.rowGroupsRead).toBe(1);
    expect(exactH3Result.stats.rowGroupsSkipped).toBe(3);

    const radiusH3Result = lake
      .path(`data/${H3.file}`)
      .select(["id"])
      .where(fn("h3_within", col("h3_8"), lit("8829a1d757fffff"), lit(1)))
      .run();
    await expect(radiusH3Result.toArray()).resolves.toEqual([{ id: 1 }, { id: 2 }]);
    expect(radiusH3Result.stats.rowGroupsRead).toBe(4);
    expect(radiusH3Result.stats.rowGroupsSkipped).toBe(0);
  });

  it("prunes Parquet row groups using min/max statistics", async () => {
    const lake = createParquetLake({ store, queryId: () => "stats-query" });
    const result = lake.path(`data/${STATS.file}`).where(lt("metric", 50)).run();
    const rows = await result.toArray();
    expect(rows).toHaveLength(STATS.rowGroupSize);
    expect(rows.map((row) => row.metric)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    expect(result.stats.rowGroupsRead).toBe(1);
    expect(result.stats.rowGroupsSkipped).toBe(2);
  });

  it("plans bounded row-group task ranges from Parquet footer statistics", async () => {
    const taskStore = memoryStore();
    await taskStore.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({ store: taskStore, queryId: () => "task-range-query" });
    const query = lake.path(`data/${STATS.file}`).select(["id"]).where(gte("metric", 100));

    const tasks = await query.planTasks();
    expect(tasks).toHaveLength(1);
    expect(tasks[0]).toMatchObject({
      path: `data/${STATS.file}`,
      projectedColumns: ["id", "metric"],
      rowGroupCount: 3,
      rowGroupRanges: [{ start: 1, end: 3 }],
    });

    const explain = await query.explain();
    expect(explain.json.tasks[0]?.rowGroupRanges).toEqual([{ start: 1, end: 3 }]);

    const golden = readFileSync(fixturePath(MANIFESTS.parquetTaskManifest), "utf8").trim();
    await expect(query.taskManifest("job_stats").then(stableStringify)).resolves.toBe(golden);
    await expect(
      lake
        .path(`data/${STATS.file}`)
        .select(["id"])
        .where(gte("metric", 100))
        .taskManifest("job_stats")
        .then(stableStringify),
    ).resolves.toBe(golden);
  });

  it("reuses cached Parquet footer metadata across task planning", async () => {
    const metadataCache = memoryCache<ParquetMetadata>();
    const taskStore = countingObjectStore(memoryStore());
    let headCalls = 0;
    const originalHead = taskStore.head.bind(taskStore);
    taskStore.head = async (path) => {
      headCalls += 1;
      return originalHead(path);
    };
    await taskStore.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({
      store: taskStore,
      metadataCache,
      queryId: () => "cached-task-range-query",
    });
    const query = lake.path(`data/${STATS.file}`).select(["id"]).where(gte("metric", 100));

    taskStore.resetCounters();
    headCalls = 0;
    const first = await query.planTasks();
    expect(first[0]).toMatchObject({
      rowGroupCount: 3,
      rowGroupRanges: [{ start: 1, end: 3 }],
    });
    expect(headCalls).toBe(1);
    expect(taskStore.counters.getRange).toBeGreaterThan(0);

    taskStore.resetCounters();
    headCalls = 0;
    const second = await query.planTasks();
    expect(second).toEqual(first);
    expect(headCalls).toBe(1);
    expect(taskStore.counters).toEqual({ get: 0, getRange: 0, bytesFetched: 0 });
  });

  it("splits task inputs into bounded, portable work units without changing query semantics", async () => {
    const metadataCache = memoryCache<ParquetMetadata>();
    const taskStore = countingObjectStore(memoryStore());
    await taskStore.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({
      store: taskStore,
      metadataCache,
      queryId: () => "portable-split-query",
    });
    const [task] = await lake
      .path(`data/${STATS.file}`)
      .select(["id"])
      .where(gte("metric", 100))
      .planTasks();
    expect(task).toBeDefined();
    const taskInput = {
      ...task,
      size: 1234,
      etag: "etag-stats",
      partitionValues: { dt: "2026-06-16" },
    };

    taskStore.resetCounters();
    const rowGroupUnits = await planParquetTaskWorkUnits(taskStore, taskInput, {
      maxRowGroupsPerTask: 1,
    });
    expect(taskStore.counters).toEqual({ get: 0, getRange: 0, bytesFetched: 0 });
    taskStore.resetCounters();
    const metadataBackedUnits = await planParquetTaskWorkUnits(
      taskStore,
      { ...taskInput, rowGroupCount: undefined },
      { maxRowGroupsPerTask: 1 },
    );
    expect(taskStore.counters.getRange).toBeGreaterThan(0);
    expect(metadataBackedUnits).toEqual(rowGroupUnits);
    taskStore.resetCounters();
    const cachedMetadataBackedUnits = await planParquetTaskWorkUnits(
      taskStore,
      { ...taskInput, rowGroupCount: undefined },
      { maxRowGroupsPerTask: 1, metadataCache },
    );
    expect(cachedMetadataBackedUnits).toEqual(rowGroupUnits);
    expect(taskStore.counters).toEqual({ get: 0, getRange: 0, bytesFetched: 0 });
    taskStore.resetCounters();
    const rowBudgetUnits = await planParquetTaskWorkUnits(taskStore, taskInput, {
      maxRowsPerTask: STATS.rowGroupSize,
    });
    expect(taskStore.counters.getRange).toBeGreaterThan(0);
    const portableUnits = JSON.parse(JSON.stringify(rowGroupUnits)) as typeof rowGroupUnits;

    expect(rowGroupUnits).toEqual(rowBudgetUnits);
    expect(portableUnits).toEqual([
      {
        ...taskInput,
        rowGroupRanges: [{ start: 1, end: 2 }],
      },
      {
        ...taskInput,
        rowGroupRanges: [{ start: 2, end: 3 }],
      },
    ]);
    expect(rowGroupUnits[0]).not.toBe(taskInput);
    expect(rowGroupUnits[0]?.rowGroupRanges).not.toBe(taskInput.rowGroupRanges);
    expect(taskInput.rowGroupRanges).toEqual([{ start: 1, end: 3 }]);
  });

  it("rejects unbounded or invalid Parquet work-unit sizing options", async () => {
    const taskStore = memoryStore();
    await taskStore.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({ store: taskStore, queryId: () => "invalid-split-query" });
    const [task] = await lake.path(`data/${STATS.file}`).planTasks();
    expect(task).toBeDefined();

    await expect(planParquetTaskWorkUnits(taskStore, task, {})).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
    });
    await expect(
      planParquetTaskWorkUnits(taskStore, task, { maxRowGroupsPerTask: 0 }),
    ).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
    });
    await expect(
      planParquetTaskWorkUnits(taskStore, task, { maxRowsPerTask: 0 }),
    ).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
    });
    await expect(
      planParquetTaskWorkUnits(taskStore, task, { maxRowsPerTask: STATS.rowGroupSize - 1 }),
    ).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
      details: {
        rowGroupRows: STATS.rowGroupSize,
        maxRowsPerTask: STATS.rowGroupSize - 1,
      },
    });
  });

  it("executes serialized task work units independently and fans results back in", async () => {
    const taskStore = countingObjectStore(memoryStore());
    await taskStore.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({ store: taskStore, queryId: () => "portable-task-query" });
    const query = lake
      .path(`data/${STATS.file}`)
      .select(["id", "metric"])
      .where(gte("metric", 100));
    const expected = await query.toArray();
    const manifest = await query.taskManifest("job_portable");
    const workUnits = [];
    for (const task of manifest.tasks) {
      workUnits.push(
        ...(await planParquetTaskWorkUnits(taskStore, task.input, { maxRowGroupsPerTask: 1 })),
      );
    }
    const workUnitManifest = createTaskManifest({ jobId: "job_portable_units", tasks: workUnits });
    const portableTasks = JSON.parse(
      JSON.stringify(workUnitManifest.tasks),
    ) as typeof workUnitManifest.tasks;

    const fanOutRows: unknown[] = [];
    for (const task of portableTasks) {
      for await (const batch of scanParquetTaskBatches(taskStore, task.input, { batchSize: 3 })) {
        fanOutRows.push(...batch);
      }
    }

    const fanOutColumnRows: unknown[] = [];
    const aggregateSpec = {
      rows: { op: "count" },
      totalMetric: { op: "sum", column: "metric" },
      doubledMetric: { op: "sum", expr: mul(col("metric"), 2) },
      coalescedMetric: { op: "sum", expr: fn("coalesce", col("metric"), 0) },
      metricExcept150: { op: "count", expr: fn("nullif", col("metric"), 150) },
      highMetricAverage: {
        op: "avg",
        expr: {
          kind: "case",
          whens: [{ when: gt("metric", 150), value: col("metric") }],
          else: lit(0),
        },
      },
      maxMetric: { op: "max", column: "metric" },
    } as const;
    for (const task of portableTasks) {
      for await (const batch of scanParquetTaskColumnBatches(taskStore, task.input, {
        batchSize: 4,
      })) {
        fanOutColumnRows.push(...materializeBatchRows(batch.batch));
      }
    }
    taskStore.resetCounters();
    const aggregateRow = await aggregateParquetTasks(
      taskStore,
      portableTasks.map((task) => task.input),
      aggregateSpec,
      { batchSize: 4, maxConcurrentTasks: 2 },
    );

    expect(portableTasks).toHaveLength(2);
    expect(portableTasks.map((task) => task.input.rowGroupRanges)).toEqual([
      [{ start: 1, end: 2 }],
      [{ start: 2, end: 3 }],
    ]);
    expect(portableTasks.map((task) => task.id)).toEqual([
      expect.stringMatching(/^job_portable_units-task-000000-/u),
      expect.stringMatching(/^job_portable_units-task-000001-/u),
    ]);
    expect(fanOutRows).toEqual(expected);
    expect(fanOutColumnRows).toEqual(expected);
    expect(aggregateRow).toEqual({
      rows: expected.length,
      totalMetric: expected.reduce(
        (sum, row) => sum + Number((row as { metric: number }).metric),
        0,
      ),
      doubledMetric:
        expected.reduce((sum, row) => sum + Number((row as { metric: number }).metric), 0) * 2,
      coalescedMetric: expected.reduce(
        (sum, row) => sum + Number((row as { metric: number }).metric),
        0,
      ),
      metricExcept150: expected.filter((row) => (row as { metric: number }).metric !== 150).length,
      highMetricAverage:
        expected.reduce((sum, row) => {
          const metric = Number((row as { metric: number }).metric);
          return sum + (metric > 150 ? metric : 0);
        }, 0) / expected.length,
      maxMetric: Math.max(...expected.map((row) => (row as { metric: number }).metric)),
    });
    expect(taskStore.counters.get).toBe(0);
    expect(taskStore.counters.getRange).toBeGreaterThan(0);
    expect(taskStore.counters.bytesFetched).toBeGreaterThan(0);

    const restoredFanIn = createVectorAggregateStates(aggregateSpec);
    for (const task of portableTasks) {
      const partial = await aggregateParquetTask(taskStore, task.input, aggregateSpec, {
        batchSize: 4,
      });
      const serializedPartial = JSON.parse(JSON.stringify(partial)) as typeof partial;
      mergeVectorAggregateStates(restoredFanIn, restoreVectorAggregateStates(serializedPartial));
    }
    expect(finalizeVectorAggregateStates(restoredFanIn)).toEqual(aggregateRow);
  });

  it("does not read unused projected column chunks during aggregate fan-in", async () => {
    const taskStore = memoryStore();
    const path = `data/${STATS.file}`;
    await taskStore.put(path, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({ store: taskStore, queryId: () => "aggregate-columns-query" });
    const query = lake.path(path).select(["id", "metric"]).where(gte("metric", 100));
    const expected = await query.toArray();
    const manifest = await query.taskManifest("job_aggregate_columns");
    const workUnits = [];
    for (const task of manifest.tasks) {
      workUnits.push(
        ...(await planParquetTaskWorkUnits(taskStore, task.input, { maxRowGroupsPerTask: 1 })),
      );
    }
    const metadata = await readParquetMetadata(taskStore, path);
    const objectHead = await taskStore.head(path);
    expect(objectHead).not.toBeNull();
    const guardedStore = rangeGuardObjectStore(taskStore, columnChunkRanges(metadata, "id"), {
      objectSize: objectHead?.size ?? 0,
      allowedFullRangeReads: 1,
    });

    await expect(
      aggregateParquetTasks(
        guardedStore,
        workUnits,
        {
          rows: { op: "count" },
          totalMetric: { op: "sum", column: "metric" },
          doubledMetric: { op: "sum", expr: mul(col("metric"), 2) },
          coalescedMetric: { op: "sum", expr: fn("coalesce", col("metric"), 0) },
          metricExcept150: { op: "count", expr: fn("nullif", col("metric"), 150) },
          highMetricAverage: {
            op: "avg",
            expr: {
              kind: "case",
              whens: [{ when: gt("metric", 150), value: col("metric") }],
              else: lit(0),
            },
          },
        },
        { batchSize: 4 },
      ),
    ).resolves.toEqual({
      rows: expected.length,
      totalMetric: expected.reduce(
        (sum, row) => sum + Number((row as { metric: number }).metric),
        0,
      ),
      doubledMetric:
        expected.reduce((sum, row) => sum + Number((row as { metric: number }).metric), 0) * 2,
      coalescedMetric: expected.reduce(
        (sum, row) => sum + Number((row as { metric: number }).metric),
        0,
      ),
      metricExcept150: expected.filter((row) => (row as { metric: number }).metric !== 150).length,
      highMetricAverage:
        expected.reduce((sum, row) => {
          const metric = Number((row as { metric: number }).metric);
          return sum + (metric > 150 ? metric : 0);
        }, 0) / expected.length,
    });
  });

  it("reuses cached Parquet footer metadata during aggregate fan-out", async () => {
    const metadataCache = memoryCache<ParquetMetadata>();
    const taskStore = countingObjectStore(memoryStore());
    const path = `data/${STATS.file}`;
    await taskStore.put(path, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({
      store: taskStore,
      metadataCache,
      queryId: () => "aggregate-metadata-cache-query",
    });
    const query = lake.path(path).select(["metric"]).where(gte("metric", 100));
    const [task] = await query.planTasks();
    expect(task).toBeDefined();
    const workUnits = await planParquetTaskWorkUnits(taskStore, task, { maxRowGroupsPerTask: 1 });
    const spec = {
      rows: { op: "count" },
      totalMetric: { op: "sum", column: "metric" },
    } as const;

    taskStore.resetCounters();
    const uncachedStats = testQueryStats();
    const uncached = await aggregateParquetTasks(taskStore, workUnits, spec, {
      batchSize: 4,
      maxConcurrentTasks: 1,
      stats: uncachedStats,
    });
    const uncachedRanges = taskStore.counters.getRange;
    expect(uncachedRanges).toBeGreaterThan(0);
    expect(uncachedStats.cacheHits).toBe(0);
    expect(uncachedStats.cacheMisses).toBeGreaterThan(0);

    taskStore.resetCounters();
    const cachedStats = testQueryStats();
    const cached = await aggregateParquetTasks(taskStore, workUnits, spec, {
      batchSize: 4,
      maxConcurrentTasks: 1,
      metadataCache,
      stats: cachedStats,
    });

    expect(cached).toEqual(uncached);
    expect(cachedStats.cacheHits).toBeGreaterThan(0);
    expect(cachedStats.cacheMisses).toBe(0);
    expect(taskStore.counters.getRange).toBeGreaterThan(0);
    expect(taskStore.counters.getRange).toBeLessThan(uncachedRanges);
  });

  it("reports cached Parquet footer metadata during grouped aggregate fan-out", async () => {
    const metadataCache = memoryCache<ParquetMetadata>();
    const taskStore = countingObjectStore(memoryStore());
    const path = `data/${STATS.file}`;
    await taskStore.put(path, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({
      store: taskStore,
      metadataCache,
      queryId: () => "group-aggregate-metadata-cache-query",
    });
    const query = lake.path(path).select(["metric"]).where(gte("metric", 100));
    const [task] = await query.planTasks();
    expect(task).toBeDefined();
    const workUnits = await planParquetTaskWorkUnits(taskStore, task, { maxRowGroupsPerTask: 1 });
    const spec = {
      rows: { op: "count" },
      totalMetric: { op: "sum", column: "metric" },
    } as const;

    taskStore.resetCounters();
    const uncachedStats = testQueryStats();
    const uncached = await aggregateParquetGroupTasks(taskStore, workUnits, ["metric"], spec, {
      batchSize: 4,
      maxConcurrentTasks: 1,
      stats: uncachedStats,
    });
    const uncachedRanges = taskStore.counters.getRange;
    expect(uncachedRanges).toBeGreaterThan(0);
    expect(uncachedStats.cacheHits).toBe(0);
    expect(uncachedStats.cacheMisses).toBeGreaterThan(0);

    taskStore.resetCounters();
    const cachedStats = testQueryStats();
    const cached = await aggregateParquetGroupTasks(taskStore, workUnits, ["metric"], spec, {
      batchSize: 4,
      maxConcurrentTasks: 1,
      metadataCache,
      stats: cachedStats,
    });

    expect(cached).toEqual(uncached);
    expect(cachedStats.cacheHits).toBeGreaterThan(0);
    expect(cachedStats.cacheMisses).toBe(0);
    expect(taskStore.counters.getRange).toBeGreaterThan(0);
    expect(taskStore.counters.getRange).toBeLessThan(uncachedRanges);
  });

  it("enforces aggregate state budgets while fanning in independent work units", async () => {
    const taskStore = memoryStore();
    await taskStore.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({ store: taskStore, queryId: () => "aggregate-budget-query" });
    const [task] = await lake
      .path(`data/${STATS.file}`)
      .select(["metric"])
      .where(gte("metric", 100))
      .planTasks();
    expect(task).toBeDefined();
    const workUnits = await planParquetTaskWorkUnits(taskStore, task, { maxRowGroupsPerTask: 1 });

    await expect(
      aggregateParquetTasks(
        taskStore,
        JSON.parse(JSON.stringify(workUnits)) as typeof workUnits,
        {
          distinctMetrics: { op: "count_distinct", column: "metric" },
        },
        { batchSize: 4, maxConcurrentTasks: 2, budget: { maxBufferedRows: 1 } },
      ),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "buffered rows", limit: 1 },
    });
  });

  it("enforces read budgets while aggregating task work units", async () => {
    const taskStore = memoryStore();
    await taskStore.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({ store: taskStore, queryId: () => "aggregate-read-budget" });
    const [task] = await lake.path(`data/${STATS.file}`).select(["metric"]).planTasks();
    expect(task).toBeDefined();

    await expect(
      aggregateParquetTasks(
        taskStore,
        [task],
        { rows: { op: "count" } },
        { budget: { maxRangeRequests: 0 } },
      ),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "range requests", limit: 0 },
    });
  });

  it("records decoded and matched rows while aggregating vector task batches", async () => {
    const taskStore = memoryStore();
    await writeParquet(taskStore, "data/vector-stats.parquet", {
      columnData: [{ name: "metric", data: [1, 2, 3, 4], type: "DOUBLE" }],
    });
    const stats = testQueryStats();

    const partial = await aggregateParquetTask(
      taskStore,
      {
        path: "data/vector-stats.parquet",
        rowGroupRanges: [{ start: 0, end: 1 }],
        projectedColumns: ["metric"],
        partitionValues: {},
        residualPredicate: gt("metric", 2),
      },
      {
        rows: { op: "count" },
        totalMetric: { op: "sum", column: "metric" },
      },
      { stats },
    );
    const restored = restoreVectorAggregateStates(partial);

    expect(finalizeVectorAggregateStates(restored)).toEqual({ rows: 2, totalMetric: 7 });
    expect(stats.rowsDecoded).toBe(4);
    expect(stats.rowsMatched).toBe(2);
    expect(stats.rowGroupsRead).toBe(1);
    expect(stats.rowGroupsSkipped).toBe(0);
  });

  it("rejects invalid aggregate fan-in concurrency", async () => {
    await expect(aggregateParquetTasks(store, [], { rows: { op: "count" } }, {})).resolves.toEqual({
      rows: 0,
    });
    await expect(
      aggregateParquetTasks(store, [], { rows: { op: "count" } }, { maxConcurrentTasks: 0 }),
    ).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
    });
  });

  it("bounds aggregate fan-in concurrency across work units", async () => {
    const taskStore = delayedHeadObjectStore(memoryStore(), 5);
    const bytes = readFileSync(fixturePath(STATS.file));
    const paths = ["data/stats-a.parquet", "data/stats-b.parquet", "data/stats-c.parquet"];
    for (const path of paths) await taskStore.put(path, bytes);
    const lake = createParquetLake({ store: taskStore, queryId: () => "portable-concurrency" });
    const expectedRows = await lake
      .path(paths[0] ?? "")
      .select(["metric"])
      .where(gte("metric", 0))
      .toArray();
    const workUnits = [];
    for (const [index, path] of paths.entries()) {
      const manifest = await lake
        .path(path)
        .select(["metric"])
        .where(gte("metric", 0))
        .taskManifest(`job_concurrency_${index}`);
      for (const task of manifest.tasks) workUnits.push(task.input);
    }

    taskStore.resetPeakHeads();
    const aggregateRow = await aggregateParquetTasks(
      taskStore,
      workUnits,
      {
        rows: { op: "count" },
        totalMetric: { op: "sum", column: "metric" },
      },
      { batchSize: 4, maxConcurrentTasks: 2 },
    );

    expect(workUnits).toHaveLength(paths.length);
    expect(aggregateRow).toEqual({
      rows: expectedRows.length * paths.length,
      totalMetric:
        expectedRows.reduce((sum, row) => sum + Number((row as { metric: number }).metric), 0) *
        paths.length,
    });
    expect(taskStore.peakActiveHeads).toBeGreaterThan(1);
    expect(taskStore.peakActiveHeads).toBeLessThanOrEqual(2);
  });

  it("reduces aggregate fan-in in task order even when work units finish out of order", async () => {
    const taskStore = delayedPathHeadObjectStore(memoryStore(), {
      "data/slow.parquet": 20,
      "data/fast.parquet": 0,
    });
    await writeParquet(taskStore, "data/slow.parquet", {
      columnData: [{ name: "metric", data: [1, 2], type: "DOUBLE" }],
    });
    await writeParquet(taskStore, "data/fast.parquet", {
      columnData: [{ name: "metric", data: [10, 20], type: "DOUBLE" }],
    });

    await expect(
      aggregateParquetTasks(
        taskStore,
        [
          {
            path: "data/slow.parquet",
            rowGroupRanges: [{ start: 0, end: 1 }],
            projectedColumns: ["metric"],
            partitionValues: {},
          },
          {
            path: "data/fast.parquet",
            rowGroupRanges: [{ start: 0, end: 1 }],
            projectedColumns: ["metric"],
            partitionValues: {},
          },
        ],
        {
          firstMetric: { op: "first", column: "metric" },
          lastMetric: { op: "last", column: "metric" },
        },
        { maxConcurrentTasks: 2 },
      ),
    ).resolves.toEqual({
      firstMetric: 1,
      lastMetric: 20,
    });
  });

  it("exposes first-class row-group planning with byte ranges", async () => {
    const plan = await planRowGroups(store, `data/${STATS.file}`, { where: gte("metric", 100) });

    expect(plan.rowGroupRanges).toEqual([{ start: 1, end: 3 }]);
    expect(plan.rowGroups.map((group) => group.index)).toEqual([1, 2]);
    expect(plan.rowGroups.map((group) => group.rowStart)).toEqual([
      STATS.rowGroupSize,
      STATS.rowGroupSize * 2,
    ]);
    for (const group of plan.rowGroups) {
      expect(group.rowCount).toBe(STATS.rowGroupSize);
      expect(group.byteRange?.offset).toEqual(expect.any(Number));
      expect(group.byteRange?.length).toEqual(expect.any(Number));
      expect(group.byteRange?.length).toBeGreaterThan(0);
    }
  });

  it("reuses cached Parquet footer metadata across scans", async () => {
    const metadataCache = memoryCache<ParquetMetadata>();
    const lake = createParquetLake({ store, metadataCache });

    const first = lake.path(`data/${STATS.file}`).where(lt("metric", 0)).run();
    await expect(first.count()).resolves.toBe(0);
    expect(first.stats.cacheMisses).toBe(1);
    expect(first.stats.cacheHits).toBe(0);
    expect(first.stats.rangeRequests).toBeGreaterThan(0);
    expect(first.stats.rowGroupsSkipped).toBe(3);

    const second = lake.path(`data/${STATS.file}`).where(lt("metric", 0)).run();
    await expect(second.count()).resolves.toBe(0);
    expect(second.stats.cacheMisses).toBe(0);
    expect(second.stats.cacheHits).toBe(1);
    expect(second.stats.rangeRequests).toBe(0);
    expect(second.stats.rowGroupsSkipped).toBe(3);
  });

  it("enables bounded runtime-local Parquet caching from config", async () => {
    const taskStore = countingObjectStore(store);
    const lake = createParquetLake({ store: taskStore, cache: { maxBytes: 1024 * 1024 } });

    const first = lake.path(`data/${STATS.file}`).where(lt("metric", 0)).run();
    await expect(first.count()).resolves.toBe(0);
    expect(taskStore.counters.getRange).toBeGreaterThan(0);

    taskStore.resetCounters();
    const second = lake.path(`data/${STATS.file}`).where(lt("metric", 0)).run();
    await expect(second.count()).resolves.toBe(0);
    expect(taskStore.counters.getRange).toBe(0);
    expect(second.stats.cacheHits).toBe(1);
    expect(second.stats.cacheMisses).toBe(0);
  });

  it("reuses decoded column batches from the shared cache budget", async () => {
    const lake = createParquetLake({
      store,
      cache: { maxBytes: 1024 * 1024, policy: "latency" },
    });

    const first = lake
      .path(`data/${STATS.file}`)
      .select(["id", "metric"])
      .where(gt("metric", 0))
      .orderBy([{ column: "metric", direction: "desc" }])
      .limit(2)
      .run();
    const firstRows = await first.toArray();
    expect(firstRows).toHaveLength(2);
    expect(first.stats.cacheMisses).toBeGreaterThan(0);

    const second = lake
      .path(`data/${STATS.file}`)
      .select(["id", "metric"])
      .where(gt("metric", 0))
      .orderBy([{ column: "metric", direction: "desc" }])
      .limit(2)
      .run();
    await expect(second.toArray()).resolves.toEqual(firstRows);
    expect(second.stats.cacheHits).toBeGreaterThan(1);
  });

  it("spends scan range cache entries from the shared cache budget", async () => {
    const taskStore = countingObjectStore(store);
    const lake = createParquetLake({
      store: taskStore,
      cache: { maxBytes: 1, policy: "io" },
      scanRangeCache: { maxBytes: 1024 * 1024 },
    });

    const first = lake.path(`data/${STATS.file}`).select(["metric"]).limit(2).run();
    await expect(first.toArray()).resolves.toHaveLength(2);
    expect(taskStore.counters.getRange).toBeGreaterThan(0);

    taskStore.resetCounters();
    const second = lake.path(`data/${STATS.file}`).select(["metric"]).limit(2).run();
    await expect(second.toArray()).resolves.toHaveLength(2);
    expect(taskStore.counters.getRange).toBeGreaterThan(0);
  });

  it("invalidates cached Parquet footer metadata when object etag changes", async () => {
    const metadataCache = memoryCache<ParquetMetadata>();
    const etagStore = memoryStore();
    const path = `data/${STATS.file}`;
    const bytes = readFileSync(fixturePath(STATS.file));
    await etagStore.put(path, bytes);
    const lake = createParquetLake({ store: etagStore, metadataCache });

    const first = lake.path(path).where(lt("metric", 0)).run();
    await expect(first.count()).resolves.toBe(0);
    expect(first.stats.cacheMisses).toBe(1);
    expect(first.stats.cacheHits).toBe(0);

    await etagStore.put(path, bytes);
    const second = lake.path(path).where(lt("metric", 0)).run();
    await expect(second.count()).resolves.toBe(0);
    expect(second.stats.cacheMisses).toBe(1);
    expect(second.stats.cacheHits).toBe(0);
    expect(second.stats.rangeRequests).toBeGreaterThan(0);
  });

  it("prunes row groups for supported min/max predicate shapes", async () => {
    const lake = createParquetLake({ store });

    const eqResult = lake.path(`data/${STATS.file}`).where(eq("metric", 105)).run();
    expect(await eqResult.count()).toBe(1);
    expect(eqResult.stats.rowGroupsRead).toBe(1);
    expect(eqResult.stats.rowGroupsSkipped).toBe(2);

    const gteResult = lake.path(`data/${STATS.file}`).where(gte("metric", 200)).run();
    expect(await gteResult.count()).toBe(10);
    expect(gteResult.stats.rowGroupsRead).toBe(1);
    expect(gteResult.stats.rowGroupsSkipped).toBe(2);

    const betweenResult = lake
      .path(`data/${STATS.file}`)
      .where(between("metric", 95, 105))
      .run();
    expect(await betweenResult.count()).toBe(6);
    expect(betweenResult.stats.rowGroupsRead).toBe(1);
    expect(betweenResult.stats.rowGroupsSkipped).toBe(2);

    const inResult = lake
      .path(`data/${STATS.file}`)
      .where(isIn("metric", [205, 999]))
      .run();
    expect(await inResult.count()).toBe(1);
    expect(inResult.stats.rowGroupsRead).toBe(1);
    expect(inResult.stats.rowGroupsSkipped).toBe(2);

    const neResult = lake.path(`data/${STATS.file}`).where(ne("label", "g1")).run();
    expect(await neResult.count()).toBe(20);
    expect(neResult.stats.rowGroupsRead).toBe(2);
    expect(neResult.stats.rowGroupsSkipped).toBe(1);

    const lteResult = lake.path(`data/${STATS.file}`).where(lte("metric", 9)).run();
    expect(await lteResult.count()).toBe(10);
    expect(lteResult.stats.rowGroupsRead).toBe(1);
    expect(lteResult.stats.rowGroupsSkipped).toBe(2);

    const orResult = lake
      .path(`data/${STATS.file}`)
      .where(or(lt("metric", 0), gt("metric", 205)))
      .run();
    expect(await orResult.count()).toBe(4);
    expect(orResult.stats.rowGroupsRead).toBe(1);
    expect(orResult.stats.rowGroupsSkipped).toBe(2);
  });

  it("keeps row-group pruning conservative for unsupported predicate shapes", async () => {
    const lake = createParquetLake({ store });

    const logical = lake
      .path(`data/${STATS.file}`)
      .where(and(eq("label", "g2"), gte("metric", 205)))
      .run();
    expect(await logical.count()).toBe(5);
    expect(logical.stats.rowGroupsRead).toBe(1);
    expect(logical.stats.rowGroupsSkipped).toBe(2);

    const unknown = lake
      .path(`data/${STATS.file}`)
      .where(not(like("label", "g%")))
      .run();
    expect(await unknown.count()).toBe(0);
    expect(unknown.stats.rowGroupsRead).toBe(3);
    expect(unknown.stats.rowGroupsSkipped).toBe(0);

    const negatedIn = lake
      .path(`data/${STATS.file}`)
      .where(notIn("metric", [0]))
      .run();
    expect(await negatedIn.count()).toBe(29);
    expect(negatedIn.stats.rowGroupsRead).toBe(3);
    expect(negatedIn.stats.rowGroupsSkipped).toBe(0);

    const expressionIn = lake
      .path(`data/${STATS.file}`)
      .where(isIn("metric", [col("id")]))
      .run();
    expect(await expressionIn.count()).toBe(10);
    expect(expressionIn.stats.rowGroupsRead).toBe(3);
    expect(expressionIn.stats.rowGroupsSkipped).toBe(0);

    const expressionBetween = lake
      .path(`data/${STATS.file}`)
      .where(between("metric", col("id"), 5))
      .run();
    expect(await expressionBetween.count()).toBe(6);
    expect(expressionBetween.stats.rowGroupsRead).toBe(3);
    expect(expressionBetween.stats.rowGroupsSkipped).toBe(0);
  });

  it("combines hive partition pruning with Parquet row-group pruning", async () => {
    const lake = createParquetLake({ store });
    const result = lake
      .hive("data/hive/**/*.parquet")
      .select(["id", "country"])
      .where(and(eq("country", "US"), gt("amount", 100)))
      .run();

    expect(await result.toArray()).toEqual([
      { id: 200, country: "US" },
      { id: 201, country: "US" },
      { id: 202, country: "US" },
      { id: 203, country: "US" },
    ]);
    expect(result.stats.filesPlanned).toBe(2);
    expect(result.stats.filesSkipped).toBe(1);
    expect(result.stats.rowGroupsRead).toBe(1);
    expect(result.stats.rowGroupsSkipped).toBe(1);

    const fullScan = lake.hive("data/hive/**/*.parquet").select(["id", "country"]).run();
    await fullScan.toArray();
    expect(result.stats.bytesRequested).toBeLessThan(fullScan.stats.bytesRequested);
    expect(result.stats.rangeRequests).toBeLessThan(fullScan.stats.rangeRequests);

    const explain = await lake
      .hive("data/hive/**/*.parquet")
      .select(["id", "country"])
      .where(and(eq("country", "US"), gt("amount", 100)))
      .explain();
    expect(explain.json.filesPlanned).toBe(2);
    expect(explain.json.filesSkipped).toBe(1);
  });

  it("rejects invalid JSON queries with LAKEQL_PARSE_ERROR", () => {
    const lake = createParquetLake({ store });
    expect(() => lake.query({ version: 2, from: `data/${SALES.file}` })).toThrowError(LakeqlError);
    expect(() => lake.query({ version: 2, from: `data/${SALES.file}` })).toThrowError(
      /version must be 1/u,
    );
  });
});

describe("rowGroupMayMatch", () => {
  it("keeps unsupported expressions conservative", () => {
    const group = rowGroupWithStats("metric", 1, 9);
    expect(rowGroupMayMatch(group, undefined)).toBe(true);
    expect(rowGroupMayMatch(group, lit(true))).toBe(true);
    expect(rowGroupMayMatch(group, col("metric"))).toBe(true);
    expect(rowGroupMayMatch(group, isNull("metric"))).toBe(true);
    expect(rowGroupMayMatch(group, like("metric", "%"))).toBe(true);
    expect(rowGroupMayMatch(group, fn("lower", col("metric")))).toBe(true);
    expect(rowGroupMayMatch(group, not(eq("metric", 1)))).toBe(true);
    expect(rowGroupMayMatch(group, eq(col("metric"), col("other")))).toBe(true);
  });

  it("keeps missing or incompatible stats conservative", () => {
    expect(rowGroupMayMatch(rowGroupWithStats("other", 1, 9), eq("metric", 5))).toBe(true);
    expect(rowGroupMayMatch(rowGroupWithStats("metric"), eq("metric", 5))).toBe(true);
    expect(rowGroupMayMatch(rowGroupWithStats("metric", 1, 9), eq("metric", "5"))).toBe(true);
    expect(
      rowGroupMayMatch(rowGroupWithStats("metric", 1, 9), isIn(fn("abs", col("metric")), [1])),
    ).toBe(true);
    expect(rowGroupMayMatch(rowGroupWithStats("metric", 1, 9), isIn("missing", [1]))).toBe(true);
    expect(rowGroupMayMatch(rowGroupWithStats("metric", 1, 9), isIn("metric", [null]))).toBe(true);
    expect(rowGroupMayMatch(rowGroupWithStats("metric", 1, 9), isIn("metric", ["x"]))).toBe(true);
    expect(
      rowGroupMayMatch(rowGroupWithStats("metric", 1, 9), between(fn("abs", col("metric")), 1, 2)),
    ).toBe(true);
    expect(
      rowGroupMayMatch(rowGroupWithStats("metric", 1, 9), between("metric", col("id"), 2)),
    ).toBe(true);
    expect(rowGroupMayMatch(rowGroupWithStats("metric", 1, 9), between("missing", 1, 2))).toBe(
      true,
    );
    expect(
      rowGroupMayMatch(rowGroupWithStats("metric", Number.NaN, Number.NaN), eq("metric", 5)),
    ).toBe(true);
    expect(
      rowGroupMayMatch(
        rowGroupWithStats("metric", 9_007_199_254_740_993n, 9_007_199_254_740_995n),
        eq("metric", 9_007_199_254_740_994),
      ),
    ).toBe(true);
  });

  it("falls back to legacy min/max row-group statistics", () => {
    expect(rowGroupMayMatch(rowGroupWithLegacyStats("metric", 1, 9), eq("metric", 5))).toBe(true);
    expect(rowGroupMayMatch(rowGroupWithLegacyStats("metric", 1, 9), eq("metric", 50))).toBe(false);
  });

  it("normalizes literal-on-left comparisons before pruning", () => {
    const group = rowGroupWithStats("metric", 1, 9);

    expect(rowGroupMayMatch(group, lt(lit(0), col("metric")))).toBe(true);
    expect(rowGroupMayMatch(group, lt(lit(200), col("metric")))).toBe(false);
    expect(rowGroupMayMatch(group, gte(lit(10), col("metric")))).toBe(true);
    expect(rowGroupMayMatch(group, gte(lit(0), col("metric")))).toBe(false);
  });

  it("proves fully matching row groups only when stats and null counts are conclusive", () => {
    const group = rowGroupWithStats("metric", 100, 199);
    const nullableGroup = rowGroupWithStats("metric", 100, 199);
    const stats = nullableGroup.columns[0]?.meta_data?.statistics;
    if (stats !== undefined) stats.null_count = 1n;

    expect(rowGroupMustMatch(group, undefined)).toBe(true);
    expect(rowGroupMustMatch(group, gte("metric", 100))).toBe(true);
    expect(rowGroupMustMatch(group, gt("metric", 99))).toBe(true);
    expect(rowGroupMustMatch(group, lte("metric", 199))).toBe(true);
    expect(rowGroupMustMatch(group, lt("metric", 200))).toBe(true);
    expect(rowGroupMustMatch(group, between("metric", 100, 199))).toBe(true);
    expect(rowGroupMustMatch(group, lt(lit(99), col("metric")))).toBe(true);
    expect(rowGroupMustMatch(group, gte("metric", 150))).toBe(false);
    expect(rowGroupMustMatch(group, eq("metric", 150))).toBe(false);
    expect(rowGroupMustMatch(nullableGroup, gte("metric", 100))).toBe(false);
  });

  it("prunes bbox and h3 function predicates with row-group stats", () => {
    const bboxGroup = rowGroupWithStatsEntries([
      ["minx", -118.45, -118.45],
      ["miny", 34.18, 34.18],
      ["maxx", -118.45, -118.45],
      ["maxy", 34.18, 34.18],
    ]);
    const prefixedBBoxGroup = rowGroupWithStatsEntries([
      ["geom_minx", -117.16, -117.16],
      ["geom_miny", 32.72, 32.72],
      ["geom_maxx", -117.16, -117.16],
      ["geom_maxy", 32.72, 32.72],
    ]);
    expect(
      rowGroupMayMatch(
        bboxGroup,
        fn("st_intersects", col("geom"), fn("st_bbox", lit(-118.5), lit(34), lit(-118), lit(34.3))),
      ),
    ).toBe(true);
    expect(
      rowGroupMayMatch(
        bboxGroup,
        fn("st_intersects", col("geom"), lit(JSON.stringify([-119, 35, -118.8, 36]))),
      ),
    ).toBe(false);
    expect(
      rowGroupMayMatch(
        prefixedBBoxGroup,
        fn("st_intersects", col("geom"), fn("st_bbox", lit(-119), lit(35), lit(-118.8), lit(36))),
      ),
    ).toBe(false);

    const h3Group = rowGroupWithStats("h3_8", "8829a1d757fffff", "8829a1d757fffff");
    expect(
      rowGroupMayMatch(h3Group, fn("h3_in", col("h3_8"), lit(JSON.stringify(["8829a1d757fffff"])))),
    ).toBe(true);
    expect(
      rowGroupMayMatch(h3Group, fn("h3_in", col("h3_8"), lit(JSON.stringify(["8829a1d74bfffff"])))),
    ).toBe(false);
    expect(
      rowGroupMayMatch(h3Group, fn("h3_within", col("h3_8"), lit("8829a1d757fffff"), lit(0))),
    ).toBe(true);
    expect(
      rowGroupMayMatch(h3Group, fn("h3_within", col("h3_8"), lit("8829a1d74bfffff"), lit(0))),
    ).toBe(false);
  });

  it("keeps unsupported function pushdown shapes conservative", () => {
    const group = rowGroupWithStats("h3_8", "8829a1d757fffff", "8829a1d757fffff");
    expect(rowGroupMayMatch(group, fn("h3_in", col("h3_8"), lit("not-json")))).toBe(true);
    expect(rowGroupMayMatch(group, fn("h3_in", col("h3_8"), lit(JSON.stringify([1]))))).toBe(true);
    expect(rowGroupMayMatch(group, fn("h3_in", lit("h3_8"), lit(JSON.stringify(["x"]))))).toBe(
      true,
    );
    expect(rowGroupMayMatch(group, fn("h3_within", col("h3_8"), lit("x"), lit(1)))).toBe(true);
    expect(rowGroupMayMatch(group, fn("h3_within", col("h3_8"), col("origin"), lit(0)))).toBe(true);
    expect(rowGroupMayMatch(group, fn("h3_within", col("h3_8"), lit("x"), col("radius")))).toBe(
      true,
    );
    expect(
      rowGroupMayMatch(
        group,
        fn("st_intersects", col("geom"), fn("st_bbox", lit(-118), lit(35), lit(-119), lit(36))),
      ),
    ).toBe(true);
    expect(rowGroupMayMatch(group, fn("st_intersects", lit("geom"), lit("[0,0,1,1]")))).toBe(true);
    expect(rowGroupMayMatch(group, fn("st_intersects", col("geom"), lit("not-json")))).toBe(true);
  });
});

describe("readParquetMetadata", () => {
  it("sees the row-group layout the fixture generator promised", async () => {
    const meta = await readParquetMetadata(store, `data/${SALES.file}`);
    expect(Number(meta.num_rows)).toBe(SALES.rows);
    const expectedGroups = Math.ceil(SALES.rows / SALES.rowGroupSize);
    expect(meta.row_groups).toHaveLength(expectedGroups);
  });
});

describe("timestamp logical values", () => {
  it("reads and queries Parquet TIMESTAMP_MICROS without precision loss", async () => {
    const path = "data/timestamp-micros.parquet";
    await writeParquet(store, path, {
      schema: [
        { name: "root", num_children: 2 },
        { name: "id", type: "INT32", repetition_type: "OPTIONAL" },
        {
          name: "loaded_at",
          type: "INT64",
          converted_type: "TIMESTAMP_MICROS",
          repetition_type: "OPTIONAL",
        },
      ],
      columnData: [
        { name: "id", data: [1, 2, 3] },
        {
          name: "loaded_at",
          data: [1_700_000_000_000_001n, 1_700_000_000_000_999n, null],
        },
      ],
    });

    const lake = createParquetLake({ store });
    await expect(lake.path(path).select(["id", "loaded_at"]).toArray()).resolves.toEqual([
      { id: 1, loaded_at: timestampFromEpoch(1_700_000_000_000_001n, "micros") },
      { id: 2, loaded_at: timestampFromEpoch(1_700_000_000_000_999n, "micros") },
      { id: 3, loaded_at: null },
    ]);
    await expect(
      lake
        .path(path)
        .where(gt("loaded_at", "2023-11-14T22:13:20.000500Z"))
        .orderBy([{ column: "loaded_at", direction: "desc" }])
        .select(["id"])
        .toArray(),
    ).resolves.toEqual([{ id: 2 }]);
  });
});

describe("rejectUnsupportedParquetSchema", () => {
  it("accepts absent or empty schema metadata", () => {
    expect(() => rejectUnsupportedParquetSchema(metadataWithSchema([]))).not.toThrow();
    expect(() =>
      rejectUnsupportedParquetSchema({ row_groups: [] } as unknown as ParquetMetadata),
    ).not.toThrow();
  });

  it("rejects unannotated Parquet groups as unsupported structs", () => {
    expect(() =>
      rejectUnsupportedParquetSchema(
        metadataWithSchema([
          { name: "root", num_children: 1 },
          { name: "address", num_children: 1, repetition_type: "OPTIONAL" },
          { name: "street", type: "BYTE_ARRAY", converted_type: "UTF8" },
        ]),
      ),
    ).toThrowError(LakeqlError);
    expect(() =>
      rejectUnsupportedParquetSchema(
        metadataWithSchema([
          { name: "root", num_children: 1 },
          { name: "address", num_children: 1, repetition_type: "OPTIONAL" },
          { name: "street", type: "BYTE_ARRAY", converted_type: "UTF8" },
        ]),
      ),
    ).toThrow(/struct/u);
  });

  it("allows projected scalar columns when unprojected columns are unsupported structs", () => {
    const metadata = metadataWithSchema([
      { name: "root", num_children: 2 },
      { name: "id", type: "INT32" },
      { name: "bbox", num_children: 4, repetition_type: "OPTIONAL" },
      { name: "xmin", type: "DOUBLE" },
      { name: "ymin", type: "DOUBLE" },
      { name: "xmax", type: "DOUBLE" },
      { name: "ymax", type: "DOUBLE" },
    ]);

    expect(() => rejectUnsupportedParquetSchema(metadata, { columns: ["id"] })).not.toThrow();
    expect(() => rejectUnsupportedParquetSchema(metadata, { columns: ["bbox"] })).toThrowError(
      LakeqlError,
    );
    expect(() => rejectUnsupportedParquetSchema(metadata)).toThrowError(LakeqlError);
  });

  it("rejects unsupported precision-sensitive decimals without rejecting timestamps", () => {
    expect(() =>
      rejectUnsupportedParquetSchema(
        metadataWithSchema([
          { name: "root", num_children: 1 },
          {
            name: "wide_decimal",
            type: "INT64",
            converted_type: "DECIMAL",
            precision: 18,
            scale: 2,
          },
        ]),
      ),
    ).toThrow(/precision 15/u);
    expect(() =>
      rejectUnsupportedParquetSchema(
        metadataWithSchema([
          { name: "root", num_children: 4 },
          {
            name: "safe_decimal",
            type: "INT32",
            converted_type: "DECIMAL",
            precision: 9,
            scale: 2,
          },
          { name: "millis", type: "INT64", converted_type: "TIMESTAMP_MILLIS" },
          { name: "micros", type: "INT64", converted_type: "TIMESTAMP_MICROS" },
          {
            name: "nanos",
            type: "INT64",
            logical_type: { type: "TIMESTAMP", unit: "NANOS", isAdjustedToUTC: false },
          },
        ]),
      ),
    ).not.toThrow();
  });

  it("allows LIST and MAP annotated groups to pass through to hyparquet", () => {
    expect(() =>
      rejectUnsupportedParquetSchema(
        metadataWithSchema([
          { name: "root", num_children: 4n },
          { name: "tags", num_children: 1, converted_type: "LIST" },
          { name: "list", num_children: 1, repetition_type: "REPEATED" },
          { name: "element", type: "BYTE_ARRAY", converted_type: "UTF8" },
          { name: "attrs", num_children: 1, logical_type: { MAP: {} } },
          { name: "key_value", num_children: 2, repetition_type: "REPEATED" },
          { name: "key", type: "BYTE_ARRAY", converted_type: "UTF8" },
          { name: "value", type: "BYTE_ARRAY", converted_type: "UTF8" },
          { name: "pairs", num_children: 1, converted_type: "MAP_KEY_VALUE" },
          { name: "pair", num_children: 2, repetition_type: "REPEATED" },
          { name: "key", type: "BYTE_ARRAY", converted_type: "UTF8" },
          { name: "value", type: "BYTE_ARRAY", converted_type: "UTF8" },
          { name: "logical_list", num_children: 1, logical_type: "LIST" },
          { name: "list", num_children: 1, repetition_type: "REPEATED" },
          { name: "element", type: "INT32" },
        ]),
      ),
    ).not.toThrow();
  });

  it("applies the struct rejection before row-group planning", () => {
    expect(() =>
      planRowGroupsFromMetadata(
        metadataWithSchema([
          { name: "root", num_children: 1 },
          { name: "payload", num_children: 1, repetition_type: "OPTIONAL" },
          { name: "id", type: "INT32" },
        ]),
        undefined,
      ),
    ).toThrowError(LakeqlError);
  });
});

describe("planRowGroupsFromMetadata", () => {
  it("derives byte ranges from row-group and column-chunk offsets when present", () => {
    const metadata = {
      row_groups: [
        {
          columns: [],
          file_offset: 10n,
          total_compressed_size: 20n,
          num_rows: 5n,
          total_byte_size: 20n,
        },
        {
          columns: [
            {
              file_offset: 100n,
              meta_data: {
                path_in_schema: ["a"],
                data_page_offset: 120n,
                dictionary_page_offset: 90n,
                total_compressed_size: 25n,
                statistics: { min_value: 1, max_value: 9 },
              },
            },
            {
              file_offset: 200n,
              meta_data: {
                path_in_schema: ["b"],
                data_page_offset: 150n,
                total_compressed_size: 10n,
                statistics: { min_value: 1, max_value: 9 },
              },
            },
          ],
          num_rows: 5n,
          total_byte_size: 35n,
        },
        {
          columns: [{ file_offset: 300n }],
          num_rows: 5n,
          total_byte_size: 0n,
        },
      ],
    } as unknown as ParquetMetadata;

    const plan = planRowGroupsFromMetadata(metadata, undefined);
    expect(plan.rowGroups).toMatchObject([
      { index: 0, rowStart: 0, rowCount: 5, byteRange: { offset: 10, length: 20 } },
      { index: 1, rowStart: 5, rowCount: 5, byteRange: { offset: 90, length: 70 } },
      { index: 2, rowStart: 10, rowCount: 5 },
    ]);
    expect(plan.rowGroups[2]?.byteRange).toBeUndefined();
  });

  it("keeps row offsets correct while pruning skipped groups", () => {
    const metadata = {
      row_groups: [
        rowGroupWithStats("metric", 0, 9),
        rowGroupWithStats("metric", 100, 109),
        rowGroupWithStats("metric", 200, 209),
      ],
    } as unknown as ParquetMetadata;

    for (const group of metadata.row_groups) {
      group.num_rows = 10n;
      group.total_byte_size = 1n;
    }

    const plan = planRowGroupsFromMetadata(metadata, gte("metric", 100));
    expect(plan.rowGroupRanges).toEqual([{ start: 1, end: 3 }]);
    expect(plan.rowGroups.map((group) => group.rowStart)).toEqual([10, 20]);
  });
});
