import { readFileSync } from "node:fs";
import {
  and,
  between,
  col,
  createOutputManifest,
  eq,
  fn,
  gt,
  gte,
  isIn,
  isNull,
  LaQLError,
  like,
  lit,
  lt,
  lte,
  memoryCache,
  memoryStore,
  ne,
  not,
  notIn,
  or,
} from "@laql/core";
import { fixturePath, GROUPBY, HIVE, SALES, STATS, TYPES, WIDE, WRITE } from "@laql/fixtures";
import type { RowGroup } from "hyparquet";
import { beforeAll, describe, expect, it } from "vitest";
import {
  createParquetLake,
  type ParquetMetadata,
  partitionedParquetOutputEntries,
  readParquetMetadata,
  readParquetObjects,
  rowGroupMayMatch,
  writeParquet,
  writePartitionedParquet,
} from "./index.js";

const store = memoryStore();

function rowGroupWithStats(
  column: string,
  minValue?: string | number,
  maxValue?: string | number,
): RowGroup {
  const statistics: NonNullable<
    NonNullable<NonNullable<RowGroup["columns"][number]["meta_data"]>["statistics"]>
  > = {};
  if (minValue !== undefined) statistics.min_value = minValue;
  if (maxValue !== undefined) statistics.max_value = maxValue;
  return {
    columns: [
      {
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
          statistics,
        },
      },
    ],
    total_byte_size: 0n,
    num_rows: 1n,
  };
}

beforeAll(async () => {
  await store.put(`data/${SALES.file}`, readFileSync(fixturePath(SALES.file)));
  await store.put(`data/copy-${SALES.file}`, readFileSync(fixturePath(SALES.file)));
  await store.put(`data/${TYPES.file}`, readFileSync(fixturePath(TYPES.file)));
  await store.put(`data/${WIDE.file}`, readFileSync(fixturePath(WIDE.file)));
  await store.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
  await store.put(`data/${GROUPBY.file}`, readFileSync(fixturePath(GROUPBY.file)));
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

  it("fails loudly with LAQL_OBJECT_NOT_FOUND on a missing object", async () => {
    await expect(readParquetObjects(store, "data/nope.parquet")).rejects.toMatchObject({
      code: "LAQL_OBJECT_NOT_FOUND",
    });
  });

  it("wraps decode failures in LAQL_PARQUET_READ_ERROR", async () => {
    await store.put("data/garbage.parquet", new TextEncoder().encode("not parquet bytes"));
    await expect(readParquetObjects(store, "data/garbage.parquet")).rejects.toThrowError(LaQLError);
    await expect(readParquetObjects(store, "data/garbage.parquet")).rejects.toMatchObject({
      code: "LAQL_PARQUET_READ_ERROR",
    });
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

  it("wraps writer failures in LAQL_PARQUET_WRITE_ERROR", async () => {
    await expect(
      writeParquet(memoryStore(), "out/bad.parquet", {
        columnData: [
          { name: "id", data: [1, 2, 3], type: "INT32" },
          { name: "name", data: ["a"], type: "STRING" },
        ],
      }),
    ).rejects.toMatchObject({ code: "LAQL_PARQUET_WRITE_ERROR" });
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
        etag: result.files[0]?.etag,
      },
      {
        taskId: "task-1",
        outputPath: "out/manifest/date=2026-01-02/country=CA/part-manifest-00001.parquet",
        partitionValues: { country: "CA", date: "2026-01-02" },
        rowCount: 1,
        byteSize: result.files[1]?.byteSize,
        etag: result.files[1]?.etag,
      },
    ]);
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
    ).rejects.toMatchObject({ code: "LAQL_TYPE_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/empty", { rows: [] }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/duplicate-partitions", {
        rows: [{ date: "2026-01-01", id: 1 }],
        partitionBy: ["date", "date"],
      }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/bad-limit", {
        rows: [{ id: 1 }],
        maxRowsPerFile: 0,
      }),
    ).rejects.toMatchObject({ code: "LAQL_TYPE_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/bad-byte-limit", {
        rows: [{ id: 1 }],
        maxBytesPerFile: 0,
      }),
    ).rejects.toMatchObject({ code: "LAQL_TYPE_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/bad-partition", {
        rows: [{ date: null, id: 1 }],
        partitionBy: ["date"],
      }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/mixed", { rows: [{ value: 1 }, { value: "x" }] }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/non-finite", { rows: [{ value: Number.NaN }] }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/object", { rows: [{ value: { nested: true } }] }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/all-null", { rows: [{ value: null }] }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/partition-only", {
        rows: [{ date: "2026-01-01" }],
        partitionBy: ["date"],
      }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/missing-required", {
        rows: [{ id: 1 }, { id: null }],
        validation: { required: ["id"] },
      }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/duplicate", {
        rows: [{ id: 1 }, { id: 1 }],
        validation: { unique: [["id"]] },
      }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/empty-unique", {
        rows: [{ id: 1 }],
        validation: { unique: [[]] },
      }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/range", {
        rows: [{ score: 101 }],
        validation: { ranges: { score: { min: 0, max: 100 } } },
      }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/range-type", {
        rows: [{ score: "101" }],
        validation: { ranges: { score: { min: 0, max: 100 } } },
      }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
    await expect(
      writePartitionedParquet(outStore, "out/enum", {
        rows: [{ category: "c" }],
        validation: { enums: { category: ["a", "b"] } },
      }),
    ).rejects.toMatchObject({ code: "LAQL_VALIDATION_ERROR" });
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
      code: "LAQL_BUDGET_EXCEEDED",
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
    ).rejects.toMatchObject({ code: "LAQL_GROUP_LIMIT_EXCEEDED" });
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

    const explain = await lake
      .hive("data/hive/**/*.parquet")
      .select(["id", "country"])
      .where(and(eq("country", "US"), gt("amount", 100)))
      .explain();
    expect(explain.json.filesPlanned).toBe(2);
    expect(explain.json.filesSkipped).toBe(1);
  });

  it("rejects invalid JSON queries with LAQL_PARSE_ERROR", () => {
    const lake = createParquetLake({ store });
    expect(() => lake.query({ version: 2, from: `data/${SALES.file}` })).toThrowError(LaQLError);
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
