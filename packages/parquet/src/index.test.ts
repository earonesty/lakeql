import { readFileSync } from "node:fs";
import {
  and,
  between,
  col,
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
  memoryStore,
  ne,
  not,
  notIn,
  or,
} from "@laql/core";
import { fixturePath, HIVE, SALES, STATS, TYPES, WIDE } from "@laql/fixtures";
import type { RowGroup } from "hyparquet";
import { beforeAll, describe, expect, it } from "vitest";
import {
  createParquetLake,
  readParquetMetadata,
  readParquetObjects,
  rowGroupMayMatch,
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

  it("prunes Parquet row groups using min/max statistics", async () => {
    const lake = createParquetLake({ store, queryId: () => "stats-query" });
    const result = lake.path(`data/${STATS.file}`).where(lt("metric", 50)).run();
    const rows = await result.toArray();
    expect(rows).toHaveLength(STATS.rowGroupSize);
    expect(rows.map((row) => row.metric)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    expect(result.stats.rowGroupsRead).toBe(1);
    expect(result.stats.rowGroupsSkipped).toBe(2);
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
