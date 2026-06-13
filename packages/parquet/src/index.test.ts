import { readFileSync } from "node:fs";
import { LaQLError, memoryStore } from "@laql/core";
import { fixturePath, SALES, TYPES } from "@laql/fixtures";
import { beforeAll, describe, expect, it } from "vitest";
import { readParquetMetadata, readParquetObjects } from "./index.js";

const store = memoryStore();

beforeAll(async () => {
  await store.put(`data/${SALES.file}`, readFileSync(fixturePath(SALES.file)));
  await store.put(`data/${TYPES.file}`, readFileSync(fixturePath(TYPES.file)));
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

describe("readParquetMetadata", () => {
  it("sees the row-group layout the fixture generator promised", async () => {
    const meta = await readParquetMetadata(store, `data/${SALES.file}`);
    expect(Number(meta.num_rows)).toBe(SALES.rows);
    const expectedGroups = Math.ceil(SALES.rows / SALES.rowGroupSize);
    expect(meta.row_groups).toHaveLength(expectedGroups);
  });
});
