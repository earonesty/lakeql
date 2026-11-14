import { existsSync, readFileSync } from "node:fs";
import { DuckDBConnection } from "@duckdb/node-api";
import { Lake, MemoryBudget, memoryStore } from "lakeql-core";
import { externalFixturePath } from "lakeql-fixtures";
import { describe, expect, it } from "vitest";
import { readParquetObjects } from "./index.js";
import { ParquetScanAdapter } from "./scan-adapter.js";

const cases = [
  "bad_data/ARROW-GH-43605.parquet",
  "data/byte_stream_split.zstd.parquet",
  "data/delta_length_byte_array.parquet",
  "data/geospatial/geography-lines.parquet",
  "data/geospatial/geography-points.parquet",
  "data/geospatial/geography-polygons.parquet",
  "data/page_v2_empty_compressed.parquet",
  "data/datapage_v2_empty_datapage.snappy.parquet",
];
const root = externalFixturePath("parquet-testing");
const suite = process.env.LAKEQL_REFERENCE === "1" && existsSync(root) ? describe : describe.skip;

suite("external fixture values and owned admission", () => {
  it.each(cases)("matches DuckDB values and owned admission for %s", async (file) => {
    const path = `${root}/${file}`;
    const connection = await DuckDBConnection.create();
    const geometry = file.includes("geospatial/");
    const normalize = (rows: Record<string, unknown>[]) =>
      rows.map((row) => {
        if (!geometry) return row;
        const bytes = row.geometry;
        if (bytes !== null && !(bytes instanceof Uint8Array))
          throw new Error("Expected raw geometry bytes");
        return {
          id: String(row.id),
          geometry:
            bytes === null
              ? null
              : Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0"))
                  .join("")
                  .toUpperCase(),
        };
      });
    try {
      const fields = geometry ? "id::VARCHAR AS id, hex(ST_AsWKB(geometry)) AS geometry" : "*";
      const result = await connection.runAndReadAll(
        `SELECT ${fields} FROM read_parquet('${path.replaceAll("'", "''")}')`,
      );
      const expected = result.getRowObjectsJson();
      const store = memoryStore();
      await store.put(file, readFileSync(path));
      expect(normalize(await readParquetObjects(store, file))).toEqual(expected);
      async function owned(maxBytes: number) {
        const pool = new MemoryBudget(maxBytes);
        const lake = new Lake({ store, scanner: new ParquetScanAdapter(store), memoryPool: pool });
        try {
          expect(normalize(await lake.path(file).batchSize(17).toArray())).toEqual(expected);
          return pool.peakBytes;
        } finally {
          expect(pool.usedBytes).toBe(0);
        }
      }
      const peak = await owned(64 * 1024 * 1024);
      expect(await owned(peak)).toBe(peak);
      await expect(owned(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    } finally {
      connection.disconnectSync();
    }
  });
});

suite("nested external map membership", () => {
  it.each([
    { file: "data/nonnullable.impala.parquet", column: "int_map_array" },
    { file: "data/nullable.impala.parquet", column: "int_Map_Array" },
  ])("preserves map membership through public owned projection: $file", async ({
    file,
    column,
  }) => {
    const path = `${root}/${file}`;
    const connection = await DuckDBConnection.create();
    try {
      const result = await connection.runAndReadAll(
        `SELECT to_json("${column}")::VARCHAR AS value FROM read_parquet('${path.replaceAll("'", "''")}')`,
      );
      const expected = result
        .getRowObjectsJson()
        .map((row) => ({ [column]: row.value === null ? null : JSON.parse(String(row.value)) }));
      const store = memoryStore();
      await store.put(file, readFileSync(path));
      expect(await readParquetObjects(store, file, { columns: [column] })).toEqual(expected);
      async function owned(cap: number) {
        const pool = new MemoryBudget(cap);
        const lake = new Lake({ store, scanner: new ParquetScanAdapter(store), memoryPool: pool });
        try {
          expect(await lake.path(file).select([column]).batchSize(1).toArray()).toEqual(expected);
          return pool.peakBytes;
        } finally {
          expect(pool.usedBytes).toBe(0);
        }
      }
      const peak = await owned(64 * 1024 * 1024);
      expect(await owned(peak)).toBeLessThanOrEqual(peak);
      await expect(owned(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    } finally {
      connection.disconnectSync();
    }
  });
});
