import { existsSync } from "node:fs";
import { eq, gte, MemoryBudget, MemoryScope, memoryCache, memoryStore } from "lakeql-core";
import { describe, expect, it } from "vitest";
import {
  createParquetLake,
  encodedParquetMetadataCache,
  readParquetMetadata,
  writeParquet,
} from "./index.js";
import { countingObjectStore } from "./test-helpers.js";

describe("encoded Parquet metadata cache", () => {
  it("preserves parsed metadata across lake instances through a byte cache", async () => {
    const store = countingObjectStore(memoryStore());
    await writeParquet(store, "data/cache.parquet", {
      rowGroupSize: 2,
      columnData: [
        { name: "serial", type: "INT64", data: [1n, 2n, 3n, 4n] },
        { name: "label", type: "BYTE_ARRAY", data: ["a", "b", "c", "d"] },
      ],
    });
    const byteCache = memoryCache<Uint8Array>();
    const metadataCache = encodedParquetMetadataCache(byteCache);

    const metadata = await readParquetMetadata(store, "data/cache.parquet");
    await metadataCache.set("roundtrip", { value: metadata });
    const roundtrip = await metadataCache.get("roundtrip");
    expect(roundtrip?.value.num_rows).toBe(metadata.num_rows);
    expect(typeof roundtrip?.value.num_rows).toBe("bigint");
    expect(roundtrip?.value.row_groups).toHaveLength(2);

    store.resetCounters();
    await createParquetLake({ store, metadataCache })
      .path("data/cache.parquet")
      .where(gte("serial", 3n))
      .planTasks();
    expect(store.counters.getRange).toBeGreaterThan(0);

    store.resetCounters();
    const tasks = await createParquetLake({ store, metadataCache })
      .path("data/cache.parquet")
      .where(gte("serial", 3n))
      .planTasks();
    expect(tasks[0]?.rowGroupRanges).toEqual([{ start: 1, end: 2 }]);
    expect(store.counters.getRange).toBe(0);
  });
});

const byteDecimalFixture = new URL(
  "../../../fixtures/external/parquet-testing/data/byte_stream_split_extended.gzip.parquet",
  import.meta.url,
);

it.skipIf(!existsSync(byteDecimalFixture))(
  "recovers consistent byte-decimal bounds for owned and unowned reads",
  async () => {
    const { readFile } = await import("node:fs/promises");
    const { parquetMetadata } = await import("hyparquet");
    const { asyncBufferFromStore } = await import("./index.js");
    const { readParquetMetadataFromFile } = await import("./metadata-cache.js");
    const bytes = await readFile(byteDecimalFixture);
    const { readCompactHeader, FOOTER_STATISTICS_PROJECTION } = await import("./compact-header.js");
    const footerView = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const footerLength = footerView.getUint32(bytes.byteLength - 8, true);
    const projected = readCompactHeader(
      { view: footerView, offset: bytes.byteLength - 8 - footerLength },
      FOOTER_STATISTICS_PROJECTION,
    );
    expect(projected.field_4).toHaveLength(1);
    const projectedColumns = (
      projected.field_4 as Array<{
        field_1: Array<{ field_3: { field_12?: Record<string, Uint8Array> } }>;
      }>
    )[0]?.field_1;
    expect(projectedColumns).toHaveLength(14);
    for (const column of projectedColumns ?? []) {
      for (const bound of Object.values(column.field_3.field_12 ?? {})) {
        expect(bound).toBeInstanceOf(Uint8Array);
        expect(bound.buffer).toBe(bytes.buffer);
      }
    }
    const raw = parquetMetadata(
      bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
    );
    const store = memoryStore();
    await store.put("decimal.parquet", bytes);
    const metadata = await readParquetMetadataFromFile(
      await asyncBufferFromStore(store, "decimal.parquet"),
    );
    let checked = 0;
    for (const [groupIndex, group] of metadata.row_groups.entries()) {
      for (const [columnIndex, column] of group.columns.entries()) {
        if (!column.meta_data?.path_in_schema[0]?.startsWith("decimal_")) continue;
        const original = raw.row_groups[groupIndex]?.columns[columnIndex]?.meta_data?.statistics;
        expect(original?.min_value ?? original?.min).toBeDefined();
        const stats = column.meta_data.statistics;
        expect(typeof stats?.min_value).toBe("number");
        expect(typeof stats?.max_value).toBe("number");
        expect(stats?.null_count).toBe(original?.null_count);
        checked++;
      }
    }
    expect(checked).toBe(2);
    const { readOwnedParquetMetadata } = await import("./metadata-cache.js");
    const { readParquetObjects } = await import("./index.js");
    const rows = await readParquetObjects(store, "decimal.parquet");
    const source = await asyncBufferFromStore(store, "decimal.parquet");
    let reads = 0;
    const file = {
      ...source,
      sliceOwned: async (...args: Parameters<NonNullable<typeof source.sliceOwned>>) => {
        reads++;
        if (!source.sliceOwned) throw new Error("Missing owned read");
        return source.sliceOwned(...args);
      },
    };
    const { decodeOwnedFullMetadata } = await import("./owned-full-metadata.js");
    const measurement = new MemoryScope(new MemoryBudget(Number.MAX_SAFE_INTEGER));
    decodeOwnedFullMetadata(
      bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
      measurement,
    );
    const budget = new MemoryBudget(
      Math.min(bytes.byteLength, 65536) + measurement.budget.peakBytes,
    );
    measurement.dispose();
    const { owner } = await readOwnedParquetMetadata("decimal.parquet", file, budget, undefined);
    expect(reads).toBe(1);
    expect(owner.value).toEqual(metadata);
    for (const column of owner.value.row_groups[0]?.columns ?? []) {
      const name = column.meta_data?.path_in_schema[0];
      if (!name?.startsWith("decimal_")) continue;
      const values = rows.map((row) => row[name] as number);
      expect(column.meta_data?.statistics?.min_value).toBe(Math.min(...values));
      expect(column.meta_data?.statistics?.max_value).toBe(Math.max(...values));
      const lake = createParquetLake({ store });
      const maximum = Math.max(...values);
      for (const boundary of [Math.min(...values), maximum]) {
        const tasks = await lake.path("decimal.parquet").where(eq(name, boundary)).planTasks();
        expect(tasks.map((task) => task.rowGroupRanges)).toEqual([[{ start: 0, end: 1 }]]);
      }
      const excluded = await lake
        .path("decimal.parquet")
        .where(gte(name, maximum + 1))
        .planTasks();
      expect(excluded.map((task) => task.rowGroupRanges)).toEqual([[]]);
    }
    expect(budget.usedBytes).toBeGreaterThan(0);
    owner.release();
    expect(budget.usedBytes).toBe(0);
  },
);

it("rejects invalid unowned footer extents before parsing metadata", async () => {
  const { readParquetMetadataFromFile } = await import("./metadata-cache.js");
  for (const byteLength of [-1, 0, 7, NaN, Infinity, 8.5]) {
    let reads = 0;
    await expect(
      readParquetMetadataFromFile({
        byteLength,
        slice: async () => {
          reads++;
          return new ArrayBuffer(0);
        },
      }),
    ).rejects.toThrow(/file length/);
    expect(reads).toBe(0);
  }
  for (const bytes of [
    Uint8Array.of(0, 0, 0, 0, 0, 0, 0, 0),
    Uint8Array.of(255, 255, 255, 255, 80, 65, 82, 49),
  ]) {
    let reads = 0;
    await expect(
      readParquetMetadataFromFile({
        byteLength: bytes.length,
        slice: async () => {
          reads++;
          return bytes.buffer;
        },
      }),
    ).rejects.toThrow(/footer/);
    expect(reads).toBe(1);
  }
});
