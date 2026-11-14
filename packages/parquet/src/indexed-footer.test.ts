import { existsSync, readFileSync } from "node:fs";
import { parquetMetadata } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { footerDirectoryBytes } from "./footer-directory.js";
import { indexedLeafBytes, openIndexedParquetFooter } from "./indexed-footer.js";
import { lakeqlParquetParsers } from "./parsers.js";

it("opens schema/envelope without materializing groups and respects directory lifetime", () => {
  const bytes = parquetWriteBuffer({
    rowGroupSize: Array(20).fill(2),
    columnData: [{ name: "id", type: "INT32", data: Array.from({ length: 40 }, (_, i) => i) }],
  });
  const oracle = parquetMetadata(bytes, { parsers: lakeqlParquetParsers });
  const capacity = footerDirectoryBytes(20) + indexedLeafBytes(oracle);
  const scope = new MemoryScope(new MemoryBudget(capacity));
  const indexed = openIndexedParquetFooter(bytes, scope);
  const { row_groups, ...envelope } = oracle;
  expect(indexed.metadata).toEqual(envelope);
  expect(indexed.metadata).not.toHaveProperty("row_groups");
  expect(indexed.rowGroupCount).toBe(20);
  expect(scope.budget.usedBytes).toBe(capacity);
  for (const index of [19, 0, 7]) expect(indexed.readRowGroup(index)).toEqual(row_groups[index]);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  expect(() => indexed.readRowGroup(0)).toThrow(/closed/);
  expect(() => openIndexedParquetFooter(bytes, scope)).toThrow(/closed/);
  const tight = new MemoryScope(new MemoryBudget(capacity - 1));
  expect(() => openIndexedParquetFooter(bytes, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

const decimalFixture = new URL(
  "../../../fixtures/external/parquet-testing/data/byte_stream_split_extended.gzip.parquet",
  import.meta.url,
);
it.skipIf(!existsSync(decimalFixture))(
  "matches normalized decimal metadata and pruning from real fixture",
  async () => {
    const { memoryStore, gte, eq } = await import("lakeql-core");
    const { asyncBufferFromStore } = await import("./store-buffer.js");
    const { readParquetMetadataFromFile } = await import("./metadata-cache.js");
    const { planRowGroupsFromMetadata, planRowGroupsFromSource } = await import(
      "./row-group-plan.js"
    );
    const bytes = readFileSync(decimalFixture);
    const store = memoryStore();
    await store.put("decimal.parquet", bytes);
    const oracle = await readParquetMetadataFromFile(
      await asyncBufferFromStore(store, "decimal.parquet"),
    );
    const scope = new MemoryScope(
      new MemoryBudget(footerDirectoryBytes(1) + indexedLeafBytes(oracle)),
    );
    const source = openIndexedParquetFooter(
      bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length),
      scope,
    );
    expect(source.readRowGroup(0)).toEqual(oracle.row_groups[0]);
    for (const column of oracle.row_groups[0]?.columns ?? []) {
      const name = column.meta_data?.path_in_schema[0];
      if (!name?.startsWith("decimal_")) continue;
      const maximum = column.meta_data?.statistics?.max_value as number;
      for (const where of [eq(name, maximum), gte(name, maximum + 1)]) {
        function* groups() {
          yield source.readRowGroup(0);
        }
        expect(planRowGroupsFromSource(source.metadata, groups(), where)).toEqual(
          planRowGroupsFromMetadata(oracle, where),
        );
      }
    }
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  },
);

it("matches GeoParquet annotations from an encoded footer and rolls back tight budgets", () => {
  const bytes = parquetWriteBuffer({
    columnData: [{ name: "geometry", type: "BYTE_ARRAY", data: [new Uint8Array([1, 2, 3])] }],
    kvMetadata: [
      {
        key: "geo",
        value: JSON.stringify({
          columns: {
            geometry: {
              encoding: "WKB",
              crs: { ids: [{ authority: "EPSG", code: 4326 }] },
            },
          },
        }),
      },
    ],
  });
  const oracle = parquetMetadata(bytes, { parsers: lakeqlParquetParsers });
  const scope = new MemoryScope(new MemoryBudget(65536));
  const source = openIndexedParquetFooter(bytes, scope);
  const { row_groups, ...metadata } = oracle;
  expect(source.metadata).toEqual(metadata);
  expect(source.readRowGroup(0)).toEqual(row_groups[0]);
  expect(source.metadata.schema[1]?.logical_type).toEqual({ type: "GEOMETRY", crs: "EPSG:4326" });
  const peak = scope.budget.peakBytes;
  expect(scope.budget.usedBytes).toBeLessThan(peak);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => openIndexedParquetFooter(bytes, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});
