import { existsSync, readFileSync } from "node:fs";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, MemoryScope, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { decodeOwnedFullMetadata } from "./owned-full-metadata.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it("admits complete metadata, matches normalized decoding, and rolls back at peak minus one", async () => {
  const bytes = parquetWriteBuffer({
    rowGroupSize: [2, 2],
    columnData: [{ name: "id", type: "INT32", data: [1, 2, 3, 4] }],
    kvMetadata: [
      { key: "geo", value: JSON.stringify({ columns: {}, unused: Array(100).fill("scratch") }) },
    ],
  });
  const store = memoryStore();
  await store.put("fixture.parquet", new Uint8Array(bytes));
  const expected = await readParquetMetadataFromFile(
    await asyncBufferFromStore(store, "fixture.parquet"),
  );
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  const actual = decodeOwnedFullMetadata(bytes, scope);
  expect(actual).toEqual(expected);
  const peak = scope.budget.peakBytes;
  expect(scope.budget.usedBytes).toBeGreaterThan(bytes.byteLength);
  expect(scope.budget.usedBytes).toBeLessThan(peak);
  const owner = scope.retainAllocations(actual);
  scope.dispose();
  expect(scope.budget.usedBytes).toBeGreaterThan(0);
  owner.release();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => decodeOwnedFullMetadata(bytes, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

const decimalFixture = new URL(
  "../../../fixtures/external/parquet-testing/data/byte_stream_split_extended.gzip.parquet",
  import.meta.url,
);
it.skipIf(!existsSync(decimalFixture))(
  "matches checked decimal bounds from a real Parquet fixture",
  async () => {
    const input = readFileSync(decimalFixture);
    const bytes = input.buffer.slice(input.byteOffset, input.byteOffset + input.byteLength);
    const store = memoryStore();
    await store.put("decimal.parquet", input);
    const expected = await readParquetMetadataFromFile(
      await asyncBufferFromStore(store, "decimal.parquet"),
    );
    const scope = new MemoryScope(new MemoryBudget(4 * 1024 * 1024));
    try {
      expect(decodeOwnedFullMetadata(bytes, scope)).toEqual(expected);
    } finally {
      scope.dispose();
    }
    expect(scope.budget.usedBytes).toBe(0);
  },
);

for (const name of ["float16_zeros_and_nans.parquet", "float16_nonzeros_and_nans.parquet"]) {
  const fixture = new URL(
    `../../../fixtures/external/parquet-testing/data/${name}`,
    import.meta.url,
  );
  it.skipIf(!existsSync(fixture))(
    `preserves admitted FLOAT16 fixture metadata: ${name}`,
    async () => {
      const input = readFileSync(fixture);
      const bytes = input.buffer.slice(input.byteOffset, input.byteOffset + input.byteLength);
      const store = memoryStore();
      await store.put(name, input);
      const expected = await readParquetMetadataFromFile(await asyncBufferFromStore(store, name));
      const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
      try {
        const actual = decodeOwnedFullMetadata(bytes, scope);
        expect(actual).toEqual(expected);
        expect(actual.schema.some((element) => element.logical_type?.type === "FLOAT16")).toBe(
          true,
        );
        const statistics = actual.row_groups[0]?.columns[0]?.meta_data?.statistics;
        const maximum = name.includes("nonzeros") ? 2 : 0;
        expect(statistics?.max_value).toBe(maximum);
        expect(statistics?.min_value).toBe(-maximum);
        expect(statistics?.null_count).toBe(1n);
      } finally {
        scope.dispose();
      }
      expect(scope.budget.usedBytes).toBe(0);
    },
  );
}
