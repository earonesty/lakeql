import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { parquetMetadata } from "hyparquet";
import { MemoryBudget, memoryStore } from "lakeql-core";
import { EXTERNAL_CONFORMANCE, externalFixturePath } from "lakeql-fixtures";
import { describe, expect, it } from "vitest";
import { createParquetLake, readParquetObjects } from "./index.js";

const parquetTestingDir = externalFixturePath(EXTERNAL_CONFORMANCE.parquetTestingDir);
const parquetFiles = existsSync(parquetTestingDir)
  ? listFiles(parquetTestingDir).filter((path) => path.endsWith(".parquet"))
  : [];
// Explicit corruptions documented by the vendored bad_data/README.md. Do not
// classify the whole directory: ARROW-GH-43605 is a valid zero-bit-width case.
const malformedNames = new Set(
  [
    "ARROW-GH-41317.parquet",
    "ARROW-GH-41321.parquet",
    "ARROW-GH-45185.parquet",
    "ARROW-GH-47662.parquet",
    "ARROW-RS-GH-11261-FLBA-DICT.parquet",
    "ARROW-RS-GH-6229-DICTHEADER.parquet",
    "ARROW-RS-GH-6229-LEVELS.parquet",
    "PARQUET-1481.parquet",
  ].map((name) => join("bad_data", name)),
);
const malformedFiles = parquetFiles.filter((file) =>
  malformedNames.has(relative(parquetTestingDir, file)),
);
const readableFiles = parquetFiles.filter(
  (file) => !malformedNames.has(relative(parquetTestingDir, file)),
);
const describeExternal =
  process.env.LAKEQL_CONFORMANCE === "1" && parquetFiles.length > 0 ? describe : describe.skip;

describeExternal("apache/parquet-testing conformance", () => {
  it.each(readableFiles)("decodes %s through the public reader", async (file) => {
    const store = memoryStore();
    const objectPath = relative(parquetTestingDir, file);
    await store.put(objectPath, readFileSync(file));

    const rows = await readParquetObjects(store, objectPath);
    expect(Array.isArray(rows)).toBe(true);
  });
  it.each(readableFiles)("streams %s through the owned query reader", async (file) => {
    const store = memoryStore();
    const objectPath = relative(parquetTestingDir, file);
    const bytes = readFileSync(file);
    const pool = new MemoryBudget(64 * 1024 * 1024);
    const lake = createParquetLake({
      store,
      memoryPool: pool,
      scanRangeCache: { maxBytes: 0 },
      budget: { maxAllocatedMemoryBytes: 64 * 1024 * 1024 },
    });
    await store.put(objectPath, bytes);
    let count = 0;
    try {
      const metadata = parquetMetadata(
        bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
      );
      for await (const row of lake.path(objectPath).batchSize(128).run().rows()) {
        expect(row).not.toBeNull();
        count++;
      }
      expect(count).toBe(Number(metadata.num_rows));
    } finally {
      expect(pool.usedBytes).toBe(0);
      expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    }
  });
  it.each(malformedFiles)("rejects documented malformed %s in both readers", async (file) => {
    const store = memoryStore();
    const objectPath = relative(parquetTestingDir, file);
    await store.put(objectPath, readFileSync(file));
    const pool = new MemoryBudget(64 * 1024 * 1024);
    const lake = createParquetLake({
      store,
      memoryPool: pool,
      scanRangeCache: { maxBytes: 0 },
      budget: { maxAllocatedMemoryBytes: pool.maxBytes },
    });
    const owned = async () => {
      // Exercise production metadata loading, not an upstream parser in the test.
      for await (const _row of lake.path(objectPath).batchSize(128).run().rows()) {
        /* drain */
      }
    };
    for (const read of [() => readParquetObjects(store, objectPath), owned]) {
      try {
        await expect(read()).rejects.toMatchObject({
          code: expect.stringMatching(/^LAKEQL_(PARQUET_READ|VALIDATION)_ERROR$/),
        });
      } finally {
        expect(pool.usedBytes).toBe(0);
        expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
      }
    }
  });
});

function listFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir).sort()) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) out.push(...listFiles(path));
    else out.push(path);
  }
  return out;
}
