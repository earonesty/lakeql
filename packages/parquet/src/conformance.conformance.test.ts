import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { parquetMetadata } from "hyparquet";
import { isTimestampValue, MemoryBudget, memoryStore, timestampFromEpoch } from "lakeql-core";
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
    const bytes = readFileSync(file);
    const metadata = parquetMetadata(
      bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
    );
    expect(rows).toHaveLength(Number(metadata.num_rows));
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
      // Collected test rows and the public oracle are outside the owned pool:
      // this checks value parity, not whole-process memory consumption.
      const actual: unknown[] = [];
      for await (const row of lake.path(objectPath).batchSize(128).run().rows()) {
        expect(row).not.toBeNull();
        actual.push(row);
        count++;
      }
      expect(count).toBe(Number(metadata.num_rows));
      const expected = await readParquetObjects(store, objectPath);
      expect(expected).toHaveLength(Number(metadata.num_rows));
      for (let index = 0; index < actual.length; index++)
        expect(actual[index], `row ${index} of ${objectPath}`).toEqual(
          queryTemporalValues(expected[index]),
        );
    } finally {
      expect(pool.usedBytes).toBe(0);
      expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    }
  });
  // Independent expected dates from shredded_variant/cases.json, covering
  // shredded, unshredded, and partially shredded values (including pre-epoch).
  it.each([
    [18, "2024-11-07"],
    [19, "1957-11-07"],
    [62, "2024-11-07"],
    [63, "1957-11-07"],
    [104, "2024-11-07"],
    [105, "1957-11-07"],
    [134, "2024-01-30"],
  ] as const)("preserves documented variant date case %i", async (number, date) => {
    const file = `shredded_variant/case-${String(number).padStart(3, "0")}.parquet`;
    const store = memoryStore();
    await store.put(file, readFileSync(join(parquetTestingDir, file)));
    const value = new Date(`${date}T00:00:00.000Z`);
    const expected = [{ id: 1, var: number === 134 ? { a: null, b: "iceberg", d: value } : value }];
    expect(await readParquetObjects(store, file)).toEqual(expected);
    const pool = new MemoryBudget(64 * 1024 * 1024);
    const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
    const output = await lake.path(file).run().toOwnedArray();
    try {
      expect(output.value).toEqual(queryTemporalValues(expected));
    } finally {
      output.release();
      expect(pool.usedBytes).toBe(0);
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

/** Query vectors canonicalize Date carriers to millisecond timestamps. Preserve
 * every other value distinction, particularly bigint precision, nulls and keys.
 */
function queryTemporalValues(value: unknown): unknown {
  if (value instanceof Date) return timestampFromEpoch(BigInt(value.getTime()), "millis");
  if (value instanceof Uint8Array || isTimestampValue(value)) return value;
  if (Array.isArray(value)) return value.map(queryTemporalValues);
  if (
    value !== null &&
    typeof value === "object" &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
  )
    return Object.fromEntries(
      Object.entries(value).map(([key, nested]) => [key, queryTemporalValues(nested)]),
    );
  return value;
}

describe("conformance temporal comparison", () => {
  it("canonicalizes nested dates without changing scalar or structural distinctions", () => {
    const precise = timestampFromEpoch(123456789n, "nanos");
    const bytes = new Uint8Array([0, 255]);
    const value = {
      before: new Date(-1),
      nested: [new Date(0), precise],
      large: 9007199254740993n,
      bytes,
      empty: [],
      absent: {},
      nullable: { a: null },
    };
    const normalized = queryTemporalValues(value);
    expect(normalized).toEqual({
      ...value,
      before: timestampFromEpoch(-1n, "millis"),
      nested: [timestampFromEpoch(0n, "millis"), precise],
    });
    expect(queryTemporalValues(precise)).toBe(precise);
    expect(queryTemporalValues(bytes)).toBe(bytes);
    const map = new Map([["a", 1]]);
    expect(queryTemporalValues(map)).toBe(map);
    expect(queryTemporalValues({})).not.toEqual({ a: null });
    expect(queryTemporalValues([])).not.toEqual(null);
    expect(queryTemporalValues(precise)).not.toEqual(timestampFromEpoch(123n, "millis"));
  });
});
