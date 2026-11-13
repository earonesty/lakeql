import { readFileSync } from "node:fs";
import { gt, MemoryBudget, memoryStore, timestampFromEpoch } from "lakeql-core";
import { expect, it } from "vitest";
import { createParquetLake, readParquetObjects } from "./index.js";
import { lakeqlParquetParsers } from "./parsers.js";

// Oracle from Apache parquet-testing/data/flba12_timestamp.md, not our decoder.
const seconds = [0n, 1n, -1n, 9223372036n, 253402300799n, -62135596800n];
const expected = seconds.map((value) => ({
  timestamp_millis: timestampFromEpoch(value * 1000n, "millis"),
  timestamp_micros: timestampFromEpoch(value * 1000000n, "micros"),
  timestamp_nanos: timestampFromEpoch(value * 1000000000n, "nanos"),
}));

it.each([
  1, 2, 6,
])("preserves the documented FLBA12 fixture through public scans at batch size %i", async (batchSize) => {
  const store = memoryStore();
  await store.put(
    "timestamps.parquet",
    readFileSync(
      new URL(
        "../../../fixtures/external/parquet-testing/data/flba12_timestamp.parquet",
        import.meta.url,
      ),
    ),
  );
  expect(await readParquetObjects(store, "timestamps.parquet")).toEqual(expected);
  const pool = new MemoryBudget(1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  expect(await lake.path("timestamps.parquet").batchSize(batchSize).run().toArray()).toEqual(
    expected,
  );
  expect(pool.usedBytes).toBe(0);
  const filtered = await lake
    .path("timestamps.parquet")
    .batchSize(batchSize)
    .where(gt("timestamp_nanos", timestampFromEpoch(9223372036n * 1000000000n, "nanos")))
    .run()
    .toArray();
  expect(filtered).toEqual([expected[4]]);
  expect(pool.usedBytes).toBe(0);
  const ordered = await lake
    .path("timestamps.parquet")
    .batchSize(batchSize)
    .orderBy([{ column: "timestamp_nanos", direction: "asc" }])
    .run()
    .toArray();
  expect(ordered).toEqual([
    expected[5],
    expected[2],
    expected[0],
    expected[1],
    expected[3],
    expected[4],
  ]);
  expect(pool.usedBytes).toBe(0);
  expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
});

it("decodes signed FLBA12 extrema from offset views and rejects invalid widths", () => {
  for (const value of [-(1n << 95n), -1n, 0n, (1n << 95n) - 1n]) {
    const backing = new Uint8Array(20);
    let bits = BigInt.asUintN(96, value);
    for (let i = 0; i < 12; i++) {
      backing[i + 3] = Number(bits & 255n);
      bits >>= 8n;
    }
    expect(lakeqlParquetParsers.timestampFromNanoseconds(backing.subarray(3, 15))).toEqual(
      timestampFromEpoch(value, "nanos"),
    );
  }
  for (const length of [0, 8, 11, 13])
    expect(() => lakeqlParquetParsers.timestampFromNanoseconds(new Uint8Array(length))).toThrow(
      /12 bytes/,
    );
});
