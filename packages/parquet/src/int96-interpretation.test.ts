import { readFileSync } from "node:fs";
import { parquetMetadata } from "hyparquet";
import {
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  memoryStore,
  SharedMemoryCache,
  timestampFromEpoch,
} from "lakeql-core";
import { expect, it } from "vitest";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { int96EpochNanos, resolveInt96Interpretation } from "./int96-interpretation.js";
import { readNestedRowGroupBatches } from "./nested-row-group-read.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it("recovers Spark microsecond boundaries without truncating ordinary nanos", () => {
  for (const micros of [-(1n << 63n), (1n << 63n) - 1n, -1n, 0n, 1n, 9089380393200000000n]) {
    const julian = BigInt.asIntN(64, micros + 2440588n * 86400000000n);
    const days = julian / 86400000000n;
    const nanos = (julian % 86400000000n) * 1000n;
    const carrier = (days << 64n) | BigInt.asUintN(64, nanos);
    expect(int96EpochNanos(carrier, "spark-micros")).toBe(micros * 1000n);
  }
  expect(int96EpochNanos((2440588n << 64n) | 123n, "julian-nanos")).toBe(123n);
  expect(() => int96EpochNanos((2440588n << 64n) | 123n, "spark-micros")).toThrow(
    /sub-microsecond/,
  );
  expect(resolveInt96Interpretation({})).toBe("julian-nanos");
  const metadata = { key_value_metadata: [{ key: "org.apache.spark.version", value: "3.4.3" }] };
  expect(resolveInt96Interpretation(metadata)).toBe("spark-micros");
  expect(resolveInt96Interpretation(metadata, "julian-nanos")).toBe("julian-nanos");
});

it("decodes the documented Spark fixture and isolates cache interpretation modes", async () => {
  const bytes = readFileSync(
    new URL(
      "../../../fixtures/external/parquet-testing/data/int96_from_spark.parquet",
      import.meta.url,
    ),
  );
  const metadata = parquetMetadata(
    bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
  );
  const micros = [
    1704141296123456n,
    1704070800000000n,
    253402225200000000n,
    1735599600000000n,
    null,
    9089380393200000000n,
  ];
  const expected = micros.map((value) => ({
    a: value === null ? null : timestampFromEpoch(value * 1000n, "nanos"),
  }));
  const store = memoryStore();
  await store.put("spark", bytes);
  const file = await asyncBufferFromStore(store, "spark");
  const pool = new MemoryBudget(1024 * 1024);
  const shared = new SharedMemoryCache({ maxBytes: 256 * 1024, memoryBudget: pool });
  const cache = new DecodedColumnCache(shared, { maxBytes: 256 * 1024, policy: "latency" });
  try {
    for (const mode of ["auto", "julian-nanos", "spark-micros", "auto"] as const) {
      const scope = new MemoryScope(pool.child(128 * 1024));
      const rows: unknown[] = [];
      try {
        for await (const owner of readNestedRowGroupBatches(file, metadata, scope, {
          batchSize: 3,
          int96Interpretation: mode,
          decodedColumnCache: cache,
          decodedColumnCacheKey: "spark",
        })) {
          try {
            rows.push(...materializeBatchRows(owner.value.batch));
          } finally {
            owner.release();
          }
        }
      } finally {
        scope.dispose();
      }
      if (mode === "julian-nanos") {
        expect(rows.slice(0, 5)).toEqual(expected.slice(0, 5));
        expect(rows[5]).not.toEqual(expected[5]);
      } else expect(rows).toEqual(expected);
    }
  } finally {
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});
