import { existsSync, readFileSync } from "node:fs";
import { MemoryBudget, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { createParquetLake, readParquetObjects } from "./index.js";

const fixture = new URL(
  "../../../fixtures/external/parquet-testing/data/fixed_length_decimal.parquet",
  import.meta.url,
);
it.skipIf(!existsSync(fixture))(
  "value-checks precision-25 fixed-width decimals through both public readers",
  async () => {
    const store = memoryStore();
    await store.put("decimal.parquet", readFileSync(fixture));
    const expected = Array.from({ length: 24 }, (_, index) => ({ value: index + 1 }));
    expect(await readParquetObjects(store, "decimal.parquet")).toEqual(expected);
    for (const batchSize of [1, 7, 64]) {
      const pool = new MemoryBudget(1024 * 1024);
      const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
      expect(await lake.path("decimal.parquet").batchSize(batchSize).toArray()).toEqual(expected);
      expect(pool.usedBytes).toBe(0);
      expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    }
  },
);

it
  .skipIf(!existsSync(fixture))
  .each([9007199254740991n, -9007199254740991n, 9007199254740992n, -9007199254740992n])(
  "checks decoded fixed-width coefficient %s before scaling",
  async (coefficient) => {
    const bytes = readFileSync(fixture);
    // Locate the first two known PLAIN coefficients (100, 200), not footer stats.
    // Keep the original uncompressed page layout and replace one 11-byte value.
    // These unfiltered reads deliberately do not use the original pruning stats.
    const marker = Buffer.alloc(22);
    marker[10] = 100;
    marker[21] = 200;
    const offset = bytes.indexOf(marker);
    expect(offset).toBeGreaterThan(0);
    expect(bytes.indexOf(marker, offset + 1)).toBe(-1);
    let encoded = BigInt.asUintN(88, coefficient);
    for (let index = 10; index >= 0; index--) {
      bytes[offset + index] = Number(encoded & 255n);
      encoded >>= 8n;
    }
    const store = memoryStore();
    await store.put("boundary.parquet", bytes);
    const pool = new MemoryBudget(1024 * 1024);
    const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
    const safe = coefficient >= -9007199254740991n && coefficient <= 9007199254740991n;
    for (const read of [
      () => readParquetObjects(store, "boundary.parquet"),
      () => lake.path("boundary.parquet").batchSize(1).toArray(),
    ]) {
      if (safe) {
        const rows = await read();
        expect(rows[0]).toEqual({ value: Number(coefficient) / 100 });
        expect(rows.slice(1)).toEqual(
          Array.from({ length: 23 }, (_, index) => ({ value: index + 2 })),
        );
      } else {
        await expect(read()).rejects.toMatchObject({
          code: "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
          details: { feature: "decimal-value-precision" },
        });
      }
      expect(pool.usedBytes).toBe(0);
      expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    }
  },
);
