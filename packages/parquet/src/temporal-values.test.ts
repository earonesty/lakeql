import type { ColumnDecoder, SchemaElement } from "hyparquet/src/types.js";
import {
  MATERIALIZATION_CHARGES as c,
  MemoryBudget,
  MemoryScope,
  timestampFromEpoch,
} from "lakeql-core";
import { expect, it, vi } from "vitest";
import { lakeqlParquetParsers } from "./parsers.js";
import { convertTemporalValues } from "./temporal-values.js";

it.each([
  {
    element: { name: "t", type: "INT32", converted_type: "DATE" },
    values: new Int32Array([-1, 0, 1]),
    expected: [new Date(-86400000), new Date(0), new Date(86400000)],
    method: "dateFromDays",
  },
  {
    element: {
      name: "t",
      type: "INT64",
      logical_type: { type: "TIMESTAMP", unit: "NANOS", isAdjustedToUTC: true },
    },
    values: new BigInt64Array([-1n, 0n, 123456789n]),
    expected: [-1n, 0n, 123456789n].map((value) => timestampFromEpoch(value, "nanos")),
    method: "timestampFromNanoseconds",
  },
])("admits temporal output before invoking $method", ({ element, values, expected, method }) => {
  const parsers = { ...lakeqlParquetParsers };
  const spy = vi.spyOn(parsers, method as "dateFromDays");
  const decoder = { element: element as SchemaElement, parsers } as ColumnDecoder;
  const bytes = c.array + values.length * (c.arraySlot + c.timestamp + c.bigint);
  const scope = new MemoryScope(new MemoryBudget(bytes));
  expect(convertTemporalValues(values, decoder, scope)).toEqual(expected);
  expect(scope.budget.usedBytes).toBe(bytes);
  scope.dispose();
  spy.mockClear();
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => convertTemporalValues(values, decoder, tight)).toThrow(/budget/);
  expect(spy).not.toHaveBeenCalled();
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  spy.mockRestore();
});

it("releases the complete temporal output reservation if scalar parsing fails", () => {
  const failure = new Error("injected temporal parser failure");
  const decoder = {
    element: { name: "t", type: "INT64", converted_type: "TIMESTAMP_MICROS" },
    parsers: {
      ...lakeqlParquetParsers,
      timestampFromMicroseconds() {
        throw failure;
      },
    },
  } as ColumnDecoder;
  const scope = new MemoryScope(new MemoryBudget(1024));
  expect(() => convertTemporalValues(new BigInt64Array([1n, 2n]), decoder, scope)).toThrow(failure);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("preserves owned timestamp values from real FLBA12 and INT96 fixtures", async ({ skip }) => {
  const { existsSync, readFileSync } = await import("node:fs");
  for (const name of [
    "flba12_timestamp.parquet",
    "int96_timestamp_order.parquet",
    "int96_from_spark.parquet",
  ]) {
    const fixture = new URL(
      `../../../fixtures/external/parquet-testing/data/${name}`,
      import.meta.url,
    );
    if (!existsSync(fixture)) return skip();
    const { memoryStore } = await import("lakeql-core");
    const { createParquetLake, readParquetObjects } = await import("./index.js");
    const store = memoryStore();
    await store.put("timestamp.parquet", readFileSync(fixture));
    // Spark fixture documentation is the oracle for the overflow value: the
    // upstream reader sign-extends its low INT96 word and loses the day word.
    const expected =
      name === "int96_from_spark.parquet"
        ? [
            1704141296123456n,
            1704070800000000n,
            253402225200000000n,
            1735599600000000n,
            null,
            9089380393200000000n,
          ].map((value) => ({
            a: value === null ? null : timestampFromEpoch(value * 1000n, "nanos"),
          }))
        : await readParquetObjects(store, "timestamp.parquet");
    const budget = new MemoryBudget(64 * 1024 * 1024);
    const lake = createParquetLake({ store, memoryPool: budget, scanRangeCache: { maxBytes: 0 } });
    const owner = await lake.path("timestamp.parquet").batchSize(3).run().toOwnedArray();
    try {
      expect(owner.value).toEqual(expected);
      expect(owner.value.length).toBeGreaterThan(0);
    } finally {
      owner.release();
    }
    expect(budget.usedBytes).toBe(0);
  }
});

it("admits INT96 conversion scratch and preserves pre-epoch nanoseconds", () => {
  const values = [(2440587n << 64n) | 86399999999999n, 2440588n << 64n, (2440588n << 64n) | 123n];
  const decoder = {
    element: { name: "t", type: "INT96" },
    parsers: lakeqlParquetParsers,
  } as ColumnDecoder;
  const retained = c.array + values.length * (c.arraySlot + c.timestamp + c.bigint);
  const peak = retained + 5 * c.bigint;
  const scope = new MemoryScope(new MemoryBudget(peak));
  expect(convertTemporalValues(values, decoder, scope)).toEqual(
    [-1n, 0n, 123n].map((value) => timestampFromEpoch(value, "nanos")),
  );
  expect(scope.budget.peakBytes).toBe(peak);
  expect(scope.budget.usedBytes).toBe(retained);
  scope.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => convertTemporalValues(values, decoder, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("admits Spark conversion workspace before parsing and rolls back under budget", () => {
  const value = (-105862232n << 64n) | BigInt.asUintN(64, -32509551616000n);
  const parsers = { ...lakeqlParquetParsers };
  const parse = vi.spyOn(parsers, "timestampFromNanoseconds");
  const decoder = {
    element: { name: "t", type: "INT96" as const },
    parsers,
    int96Interpretation: "spark-micros" as const,
  } as ColumnDecoder & { int96Interpretation: "spark-micros" };
  const retained = c.array + c.arraySlot + c.timestamp + c.bigint;
  const peak = retained + 8 * c.bigint;
  const scope = new MemoryScope(new MemoryBudget(peak));
  expect(convertTemporalValues([value], decoder, scope)).toEqual([
    timestampFromEpoch(9089380393200000000000n, "nanos"),
  ]);
  expect(scope.budget.usedBytes).toBe(retained);
  scope.dispose();
  parse.mockClear();
  const denied = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => convertTemporalValues([value], decoder, denied)).toThrow(/budget/i);
  expect(parse).not.toHaveBeenCalled();
  expect(denied.budget.usedBytes).toBe(0);
  denied.dispose();
});
