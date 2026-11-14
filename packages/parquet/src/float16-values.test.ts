import { parseFloat16 } from "hyparquet/src/convert.js";
import { MATERIALIZATION_CHARGES as c, MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { convertFloat16Values } from "./float16-values.js";

it("preserves every half-float bit pattern with one admitted output array", () => {
  const values = Array.from({ length: 65536 }, (_, bits) => Uint8Array.of(bits & 255, bits >> 8));
  const bytes = c.array + values.length * (c.arraySlot + c.number);
  const scope = new MemoryScope(new MemoryBudget(bytes));
  const output = convertFloat16Values(values, scope);
  for (let bits = 0; bits < values.length; bits++)
    expect(Object.is(output[bits], parseFloat16(values[bits]))).toBe(true);
  expect(Object.is(output[0x8000], -0)).toBe(true);
  expect(output[0x7c00]).toBe(Infinity);
  expect(output[0xfc00]).toBe(-Infinity);
  expect(Number.isNaN(output[0x7e00])).toBe(true);
  expect(scope.budget.usedBytes).toBe(bytes);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => convertFloat16Values(values, tight)).toThrow(/budget/);
  expect(tight.budget.peakBytes).toBe(0);
  tight.dispose();
});

it("preserves absent values and rejects malformed physical widths before allocation", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(convertFloat16Values([undefined, null, Uint8Array.of(0, 0)], scope)).toEqual([
    undefined,
    undefined,
    0,
  ]);
  scope.dispose();
  for (const value of [Uint8Array.of(0), new Uint8Array(3), 1, "x"]) {
    const rejected = new MemoryScope(new MemoryBudget(65536));
    expect(() => convertFloat16Values([value], rejected)).toThrow(/FLOAT16/);
    expect(rejected.budget.peakBytes).toBe(0);
    rejected.dispose();
  }
});

for (const name of ["float16_zeros_and_nans.parquet", "float16_nonzeros_and_nans.parquet"]) {
  it(`preserves owned values and releases allocations for real fixture ${name}`, async ({
    skip,
  }) => {
    const { existsSync, readFileSync } = await import("node:fs");
    const fixture = new URL(
      `../../../fixtures/external/parquet-testing/data/${name}`,
      import.meta.url,
    );
    if (!existsSync(fixture)) return skip();
    const { memoryStore } = await import("lakeql-core");
    const { createParquetLake, readParquetObjects } = await import("./index.js");
    const store = memoryStore();
    await store.put(name, readFileSync(fixture));
    const expected = await readParquetObjects(store, name);
    const budget = new MemoryBudget(64 * 1024 * 1024);
    const lake = createParquetLake({ store, memoryPool: budget, scanRangeCache: { maxBytes: 0 } });
    const owner = await lake.path(name).batchSize(3).run().toOwnedArray();
    try {
      expect(owner.value).toEqual(expected);
      expect(owner.value.length).toBeGreaterThan(0);
    } finally {
      owner.release();
    }
    expect(budget.usedBytes).toBe(0);
  });
}
