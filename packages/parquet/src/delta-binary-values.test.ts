import { existsSync } from "node:fs";
import { ByteWriter } from "hyparquet-writer/src/bytewriter.js";
import {
  deltaBinaryPack,
  deltaByteArray,
  deltaLengthByteArray,
} from "hyparquet-writer/src/delta.js";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { readPackedDeltaBinaryValues } from "./delta-binary-values.js";

it.each([false, true])("decodes admitted packed delta bytes, prefixes=%s", (prefixes) => {
  for (const text of [[], [""], ["prefix-é", "prefix-😀", "", "tail", "tail"]]) {
    const values = text.map((value) => new TextEncoder().encode(value));
    const writer = new ByteWriter();
    (prefixes ? deltaByteArray : deltaLengthByteArray)(writer, values);
    const bytes = writer.getBuffer();
    const reader = { view: new DataView(bytes), offset: 0 };
    const scope = new MemoryScope(new MemoryBudget(65536));
    const packed = readPackedDeltaBinaryValues(reader, values.length, prefixes, scope);
    for (let i = 0; i < values.length; i++) expect(packed.value(i)).toEqual(values[i]);
    expect(reader.offset).toBe(bytes.byteLength);
    const peak = scope.budget.peakBytes;
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
    if (peak > 0) {
      const tight = new MemoryScope(new MemoryBudget(peak - 1));
      const retry = { view: new DataView(bytes), offset: 0 };
      expect(() => readPackedDeltaBinaryValues(retry, values.length, prefixes, tight)).toThrow(
        /budget|admitted/,
      );
      expect(tight.budget.usedBytes).toBe(0);
      expect(retry.offset).toBe(0);
      tight.dispose();
    }
  }
});
it("rejects negative lengths and prefixes beyond the preceding value transactionally", () => {
  for (const prefixes of [false, true]) {
    const writer = new ByteWriter();
    if (prefixes) deltaBinaryPack(writer, [1]);
    deltaBinaryPack(writer, [prefixes ? 0 : -1]);
    const scope = new MemoryScope(new MemoryBudget(65536));
    const reader = { view: new DataView(writer.getBuffer()), offset: 0 };
    expect(() => readPackedDeltaBinaryValues(reader, 1, prefixes, scope)).toThrow(/extent/);
    expect(scope.budget.usedBytes).toBe(0);
    expect(reader.offset).toBe(0);
    scope.dispose();
  }
});

it.skipIf(
  !["delta_byte_array.parquet", "delta_length_byte_array.parquet"].every((name) =>
    existsSync(new URL(`../../../fixtures/external/parquet-testing/data/${name}`, import.meta.url)),
  ),
)("matches the upstream reader on real external delta Parquet fixtures", async () => {
  const { readFileSync } = await import("node:fs");
  const { memoryStore } = await import("lakeql-core");
  const { createParquetLake, readParquetObjects } = await import("./index.js");
  for (const name of ["delta_byte_array.parquet", "delta_length_byte_array.parquet"]) {
    const bytes = readFileSync(
      new URL(`../../../fixtures/external/parquet-testing/data/${name}`, import.meta.url),
    );
    const store = memoryStore();
    await store.put(name, bytes);
    const expected = await readParquetObjects(store, name);
    const pool = new MemoryBudget(64 * 1024 * 1024);
    const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
    const owner = await lake.path(name).batchSize(17).run().toOwnedArray();
    try {
      expect(owner.value).toEqual(expected);
    } finally {
      owner.release();
    }
    expect(pool.usedBytes).toBe(0);
  }
});

it.each([
  false,
  true,
])("rejects every truncated delta stream without retaining allocations, prefixes=%s", (prefixes) => {
  const writer = new ByteWriter();
  const values = ["prefix-one", "prefix-two", "tail"].map((value) =>
    new TextEncoder().encode(value),
  );
  (prefixes ? deltaByteArray : deltaLengthByteArray)(writer, values);
  const encoded = writer.getBuffer();
  for (let length = 0; length < encoded.byteLength; length++) {
    const scope = new MemoryScope(new MemoryBudget(65536));
    const reader = { view: new DataView(encoded, 0, length), offset: 0 };
    expect(() => readPackedDeltaBinaryValues(reader, values.length, prefixes, scope)).toThrow();
    expect(reader.offset).toBe(0);
    expect(scope.budget.usedBytes).toBe(0);
    scope.dispose();
  }
});

it("rejects advertised expansion before constructing an oversized payload", () => {
  const writer = new ByteWriter();
  deltaBinaryPack(writer, [0, 0x7fffffff]);
  deltaBinaryPack(writer, [0, 0]);
  const scope = new MemoryScope(new MemoryBudget(1024));
  const reader = { view: new DataView(writer.getBuffer()), offset: 0 };
  expect(() => readPackedDeltaBinaryValues(reader, 2, true, scope)).toThrow(/extent/);
  expect(scope.budget.peakBytes).toBe(16);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("admits compressed prefix expansion before allocating its packed output", () => {
  const count = 1024;
  const writer = new ByteWriter();
  deltaBinaryPack(
    writer,
    Array.from({ length: count }, (_, i) => (i === 0 ? 0 : 1024)),
  );
  deltaBinaryPack(
    writer,
    Array.from({ length: count }, (_, i) => (i === 0 ? 1024 : 0)),
  );
  writer.appendBytes(new Uint8Array(1024));
  const encoded = writer.getBuffer();
  expect(encoded.byteLength).toBeLessThan(2048);
  const scope = new MemoryScope(new MemoryBudget(32768));
  const reader = { view: new DataView(encoded), offset: 0 };
  expect(() => readPackedDeltaBinaryValues(reader, count, true, scope)).toThrow(/budget/);
  expect(scope.budget.peakBytes).toBe(count * 16);
  expect(scope.budget.usedBytes).toBe(0);
  expect(reader.offset).toBe(0);
  scope.dispose();
});
