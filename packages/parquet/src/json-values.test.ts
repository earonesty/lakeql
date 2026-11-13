import { MATERIALIZATION_CHARGES as c, MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { jsonAllocationBytes } from "./json-allocation.js";
import { convertJsonValues } from "./json-values.js";

const encode = (value: string) => new TextEncoder().encode(value);

it("admits JSON trees and releases decoded text while retaining parsed values", () => {
  const texts = [
    '{"__proto__":{"x":1},"unicode":"雪😀","nested":[null,true,1,"\\u0041"]}',
    "[]",
    "null",
  ];
  const values = texts.map(encode);
  const retained =
    c.array +
    texts.length * c.arraySlot +
    texts.reduce((sum, text) => sum + jsonAllocationBytes(text), 0) +
    2 * c.object +
    4 * c.property;
  const scope = new MemoryScope(new MemoryBudget(65536));
  const output = convertJsonValues(values, scope);
  expect(output).toEqual([
    new Map<string, unknown>([
      ["__proto__", new Map([["x", 1]])],
      ["unicode", "雪😀"],
      ["nested", [null, true, 1, "A"]],
    ]),
    [],
    null,
  ]);
  expect(convertJsonValues(values)).toEqual(output);
  expect(scope.budget.usedBytes).toBe(retained);
  expect(scope.budget.peakBytes).toBeGreaterThan(retained);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(convertJsonValues(values, exact)).toEqual(output);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => convertJsonValues(values, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it.each([
  '{"bad":',
  "",
  "[1,]",
  "undefined",
])("rolls back earlier parsed trees on invalid JSON %s", (invalid) => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(() => convertJsonValues([encode('{"valid":[1,2,3]}'), encode(invalid)], scope)).toThrow();
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("reads the real JSON Parquet fixture with owned results", async ({ skip }) => {
  const { existsSync, readFileSync } = await import("node:fs");
  const fixture = new URL(
    "../../../fixtures/external/parquet-testing/data/json.parquet",
    import.meta.url,
  );
  if (!existsSync(fixture)) return skip();
  const { memoryStore } = await import("lakeql-core");
  const { createParquetLake, readParquetObjects } = await import("./index.js");
  const store = memoryStore();
  await store.put("json.parquet", readFileSync(fixture));
  const expected = await readParquetObjects(store, "json.parquet");
  const budget = new MemoryBudget(64 * 1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: budget, scanRangeCache: { maxBytes: 0 } });
  const mixedOwner = await lake.path("json.parquet").batchSize(3).run().toOwnedArray();
  try {
    expect(mixedOwner.value).toEqual(expected);
  } finally {
    mixedOwner.release();
  }
  expect(budget.usedBytes).toBe(0);
  const owner = await lake.path("json.parquet").batchSize(1).run().toOwnedArray();
  try {
    expect(owner.value).toEqual(expected);
    expect(owner.value.length).toBeGreaterThan(0);
  } finally {
    owner.release();
  }
  expect(budget.usedBytes).toBe(0);
});

it("constructs compact union children from the actual mixed JSON fixture", async ({ skip }) => {
  const { existsSync, readFileSync } = await import("node:fs");
  const fixture = new URL(
    "../../../fixtures/external/parquet-testing/data/json.parquet",
    import.meta.url,
  );
  if (!existsSync(fixture)) return skip();
  const { memoryStore } = await import("lakeql-core");
  const { readParquetObjects } = await import("./index.js");
  const { buildUnionChildren } = await import("../../core/src/union-construction.js");
  const { vectorValue } = await import("../../core/src/batch.js");
  const store = memoryStore();
  await store.put("json.parquet", readFileSync(fixture));
  const rows = await readParquetObjects(store, "json.parquet");
  const values = rows.map((row) => row.json_field);
  const scope = new MemoryScope(new MemoryBudget(64 * 1024 * 1024));
  const result = buildUnionChildren(
    { length: values.length, get: (row) => values[row] },
    3,
    (row) => (values[row] == null ? 0 : Array.isArray(values[row]) ? 1 : 2),
    scope,
  );
  const output = Array.from(result.routing.tags, (tag, row) => {
    const child = result.children[tag];
    if (child === undefined) throw Error("Missing child");
    return vectorValue(child, result.routing.offsets[row] ?? 0);
  });
  // Existing struct vectors normalize absent fields to SQL null.
  expect(output).toEqual([{ a: 1, b: null }, { a: 1, b: null }, [1, null, 3], null]);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it.each([1, 3, 32])("preserves dynamic JSON members across batches of %i", async (batchSize) => {
  const { parquetWriteBuffer } = await import("hyparquet-writer");
  const { memoryStore, materializeBatchRows } = await import("lakeql-core");
  const { createParquetLake, readParquetObjects, readParquetColumnBatches } = await import(
    "./index.js"
  );
  const values = [
    { a: 1 },
    { a: 1, b: null },
    {},
    { nested: [{ x: 1 }, {}, { y: null }], nullable: null },
    JSON.parse('{"constructor":[],"雪":{}}'),
    [[], {}, null, { list: [{ a: 1 }, { b: 2 }] }],
    null,
  ];
  const store = memoryStore();
  await store.put(
    "dynamic.parquet",
    new Uint8Array(
      parquetWriteBuffer({
        columnData: [{ name: "value", type: "JSON", data: values }],
        rowGroupSize: [3, 4],
      }),
    ),
  );
  const expected = values.map((value) => ({ value }));
  expect(await readParquetObjects(store, "dynamic.parquet", { batchSize })).toEqual(expected);
  const columns = [];
  for await (const batch of readParquetColumnBatches(store, "dynamic.parquet", { batchSize }))
    columns.push(...materializeBatchRows(batch.batch));
  expect(columns).toEqual(expected);
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const output = await lake.path("dynamic.parquet").batchSize(batchSize).run().toOwnedArray();
  try {
    expect(output.value).toEqual(expected);
  } finally {
    output.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it("keeps prototype-named members as own data properties in public row normalization", async () => {
  const { normalizeDecodedRows } = await import("./decoded-rows.js");
  const source = JSON.parse('{"value":{"__proto__":{"safe":true},"constructor":null}}');
  const rows = normalizeDecodedRows([source]);
  expect(rows).toEqual([source]);
  expect(Object.getPrototypeOf(rows[0]?.value)).toBe(Object.prototype);
  expect(Object.hasOwn(rows[0]?.value as object, "__proto__")).toBe(true);
});
