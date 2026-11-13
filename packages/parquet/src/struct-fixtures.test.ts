import { existsSync, readFileSync } from "node:fs";
import { parquetMetadata } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, materializeBatchRows, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { readOwnedParquetColumnBatches } from "./column-batches.js";
import { createParquetLake, readParquetObjects } from "./index.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it.each([
  1, 2, 5,
])("decodes nullable struct fixtures through owned fallback batches of %i", async (batchSize) => {
  const values = [
    null,
    { score: null, detail: { name: "α" } },
    { score: 7, detail: null },
    { score: -2, detail: { name: null } },
    { score: 0, detail: { name: "" } },
  ];
  const bytes = parquetWriteBuffer({
    schema: [
      { name: "root", num_children: 1 },
      { name: "record", num_children: 2, repetition_type: "OPTIONAL" },
      { name: "score", type: "INT32", repetition_type: "OPTIONAL" },
      { name: "detail", num_children: 1, repetition_type: "OPTIONAL" },
      { name: "name", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
    ],
    rowGroupSize: [2, 3],
    columnData: [{ name: "record", data: values }],
  });
  const store = memoryStore();
  await store.put("struct.parquet", new Uint8Array(bytes));
  const file = await asyncBufferFromStore(store, "struct.parquet");
  const budget = new MemoryBudget(65536);
  const rows: unknown[] = [];
  for await (const owner of readOwnedParquetColumnBatches(file, parquetMetadata(bytes), budget, {
    batchSize,
  })) {
    try {
      rows.push(...materializeBatchRows(owner.value.batch));
    } finally {
      owner.release();
    }
  }
  expect(rows).toEqual(values.map((record) => ({ record })));
  expect(budget.usedBytes).toBe(0);
  expect(budget.peakBytes).toBeGreaterThan(0);
  expect(await readParquetObjects(store, "struct.parquet")).toEqual(
    values.map((record) => ({ record })),
  );
  const lake = createParquetLake({ store, memoryPool: budget, scanRangeCache: { maxBytes: 0 } });
  expect(await lake.path("struct.parquet").batchSize(batchSize).run().toArray()).toEqual(
    values.map((record) => ({ record })),
  );
  expect(budget.usedBytes).toBe(0);
});

const externalStruct = new URL(
  "../../../fixtures/external/parquet-testing/data/nested_structs.rust.parquet",
  import.meta.url,
);
it.skipIf(!existsSync(externalStruct))(
  "preserves external nested struct values and integer precision",
  async () => {
    const store = memoryStore();
    await store.put("external.parquet", readFileSync(externalStruct));
    // Independently checked against DuckDB; the sum exceeds Number's safe range.
    const expected = [
      {
        roll_num: {
          min: 190406409000602n,
          max: 190407175004000n,
          mean: 190406671229999n,
          count: 495n,
          sum: 94251302258849568n,
          variance: 0n,
        },
        BIA_3: { min: 0, max: 0, mean: 0, count: 0n, sum: 0, variance: 0 },
      },
    ];
    const columns = ["roll_num", "BIA_3"];
    expect(await readParquetObjects(store, "external.parquet", { columns })).toEqual(expected);
    const lake = createParquetLake({ store });
    expect(
      await lake.path("external.parquet").select(columns).batchSize(1).run().toArray(),
    ).toEqual(expected);
  },
);

it("rejects incompatible nested schemas during multi-file planning", async () => {
  const { ParquetScanAdapter } = await import("./scan-adapter.js");
  const store = memoryStore();
  for (const [path, type] of [
    ["a.parquet", "INT32"],
    ["b.parquet", "DOUBLE"],
  ] as const) {
    const bytes = parquetWriteBuffer({
      schema: [
        { name: "root", num_children: 1 },
        { name: "record", num_children: 1, repetition_type: "OPTIONAL" },
        { name: "value", type, repetition_type: "OPTIONAL" },
      ],
      columnData: [{ name: "record", data: [{ value: 1 }] }],
    });
    await store.put(path, new Uint8Array(bytes));
  }
  const objects = [];
  for await (const object of store.list("")) objects.push(object);
  const budget = new MemoryBudget(65536);
  await expect(
    new ParquetScanAdapter(store).planObjects(objects, { memoryBudget: budget }),
  ).rejects.toMatchObject({ code: "LAKEQL_SCHEMA_CONFLICT", details: { column: "record" } });
  expect(budget.usedBytes).toBe(0);
});
