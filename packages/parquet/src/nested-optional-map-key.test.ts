import { readFileSync } from "node:fs";
import type { SchemaElement } from "hyparquet";
import { getSchemaPath } from "hyparquet/src/schema.js";
import { MemoryBudget, MemoryScope, memoryStore, vectorFromValues, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { createParquetLake } from "./index.js";
import { executeNestedSchema } from "./nested-schema-execute.js";
import { compileNestedSchema } from "./nested-schema-plan.js";

it("reads the legacy optional-key MAP fixture through the owned query path", async () => {
  const store = memoryStore();
  await store.put(
    "map.parquet",
    readFileSync(
      new URL(
        "../../../fixtures/external/parquet-testing/data/incorrect_map_schema.parquet",
        import.meta.url,
      ),
    ),
  );
  const pool = new MemoryBudget(128 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const owner = await lake.path("map.parquet").batchSize(1).run().toOwnedArray();
  try {
    expect(owner.value).toEqual([{ my_map: { name: "report", parent: "another" } }]);
  } finally {
    owner.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([false, true])("validates actual optional map key presence (null=%s)", (nullKey) => {
  const schema: SchemaElement[] = [
    { name: "root", num_children: 1 },
    { name: "m", converted_type: "MAP", repetition_type: "OPTIONAL", num_children: 1 },
    { name: "key_value", repetition_type: "REPEATED", num_children: 2 },
    { name: "key", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
    { name: "value", type: "INT32", repetition_type: "OPTIONAL" },
  ];
  const paths = ["key", "value"].map((name) => getSchemaPath(schema, ["m", "key_value", name]));
  const map = paths[0]?.[1];
  if (!map) throw Error("Missing map");
  const scope = new MemoryScope(new MemoryBudget(65536));
  try {
    const plan = compileNestedSchema(map, paths, scope);
    const inputs = paths.map((schemaPath, i) => ({
      schemaPath,
      values: vectorFromValues(i === 0 ? (nullKey ? [] : ["key"]) : [1], scope),
      levels: {
        repetition: Uint8Array.of(0),
        definition: Uint8Array.of(i === 0 && nullKey ? 2 : 3),
        rowsStarted: 1,
        presentCount: i === 0 && nullKey ? 0 : 1,
        valueOffset: 0,
      },
    }));
    const baseline = scope.budget.usedBytes;
    if (nullKey) {
      expect(() => executeNestedSchema(plan, inputs, scope)).toThrow(/Null Parquet map key/);
      expect(scope.budget.usedBytes).toBe(baseline);
    } else expect(vectorValue(executeNestedSchema(plan, inputs, scope), 0)).toEqual({ key: 1 });
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});
