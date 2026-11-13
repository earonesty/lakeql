import { existsSync, readFileSync } from "node:fs";
import { MemoryBudget, materializeBatchRows, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { createParquetLake, readParquetColumnBatches, readParquetObjects } from "./index.js";

const numbers = [[0, 1, 2, 3], [], [4], [5, 6, 7, 8]];
const strings = [
  ["foo", "zero", "one", "two"],
  ["three"],
  ["four"],
  ["five", "six", "seven", "eight"],
];
const cases = [
  {
    name: "map_no_value.parquet",
    // Fixture generator writes three entries per row, no value column.
    expected: [
      [1, 2, 3],
      [4, 5, 6],
      [7, 8, 9],
    ].map((keys) => ({
      my_map: Object.fromEntries(keys.map((key) => [key, null])),
      my_map_no_v: Object.fromEntries(keys.map((key) => [key, null])),
      my_list: keys,
    })),
  },
  {
    name: "repeated_primitive_no_list.parquet",
    // Independent values from the decoded-page/DuckDB fixture oracle.
    expected: numbers.map((values, index) => ({
      Int32_list: values,
      String_list: strings[index],
      group_of_lists: { Int32_list_in_group: values, String_list_in_group: strings[index] },
    })),
  },
];
for (const { name, expected } of cases) {
  const fixture = new URL(
    `../../../fixtures/external/parquet-testing/data/${name}`,
    import.meta.url,
  );
  it.skipIf(!existsSync(fixture)).each([1, 2, 8])(
    `${name} preserves collection shape with batch size %i`,
    async (batchSize) => {
      const store = memoryStore();
      await store.put(name, readFileSync(fixture));
      expect(await readParquetObjects(store, name, { batchSize })).toEqual(expected);
      const rows = [];
      for await (const batch of readParquetColumnBatches(store, name, { batchSize }))
        rows.push(...materializeBatchRows(batch.batch));
      expect(rows).toEqual(expected);
      const pool = new MemoryBudget(64 * 1024 * 1024);
      const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
      const output = await lake.path(name).batchSize(batchSize).run().toOwnedArray();
      try {
        expect(output.value).toEqual(expected);
      } finally {
        output.release();
      }
      expect(pool.usedBytes).toBe(0);
      // Projection must use the same assembly contract.
      const column = Object.keys(expected[0] ?? {})[0];
      if (!column) throw Error("Missing fixture column");
      expect(await readParquetObjects(store, name, { batchSize, columns: [column] })).toEqual(
        expected.map((row) => ({ [column]: (row as Record<string, unknown>)[column] })),
      );
    },
  );
}

it("recognizes key-only maps without accepting missing or repeated keys", async () => {
  const { getSchemaPath, isMapLike } = await import("hyparquet/src/schema.js");
  const key = { name: "key", type: "INT32", repetition_type: "REQUIRED" } as const;
  const value = { name: "value", type: "INT32", repetition_type: "OPTIONAL" } as const;
  const check = (fields: import("hyparquet").SchemaElement[]) => {
    const tree = getSchemaPath(
      [
        { name: "root", num_children: 1 },
        { name: "m", converted_type: "MAP", repetition_type: "OPTIONAL", num_children: 1 },
        { name: "key_value", repetition_type: "REPEATED", num_children: fields.length },
        ...fields,
      ],
      ["m"],
    ).at(-1);
    return isMapLike(tree);
  };
  expect(check([key])).toBe(true);
  expect(check([value, key])).toBe(true);
  expect(check([])).toBe(false);
  expect(check([value])).toBe(false);
  expect(check([key, { ...value, name: "other" }])).toBe(false);
  expect(check([{ ...key, repetition_type: "REPEATED" }])).toBe(false);
});
