import { parquetMetadata } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { createParquetLake } from "./index.js";

it.each([
  ["1234567890123456", "z"],
  ["12345678901234567", "z"],
  ["dictionary text 1 猫", "dictionary text 9 猫"],
  ["😀", "\ue000"],
])("never prunes real values using unsafe writer bounds: %j", async (...values) => {
  const data = [null, ...values, ...values, null];
  const buffer = parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    pageSize: 8,
    columnData: [
      {
        name: "text",
        type: "STRING",
        encoding: "PLAIN",
        data,
        columnIndex: true,
        offsetIndex: true,
      },
    ],
  });
  const metadata = parquetMetadata(buffer);
  const chunk = metadata.row_groups[0]?.columns[0];
  const stats = chunk?.meta_data?.statistics;
  expect(stats?.null_count).toBe(2n);
  const unsafe = values.some((value) => value.length > 16 || /[\uD800-\uDFFF]/.test(value));
  if (unsafe) {
    expect(stats?.min_value).toBeUndefined();
    expect(stats?.max_value).toBeUndefined();
    expect(chunk?.column_index_offset).toBeUndefined();
  } else {
    expect(stats?.min_value).toBe(values[0]);
    expect(stats?.max_value).toBe(values[1]);
  }
  expect(chunk?.offset_index_offset).toBeDefined();
  const store = memoryStore();
  await store.put("bounds.parquet", new Uint8Array(buffer));
  const pool = new MemoryBudget(8 * 1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  for (const value of values) {
    expect(
      await lake
        .path("bounds.parquet")
        .where({
          kind: "compare",
          op: "eq",
          left: { kind: "column", name: "text" },
          right: { kind: "literal", value },
        })
        .toArray(),
    ).toEqual([{ text: value }, { text: value }]);
  }
  expect(pool.usedBytes).toBe(0);
});
