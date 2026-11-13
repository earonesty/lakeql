import { memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { createParquetLake, writeParquet } from "./index.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it.each([
  "RLE_DICTIONARY",
  "PLAIN_DICTIONARY",
] as const)("writes valid fallback pages when %s is requested for unique raw bytes", async (encoding) => {
  const store = memoryStore();
  const values = [Uint8Array.of(255, 0), null, new Uint8Array()];
  await writeParquet(store, "fallback.parquet", {
    codec: "UNCOMPRESSED",
    schema: [
      { name: "root", num_children: 1 },
      { name: "raw", type: "BYTE_ARRAY", repetition_type: "OPTIONAL" },
    ],
    columnData: [{ name: "raw", encoding, data: values }],
  });
  const metadata = await readParquetMetadataFromFile(
    await asyncBufferFromStore(store, "fallback.parquet"),
  );
  const column = metadata.row_groups[0]?.columns[0]?.meta_data;
  expect(column?.dictionary_page_offset).toBeUndefined();
  expect(column?.encodings).not.toContain(encoding);
  expect(column?.encodings).toContain("PLAIN");
  expect(await createParquetLake({ store }).path("fallback.parquet").toArray()).toEqual(
    values.map((raw) => ({ raw })),
  );
});

it.each([
  false,
  true,
])("preserves adaptive dictionary selection (repeated=%s)", async (repeated) => {
  const store = memoryStore();
  const values = repeated ? ["é", "猫", "é", "猫", "é", "猫"] : ["é", "猫", "", "tail"];
  await writeParquet(store, "text.parquet", {
    columnData: [{ name: "text", type: "STRING", encoding: "RLE_DICTIONARY", data: values }],
  });
  const metadata = await readParquetMetadataFromFile(
    await asyncBufferFromStore(store, "text.parquet"),
  );
  const column = metadata.row_groups[0]?.columns[0]?.meta_data;
  expect(column?.dictionary_page_offset !== undefined).toBe(repeated);
  expect(column?.encodings.includes("RLE_DICTIONARY")).toBe(repeated);
  expect(await createParquetLake({ store }).path("text.parquet").toArray()).toEqual(
    values.map((text) => ({ text })),
  );
});

it("uses valid plain fallback when the dictionary exceeds the page budget", async () => {
  const store = memoryStore();
  const values = Array.from({ length: 20 }, (_, index) =>
    (index % 2 === 0 ? "a" : "b").repeat(128),
  );
  await writeParquet(store, "page-budget.parquet", {
    pageSize: 32,
    columnData: [{ name: "text", type: "STRING", encoding: "RLE_DICTIONARY", data: values }],
  });
  const metadata = await readParquetMetadataFromFile(
    await asyncBufferFromStore(store, "page-budget.parquet"),
  );
  const column = metadata.row_groups[0]?.columns[0]?.meta_data;
  expect(column?.dictionary_page_offset).toBeUndefined();
  expect(column?.encodings).not.toContain("RLE_DICTIONARY");
  expect(await createParquetLake({ store }).path("page-budget.parquet").toArray()).toEqual(
    values.map((text) => ({ text })),
  );
});
