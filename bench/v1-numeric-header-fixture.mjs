import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { zstdCompressSync } from "node:zlib";
import {
  batchFromVectors,
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
} from "../packages/core/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import {
  planFlatNumericVectorHeader,
  preparePageWindow,
  readParquetVectorBatchesFromFile,
} from "../packages/parquet/dist/vector-batches.js";
import { DEFAULT_PARSERS } from "../packages/parquet/node_modules/hyparquet/src/convert.js";
import { getSchemaPath } from "../packages/parquet/node_modules/hyparquet/src/schema.js";
import { snappyCompress } from "../packages/parquet/node_modules/hyparquet-writer/src/snappy.js";

for (const name of ["nullable.impala.parquet", "nonnullable.impala.parquet"]) {
  const bytes = await readFile(
    new URL(`../fixtures/external/parquet-testing/data/${name}`, import.meta.url),
  );
  const file = {
    byteLength: bytes.length,
    async slice(start, end) {
      return bytes.buffer.slice(bytes.byteOffset + start, bytes.byteOffset + end);
    },
  };
  const metadata = await readParquetMetadataFromFile(file);
  const column = metadata.row_groups[0].columns.find(
    (x) => x.meta_data.path_in_schema.join(".").toLowerCase() === "id",
  ).meta_data;
  const columnName = column.path_in_schema.join(".");
  assert.equal(column.codec, "UNCOMPRESSED");
  const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
  const decoder = {
    ...column,
    pathInSchema: column.path_in_schema,
    schemaPath,
    element: schemaPath.at(-1).element,
    parsers: DEFAULT_PARSERS,
  };
  const start = Number(column.data_page_offset);
  const page = await preparePageWindow(
    file,
    start,
    start + Number(column.total_compressed_size),
    0,
  );
  assert.equal(page.header.type, "DATA_PAGE");
  const original = new Uint8Array(await file.slice(page.bodyOffset, page.bodyEnd));
  const oracle = [];
  for await (const batch of readParquetVectorBatchesFromFile(file, metadata, {
    columns: [columnName],
  }))
    oracle.push(...materializeBatchRows(batch.batch).map((row) => ({ id: row[columnName] })));
  for (const codec of ["UNCOMPRESSED", "SNAPPY", "ZSTD"]) {
    const input =
      codec === "SNAPPY"
        ? snappyCompress(original)
        : codec === "ZSTD"
          ? new Uint8Array(zstdCompressSync(original))
          : original;
    const header = { ...page.header, compressed_page_size: input.length };
    const plan = planFlatNumericVectorHeader(header, { ...decoder, codec });
    assert.ok(plan);
    const budget = new MemoryBudget(plan.bytes + input.byteLength);
    const transport = new MemoryScope(budget);
    const ownedInput = transport.allocate(input.byteLength, "fixture body", () =>
      input.slice(),
    ).value;
    const scope = new MemoryScope(budget);
    scope.admit(plan.bytes, "external V1 vector");
    const vector = plan.execute(ownedInput, scope);
    if (plan.mayBorrowInput) scope.adopt(transport.retainAllocations(vector));
    transport.dispose();
    const owner = scope.retainAllocations(vector);
    scope.dispose();
    assert.deepEqual(
      materializeBatchRows(batchFromVectors({ id: owner.value })),
      oracle.slice(0, header.data_page_header.num_values),
    );
    assert.ok(owner.bytes > 0);
    const retainedBytes = owner.bytes;
    owner.release();
    assert.equal(budget.usedBytes, 0);
    console.log(
      JSON.stringify({
        fixture: name,
        codec,
        rows: header.data_page_header.num_values,
        admittedBytes: plan.bytes,
        inputBytes: input.byteLength,
        peakBytes: budget.peakBytes,
        retainedBytes,
        scope:
          codec === "UNCOMPRESSED"
            ? "Actual external Impala V1 page"
            : "External Impala V1 page body recompressed locally; not a separately produced compressed Parquet file",
      }),
    );
  }
}
