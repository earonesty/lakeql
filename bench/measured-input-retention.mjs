import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createNumericPressureFixture } from "../fixtures/src/numeric-pressure.ts";
import { MemoryBudget, MemoryScope, memoryStore } from "../packages/core/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { planNumericPageGroup } from "../packages/parquet/dist/numeric-page-group.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/store-buffer.js";
import { preparePageWindow } from "../packages/parquet/dist/vector-batches.js";
import { DEFAULT_PARSERS } from "../packages/parquet/node_modules/hyparquet/src/convert.js";
import { getSchemaPath } from "../packages/parquet/node_modules/hyparquet/src/schema.js";

assert.equal(typeof global.gc, "function", "Run with node --expose-gc");
const held = [];
async function prepare() {
  const store = memoryStore();
  await store.put("input.parquet", createNumericPressureFixture());
  const file = await asyncBufferFromStore(store, "input.parquet");
  const metadata = await readParquetMetadataFromFile(file);
  const entries = [];
  for (const chunk of metadata.row_groups[0].columns) {
    const column = chunk.meta_data;
    const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
    const start = Number(column.data_page_offset);
    const page = await preparePageWindow(
      file,
      start,
      start + Number(column.total_compressed_size),
      0,
    );
    entries.push({
      page,
      decoder: {
        ...column,
        pathInSchema: column.path_in_schema,
        schemaPath,
        element: schemaPath.at(-1).element,
        parsers: DEFAULT_PARSERS,
      },
    });
  }
  const weak = [];
  const source = {
    ...file,
    async sliceOwned(...args) {
      const output = await file.sliceOwned(...args);
      weak.push(new WeakRef(output.value));
      return output;
    },
  };
  const plan = planNumericPageGroup(entries, source);
  assert.ok(plan);
  const budget = new MemoryBudget(plan.bytes);
  const scope = new MemoryScope(budget);
  const measured = await plan.measureOwned(source, scope);
  measured.dispose();
  measured.dispose();
  scope.dispose();
  assert.equal(budget.usedBytes, 0);
  held.push(measured);
  return weak;
}
const weak = await prepare();
for (let attempt = 0; attempt < 10; attempt++) {
  await new Promise(setImmediate);
  global.gc();
  global.gc();
  if (weak.every((item) => item.deref() === undefined)) break;
}
const collected = weak.map((item) => item.deref() === undefined);
assert.equal(held.length, 1);
console.log(
  JSON.stringify(
    {
      node: process.version,
      sourceSha256: createHash("sha256")
        .update(
          await readFile(new URL("../packages/parquet/src/numeric-page-group.ts", import.meta.url)),
        )
        .digest("hex"),
      heldDisposedPlans: held.length,
      inputBuffers: weak.length,
      collected,
      finalAdmittedBytes: 0,
      limitation:
        "Forced-GC Node reachability diagnostic; not a production GC requirement or whole-runtime cap.",
    },
    null,
    2,
  ),
);
if (process.env.LAKEQL_EXPECT_MEASURED_COLLECTION === "1") assert.ok(collected.every(Boolean));
