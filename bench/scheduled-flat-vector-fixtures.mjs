import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { zstdCompressSync } from "node:zlib";
import { DuckDBConnection } from "@duckdb/node-api";
import {
  batchFromVectors,
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
} from "../packages/core/dist/index.js";
import { MemoryStageScheduler } from "../packages/core/dist/memory-stage-scheduler.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { planFlatNumericVector } from "../packages/parquet/dist/vector-batches.js";
import { Encodings, PageTypes } from "../packages/parquet/node_modules/hyparquet/src/constants.js";
import { DEFAULT_PARSERS } from "../packages/parquet/node_modules/hyparquet/src/convert.js";
import { getSchemaPath } from "../packages/parquet/node_modules/hyparquet/src/schema.js";
import { deserializeTCompactProtocol } from "../packages/parquet/node_modules/hyparquet/src/thrift.js";

const require = createRequire(new URL("../packages/parquet/package.json", import.meta.url));
const { parquetWriteBuffer } = await import(pathToFileURL(require.resolve("hyparquet-writer")));
const directory = await mkdtemp(join(tmpdir(), "lakeql-flat-pages-"));
const connection = await DuckDBConnection.create();
try {
  for (const codec of ["UNCOMPRESSED", "SNAPPY", "ZSTD"])
    for (const type of ["INT32", "FLOAT", "DOUBLE", "DECIMAL"])
      for (const encoding of ["PLAIN", "BYTE_STREAM_SPLIT"])
        for (const nullable of [false, true]) {
          if ((type === "INT32" || type === "DECIMAL") && encoding === "BYTE_STREAM_SPLIT")
            continue;
          const data = Array.from({ length: 1000 }, (_, i) =>
            nullable && i % 3 === 0 ? null : type === "DOUBLE" || type === "DECIMAL" ? i + 0.25 : i,
          );
          const file = parquetWriteBuffer({
            codec,
            compressors: { ZSTD: zstdCompressSync },
            ...(type === "DECIMAL"
              ? {
                  schema: [
                    { name: "schema", num_children: 1 },
                    {
                      name: "x",
                      type: "INT64",
                      converted_type: "DECIMAL",
                      precision: 15,
                      scale: 2,
                      repetition_type: nullable ? "OPTIONAL" : "REQUIRED",
                    },
                  ],
                }
              : {}),
            columnData: [{ name: "x", ...(type === "DECIMAL" ? {} : { type }), encoding, data }],
            pageSize: 1 << 20,
          });
          const path = join(directory, "fixture.parquet");
          await writeFile(path, new Uint8Array(file));
          const reference = (
            await connection.runAndReadAll(
              `SELECT ${type === "DECIMAL" ? "CAST(x AS DOUBLE)" : "x"} AS x FROM read_parquet('${path}')`,
            )
          )
            .getRowObjects()
            .map((row) => row.x);
          const metadata = await readParquetMetadataFromFile({
            byteLength: file.byteLength,
            async slice(a, b) {
              return file.slice(a, b);
            },
          });
          const column = metadata.row_groups[0].columns[0].meta_data;
          const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
          const decoder = {
            ...column,
            pathInSchema: column.path_in_schema,
            schemaPath,
            element: schemaPath.at(-1).element,
            parsers: DEFAULT_PARSERS,
          };
          const reader = { view: new DataView(file), offset: Number(column.data_page_offset) };
          const raw = deserializeTCompactProtocol(reader);
          const header = {
            type: PageTypes[raw.field_1],
            uncompressed_page_size: raw.field_2,
            compressed_page_size: raw.field_3,
            ...(raw.field_5
              ? {
                  data_page_header: {
                    num_values: raw.field_5.field_1,
                    encoding: Encodings[raw.field_5.field_2],
                    definition_level_encoding: Encodings[raw.field_5.field_3],
                    repetition_level_encoding: Encodings[raw.field_5.field_4],
                  },
                }
              : {}),
            ...(raw.field_8
              ? {
                  data_page_header_v2: {
                    num_values: raw.field_8.field_1,
                    num_nulls: raw.field_8.field_2,
                    num_rows: raw.field_8.field_3,
                    encoding: Encodings[raw.field_8.field_4],
                    definition_levels_byte_length: raw.field_8.field_5,
                    repetition_levels_byte_length: raw.field_8.field_6,
                    is_compressed: raw.field_8.field_7 ?? true,
                  },
                }
              : {}),
          };
          const payload = new Uint8Array(file, reader.offset, header.compressed_page_size);
          for (const alignment of [0, 1]) {
            const sample = new Uint8Array(payload.length + alignment);
            sample.set(payload, alignment);
            const predicted = planFlatNumericVector(sample.subarray(alignment), header, decoder);
            assert.ok(predicted);
            const inputBytes = sample.length;
            const cap = inputBytes + 2 * predicted.bytes;
            const root = new MemoryBudget(cap);
            const source = new MemoryScope(root.child());
            const backing = source.allocate(inputBytes, "encoded fixture page", () =>
              sample.slice(),
            ).value;
            const input = source.retainAllocations(backing.subarray(alignment));
            source.dispose();
            const plan = planFlatNumericVector(input.value, header, decoder);
            assert.equal(plan.bytes, predicted.bytes);
            const scheduler = new MemoryStageScheduler(root);
            const run = () =>
              scheduler.run({
                budget: root.child(),
                bytes: plan.bytes,
                purpose: "flat fixture decode",
                inputs: [input],
                execute(scope, _signal, pins) {
                  const result = plan.execute(scope);
                  if (!plan.borrowsInput) for (const pin of pins) pin.release();
                  return result;
                },
              });
            const pending = [run(), run()];
            input.release();
            const outputs = await Promise.all(pending);
            for (const output of outputs) {
              const rows = materializeBatchRows(batchFromVectors({ x: output.value })).map(
                (row) => row.x,
              );
              assert.deepEqual(rows, reference);
            }
            const outputBytes =
              reference.length * (type === "DOUBLE" || type === "DECIMAL" ? 8 : 4) +
              (nullable ? reference.length : 0);
            const retainedPerJob = plan.borrowsInput ? 0 : outputBytes;
            const nonRetainedExtent = plan.bytes - retainedPerJob;
            assert.ok(root.peakBytes <= cap);
            assert.equal(root.usedBytes, 2 * retainedPerJob + (plan.borrowsInput ? inputBytes : 0));
            for (const output of outputs) output.release();
            assert.equal(root.usedBytes, 0);
            if (plan.bytes > 0) {
              const small = new MemoryBudget(inputBytes + plan.bytes - 1);
              const smallSource = new MemoryScope(small.child());
              const owned = smallSource.allocate(inputBytes, "encoded page", () =>
                sample.slice(),
              ).value;
              const pin = smallSource.retainAllocations(owned.subarray(alignment));
              smallSource.dispose();
              const smallPlan = planFlatNumericVector(pin.value, header, decoder);
              try {
                await assert.rejects(
                  new MemoryStageScheduler(small).run({
                    budget: small.child(),
                    bytes: smallPlan.bytes,
                    purpose: "below decode extent",
                    inputs: [pin],
                    execute(scope) {
                      return smallPlan.execute(scope);
                    },
                  }),
                  /budget/,
                );
                assert.equal(small.peakBytes, inputBytes);
              } finally {
                pin.release();
              }
              assert.equal(small.usedBytes, 0);
            }
            console.log(
              JSON.stringify({
                type,
                codec,
                encoding,
                nullable,
                alignment,
                pageVersion: header.type,
                rows: reference.length,
                inputBytes,
                newBytesPerJob: plan.bytes,
                borrowsInput: plan.borrowsInput,
                concurrentJobs: 2,
                capMinusOneVerified: plan.bytes > 0,
                admittedPoolCap: cap,
                nonRetainedExtentPerJob: nonRetainedExtent,
                peakBytes: root.peakBytes,
                finalBytes: root.usedBytes,
                scope:
                  "Writer-generated local Parquet compared to DuckDB; encoded page backing and final numeric vectors charged; excludes metadata, writer, cloud and whole heap",
              }),
            );
          }
        }
} finally {
  connection.disconnectSync();
  await rm(directory, { recursive: true, force: true });
}
