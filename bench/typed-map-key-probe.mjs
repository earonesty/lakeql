import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DuckDBConnection } from "@duckdb/node-api";
import {
  batchFromColumns,
  materializeBatchRows,
  memoryStore,
} from "../packages/core/dist/index.js";
import { readParquetColumnBatch } from "../packages/parquet/dist/column-batches.js";
import { readParquetMetadata, writeParquet } from "../packages/parquet/dist/index.js";
import { nestedDifference } from "./nested-value-comparison.mjs";

const directory = await mkdtemp(join(tmpdir(), "lakeql-map-keys-"));
const connection = await DuckDBConnection.create();
try {
  for (const [type, keys] of [
    ["INT64", [1n, 9007199254740993n]],
    ["BOOLEAN", [true, false]],
  ]) {
    const input = new Map(keys.map((key, index) => [key, index + 1]));
    const store = memoryStore();
    const path = `${type}.parquet`;
    await writeParquet(store, path, {
      codec: "SNAPPY",
      schema: [
        { name: "root", num_children: 1 },
        { name: "attrs", converted_type: "MAP", repetition_type: "OPTIONAL", num_children: 1 },
        { name: "key_value", repetition_type: "REPEATED", num_children: 2 },
        { name: "key", type, repetition_type: "REQUIRED" },
        { name: "value", type: "INT32", repetition_type: "OPTIONAL" },
      ],
      columnData: [{ name: "attrs", data: [input] }],
    });
    const bytes = await store.get(path);
    const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    const source = {
      byteLength: buffer.byteLength,
      async slice(start, end) {
        return buffer.slice(start, end);
      },
    };
    const metadata = await readParquetMetadata(store, path);
    const decoded = await readParquetColumnBatch(source, metadata, ["attrs"], 0, 1);
    const original = batchFromColumns({ attrs: [input] });
    const localPath = join(directory, path);
    await writeFile(localPath, bytes);
    const reference = await connection.runAndReadAll(
      `SELECT attrs FROM read_parquet('${localPath.replaceAll("'", "''")}')`,
    );
    const map = reference.getRowObjects()[0].attrs;
    // Validate the fixture against native typed reference values before diagnosing
    // either LakeQL boundary. A stringified JSON comparison would miss this loss.
    assert.equal(nestedDifference(input, map), undefined);
    const coreRows = materializeBatchRows(original);
    const decodedRows = materializeBatchRows(decoded);
    console.log(
      JSON.stringify({
        keyPhysicalType: type,
        inputVectorKeyType: original.columns.attrs.keys.type,
        decodedVectorKeyType: decoded.columns.attrs.keys.type,
        referenceKeyTypes: map.entries.map((entry) => typeof entry.key),
        coreMaterializationDifference: nestedDifference(coreRows[0].attrs, map) ?? null,
        parquetMaterializationDifference: nestedDifference(decodedRows[0].attrs, map) ?? null,
        materializedCoreRows: coreRows,
        materializedDecodedRows: decodedRows,
        scope:
          "real writer-produced local fixture; key representation diagnostic, not a memory-cap test",
      }),
    );
  }
} finally {
  connection.disconnectSync();
  await rm(directory, { recursive: true, force: true });
}
