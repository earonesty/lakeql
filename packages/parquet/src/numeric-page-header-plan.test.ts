import { gzipSync, zstdCompressSync } from "node:zlib";
import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { getSchemaPath } from "hyparquet/src/schema.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import { parquetWriteBuffer } from "hyparquet-writer";
import { batchFromVectors, MemoryBudget, MemoryScope, materializeBatchRows } from "lakeql-core";
import { expect, it } from "vitest";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { planFlatNumericPage, planFlatNumericPageHeader } from "./numeric-page.js";
import { planFlatNumericVectorHeader, preparePageWindow } from "./vector-batches.js";

it.each([
  "UNCOMPRESSED",
  "SNAPPY",
  "ZSTD",
  "GZIP",
] as const)("bounds real %s page decode before fetching its body", async (codec) => {
  for (const type of ["BOOLEAN", "INT32", "FLOAT", "DOUBLE", "DECIMAL"] as const) {
    for (const encoding of ["PLAIN", "BYTE_STREAM_SPLIT"] as const) {
      if (
        (type === "BOOLEAN" || type === "INT32" || type === "DECIMAL") &&
        encoding === "BYTE_STREAM_SPLIT"
      )
        continue;
      for (const nullable of [false, true, "all"] as const) {
        const values = Array.from({ length: 64 }, (_, i) =>
          nullable === "all" || (nullable && i % 3 === 0)
            ? null
            : type === "BOOLEAN"
              ? i % 2
              : i + 1,
        );
        const bytes = parquetWriteBuffer({
          codec,
          compressors: { ZSTD: zstdCompressSync, GZIP: gzipSync },
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
          columnData: [
            {
              name: "x",
              ...(type === "DECIMAL" ? {} : { type }),
              encoding,
              data:
                type === "BOOLEAN" ? values.map((x) => (x === null ? null : Boolean(x))) : values,
            },
          ],
        });
        const file = {
          byteLength: bytes.byteLength,
          async slice(start: number, end: number) {
            return bytes.slice(start, end);
          },
        };
        const metadata = await readParquetMetadataFromFile(file);
        const column = metadata.row_groups[0]?.columns[0]?.meta_data;
        if (!column) throw new Error("Missing column");
        const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
        const leaf = schemaPath.at(-1);
        if (!leaf) throw new Error("Missing schema");
        const decoder: ColumnDecoder = {
          ...column,
          pathInSchema: column.path_in_schema,
          schemaPath,
          element: leaf.element,
          parsers: DEFAULT_PARSERS,
        };
        const start = Number(column.data_page_offset);
        const prepared = await preparePageWindow(
          file,
          start,
          start + Number(column.total_compressed_size),
          0,
        );
        if (!prepared) throw new Error("Missing prepared page");
        const estimate = planFlatNumericPageHeader(prepared.header, decoder);
        if (!estimate) throw new Error("Missing header capability");
        expect(estimate.rowCount).toBe(values.length);
        expect(estimate.presentCount).toBe(values.filter((x) => x !== null).length);
        for (const alignment of [0, 1]) {
          const input = new Uint8Array(prepared.bodyEnd - prepared.bodyOffset + alignment);
          input.set(
            new Uint8Array(bytes, prepared.bodyOffset, prepared.bodyEnd - prepared.bodyOffset),
            alignment,
          );
          const plan = planFlatNumericPage(input.subarray(alignment), prepared.header, decoder);
          if (!plan) throw new Error("Missing precise plan");
          expect(plan.bytes).toBeLessThanOrEqual(estimate.bytes);
          const budget = new MemoryBudget(estimate.bytes);
          const scope = new MemoryScope(budget);
          scope.admit(estimate.bytes, "header-planned decode");
          const decoded = plan.execute(scope);
          expect(Array.from(decoded.values)).toEqual(
            values.filter((x) => x !== null).map((x) => (type === "DECIMAL" ? BigInt(x * 100) : x)),
          );
          expect(budget.peakBytes).toBeLessThanOrEqual(estimate.bytes);
          scope.dispose();
          expect(budget.usedBytes).toBe(0);
          const vectorPlan = planFlatNumericVectorHeader(prepared.header, decoder);
          if (!vectorPlan) throw new Error("Missing header vector capability");
          const vectorBudget = new MemoryBudget(vectorPlan.bytes);
          const vectorScope = new MemoryScope(vectorBudget);
          vectorScope.admit(vectorPlan.bytes, "header-planned vector");
          const vector = vectorPlan.execute(input.subarray(alignment), vectorScope);
          const owner = vectorScope.retainAllocations(vector);
          vectorScope.dispose();
          expect(materializeBatchRows(batchFromVectors({ x: owner.value }))).toEqual(
            values.map((x) => ({ x: type === "BOOLEAN" && x !== null ? Boolean(x) : x })),
          );
          expect(vectorBudget.peakBytes).toBeLessThanOrEqual(vectorPlan.bytes);
          owner.release();
          expect(vectorBudget.usedBytes).toBe(0);
        }
        expect(
          planFlatNumericPageHeader(
            { ...prepared.header, type: "DATA_PAGE", data_page_header_v2: undefined },
            decoder,
          ),
        ).toBeUndefined();
      }
    }
  }
});
