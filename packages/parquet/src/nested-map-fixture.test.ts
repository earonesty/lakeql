import { existsSync, readFileSync } from "node:fs";
import { parquetMetadata } from "hyparquet";
import { Encodings } from "hyparquet/src/constants.js";
import { getSchemaPath } from "hyparquet/src/schema.js";
import { deserializeTCompactProtocol } from "hyparquet/src/thrift.js";
import type { ColumnDecoder, PageHeader } from "hyparquet/src/types.js";
import { MemoryBudget, MemoryScope, vectorLength, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { decodeNestedColumnPage } from "./nested-column-page.js";
import { decodeNestedDictionaryPage } from "./nested-dictionary-page.js";
import { nestedMapVector } from "./nested-map-vector.js";
import { validateNestedNodeAlignment } from "./nested-node-alignment.js";
import { indexNestedNode } from "./nested-node-index.js";
import { executeNestedSchema } from "./nested-schema-execute.js";
import { compileNestedSchema } from "./nested-schema-plan.js";
import { lakeqlParquetParsers } from "./parsers.js";

const fixture = new URL(
  "../../../fixtures/external/parquet-testing/data/nested_maps.snappy.parquet",
  import.meta.url,
);
it.skipIf(!existsSync(fixture))(
  "assembles real Snappy nested maps from independent key/value streams",
  () => {
    const file = readFileSync(fixture);
    const buffer = file.buffer.slice(file.byteOffset, file.byteOffset + file.byteLength);
    const metadata = parquetMetadata(buffer);
    const run = (scope: MemoryScope) => {
      const columns = (metadata.row_groups[0]?.columns ?? []).slice(0, 3).map((chunk) => {
        const column = chunk.meta_data;
        if (column?.codec !== "SNAPPY" || column.dictionary_page_offset !== undefined)
          throw Error("Unexpected fixture layout");
        const reader = { view: new DataView(buffer), offset: Number(column.data_page_offset) };
        let raw = deserializeTCompactProtocol(reader);
        const dictionary = raw.field_7
          ? {
              count: raw.field_7.field_1,
              encoding: Encodings[raw.field_7.field_2],
              bytes: new Uint8Array(buffer, reader.offset, raw.field_3),
              size: raw.field_2,
            }
          : undefined;
        if (dictionary) {
          reader.offset += raw.field_3;
          raw = deserializeTCompactProtocol(reader);
        }
        const header = {
          type: raw.field_8 ? "DATA_PAGE_V2" : "DATA_PAGE",
          compressed_page_size: raw.field_3,
          uncompressed_page_size: raw.field_2,
          data_page_header: raw.field_5 && {
            num_values: raw.field_5.field_1,
            encoding: Encodings[raw.field_5.field_2],
            definition_level_encoding: Encodings[raw.field_5.field_3],
            repetition_level_encoding: Encodings[raw.field_5.field_4],
          },
          data_page_header_v2: raw.field_8 && {
            num_values: raw.field_8.field_1,
            num_nulls: raw.field_8.field_2,
            num_rows: raw.field_8.field_3,
            encoding: Encodings[raw.field_8.field_4],
            definition_levels_byte_length: raw.field_8.field_5,
            repetition_levels_byte_length: raw.field_8.field_6,
            is_compressed: raw.field_8.field_7 ?? true,
          },
        } as PageHeader;
        const path = getSchemaPath(metadata.schema, column.path_in_schema);
        const element = path.at(-1)?.element;
        if (!element) throw Error("Missing leaf");
        const decoder: ColumnDecoder = {
          type: column.type,
          codec: column.codec,
          element,
          schemaPath: path,
          pathInSchema: column.path_in_schema,
          parsers: lakeqlParquetParsers,
        };
        const dictionaryVector =
          dictionary === undefined
            ? undefined
            : decodeNestedDictionaryPage(
                dictionary.bytes,
                {
                  type: "DICTIONARY_PAGE",
                  compressed_page_size: dictionary.bytes.length,
                  uncompressed_page_size: dictionary.size,
                  dictionary_page_header: {
                    num_values: dictionary.count,
                    encoding: dictionary.encoding,
                  },
                } as PageHeader,
                decoder,
                scope,
              );
        const page = decodeNestedColumnPage(
          new Uint8Array(buffer, reader.offset, raw.field_3),
          header,
          decoder,
          undefined,
          scope,
          dictionaryVector,
        );
        if (!page) throw Error("Unsupported fixture page");
        return { page, path };
      });
      const [outerKeys, innerKeys, innerValues] = columns;
      if (!outerKeys || !innerKeys || !innerValues) throw Error("Missing map columns");
      const projected = (column: typeof outerKeys) => ({
        schemaPath: column.path,
        levels: column.page.physical.levels,
      });
      for (const depth of [1, 2])
        validateNestedNodeAlignment(projected(outerKeys), projected(innerKeys), depth, scope);
      for (const depth of [3, 4])
        validateNestedNodeAlignment(projected(innerKeys), projected(innerValues), depth, scope);

      const node = (column: typeof outerKeys, depth: number) =>
        indexNestedNode(column.page.physical.levels, column.path, depth, scope);
      const innerParent = node(innerKeys, 3),
        innerEntries = node(innerKeys, 4);
      expect(node(innerValues, 3)).toEqual(innerParent);
      expect(node(innerValues, 4)).toEqual(innerEntries);
      const inner = nestedMapVector(
        innerParent,
        innerEntries,
        innerKeys.page.slots,
        innerValues.page.slots,
        scope,
      );
      const outerParent = node(outerKeys, 1),
        outerEntries = node(outerKeys, 2);
      // These columns have different event counts because the inner maps have varied sizes.
      expect(node(innerKeys, 1).valid).toEqual(outerParent.valid);
      expect(node(innerKeys, 2).valid).toEqual(outerEntries.valid);
      const outer = nestedMapVector(outerParent, outerEntries, outerKeys.page.slots, inner, scope);
      const schema = outerKeys.path[1];
      if (!schema) throw Error("Missing map schema");
      const plan = compileNestedSchema(
        schema,
        columns.map((column) => column.path),
        scope,
      );
      const compiled = executeNestedSchema(
        plan,
        columns.map((column) => ({ ...projected(column), values: column.page.values })),
        scope,
      );
      expect(
        Array.from({ length: vectorLength(compiled) }, (_, i) => vectorValue(compiled, i)),
      ).toEqual(Array.from({ length: vectorLength(outer) }, (_, i) => vectorValue(outer, i)));
      // Independently verified with DuckDB read_parquet; maps use core's object representation.
      expect(Array.from({ length: vectorLength(outer) }, (_, i) => vectorValue(outer, i))).toEqual([
        { a: { "1": true, "2": false } },
        { b: { "1": true } },
        { c: null },
        { d: {} },
        { e: { "1": true } },
        { f: { "3": true, "4": false, "5": true } },
      ]);
    };
    const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
    run(scope);
    const peak = scope.budget.peakBytes;
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
    const exact = new MemoryScope(new MemoryBudget(peak));
    run(exact);
    exact.dispose();
    const tight = new MemoryScope(new MemoryBudget(peak - 1));
    expect(() => run(tight)).toThrow(/budget/);
    tight.dispose();
    expect(tight.budget.usedBytes).toBe(0);
  },
);
