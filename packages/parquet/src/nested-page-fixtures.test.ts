import { existsSync, readFileSync } from "node:fs";
import { parquetMetadata } from "hyparquet";
import { assembleLists } from "hyparquet/src/assemble.js";
import { Encodings } from "hyparquet/src/constants.js";
import { convert, DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { readDataPage } from "hyparquet/src/datapage.js";
import { readPlain } from "hyparquet/src/plain.js";
import { getMaxDefinitionLevel, getSchemaPath } from "hyparquet/src/schema.js";
import { deserializeTCompactProtocol } from "hyparquet/src/thrift.js";
import type { ColumnDecoder, PageHeader } from "hyparquet/src/types.js";
import {
  MemoryBudget,
  MemoryScope,
  vectorFromSource,
  vectorLength,
  vectorValue,
} from "lakeql-core";
import { expect, it } from "vitest";
import { decodeNestedColumnPage } from "./nested-column-page.js";
import { decodeNestedDictionaryPage } from "./nested-dictionary-page.js";
import { nestedLeafVector } from "./nested-leaf-vector.js";
import { nestedListVector } from "./nested-list-vector.js";
import { indexNestedNode } from "./nested-node-index.js";
import { readNestedPage } from "./nested-page.js";
import { indexNestedRows } from "./nested-row-index.js";
import { executeNestedSchema } from "./nested-schema-execute.js";
import { compileNestedSchema } from "./nested-schema-plan.js";
import { validateNestedTransitions } from "./nested-transitions.js";
import { rawBytes as workerJsonBytes } from "./nested-worker-fixture.js";
import { lakeqlParquetParsers } from "./parsers.js";

for (const { name, columns } of [
  { name: "repeated_primitive_no_list.parquet", columns: 4 },
  { name: "float16_nonzeros_and_nans.parquet", columns: 1 },
  { name: "float16_zeros_and_nans.parquet", columns: 1 },
]) {
  const fixture = new URL(
    `../../../fixtures/external/parquet-testing/data/${name}`,
    import.meta.url,
  );
  it.skipIf(!existsSync(fixture))(
    `matches real dictionary leaf values and levels in ${name}`,
    () => {
      const file = readFileSync(fixture);
      const buffer = file.buffer.slice(file.byteOffset, file.byteOffset + file.byteLength);
      const metadata = parquetMetadata(buffer);
      let checked = 0;
      for (const chunk of metadata.row_groups[0]?.columns ?? []) {
        const column = chunk.meta_data;
        if (column?.codec !== "UNCOMPRESSED" || column.dictionary_page_offset === undefined)
          throw Error("Unexpected fixture layout");
        const reader = {
          view: new DataView(buffer),
          offset: Number(column.dictionary_page_offset),
        };
        const dictionary = deserializeTCompactProtocol(reader);
        const dictionaryOffset = reader.offset;
        reader.offset = Number(column.data_page_offset);
        const raw = deserializeTCompactProtocol(reader);
        const dataHeader = {
          num_values: raw.field_5.field_1,
          encoding: Encodings[raw.field_5.field_2],
          definition_level_encoding: Encodings[raw.field_5.field_3],
          repetition_level_encoding: Encodings[raw.field_5.field_4],
        };
        const header = {
          type: "DATA_PAGE",
          compressed_page_size: raw.field_3,
          uncompressed_page_size: raw.field_2,
          data_page_header: dataHeader,
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
          parsers: DEFAULT_PARSERS,
        };
        const bytes = new Uint8Array(buffer, reader.offset, raw.field_3);
        if (!header.data_page_header) throw Error("Missing data header");
        const expected = readDataPage(bytes, header.data_page_header, decoder);
        const scope = new MemoryScope(new MemoryBudget(65536));
        const result = readNestedPage(
          bytes,
          header,
          decoder,
          true,
          scope,
          dictionary.field_7.field_1,
        );
        expect(result).toBeDefined();
        expect(Array.from(result?.values ?? [])).toEqual(Array.from(expected.dataPage));
        expect(Array.from(result?.levels.repetition ?? [])).toEqual(expected.repetitionLevels);
        expect(Array.from(result?.levels.definition ?? [])).toEqual(
          expected.definitionLevels.length
            ? expected.definitionLevels
            : Array.from({ length: dataHeader.num_values }, () => getMaxDefinitionLevel(path)),
        );
        if (result === undefined) throw Error("Missing physical page");
        validateNestedTransitions(result.levels, path, undefined, scope);
        const rowIndex = indexNestedRows(result.levels, getMaxDefinitionLevel(path), scope);
        expect(rowIndex.continuesPrevious).toBe(false);
        expect(rowIndex.eventOffsets.length).toBe(result.levels.rowsStarted + 1);
        expect(rowIndex.eventOffsets.at(-1)).toBe(dataHeader.num_values);
        expect(rowIndex.valueOffsets.at(-1)).toBe(result.levels.presentCount);

        const dictionaryCount = dictionary.field_7.field_1;
        const dictionaryBytes = new DataView(buffer, dictionaryOffset, dictionary.field_3);
        const oracleDictionary = convert(
          readPlain(
            { view: dictionaryBytes, offset: 0 },
            column.type,
            dictionaryCount,
            element.type_length,
          ),
          decoder,
        );
        const dictionaryVector = decodeNestedDictionaryPage(
          new Uint8Array(buffer, dictionaryOffset, dictionary.field_3),
          {
            type: "DICTIONARY_PAGE",
            compressed_page_size: dictionary.field_3,
            uncompressed_page_size: dictionary.field_2,
            dictionary_page_header: {
              num_values: dictionaryCount,
              encoding: Encodings[dictionary.field_7.field_2],
            },
          } as PageHeader,
          decoder,
          scope,
        );
        if (dictionaryVector === undefined) throw Error("Unsupported fixture dictionary");
        const leaf = nestedLeafVector(result, decoder, scope, dictionaryVector);
        if (leaf === undefined) throw Error("Unsupported fixture leaf");
        expect(vectorLength(leaf)).toBe(result.levels.presentCount);
        expect(Array.from({ length: vectorLength(leaf) }, (_, i) => vectorValue(leaf, i))).toEqual(
          Array.from(expected.dataPage, (index) => oracleDictionary[Number(index)]),
        );
        if (leaf.type !== "dict") throw Error("Expected dictionary leaf");
        expect(leaf.indices).toBe(result.values);
        expect(leaf.dictionary).toBe(dictionaryVector);
        if (name === "repeated_primitive_no_list.parquet") {
          const parent = indexNestedNode(result.levels, path, path.length - 2, scope);
          const child = indexNestedNode(result.levels, path, path.length - 1, scope);
          const lists = nestedListVector(parent, child, leaf, scope);
          const decoded = Array.from(expected.dataPage, (index) => oracleDictionary[Number(index)]);
          const reference = assembleLists(
            [],
            expected.definitionLevels,
            expected.repetitionLevels,
            decoded,
            path,
          );
          // Independently checked with DuckDB: an absent legacy REPEATED element
          // is an empty list. Upstream assembleLists currently emits null here.
          const expectedLists =
            column.type === "INT32"
              ? [[0, 1, 2, 3], [], [4], [5, 6, 7, 8]]
              : [
                  ["foo", "zero", "one", "two"],
                  ["three"],
                  ["four"],
                  ["five", "six", "seven", "eight"],
                ];
          expect(
            Array.from({ length: vectorLength(lists) }, (_, i) => vectorValue(lists, i)),
          ).toEqual(expectedLists);
          expect(Array.from(reference, (value) => value ?? [])).toEqual(expectedLists);
          const repeated = path.at(-1);
          if (!repeated) throw Error("Missing repeated schema");
          const plan = compileNestedSchema(repeated, [path], scope);
          const assemble = (target: MemoryScope) => {
            const vector = executeNestedSchema(
              plan,
              [
                {
                  schemaPath: path,
                  levels: result.levels,
                  values: leaf,
                },
              ],
              target,
            );
            expect(
              Array.from({ length: vectorLength(vector) }, (_, i) => vectorValue(vector, i)),
            ).toEqual(expectedLists);
          };
          const assembly = scope.childScope();
          const before = scope.budget.usedBytes;
          assemble(assembly);
          const charge = scope.budget.usedBytes - before;
          assembly.dispose();
          expect(scope.budget.usedBytes).toBe(before);
          const limited = new MemoryScope(new MemoryBudget(charge - 1));
          expect(() => assemble(limited)).toThrow(/budget/);
          expect(limited.budget.usedBytes).toBe(0);
          limited.dispose();
        }

        scope.dispose();
        expect(scope.budget.usedBytes).toBe(0);
        checked++;
      }
      expect(checked).toBe(columns);
    },
  );
}

for (const name of ["json.parquet", "flba12_timestamp.parquet", "fixed_length_decimal.parquet"]) {
  const fixture = new URL(
    `../../../fixtures/external/parquet-testing/data/${name}`,
    import.meta.url,
  );
  it.skipIf(!existsSync(fixture))(`converts actual packed logical pages in ${name}`, () => {
    const file = readFileSync(fixture);
    const buffer = file.buffer.slice(file.byteOffset, file.byteOffset + file.byteLength);
    const metadata = parquetMetadata(buffer);
    let checked = 0;
    for (const chunk of metadata.row_groups[0]?.columns ?? []) {
      const column = chunk.meta_data;
      if (column?.codec !== "UNCOMPRESSED" || column.dictionary_page_offset !== undefined)
        throw Error("Unexpected direct fixture layout");
      const reader = { view: new DataView(buffer), offset: Number(column.data_page_offset) };
      const raw = deserializeTCompactProtocol(reader);
      const header = {
        type: "DATA_PAGE",
        compressed_page_size: raw.field_3,
        uncompressed_page_size: raw.field_2,
        data_page_header: {
          num_values: raw.field_5.field_1,
          encoding: Encodings[raw.field_5.field_2],
          definition_level_encoding: Encodings[raw.field_5.field_3],
          repetition_level_encoding: Encodings[raw.field_5.field_4],
        },
      } as PageHeader;
      const path = getSchemaPath(metadata.schema, column.path_in_schema);
      const element = path.at(-1)?.element;
      if (!element || !header.data_page_header) throw Error("Missing leaf/header");
      const decoder: ColumnDecoder = {
        type: column.type,
        codec: column.codec,
        element,
        schemaPath: path,
        pathInSchema: column.path_in_schema,
        parsers: lakeqlParquetParsers,
      };
      const bytes = new Uint8Array(buffer, reader.offset, raw.field_3);
      if (name === "json.parquet") expect(Array.from(bytes)).toEqual(workerJsonBytes);
      const expected = convert(
        readDataPage(bytes, header.data_page_header, decoder).dataPage,
        decoder,
      );
      // Scalar vectors may normalize carriers; dynamic JSON must preserve member presence.
      const expectedVector = vectorFromSource({ length: expected.length, get: (i) => expected[i] });
      const canonical =
        element.converted_type === "JSON"
          ? expected
          : Array.from({ length: expected.length }, (_, i) => vectorValue(expectedVector, i));
      const run = (scope: MemoryScope) => {
        const page = decodeNestedColumnPage(bytes, header, decoder, undefined, scope);
        if (!page) throw Error("Unsupported column fixture");
        const vector = page.values;
        expect(
          Array.from({ length: vectorLength(vector) }, (_, i) => vectorValue(vector, i)),
        ).toEqual(canonical);
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
      checked++;
    }
    expect(checked).toBe(name === "flba12_timestamp.parquet" ? 3 : 1);
  });
}
