import { existsSync, readFileSync } from "node:fs";
import { parquetMetadata } from "hyparquet";
import {
  getMaxDefinitionLevel,
  getMaxRepetitionLevel,
  getSchemaPath,
} from "hyparquet/src/schema.js";
import { deserializeTCompactProtocol } from "hyparquet/src/thrift.js";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { readNestedLevels } from "./nested-levels.js";

const options = { count: 3, maxRepetition: 1, maxDefinition: 2, startsColumn: true };
const rep = [2, 0, 2, 1, 2, 0];
const def = [2, 2, 2, 1, 2, 2];
it.each([false, true])("decodes nested streams with exact admission, v2=%s", (v2) => {
  const bytes = Uint8Array.from(
    v2 ? [...rep, ...def, 255] : [6, 0, 0, 0, ...rep, 6, 0, 0, 0, ...def, 255],
  );
  const config = { ...options, ...(v2 ? { repetitionBytes: 6, definitionBytes: 6 } : {}) };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const result = readNestedLevels(bytes, config, scope);
  expect(Array.from(result.repetition ?? [])).toEqual([0, 1, 0]);
  expect(Array.from(result.definition ?? [])).toEqual([2, 1, 2]);
  expect(result).toMatchObject({ rowsStarted: 2, presentCount: 2, valueOffset: bytes.length - 1 });
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  readNestedLevels(bytes, config, exact);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => readNestedLevels(bytes, config, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
});

it("allows row continuation only after the first column page and rolls back malformed levels", () => {
  const bytes = Uint8Array.from([2, 1, 2, 2]);
  const config = { ...options, count: 1, repetitionBytes: 2, definitionBytes: 2 };
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(() => readNestedLevels(bytes, config, scope)).toThrow(/Malformed/);
  expect(scope.budget.usedBytes).toBe(0);
  expect(readNestedLevels(bytes, { ...config, startsColumn: false }, scope).rowsStarted).toBe(0);
  scope.dispose();
  const bad = new MemoryScope(new MemoryBudget(65536));
  expect(() => readNestedLevels(Uint8Array.from([2, 0, 2, 3]), config, bad)).toThrow(/Malformed/);
  expect(bad.budget.usedBytes).toBe(0);
  bad.dispose();
});

const malformedFixture = new URL(
  "../../../fixtures/external/parquet-testing/bad_data/ARROW-GH-45185.parquet",
  import.meta.url,
);
it.skipIf(!existsSync(malformedFixture))(
  "rejects the real first-row continuation fixture before child reconstruction",
  () => {
    const bytes = readFileSync(malformedFixture);
    const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    const metadata = parquetMetadata(buffer);
    const column = metadata.row_groups[0]?.columns[0]?.meta_data;
    if (column?.codec !== "UNCOMPRESSED") throw Error("Unexpected fixture layout");
    const reader = { view: new DataView(buffer), offset: Number(column.data_page_offset) };
    const header = deserializeTCompactProtocol(reader);
    const path = getSchemaPath(metadata.schema, column.path_in_schema);
    const scope = new MemoryScope(new MemoryBudget(4096));
    expect(() =>
      readNestedLevels(
        new Uint8Array(buffer, reader.offset, header.field_3),
        {
          count: header.field_5.field_1,
          maxRepetition: getMaxRepetitionLevel(path),
          maxDefinition: getMaxDefinitionLevel(path),
          startsColumn: true,
        },
        scope,
      ),
    ).toThrow(/Malformed nested/);
    expect(scope.budget.usedBytes).toBe(0);
    scope.dispose();
  },
);
