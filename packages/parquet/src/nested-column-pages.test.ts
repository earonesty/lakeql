import { existsSync, readFileSync } from "node:fs";
import { parquetMetadata } from "hyparquet";
import { getSchemaPath } from "hyparquet/src/schema.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import {
  MemoryBudget,
  MemoryScope,
  memoryStore,
  type OwnedMemory,
  type Vector,
  vectorLength,
  vectorValue,
} from "lakeql-core";
import { expect, it } from "vitest";
import { readNestedColumnPages } from "./nested-column-pages.js";
import { readNestedColumnRows } from "./nested-column-rows.js";
import { readAlignedNestedRows } from "./nested-row-alignment.js";
import { joinNestedRowFragments } from "./nested-row-fragments.js";
import { executeNestedSchema } from "./nested-schema-execute.js";
import { compileNestedSchema } from "./nested-schema-plan.js";
import { lakeqlParquetParsers } from "./parsers.js";
import { asyncBufferFromStore } from "./store-buffer.js";

const fixture = new URL(
  "../../../fixtures/external/parquet-testing/data/nested_maps.snappy.parquet",
  import.meta.url,
);
for (const earlyClose of [false, true])
  it.skipIf(!existsSync(fixture))(
    `streams real nested pages with retained output (earlyClose=${earlyClose})`,
    async () => {
      const bytes = readFileSync(fixture);
      const metadata = parquetMetadata(
        bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
      );
      const store = memoryStore();
      await store.put("maps.parquet", bytes);
      const file = await asyncBufferFromStore(store, "maps.parquet");
      const expected = [
        ["a", "b", "c", "d", "e", "f"],
        [1, 2, 1, 1, 3, 4, 5],
        [true, false, true, true, true, false, true],
      ];
      for (let i = 0; i < 3; i++) {
        const column = metadata.row_groups[0]?.columns[i]?.meta_data;
        if (!column) throw Error("Missing column");
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
        const budget = new MemoryBudget(1024 * 1024);
        const scope = new MemoryScope(budget);
        const iterator = readNestedColumnPages(file, column, decoder, scope);
        const next = await iterator.next();
        if (next.done) throw Error("Missing data page");
        const retained = next.value.retain();
        if (earlyClose) await iterator.return(undefined);
        else expect((await iterator.next()).done).toBe(true);
        scope.dispose();
        try {
          expect(budget.usedBytes).toBeGreaterThan(0);
          const vector = retained.value.slots;
          expect(
            Array.from({ length: vectorLength(vector) }, (_, j) => vectorValue(vector, j)),
          ).toEqual(expected[i]);
        } finally {
          retained.release();
        }
        expect(budget.usedBytes).toBe(0);

        const failedScope = new MemoryScope(new MemoryBudget(1024 * 1024));
        const invalid = readNestedColumnPages(
          file,
          { ...column, num_values: column.num_values + 1n },
          decoder,
          failedScope,
        );
        const first = await invalid.next();
        expect(first.done).toBe(false);
        await expect(invalid.next()).rejects.toThrow(/Invalid nested column/);
        failedScope.dispose();
        expect(failedScope.budget.usedBytes).toBe(0);
        const tight = new MemoryScope(new MemoryBudget(1));
        await expect(readNestedColumnPages(file, column, decoder, tight).next()).rejects.toThrow(
          /budget/,
        );
        expect(tight.budget.usedBytes).toBe(0);
        tight.dispose();
      }
      if (!earlyClose) {
        const alignedScope = new MemoryScope(new MemoryBudget(1024 * 1024));
        const paths: ColumnDecoder["schemaPath"][] = [];
        const sources = [0, 1, 2].map((i) => {
          const column = metadata.row_groups[0]?.columns[i]?.meta_data;
          if (!column) throw Error("Missing column");
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
          paths.push(path);
          return readNestedColumnRows(
            readNestedColumnPages(file, column, decoder, alignedScope),
            alignedScope,
          );
        });
        const schema = paths[0]?.[1];
        if (!schema) throw Error("Missing map schema");
        const plan = compileNestedSchema(schema, paths, alignedScope);
        const maps = [
          { a: { "1": true, "2": false } },
          { b: { "1": true } },
          { c: null },
          { d: {} },
          { e: { "1": true } },
          { f: { "3": true, "4": false, "5": true } },
        ];
        let retained: OwnedMemory<Vector> | undefined;
        let row = 0;
        try {
          for await (const owner of readAlignedNestedRows(sources, alignedScope, 2)) {
            expect(owner.value.row).toBe(row);
            expect(owner.value.columns.map((column) => column.row)).toEqual([row, row, row]);
            const counts = owner.value.columns.map((column) =>
              column.fragments.reduce(
                (sum, fragment) => sum + fragment.valueEnd - fragment.valueStart,
                0,
              ),
            );
            expect(counts).toEqual([1, [2, 1, 0, 0, 1, 3][row], [2, 1, 0, 0, 1, 3][row]]);
            const assembly = alignedScope.childScope();
            try {
              assembly.adopt(owner.retain());
              const inputs = owner.value.columns.map((column, i) => {
                const schemaPath = paths[i];
                if (!schemaPath) throw Error("Missing schema path");
                return { schemaPath, ...joinNestedRowFragments(column, schemaPath, assembly) };
              });
              const vector = executeNestedSchema(plan, inputs, assembly);
              expect(vectorLength(vector)).toBe(1);
              expect(vectorValue(vector, 0)).toEqual(maps[row]);
              retained?.release();
              retained = assembly.retainAllocations(vector);
            } finally {
              assembly.dispose();
            }
            row++;
          }
          expect(row).toBe(6);
        } finally {
          alignedScope.dispose();
        }
        try {
          expect(retained).toBeDefined();
          if (retained) expect(vectorValue(retained.value, 0)).toEqual(maps[5]);
        } finally {
          retained?.release();
        }
        expect(alignedScope.budget.usedBytes).toBe(0);
      }
    },
  );
