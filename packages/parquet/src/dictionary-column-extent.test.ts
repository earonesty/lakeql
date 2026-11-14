import { existsSync, readFileSync } from "node:fs";
import { MemoryBudget, MemoryScope, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { dictionaryColumnEnd } from "./dictionary-column-extent.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { recoverColumnExtents } from "./recover-column-extents.js";
import { asyncBufferFromStore } from "./store-buffer.js";

const fixture = new URL(
  "../../../fixtures/external/parquet-testing/data/nation.dict-malformed.parquet",
  import.meta.url,
);
it.skipIf(!existsSync(fixture))(
  "proves omitted dictionary headers within physical boundaries",
  async () => {
    const store = memoryStore();
    await store.put("fixture", new Uint8Array(readFileSync(fixture)));
    const file = await asyncBufferFromStore(store, "fixture");
    const metadata = await readParquetMetadataFromFile(file);
    const columns = metadata.row_groups[0]?.columns.map((column) => column.meta_data);
    if (!columns || columns.some((column) => column === undefined))
      throw Error("Missing fixture columns");
    const pool = new MemoryBudget(65536);
    const scope = new MemoryScope(pool);
    try {
      for (let index = 0; index < columns.length; index++) {
        const column = columns[index];
        if (!column) throw Error("Missing column");
        const next = columns[index + 1];
        const bound = next
          ? Number(next.dictionary_page_offset ?? next.data_page_offset)
          : file.byteLength - metadata.metadata_length - 8;
        const declared = Number(column.data_page_offset + column.total_compressed_size);
        expect(await dictionaryColumnEnd(file, column, bound, scope)).toBe(
          declared + (index % 2 ? 15 : 0),
        );
        expect(Number(column.data_page_offset + column.total_compressed_size)).toBe(declared);
        expect(pool.usedBytes).toBe(0);
        if (index % 2) {
          await expect(
            dictionaryColumnEnd(file, column, declared + 14, scope),
          ).rejects.toMatchObject({ code: "LAKEQL_PARQUET_READ_ERROR" });
          await expect(
            dictionaryColumnEnd(
              file,
              { ...column, num_values: column.num_values + 1n },
              bound,
              scope,
            ),
          ).rejects.toMatchObject({ code: "LAKEQL_PARQUET_READ_ERROR" });
          expect(pool.usedBytes).toBe(0);
          await expect(
            dictionaryColumnEnd(
              file,
              { ...column, total_compressed_size: column.total_compressed_size - 1n },
              bound,
              scope,
            ),
          ).rejects.toMatchObject({ code: "LAKEQL_PARQUET_READ_ERROR" });
          // Already complete metadata must not receive another header's padding.
          expect(
            await dictionaryColumnEnd(
              file,
              { ...column, total_compressed_size: column.total_compressed_size + 15n },
              bound,
              scope,
            ),
          ).toBe(declared + 15);
          const tight = new MemoryScope(new MemoryBudget(1));
          try {
            await expect(dictionaryColumnEnd(file, column, bound, tight)).rejects.toMatchObject({
              code: "LAKEQL_BUDGET_EXCEEDED",
            });
            expect(tight.budget.usedBytes).toBe(0);
          } finally {
            tight.dispose();
          }
        }
      }
      const corrected = await recoverColumnExtents(file, metadata, {}, scope);
      expect(corrected).not.toBe(metadata);
      expect(corrected.schema).toBe(metadata.schema);
      expect(corrected.row_groups[0]?.columns[0]).toBe(metadata.row_groups[0]?.columns[0]);
      const before = metadata.row_groups[0]?.columns[1]?.meta_data;
      const after = corrected.row_groups[0]?.columns[1]?.meta_data;
      expect(before?.total_compressed_size).toBe(322n);
      expect(before?.dictionary_page_offset).toBeUndefined();
      expect(after?.total_compressed_size).toBe(337n);
      expect(after?.dictionary_page_offset).toBe(129n);
      expect(pool.usedBytes).toBeGreaterThan(0);
      const used = pool.usedBytes;
      const noRead = {
        ...file,
        slice: async () => {
          throw Error("Repeated recovery read");
        },
        sliceOwned: async () => {
          throw Error("Repeated owned recovery read");
        },
      };
      expect(await recoverColumnExtents(noRead, corrected, {}, scope)).toBe(corrected);
      expect(await recoverColumnExtents(noRead, corrected, { columns: ["name"] }, scope)).toBe(
        corrected,
      );
      expect(pool.usedBytes).toBe(used);
      const nameOnly = await recoverColumnExtents(file, metadata, { columns: ["name"] }, scope);
      expect(nameOnly.row_groups[0]?.columns[3]?.meta_data?.total_compressed_size).toBe(2002n);
      const comment = await recoverColumnExtents(
        file,
        nameOnly,
        { columns: ["comment_col"] },
        scope,
      );
      expect(comment.row_groups[0]?.columns[3]?.meta_data?.total_compressed_size).toBe(2017n);
      expect(nameOnly.row_groups[0]?.columns[3]?.meta_data?.total_compressed_size).toBe(2002n);
    } finally {
      scope.dispose();
    }
    expect(pool.usedBytes).toBe(0);
  },
);
