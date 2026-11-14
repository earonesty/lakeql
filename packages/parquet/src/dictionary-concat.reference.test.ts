import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DuckDBConnection } from "@duckdb/node-api";
import { parquetWriteBuffer } from "hyparquet-writer";
import {
  type Batch,
  concatBatches,
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  memoryStore,
} from "lakeql-core";
import { describe, expect, it } from "vitest";
import { asyncBufferFromStore, readParquetMetadata } from "./index.js";
import { readParquetVectorBatchesFromFile } from "./vector-batches.js";

const suite = process.env.LAKEQL_REFERENCE === "1" ? describe : describe.skip;
suite("encoded concatenation", () => {
  it.each([
    [17, false, true, "RLE_DICTIONARY"],
    [128, false, true, "RLE_DICTIONARY"],
    [17, true, true, "RLE_DICTIONARY"],
    [17, true, false, "RLE_DICTIONARY"],
    [128, true, true, "RLE_DICTIONARY"],
    [128, true, false, "RLE_DICTIONARY"],
    [17, true, true, "PLAIN"],
    [128, true, true, "PLAIN"],
  ] as const)("matches DuckDB across SNAPPY batches with batch size %i, text=%s, scoped=%s, encoding=%s", async (batchSize, text, scoped, encoding) => {
    const directory = mkdtempSync(join(tmpdir(), "lakeql-dictionary-concat-"));
    const connection = await DuckDBConnection.create();
    const decodeScope = new MemoryScope(new MemoryBudget(64 * 1024 * 1024));
    try {
      const store = memoryStore();
      const batches: Batch[] = [];
      const expected: Record<string, unknown>[] = [];
      for (let file = 0; file < 2; file++) {
        const data = Array.from({ length: 128 }, (_, row) =>
          encoding === "RLE_DICTIONARY" && row % 11 === 0
            ? null
            : text
              ? row % 7 === 0
                ? ""
                : `\ufeffé中-${file}-${row % 7}`
              : 9007199254740993n + BigInt(file * 100 + (row % 7)),
        );
        const bytes = new Uint8Array(
          parquetWriteBuffer({
            schema: [
              { name: "root", num_children: 1 },
              {
                name: "value",
                type: text ? "BYTE_ARRAY" : "INT64",
                repetition_type: "OPTIONAL",
                ...(text ? { converted_type: "UTF8" as const } : {}),
              },
            ],
            columnData: [{ name: "value", data, encoding }],
            codec: "SNAPPY",
          }),
        );
        const path = join(directory, `${file}.parquet`);
        writeFileSync(path, bytes);
        expected.push(
          ...(
            await connection.runAndReadAll(
              `SELECT ${text ? "hex(value)" : "value"} AS value FROM read_parquet('${path.replaceAll("'", "''")}')`,
            )
          )
            .getRowObjects()
            .map((row) =>
              text && typeof row.value === "string"
                ? { value: Buffer.from(row.value, "hex").toString("utf8") }
                : row,
            ),
        );
        const key = `${file}.parquet`;
        await store.put(key, bytes);
        const metadata = await readParquetMetadata(store, key);
        const source = await asyncBufferFromStore(store, key);
        for await (const { batch } of readParquetVectorBatchesFromFile(source, metadata, {
          columns: ["value"],
          batchSize,
          ...(scoped ? { allocationScope: decodeScope } : {}),
        })) {
          expect(batch.columns.value?.type).toBe(encoding === "PLAIN" ? "utf8-view" : "dict");
          batches.push(batch);
        }
      }
      expect(batches).toHaveLength(2 * Math.ceil(128 / batchSize));
      const first = batches[0]?.columns.value;
      const second = batches.at(-1)?.columns.value;
      if (encoding === "RLE_DICTIONARY") {
        if (first?.type !== "dict" || second?.type !== "dict")
          throw new Error("Expected decoded dictionaries");
        expect(first.dictionary).not.toBe(second.dictionary);
      }
      // Decoder buffers are borrowed here. Cap only the concat output backing.
      function run(cap: number): number {
        const scope = new MemoryScope(new MemoryBudget(cap));
        try {
          const combined = concatBatches(batches, scope);
          expect(combined.columns.value?.type).toBe(
            encoding === "PLAIN" ? "utf8-view" : text ? "dict" : "i64",
          );
          expect(materializeBatchRows(combined)).toEqual(expected);
          return scope.budget.peakBytes;
        } finally {
          scope.dispose();
          expect(scope.budget.usedBytes).toBe(0);
        }
      }
      const peak = run(64 * 1024 * 1024);
      expect(run(peak)).toBe(peak);
      expect(() => run(peak - 1)).toThrow("budget");
    } finally {
      decodeScope.dispose();
      expect(decodeScope.budget.usedBytes).toBe(0);
      connection.disconnectSync();
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
