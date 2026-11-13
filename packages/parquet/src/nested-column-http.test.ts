import { existsSync, readFileSync } from "node:fs";
import { createServer } from "node:http";
import { parquetMetadata } from "hyparquet";
import { getSchemaPath } from "hyparquet/src/schema.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import {
  MemoryBudget,
  MemoryScope,
  type OwnedMemory,
  SharedMemoryCache,
  type Vector,
  vectorLength,
  vectorValue,
  withObjectStoreReadControls,
} from "lakeql-core";
import { expect, it, vi } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import type { decodeNestedColumnPage } from "./nested-column-page.js";
import { readNestedColumnPages } from "./nested-column-pages.js";
import { readNestedColumnRows } from "./nested-column-rows.js";
import { readNestedProjectedBatches } from "./nested-projected-batches.js";
import { readAlignedNestedRows } from "./nested-row-alignment.js";
import { joinNestedRowFragments } from "./nested-row-fragments.js";
import { executeNestedSchema } from "./nested-schema-execute.js";
import { compileNestedSchema } from "./nested-schema-plan.js";
import { lakeqlParquetParsers } from "./parsers.js";
import { cachedRangeBuffer } from "./range-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";

const fixture = new URL(
  "../../../fixtures/external/parquet-testing/data/nested_maps.snappy.parquet",
  import.meta.url,
);
for (const ignoreRange of [false, true])
  it.skipIf(!existsSync(fixture))(
    `streams owned nested columns over HTTP (ignored ranges=${ignoreRange})`,
    async () => {
      const bytes = readFileSync(fixture);
      const metadata = parquetMetadata(
        bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
      );
      let abortOnRead: (() => void) | undefined;
      let reads = 0,
        active = 0,
        peak = 0;
      const server = createServer((request, response) => {
        const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
        if (!match) {
          response.writeHead(400).end();
          return;
        }
        reads++;
        abortOnRead?.();
        peak = Math.max(peak, ++active);
        const start = ignoreRange ? 0 : Number(match[1]);
        const end = ignoreRange ? bytes.length : Math.min(bytes.length, Number(match[2]) + 1);
        setTimeout(() => {
          response.writeHead(ignoreRange ? 200 : 206, {
            "Content-Length": end - start,
            ETag: '"nested-map-v1"',
            ...(ignoreRange
              ? {}
              : { "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}` }),
          });
          response.end(bytes.subarray(start, end));
          active--;
        }, 2);
      });
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const pool = new MemoryBudget(2 * 1024 * 1024);
      const cache = new SharedMemoryCache({ maxBytes: 128 * 1024, memoryBudget: pool });
      const decodedColumnCache = new DecodedColumnCache(cache, {
        maxBytes: 128 * 1024,
        policy: "latency",
      });
      const decodedOptions = { decodedColumnCache, decodedColumnCacheKey: "nested-map-http" };
      const decodedReads = vi.spyOn(decodedColumnCache, "getValue");
      const retained: Array<{
        owner: OwnedMemory<NonNullable<ReturnType<typeof decodeNestedColumnPage>>>;
        expected: unknown[];
      }> = [];
      const assembled: OwnedMemory<Vector>[] = [];
      const expected = [
        ["a", "b", "c", "d", "e", "f"],
        [1, 2, 1, 1, 3, 4, 5],
        [true, false, true, true, true, false, true],
      ];
      try {
        const address = server.address();
        if (!address || typeof address === "string") throw Error("Missing server");
        const store = withObjectStoreReadControls(
          new HttpObjectStore({ baseUrl: `http://127.0.0.1:${address.port}/` }),
          { maxConcurrentReads: 2 },
        );
        const source = await asyncBufferFromStore(store, "maps.parquet");
        const file = cachedRangeBuffer(
          source,
          { maxBytes: 128 * 1024, coalesceBytes: 0, sharedCache: cache },
          "nested-map-http",
        );
        for (const warm of [false, true]) {
          reads = 0;
          decodedReads.mockClear();
          await Promise.all(
            [0, 1, 2].map(async (i) => {
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
              const scope = new MemoryScope(pool.child(128 * 1024));
              const iterator = readNestedColumnPages(file, column, decoder, scope, decodedOptions);
              try {
                const first = await iterator.next();
                if (first.done) throw Error("Missing page");
                const owner = first.value.retain();
                retained.push({ owner, expected: expected[i] ?? [] });
                expect((await iterator.next()).done).toBe(true);
                const vector = owner.value.slots;
                expect(
                  Array.from({ length: vectorLength(vector) }, (_, j) => vectorValue(vector, j)),
                ).toEqual(expected[i]);
              } finally {
                await iterator.return(undefined);
                scope.dispose();
              }
            }),
          );
          if (warm) {
            expect(reads).toBe(0);
            expect(
              decodedReads.mock.results.filter(
                (result) => result.type === "return" && result.value !== undefined,
              ),
            ).toHaveLength(3);
          } else expect(reads).toBeGreaterThan(0);
        }
        cache.deleteWhere(() => true);
        const maps = [
          { a: { "1": true, "2": false } },
          { b: { "1": true } },
          { c: null },
          { d: {} },
          { e: { "1": true } },
          { f: { "3": true, "4": false, "5": true } },
        ];
        for (const warm of [false, true]) {
          reads = 0;
          decodedReads.mockClear();
          const scope = new MemoryScope(pool.child(128 * 1024));
          try {
            const paths: ColumnDecoder["schemaPath"][] = [];
            const sources = [0, 1, 2].map((i) => {
              const column = metadata.row_groups[0]?.columns[i]?.meta_data;
              if (!column) throw Error("Missing column");
              const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
              paths.push(schemaPath);
              const element = schemaPath.at(-1)?.element;
              if (!element) throw Error("Missing leaf");
              const decoder: ColumnDecoder = {
                type: column.type,
                codec: column.codec,
                element,
                schemaPath,
                pathInSchema: column.path_in_schema,
                parsers: lakeqlParquetParsers,
              };
              return readNestedColumnRows(
                readNestedColumnPages(file, column, decoder, scope, decodedOptions),
                scope,
              );
            });
            const schema = paths[0]?.[1];
            if (!schema) throw Error("Missing schema");
            const plan = compileNestedSchema(schema, paths, scope);
            let row = 0;
            for await (const owner of readAlignedNestedRows(sources, scope, 2)) {
              const assembly = scope.childScope();
              try {
                assembly.adopt(owner.retain());
                const inputs = owner.value.columns.map((column, i) => {
                  const schemaPath = paths[i];
                  if (!schemaPath) throw Error("Missing schema path");
                  return { schemaPath, ...joinNestedRowFragments(column, schemaPath, assembly) };
                });
                const vector = executeNestedSchema(plan, inputs, assembly);
                expect(vectorLength(vector)).toBe(1);
                expect(vectorValue(vector, 0)).toEqual(maps[row++]);
                assembled.push(assembly.retainAllocations(vector));
              } finally {
                assembly.dispose();
              }
            }
            expect(row).toBe(6);
            if (warm) {
              expect(reads).toBe(0);
              expect(
                decodedReads.mock.results.filter(
                  (result) => result.type === "return" && result.value !== undefined,
                ),
              ).toHaveLength(3);
            } else expect(reads).toBeGreaterThan(0);
          } finally {
            scope.dispose();
          }
        }
        cache.deleteWhere(() => true);
        for (const [i, owner] of assembled.entries()) {
          expect(vectorValue(owner.value, 0)).toEqual(maps[i % 6]);
          owner.release();
        }
        for (const { owner, expected } of retained) {
          const vector = owner.value.slots;
          expect(
            Array.from({ length: vectorLength(vector) }, (_, i) => vectorValue(vector, i)),
          ).toEqual(expected);
          owner.release();
        }
        expect(pool.usedBytes).toBe(0);
        expect(peak).toBeLessThanOrEqual(2);
        expect(peak).toBeGreaterThan(1);
        const descriptors = (metadata.row_groups[0]?.columns ?? []).slice(0, 3).map((chunk) => {
          const column = chunk.meta_data;
          if (!column) throw Error("Missing column");
          const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
          const element = schemaPath.at(-1)?.element;
          if (!element) throw Error("Missing leaf");
          return {
            metadata: column,
            decoder: {
              schemaPath,
              element,
              type: column.type,
              codec: column.codec,
              pathInSchema: column.path_in_schema,
              parsers: lakeqlParquetParsers,
            },
          };
        });
        const root = descriptors[0]?.decoder.schemaPath[0];
        const name = descriptors[0]?.metadata.path_in_schema[0];
        if (!root || !name) throw Error("Missing root");
        const controller = new AbortController();
        const canceled = new MemoryScope(pool.child(128 * 1024), [controller.signal]);
        let triggered = false;
        abortOnRead = () => {
          abortOnRead = undefined;
          triggered = true;
          controller.abort();
        };
        try {
          await expect(
            readNestedProjectedBatches(file, root, descriptors, [name], canceled, {
              rowStart: 0,
              rowEnd: 6,
              batchSize: 2,
              maxConcurrentReads: 2,
            }).next(),
          ).rejects.toThrow(/abort|disposed/i);
          expect(triggered).toBe(true);
        } finally {
          abortOnRead = undefined;
          canceled.dispose();
        }
        cache.deleteWhere(() => true);
        expect(pool.usedBytes).toBe(0);
      } finally {
        for (const owner of assembled) owner.release();
        for (const { owner } of retained) owner.release();
        cache.deleteWhere(() => true);
        await new Promise<void>((resolve) => server.close(() => resolve()));
      }
    },
  );
