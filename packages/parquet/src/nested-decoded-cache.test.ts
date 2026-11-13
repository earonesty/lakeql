import { readFileSync } from "node:fs";
import { parquetMetadata } from "hyparquet";
import { getSchemaPath } from "hyparquet/src/schema.js";
import {
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  memoryStore,
  type OwnedMemory,
  SharedMemoryCache,
  vectorValue,
} from "lakeql-core";
import { expect, it, vi } from "vitest";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { readNestedColumnPages, readNestedCompactColumnPages } from "./nested-column-pages.js";
import { nestedListFile } from "./nested-list-worker-fixture.js";
import { readNestedRowGroupBatches } from "./nested-row-group-read.js";
import { lakeqlParquetParsers } from "./parsers.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import type { ParquetColumnBatch } from "./types.js";

it("reuses nested decoded pages across jobs and keeps output alive after eviction", async () => {
  const bytes = readFileSync(
    new URL(
      "../../../fixtures/external/parquet-testing/data/nested_lists.snappy.parquet",
      import.meta.url,
    ),
  );
  expect(nestedListFile).toEqual(new Uint8Array(bytes));
  const metadata = parquetMetadata(
    bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
  );
  const store = memoryStore();
  await store.put("nested", bytes);
  const file = await asyncBufferFromStore(store, "nested");
  const pool = new MemoryBudget(4 * 1024 * 1024);
  const shared = new SharedMemoryCache({ maxBytes: 1024 * 1024, memoryBudget: pool });
  const cache = new DecodedColumnCache(shared, { maxBytes: 1024 * 1024, policy: "latency" });
  const getValue = vi.spyOn(cache, "getValue");
  const hits = () =>
    getValue.mock.results.filter((result) => result.type === "return" && result.value !== undefined)
      .length;
  const outputs: OwnedMemory<ParquetColumnBatch>[] = [];
  const runs: unknown[][] = [];
  try {
    for (let run = 0; run < 3; run++) {
      getValue.mockClear();
      const scope = new MemoryScope(pool.child(1024 * 1024));
      const rows: unknown[] = [];
      try {
        for await (const owner of readNestedRowGroupBatches(
          run === 2 ? { ...file, etag: "changed" } : file,
          metadata,
          scope,
          {
            batchSize: 2,
            decodedColumnCache: cache,
            decodedColumnCacheKey: "nested",
          },
        )) {
          rows.push(...materializeBatchRows(owner.value.batch));
          outputs.push(owner);
        }
      } finally {
        scope.dispose();
      }
      runs.push(rows);
      if (run === 1) expect(hits()).toBeGreaterThan(0);
      else expect(hits()).toBe(0);
    }
    expect(runs[0]).toHaveLength(3);
    expect(runs[1]).toEqual(runs[0]);
    expect(runs[2]).toEqual(runs[0]);
    shared.deleteWhere(() => true);
    expect(outputs.flatMap((owner) => materializeBatchRows(owner.value.batch))).toEqual(
      runs.flat(),
    );
  } finally {
    for (const owner of outputs) owner.release();
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});

it("does not attach replacement dictionary backing to a cached nested page", async () => {
  const bytes = readFileSync(
    new URL(
      "../../../fixtures/external/parquet-testing/data/nested_lists.snappy.parquet",
      import.meta.url,
    ),
  );
  const metadata = parquetMetadata(
    bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
  );
  const column = metadata.row_groups[0]?.columns[1]?.meta_data;
  if (!column) throw Error("Missing dictionary column");
  const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
  const element = schemaPath.at(-1)?.element;
  if (!element) throw Error("Missing element");
  const decoder = {
    type: column.type,
    codec: column.codec,
    schemaPath,
    element,
    pathInSchema: column.path_in_schema,
    parsers: lakeqlParquetParsers,
  };
  const store = memoryStore();
  await store.put("nested", bytes);
  const file = await asyncBufferFromStore(store, "nested");
  const pool = new MemoryBudget(1024 * 1024);
  const shared = new SharedMemoryCache({ maxBytes: 512 * 1024, memoryBudget: pool });
  const cache = new DecodedColumnCache(shared, { maxBytes: 512 * 1024, policy: "latency" });
  const dictionaryReads = vi.spyOn(cache, "getVector");
  const pageReads = vi.spyOn(cache, "getValue");
  const run = async () => {
    const scope = new MemoryScope(pool.child(128 * 1024));
    const iterator = readNestedColumnPages(file, column, decoder, scope, {
      decodedColumnCache: cache,
      decodedColumnCacheKey: "nested",
    });
    try {
      const next = await iterator.next();
      if (next.done) throw Error("Missing page");
      const retained = next.value.retain();
      await iterator.return(undefined);
      return retained;
    } finally {
      await iterator.return(undefined);
      scope.dispose();
    }
  };
  try {
    const cold = await run();
    const coldBytes = cold.bytes;
    cold.release();
    const warm = await run();
    const warmBytes = warm.bytes;
    // A cached page already owns its original transport backing. Header probes
    // from this job must not be added to either cached dictionary or page output.
    expect(warmBytes).toBe(coldBytes);
    warm.release();
    const dictionaryKey = dictionaryReads.mock.calls[0]?.[0];
    expect(dictionaryKey).toBeDefined();
    shared.delete(`decoded-vector:${dictionaryKey}`);
    dictionaryReads.mockClear();
    pageReads.mockClear();
    const replaced = await run();
    try {
      expect(dictionaryReads.mock.results).toHaveLength(1);
      expect(dictionaryReads.mock.results[0]?.value).toBeUndefined();
      expect(pageReads.mock.results[0]?.value).toBeDefined();
      expect(replaced.bytes).toBe(warmBytes);
      shared.deleteWhere(() => true);
      expect(vectorValue(replaced.value.slots, 0)).toBe(1);
    } finally {
      replaced.release();
    }
  } finally {
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});

it("separates compact and leaf-slot cache shapes across cold and warm reads", async () => {
  const metadata = parquetMetadata(nestedListFile.slice().buffer);
  const column = metadata.row_groups[0]?.columns[1]?.meta_data;
  if (!column) throw Error("Missing column");
  const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
  const element = schemaPath.at(-1)?.element;
  if (!element) throw Error("Missing leaf");
  const decoder = {
    type: column.type,
    codec: column.codec,
    schemaPath,
    element,
    pathInSchema: column.path_in_schema,
    parsers: lakeqlParquetParsers,
  };
  const store = memoryStore();
  await store.put("nested", nestedListFile);
  const file = await asyncBufferFromStore(store, "nested");
  const pool = new MemoryBudget(1024 * 1024);
  const shared = new SharedMemoryCache({ maxBytes: 512 * 1024, memoryBudget: pool });
  const cache = new DecodedColumnCache(shared, { maxBytes: 512 * 1024, policy: "latency" });
  const hits = vi.spyOn(cache, "getValue");
  const dictionaries = vi.spyOn(cache, "getVector");
  try {
    for (let run = 0; run < 4; run++) {
      hits.mockClear();
      dictionaries.mockClear();
      const scope = new MemoryScope(pool.child(128 * 1024));
      const expanded = run % 2 === 1;
      const read = expanded ? readNestedColumnPages : readNestedCompactColumnPages;
      try {
        for await (const owner of read(file, column, decoder, scope, {
          decodedColumnCache: cache,
          decodedColumnCacheKey: "nested",
        })) {
          expect("slots" in owner.value).toBe(expanded);
          expect(vectorValue(owner.value.values, 0)).toBe(1);
        }
        const warm = hits.mock.results.some(
          (result) => result.type === "return" && result.value !== undefined,
        );
        expect(warm).toBe(run >= 2);
        expect(
          dictionaries.mock.results.some(
            (result) => result.type === "return" && result.value !== undefined,
          ),
        ).toBe(run > 0);
      } finally {
        scope.dispose();
      }
    }
  } finally {
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});
