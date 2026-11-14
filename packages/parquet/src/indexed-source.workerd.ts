/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { env } from "cloudflare:test";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { type R2BucketLike, R2ObjectStore } from "../../r2/src/index.js";
import { acquireIndexedParquetSource } from "./indexed-source.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it.each([
  "source",
  "parent",
  "abort",
])("retains native R2 footer groups after %s closure", async (close) => {
  const bindings = env as unknown as {
    NUMERIC_PRESSURE_FIXTURE: string;
    MEMORY_TEST_BUCKET: R2BucketLike;
  };
  const store = new R2ObjectStore(bindings.MEMORY_TEST_BUCKET);
  const path = `indexed-source-${close}.parquet`;
  await store.put(
    path,
    Uint8Array.from(atob(bindings.NUMERIC_PRESSURE_FIXTURE), (c) => c.charCodeAt(0)),
  );
  const file = await asyncBufferFromStore(store, path);
  const metadata = await readParquetMetadataFromFile(file);
  const pool = new MemoryBudget(65536);
  const parent = new MemoryScope(pool);
  const controller = new AbortController();
  const source = await acquireIndexedParquetSource(file, parent, [controller.signal]);
  const first = source.acquireRowGroup(0);
  const second = source.acquireRowGroup(0);
  try {
    if (close === "parent") parent.dispose();
    else if (close === "abort") controller.abort();
    else source.dispose();
    expect(() => source.metadata).toThrow(/closed/);
    expect(() => source.acquireRowGroup(0)).toThrow(/closed/);
    expect(first.value).toEqual(metadata.row_groups[0]);
    expect(pool.usedBytes).toBeGreaterThan(0);
    first.release();
    expect(second.value).toEqual(metadata.row_groups[0]);
    expect(pool.usedBytes).toBe(second.bytes);
    second.release();
    source.dispose();
    parent.dispose();
    expect(pool.usedBytes).toBe(0);
  } finally {
    first.release();
    second.release();
    source.dispose();
    parent.dispose();
    await store.delete(path);
  }
});
