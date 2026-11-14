/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { env } from "cloudflare:test";
import { MemoryBudget, materializeBatchRows } from "lakeql-core";
import { expect, it } from "vitest";
import { type R2BucketLike, R2ObjectStore } from "../../r2/src/index.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import { preparePageWindow } from "./vector-batches.js";

it("measures ordinary R2 numeric scans against shared pinned headroom", async () => {
  const bindings = env as unknown as {
    NUMERIC_PRESSURE_FIXTURE: string;
    MEMORY_TEST_BUCKET: R2BucketLike;
  };
  const bytes = Uint8Array.from(atob(bindings.NUMERIC_PRESSURE_FIXTURE), (c) => c.charCodeAt(0));
  const store = new R2ObjectStore(bindings.MEMORY_TEST_BUCKET);
  const path = "numeric-shared-pressure.parquet";
  await store.put(path, bytes);
  const file = await asyncBufferFromStore(store, path);
  const metadata = await readParquetMetadataFromFile(file);
  try {
    for (const headroom of [8192, 1]) {
      const pool = new MemoryBudget(1024 * 1024 + headroom);
      const pinned = pool.reserve(1024 * 1024, "other job retained output");
      const job = pool.child(1024 * 1024);
      const read = async () => {
        const rows = [];
        for await (const output of readOwnedParquetVectorBatches(file, metadata, job, {
          columns: ["a", "b"],
          batchSize: 1,
          canStopEarly: true,
        })) {
          rows.push(...materializeBatchRows(output.value.batch));
          output.release();
        }
        return rows;
      };
      try {
        if (headroom === 8192) await expect(read()).resolves.toEqual([{ a: 0, b: null }]);
        else await expect(read()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
        expect(job.usedBytes).toBe(0);
        expect(pool.usedBytes).toBe(pinned.bytes);
      } finally {
        pinned.release();
      }
      expect(pool.usedBytes).toBe(0);
    }
  } finally {
    await store.delete(path);
  }
});

it.each([
  0, 1,
])("drains cancelled measured R2 input %s and reuses the shared pool", async (blocked) => {
  const bindings = env as unknown as {
    NUMERIC_PRESSURE_FIXTURE: string;
    MEMORY_TEST_BUCKET: R2BucketLike;
  };
  const bucket = bindings.MEMORY_TEST_BUCKET;
  const path = `numeric-cancel-${blocked}.parquet`;
  const bytes = Uint8Array.from(atob(bindings.NUMERIC_PRESSURE_FIXTURE), (c) => c.charCodeAt(0));
  await bucket.put(path, bytes);
  const original = await asyncBufferFromStore(new R2ObjectStore(bucket), path);
  const metadata = await readParquetMetadataFromFile(original);
  const offsets: number[] = [];
  for (const chunk of metadata.row_groups[0]?.columns ?? []) {
    const column = chunk.meta_data;
    if (!column) throw new Error("Missing column");
    const start = Number(column.data_page_offset);
    const page = await preparePageWindow(
      original,
      start,
      start + Number(column.total_compressed_size),
      0,
    );
    if (!page) throw new Error("Missing page");
    offsets.push(page.bodyOffset);
  }
  let finish!: () => void;
  let enter!: () => void;
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  let delay = true;
  let bodyReads = 0;
  const delayed: R2BucketLike = {
    head: bucket.head.bind(bucket),
    put: bucket.put.bind(bucket),
    delete: bucket.delete.bind(bucket),
    list: bucket.list.bind(bucket),
    async get(key, options) {
      const object = await bucket.get(key, options);
      const offset = options?.range?.offset;
      if (offset !== undefined && offsets.includes(offset)) bodyReads++;
      if (delay && offset === offsets[blocked]) {
        enter();
        await gate;
      }
      return object;
    },
  };
  const file = await asyncBufferFromStore(new R2ObjectStore(delayed), path);
  const pool = new MemoryBudget(1024 * 1024 + 8192);
  const pin = pool.reserve(1024 * 1024, "retained sibling output");
  const job = pool.child(1024 * 1024);
  const controller = new AbortController();
  const scan = async (signals: AbortSignal[] = []) => {
    const rows = [];
    for await (const output of readOwnedParquetVectorBatches(
      file,
      metadata,
      job,
      {
        columns: ["a", "b"],
        batchSize: 1,
        canStopEarly: true,
      },
      signals,
    )) {
      rows.push(...materializeBatchRows(output.value.batch));
      output.release();
    }
    return rows;
  };
  const pending = scan([controller.signal]);
  const rejected = expect(pending).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  try {
    await entered;
    controller.abort();
    expect(job.usedBytes).toBeGreaterThan(0);
    expect(pool.usedBytes).toBeGreaterThan(pin.bytes);
    finish();
    await rejected;
    expect(bodyReads).toBe(blocked + 1);
    expect(job.usedBytes).toBe(0);
    expect(pool.usedBytes).toBe(pin.bytes);
    delay = false;
    await expect(scan()).resolves.toEqual([{ a: 0, b: null }]);
    expect(job.usedBytes).toBe(0);
    expect(pool.usedBytes).toBe(pin.bytes);
  } finally {
    finish();
    pin.release();
    await bucket.delete(path);
  }
  expect(pool.usedBytes).toBe(0);
});
