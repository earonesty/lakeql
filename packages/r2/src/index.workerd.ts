/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { env } from "cloudflare:test";
import {
  MemoryBudget,
  MemoryScope,
  ObjectStoreJsonCache,
  withObjectStoreReadControls,
} from "lakeql-core";
import { expect, it } from "vitest";
import { type R2BucketLike, R2ObjectStore } from "./index.js";

it("reads bounded ranges from a real Workers R2 binding", async () => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const bytes = Uint8Array.from({ length: 4096 }, (_, index) => index % 251);
  await store.put("owned-range-fixture", bytes);
  const budget = new MemoryBudget(128);
  const scope = new MemoryScope(budget);
  try {
    const range = await store.getRangeOwned(
      "owned-range-fixture",
      { offset: 1000, length: 128 },
      scope,
    );
    expect([...range.value]).toEqual([...bytes.slice(1000, 1128)]);
    expect(budget.usedBytes).toBe(128);
    range.release();
    const tail = await store.getRangeOwned(
      "owned-range-fixture",
      { offset: 4090, length: 128 },
      scope,
    );
    expect([...tail.value]).toEqual([...bytes.slice(4090)]);
    expect(budget.usedBytes).toBe(6);
    expect(tail.value.byteOffset).toBe(0);
    expect(tail.value.buffer.byteLength).toBe(6);
    expect(
      store.getRangeOwnedAllocation("owned-range-fixture", { offset: 4090, length: 128 }),
    ).toEqual({ bytes: 128, output: "exact-buffer" });
    tail.release();
    const tooSmall = new MemoryScope(new MemoryBudget(127));
    await expect(
      store.getRangeOwned("owned-range-fixture", { offset: 1000, length: 128 }, tooSmall),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
    expect(tooSmall.budget.usedBytes).toBe(0);
    tooSmall.dispose();
  } finally {
    scope.dispose();
    await store.delete("owned-range-fixture");
  }
  expect(budget.usedBytes).toBe(0);
});

it("writes owned JSON cache entries through a real R2 binding", async () => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const scope = new MemoryScope(pool);
  const value = {
    files: [{ path: "雪.parquet", size: 12, lastModified: new Date("2026-11-14T00:00:00Z") }],
    bytes: new Uint8Array([0, 255]),
  };
  const owner = scope.allocate(2048, "fixture input", () => ({ value }));
  const cache = new ObjectStoreJsonCache({ store, prefix: "owned-write-cache" });
  try {
    await cache.setOwnedEntry("key", owner);
    expect(pool.usedBytes).toBe(2048);
    const stored = await bucket.get("owned-write-cache/key.json");
    expect(new TextDecoder().decode(await stored?.arrayBuffer())).toBe(JSON.stringify({ value }));
    store.head = async () => {
      throw new Error("legacy metadata path");
    };
    const read = await cache.getOwnedEntry("key", pool);
    expect(read?.value).toEqual(JSON.parse(JSON.stringify({ value })));
    read?.release();
  } finally {
    scope.dispose();
    await bucket.delete("owned-write-cache/key.json");
  }
  expect(pool.usedBytes).toBe(0);
});

it("holds the R2 metadata slot until a cancelled binding call settles", async () => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const path = "cancelled-metadata-fixture";
  await bucket.put(path, new Uint8Array([1, 2, 3]));
  let enter!: () => void;
  let finish!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let calls = 0;
  const delayed: R2BucketLike = {
    get: bucket.get.bind(bucket),
    put: bucket.put.bind(bucket),
    delete: bucket.delete.bind(bucket),
    list: bucket.list.bind(bucket),
    async head(key) {
      const call = ++calls;
      const metadata = await bucket.head(key);
      if (call === 1) {
        enter();
        await gate;
      }
      return metadata;
    },
  };
  const store = withObjectStoreReadControls(new R2ObjectStore(delayed), { maxConcurrentReads: 1 });
  const pool = new MemoryBudget(65536);
  const firstScope = new MemoryScope(pool);
  const nextScope = new MemoryScope(pool);
  try {
    const head = store.headOwned;
    if (head === undefined) throw Error("Missing owned metadata");
    const first = head(path, firstScope);
    const rejected = expect(first).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    await entered;
    firstScope.dispose();
    const next = head(path, nextScope);
    await Promise.resolve();
    await Promise.resolve();
    expect(calls).toBe(1);
    expect(pool.usedBytes).toBe(0);
    finish();
    await rejected;
    const owner = await next;
    expect(owner?.value.size).toBe(3);
    expect(calls).toBe(2);
    expect(pool.usedBytes).toBeGreaterThan(0);
    owner?.release();
    nextScope.dispose();
    expect(pool.usedBytes).toBe(0);
  } finally {
    finish();
    firstScope.dispose();
    nextScope.dispose();
    await bucket.delete(path);
  }
});
