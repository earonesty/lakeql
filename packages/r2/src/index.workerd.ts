/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { env } from "cloudflare:test";
import {
  MATERIALIZATION_CHARGES,
  MemoryBudget,
  MemoryScope,
  MemoryStageScheduler,
  materializeOwnedBatchRows,
  ObjectStoreJsonCache,
  retainContainerPayload,
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

it("keeps a native R2 range charged through settlement when another owner throws on disposal", async () => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const path = "cleanup-failure-range";
  await bucket.put(
    path,
    Uint8Array.from({ length: 256 }, (_, index) => index),
  );
  let enter!: () => void;
  let finish!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const delayed: R2BucketLike = {
    head: bucket.head.bind(bucket),
    put: bucket.put.bind(bucket),
    delete: bucket.delete.bind(bucket),
    list: bucket.list.bind(bucket),
    async get(key, options) {
      const object = await bucket.get(key, options);
      enter();
      await gate;
      return object;
    },
  };
  const scope = new MemoryScope(new MemoryBudget(256));
  const original = scope.allocate(8, "throwing owner", () => ({})).retain();
  scope.adopt({
    value: original.value,
    bytes: original.bytes,
    budget: original.budget,
    retain: original.retain,
    retainIn: original.retainIn,
    release() {
      original.release();
      throw new Error("cleanup failure");
    },
  });
  scope.allocate(8, "later owner", () => ({}));
  const pending = new R2ObjectStore(delayed).getRangeOwned(
    path,
    { offset: 64, length: 128 },
    scope,
  );
  const rejected = expect(pending).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  try {
    await entered;
    expect(() => scope.dispose()).toThrow("cleanup failure");
    expect(scope.signal.aborted).toBe(true);
    expect(scope.budget.usedBytes).toBe(128);
    finish();
    await rejected;
    expect(scope.budget.usedBytes).toBe(0);
  } finally {
    finish();
    scope.dispose();
    await bucket.delete(path);
  }
});

it("advances queued native R2 reads after scheduled output cleanup fails", async () => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const path = "scheduled-cleanup-range";
  const bytes = Uint8Array.from({ length: 256 }, (_, index) => index);
  await bucket.put(path, bytes);
  let enter!: () => void;
  let finish!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const delayed: R2BucketLike = {
    head: bucket.head.bind(bucket),
    put: bucket.put.bind(bucket),
    delete: bucket.delete.bind(bucket),
    list: bucket.list.bind(bucket),
    async get(key, options) {
      const object = await bucket.get(key, options);
      enter();
      await gate;
      return object;
    },
  };
  const pool = new MemoryBudget(136);
  const scheduler = new MemoryStageScheduler(pool);
  const firstStore = new R2ObjectStore(delayed);
  const nextStore = new R2ObjectStore(bucket);
  const first = scheduler.run({
    budget: pool.child(136),
    bytes: 136,
    purpose: "first R2 read",
    async execute(scope) {
      const original = scope.allocate(8, "throwing owner", () => ({})).retain();
      scope.adopt({
        value: original.value,
        bytes: original.bytes,
        budget: original.budget,
        retain: original.retain,
        retainIn: original.retainIn,
        release() {
          original.release();
          throw new Error("scheduled cleanup failure");
        },
      });
      return (await firstStore.getRangeOwned(path, { offset: 32, length: 128 }, scope)).value;
    },
  });
  const rejected = expect(first).rejects.toThrow("scheduled cleanup failure");
  try {
    await entered;
    let nextStarted = false;
    const next = scheduler.run({
      budget: pool.child(128),
      bytes: 128,
      purpose: "next R2 read",
      async execute(scope) {
        nextStarted = true;
        return (await nextStore.getRangeOwned(path, { offset: 64, length: 128 }, scope)).value;
      },
    });
    expect(nextStarted).toBe(false);
    expect(scheduler.queuedStages).toBe(1);
    finish();
    await rejected;
    const result = await next;
    expect([...result.value]).toEqual([...bytes.slice(64, 192)]);
    result.release();
    expect(pool.usedBytes).toBe(0);
    expect(scheduler.activeStages).toBe(0);
    expect(scheduler.queuedStages).toBe(0);
  } finally {
    finish();
    await bucket.delete(path);
  }
});

it.each([
  false,
  true,
])("preserves native R2 binary output when container-transfer pressure=%s", async (pressure) => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const path = `container-transfer-${pressure}`;
  await store.put(path, Uint8Array.of(9, 1, 2, 3, 8));
  const pool = new MemoryBudget(4096);
  const source = new MemoryScope(pool);
  const target = new MemoryScope(pool);
  let output: ReturnType<typeof materializeOwnedBatchRows> | undefined;
  let pinned: ReturnType<MemoryBudget["reserve"]> | undefined;
  try {
    const range = await store.getRangeOwned(path, { offset: 1, length: 3 }, source);
    const input = source.retainAllocations({
      rowCount: 1,
      columns: {
        bytes: { type: "binary-view" as const, data: range.value, fixedLength: 3, length: 1 },
      },
    });
    try {
      output = materializeOwnedBatchRows(input);
    } finally {
      input.release();
      source.dispose();
    }
    const expected = output.value[0]?.bytes;
    const retainedBytes = pool.usedBytes;
    if (pressure) pinned = pool.reserve(pool.maxBytes - pool.usedBytes, "other job pinned output");
    retainContainerPayload(output, target);
    output.release();
    expect(expected).toEqual(Uint8Array.of(1, 2, 3));
    expect(pool.usedBytes).toBe(
      (pinned?.bytes ?? 0) +
        retainedBytes -
        (pressure ? 0 : MATERIALIZATION_CHARGES.array + MATERIALIZATION_CHARGES.arraySlot),
    );
    target.dispose();
    expect(pool.usedBytes).toBe(pinned?.bytes ?? 0);
  } finally {
    output?.release();
    source.dispose();
    target.dispose();
    pinned?.release();
    await store.delete(path);
  }
  expect(pool.usedBytes).toBe(0);
});
