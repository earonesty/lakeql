import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { cachedObjectStore } from "./object-store-cache.js";
import { ObjectStoreJsonCache } from "./object-store-json-cache.js";
import { Lake } from "./query.js";
import { readObjectHead } from "./read-object-head.js";
import { type ObjectStore, uriObjectStore, withObjectStoreReadControls } from "./store.js";

it("routes discovery and JSON cache metadata through the owned capability", async () => {
  const backing = memoryStore();
  await backing.put("table", new Uint8Array(1));
  let reads = 0;
  const store: ObjectStore = backing;
  const head = backing.head.bind(backing);
  store.headOwned = async (path, scope) => {
    reads++;
    const metadata = await head(path);
    if (metadata === null) return null;
    return scope.allocate(4096, "metadata fixture", () => metadata).retain();
  };
  store.head = async () => {
    throw Error("legacy head");
  };
  const pool = new MemoryBudget(65536);
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        expect(pool.usedBytes).toBeGreaterThanOrEqual(4096);
        yield [{ id: 1 }];
      },
    },
  });
  expect(await lake.path("table").toArray()).toEqual([{ id: 1 }]);
  expect(pool.usedBytes).toBe(0);
  const cache = new ObjectStoreJsonCache({ store, prefix: "cache" });
  await cache.set("key", { value: [1, 2] });
  const owner = await cache.getOwnedEntry("key", pool);
  expect(owner?.value).toEqual({ value: [1, 2] });
  owner?.release();
  expect(pool.usedBytes).toBe(0);
  expect(reads).toBe(2);
});

it("normalizes owned HEAD paths and releases late results after cancellation", async () => {
  const base: ObjectStore = memoryStore();
  let finish!: () => void, enter!: () => void;
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  base.headOwned = async (path, scope) => {
    expect(path).toBe("file");
    enter();
    await pending;
    return scope.allocate(1024, "late head", () => ({ size: 10 })).retain();
  };
  const controller = new AbortController();
  const store = withObjectStoreReadControls(
    cachedObjectStore(
      uriObjectStore(base, { scheme: "s3", authority: "bucket", prefix: "prefix" }),
    ),
    { maxConcurrentReads: 1, signal: controller.signal },
  );
  const scope = new MemoryScope(new MemoryBudget(65536));
  const work = readObjectHead(store, "s3://bucket/prefix/file", scope);
  const checked = expect(work).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  await entered;
  controller.abort();
  finish();
  await checked;
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("releases independently owned metadata when scope adoption fails", async () => {
  const base: ObjectStore = memoryStore();
  const foreign = new MemoryScope(new MemoryBudget(65536));
  base.headOwned = async () => foreign.allocate(1024, "foreign head", () => ({ size: 1 })).retain();
  const target = new MemoryScope(new MemoryBudget(65536));
  await expect(readObjectHead(base, "file", target)).rejects.toMatchObject({
    code: "LAKEQL_VALIDATION_ERROR",
  });
  foreign.dispose();
  target.dispose();
  expect(foreign.budget.usedBytes).toBe(0);
  expect(target.budget.usedBytes).toBe(0);
});

it("shares configured read concurrency across default-budget store bindings", async () => {
  const base: ObjectStore = memoryStore();
  base.withDefaultMemoryBudget = () => base;
  let active = 0,
    peak = 0;
  base.headOwned = async (_path, scope) => {
    active++;
    peak = Math.max(peak, active);
    await new Promise((resolve) => setTimeout(resolve, 5));
    active--;
    return scope.allocate(64, "head", () => ({ size: 1 })).retain();
  };
  const controlled = withObjectStoreReadControls(base, { maxConcurrentReads: 1 });
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const first = controlled.withDefaultMemoryBudget?.(pool);
  const second = controlled.withDefaultMemoryBudget?.(pool);
  const owners = await Promise.all([
    first?.headOwned?.("a", scope),
    second?.headOwned?.("b", scope),
  ]);
  expect(peak).toBe(1);
  for (const owner of owners) owner?.release();
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
});
