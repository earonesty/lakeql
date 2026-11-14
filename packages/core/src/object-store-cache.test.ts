import { describe, expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { cachedObjectStore, SharedMemoryCache } from "./object-store-cache.js";
import type { ObjectStore } from "./store.js";

function countingStore(inner: ObjectStore): ObjectStore & {
  counters: { get: number; getRange: number; head: number; bytes: number };
} {
  const counters = { get: 0, getRange: 0, head: 0, bytes: 0 };
  return {
    counters,
    async get(path) {
      counters.get += 1;
      const bytes = await inner.get(path);
      counters.bytes += bytes?.byteLength ?? 0;
      return bytes;
    },
    async getRange(path, range) {
      counters.getRange += 1;
      const bytes = await inner.getRange(path, range);
      counters.bytes += bytes.byteLength;
      return bytes;
    },
    put(path, body, options) {
      return inner.put(path, body, options);
    },
    delete(path) {
      return inner.delete(path);
    },
    list(prefix, options) {
      return inner.list(prefix, options);
    },
    async head(path) {
      counters.head += 1;
      return inner.head(path);
    },
  };
}

describe("cachedObjectStore", () => {
  it("automatically reclaims cache space for queries while preserving pinned payloads", () => {
    const pool = new MemoryBudget(64);
    const cache = new SharedMemoryCache({ maxBytes: 64, memoryBudget: pool });
    cache.set("pinned", new Uint8Array(32), 32);
    const pin = cache.acquire("pinned");
    cache.set("reusable", new Uint8Array(32), 32);
    const query = pool.child();
    const allocation = query.reserve(32, "decode");
    expect(cache.get("reusable")).toBeUndefined();
    expect(pool.usedBytes).toBe(64);
    expect(query.tryReserve(1, "optional sort keys")).toBeUndefined();
    pin?.release();
    expect(pool.usedBytes).toBe(32);
    const more = query.reserve(32, "next batch");
    allocation.release();
    more.release();
    expect(pool.usedBytes).toBe(0);
    cache.set("reused", new Uint8Array(64), 64);
    const next = query.reserve(64, "new query");
    expect(cache.get("reused")).toBeUndefined();
    next.release();
  });

  it("does not evict caches for allocations larger than the query capacity", () => {
    const pool = new MemoryBudget(64);
    const cache = new SharedMemoryCache({ maxBytes: 64, memoryBudget: pool });
    cache.set("warm", new Uint8Array(64), 64);
    expect(pool.child(16).tryReserve(17, "too large")).toBeUndefined();
    expect(cache.get("warm")).toBeDefined();
    cache.delete("warm");
  });

  it("reserves before cache allocation and keeps evicted reader payloads charged", () => {
    const pool = new MemoryBudget(8);
    const cache = new SharedMemoryCache({ maxBytes: 8, memoryBudget: pool });
    expect(
      cache.setAllocated("a", 8, () => {
        expect(pool.usedBytes).toBe(8);
        return new Uint8Array(8);
      }),
    ).toBe(true);
    const reader = cache.acquire<Uint8Array>("a");
    expect(reader?.value.byteLength).toBe(8);
    cache.delete("a");
    let allocated = false;
    expect(
      cache.setAllocated("b", 1, () => {
        allocated = true;
        return 1;
      }),
    ).toBe(false);
    expect(allocated).toBe(false);
    expect(pool.usedBytes).toBe(8);
    reader?.release();
    reader?.release();
    expect(pool.usedBytes).toBe(0);
    expect(cache.setAllocated("b", 8, () => new Uint8Array(8))).toBe(true);
    cache.deleteWhere(() => true);
    expect(pool.usedBytes).toBe(0);
  });

  it("shares admission with active queries and cleans up failed cache factories", () => {
    const pool = new MemoryBudget(10);
    const query = pool.child().reserve(6, "query");
    const cache = new SharedMemoryCache({ maxBytes: 10, memoryBudget: pool });
    cache.set("a", new Uint8Array(4), 4);
    expect(() =>
      cache.setAllocated("b", 4, () => {
        throw new Error("allocation failed");
      }),
    ).toThrow("allocation failed");
    expect(pool.usedBytes).toBe(6);
    expect(cache.get("a")).toBeUndefined();
    expect(cache.acquire("missing")).toBeUndefined();
    query.release();
    expect(pool.usedBytes).toBe(0);
    expect(() => cache.set("bad", null, -1)).toThrow("Invalid cache byte count");
  });

  it("caches exact range reads defensively", async () => {
    const backing = memoryStore();
    await backing.put("data.bin", new Uint8Array([1, 2, 3, 4]));
    const counted = countingStore(backing);
    const cached = cachedObjectStore(counted);

    const first = await cached.getRange("data.bin", { offset: 1, length: 2 });
    first[0] = 9;
    const second = await cached.getRange("data.bin", { offset: 1, length: 2 });

    expect(second).toEqual(new Uint8Array([2, 3]));
    expect(counted.counters).toEqual({ get: 0, getRange: 1, head: 0, bytes: 2 });
  });

  it("evicts least-recently-used byte entries by maxBytes", async () => {
    const backing = memoryStore();
    await backing.put("data.bin", new Uint8Array([1, 2, 3, 4]));
    const counted = countingStore(backing);
    const cached = cachedObjectStore(counted, { maxBytes: 3 });

    await cached.getRange("data.bin", { offset: 0, length: 2 });
    await cached.getRange("data.bin", { offset: 2, length: 2 });
    await cached.getRange("data.bin", { offset: 0, length: 2 });

    expect(counted.counters).toEqual({ get: 0, getRange: 3, head: 0, bytes: 6 });
  });

  it("expires entries when ttlMs is set", async () => {
    const backing = memoryStore();
    await backing.put("data.bin", new Uint8Array([1, 2, 3, 4]));
    const counted = countingStore(backing);
    const cached = cachedObjectStore(counted, { ttlMs: 1 });

    await cached.getRange("data.bin", { offset: 0, length: 2 });
    await new Promise((resolve) => setTimeout(resolve, 5));
    await cached.getRange("data.bin", { offset: 0, length: 2 });

    expect(counted.counters).toEqual({ get: 0, getRange: 2, head: 0, bytes: 4 });
  });

  it("caches full objects and missing objects defensively", async () => {
    const backing = memoryStore();
    await backing.put("data.bin", new Uint8Array([1, 2, 3]));
    const counted = countingStore(backing);
    const cached = cachedObjectStore(counted);

    const first = await cached.get("data.bin");
    if (first === null) throw new Error("missing object");
    first[0] = 9;

    expect(await cached.get("data.bin")).toEqual(new Uint8Array([1, 2, 3]));
    expect(await cached.get("missing.bin")).toBeNull();
    expect(await cached.get("missing.bin")).toBeNull();
    expect(counted.counters).toEqual({ get: 2, getRange: 0, head: 0, bytes: 3 });
  });

  it("caches object heads defensively", async () => {
    const backing = memoryStore();
    await backing.put("data.bin", new Uint8Array([1, 2, 3]), { contentType: "application/bin" });
    const counted = countingStore(backing);
    const cached = cachedObjectStore(counted);

    const first = await cached.head("data.bin");
    if (first === null) throw new Error("missing head");
    first.lastModified = new Date(0);
    first.contentType = "mutated";

    const second = await cached.head("data.bin");
    expect(second?.size).toBe(3);
    expect(second?.contentType).toBe("application/bin");
    expect(second?.lastModified.getTime()).not.toBe(0);
    expect(await cached.head("missing.bin")).toBeNull();
    expect(await cached.head("missing.bin")).toBeNull();
    expect(counted.counters).toEqual({ get: 0, getRange: 0, head: 2, bytes: 0 });
  });

  it("invalidates all cached path entries around writes and deletes", async () => {
    const backing = memoryStore();
    await backing.put("data.bin", new Uint8Array([1, 2, 3, 4]));
    const counted = countingStore(backing);
    const cached = cachedObjectStore(counted);

    await cached.get("data.bin");
    await cached.getRange("data.bin", { offset: 0, length: 2 });
    await cached.head("data.bin");
    await cached.put("data.bin", new Uint8Array([5, 6, 7, 8]));

    expect(await cached.get("data.bin")).toEqual(new Uint8Array([5, 6, 7, 8]));
    expect(await cached.getRange("data.bin", { offset: 0, length: 2 })).toEqual(
      new Uint8Array([5, 6]),
    );
    expect(await cached.head("data.bin")).toMatchObject({ size: 4 });

    await cached.delete("data.bin");
    expect(await cached.get("data.bin")).toBeNull();
    expect(await cached.head("data.bin")).toBeNull();
    expect(counted.counters.get).toBe(3);
    expect(counted.counters.getRange).toBe(2);
    expect(counted.counters.head).toBe(3);
  });

  it("evicts lower-priority entries before higher-priority entries", () => {
    const cache = new SharedMemoryCache({ maxBytes: 4 });

    cache.set("decoded", "decoded", 2, { priority: 2 });
    cache.set("range", "range", 2, { priority: 3 });
    cache.set("next", "next", 2, { priority: 2 });

    expect(cache.get<string>("decoded")).toBeUndefined();
    expect(cache.get<string>("range")?.value).toBe("range");
    expect(cache.get<string>("next")?.value).toBe("next");
  });

  it("does not admit entries larger than the shared byte budget", () => {
    const cache = new SharedMemoryCache({ maxBytes: 2 });

    cache.set("too-large", "value", 3);

    expect(cache.get<string>("too-large")).toBeUndefined();
  });

  it("uses policy priorities across cached objects and ranges", async () => {
    const backing = memoryStore();
    await backing.put("object-a", new Uint8Array([1, 1]));
    await backing.put("object-b", new Uint8Array([2, 2]));
    await backing.put("object-c", new Uint8Array([3, 3]));

    const ioCounted = countingStore(backing);
    const ioCache = cachedObjectStore(ioCounted, { maxBytes: 4, policy: "io" });
    await ioCache.get("object-a");
    await ioCache.getRange("object-b", { offset: 0, length: 2 });
    await ioCache.get("object-c");
    await ioCache.get("object-a");
    await ioCache.getRange("object-b", { offset: 0, length: 2 });

    expect(ioCounted.counters.get).toBe(3);
    expect(ioCounted.counters.getRange).toBe(2);

    const latencyCounted = countingStore(backing);
    const latencyCache = cachedObjectStore(latencyCounted, { maxBytes: 4, policy: "latency" });
    await latencyCache.getRange("object-a", { offset: 0, length: 2 });
    await latencyCache.get("object-b");
    await latencyCache.getRange("object-c", { offset: 0, length: 2 });
    await latencyCache.getRange("object-a", { offset: 0, length: 2 });

    expect(latencyCounted.counters.getRange).toBe(3);
    expect(latencyCounted.counters.get).toBe(1);
  });
});

describe("cache allocation ownership", () => {
  it("retains existing buffers once and pins them beyond eviction", () => {
    const budget = new MemoryBudget(8);
    const scope = new MemoryScope(budget);
    const cache = new SharedMemoryCache({ maxBytes: 8, memoryBudget: budget });
    const owner = scope.allocate(8, "read", () => Uint8Array.of(1, 2, 3, 4, 5, 6, 7, 8));
    expect(cache.setOwned("read", owner)).toBe(true);
    scope.dispose();
    expect(budget.usedBytes).toBe(8);
    expect(budget.peakBytes).toBe(8);
    const pin = cache.acquireOwned<Uint8Array>("read");
    const borrowed = cache.acquire<Uint8Array>("read");
    cache.delete("read");
    expect(pin?.value.at(-1)).toBe(8);
    expect(budget.usedBytes).toBe(8);
    pin?.release();
    expect(budget.usedBytes).toBe(8);
    borrowed?.release();
    expect(budget.usedBytes).toBe(0);
  });

  it("reclaims unpinned owned cache entries under allocation pressure", () => {
    const budget = new MemoryBudget(8);
    const scope = new MemoryScope(budget);
    const cache = new SharedMemoryCache({ memoryBudget: budget });
    const owner = scope.allocate(8, "read", () => new Uint8Array(8));
    cache.setOwned("read", owner);
    owner.release();
    const replacement = scope.allocate(8, "operator", () => new Uint8Array(8));
    expect(cache.acquireOwned("read")).toBeUndefined();
    expect(budget.usedBytes).toBe(8);
    replacement.release();
    expect(budget.usedBytes).toBe(0);
  });

  it("rejects incompatible budgets and oversized retention without taking ownership", () => {
    const budget = new MemoryBudget(8);
    const scope = new MemoryScope(budget);
    const owner = scope.allocate(8, "read", () => new Uint8Array(8));
    expect(new SharedMemoryCache().setOwned("read", owner)).toBe(false);
    expect(
      new SharedMemoryCache({ memoryBudget: new MemoryBudget(8) }).setOwned("read", owner),
    ).toBe(false);
    expect(
      new SharedMemoryCache({ memoryBudget: budget, maxBytes: 7 }).setOwned("read", owner),
    ).toBe(false);
    scope.dispose();
    expect(budget.usedBytes).toBe(0);
  });

  it("replaces owned entries safely and does not invent ownership for ordinary entries", () => {
    const budget = new MemoryBudget(16);
    const scope = new MemoryScope(budget);
    const cache = new SharedMemoryCache({ memoryBudget: budget });
    const first = scope.allocate(8, "first", () => new Uint8Array(8));
    const second = scope.allocate(8, "second", () => new Uint8Array(8));
    cache.setOwned("key", first);
    first.release();
    cache.setOwned("key", second);
    second.release();
    expect(budget.usedBytes).toBe(8);
    cache.set("key", Uint8Array.of(1), 1);
    expect(budget.usedBytes).toBe(1);
    expect(cache.acquireOwned("key")).toBeUndefined();
    cache.delete("key");
    expect(budget.usedBytes).toBe(0);
  });
});

it("preserves owned reads through caching and charges defensive output copies", async () => {
  const budget = new MemoryBudget(2048);
  const scope = new MemoryScope(budget);
  const base = countingStore(memoryStore());
  await base.put("file", Uint8Array.of(1, 2, 3, 4));
  let reads = 0;
  const inner: ObjectStore = {
    ...base,
    async getRangeOwned(path, range, scope) {
      reads += 1;
      return scope.allocateAsync(range.length, "source range", () => base.getRange(path, range));
    },
  };
  const cache = new SharedMemoryCache({ memoryBudget: budget });
  const store = cachedObjectStore(inner, {}, cache);
  const first = await store.getRangeOwned?.("file", { offset: 0, length: 4 }, scope);
  const withOutput = budget.usedBytes;
  expect(withOutput).toBeGreaterThan(8);
  first?.value.fill(9);
  first?.release();
  const second = await store.getRangeOwned?.("file", { offset: 0, length: 4 }, scope);
  expect(second?.value).toEqual(Uint8Array.of(1, 2, 3, 4));
  expect(budget.usedBytes).toBe(withOutput);
  expect(reads).toBe(1);
  second?.release();
  await store.put("file", Uint8Array.of(5, 6, 7, 8));
  expect(budget.usedBytes).toBe(0);
  const third = await store.getRangeOwned?.("file", { offset: 0, length: 4 }, scope);
  expect(third?.value).toEqual(Uint8Array.of(5, 6, 7, 8));
  expect(reads).toBe(2);
  scope.dispose();
  cache.deleteWhere(() => true);
  expect(budget.usedBytes).toBe(0);
});

it("releases a cache pin when defensive-copy admission fails", async () => {
  const budget = new MemoryBudget(6);
  const cache = new SharedMemoryCache({ memoryBudget: budget });
  const base = countingStore(memoryStore());
  const store = cachedObjectStore(
    {
      ...base,
      async getRangeOwned(_path, range, scope) {
        return scope.allocate(range.length, "read", () => new Uint8Array(range.length));
      },
    },
    {},
    cache,
  );
  const scope = new MemoryScope(budget);
  const first = await store.getRangeOwned?.("a", { offset: 0, length: 4 }, scope);
  // The cache cannot retain a separate defensive copy alongside the first output.
  expect(budget.usedBytes).toBe(4);
  first?.release();
  cache.set("range:a:0:4", { kind: "range", path: "a", value: new Uint8Array(4) }, 4);
  await expect(store.getRangeOwned?.("a", { offset: 0, length: 4 }, scope)).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  // Reclamation may evict the cache record, but the pin stays charged until unwind.
  expect(budget.usedBytes).toBe(0);
  expect(budget.peakBytes).toBe(4);
});

it("reclaims cached child bytes when a sibling needs their shared parent pool", () => {
  const pool = new MemoryBudget(8);
  const cacheBudget = pool.child(16);
  const queryBudget = pool.child(16);
  const cache = new SharedMemoryCache({ memoryBudget: cacheBudget });
  cache.setAllocated("cached", 8, () => new Uint8Array(8));
  const query = queryBudget.reserve(8, "sibling query");
  expect(cache.get("cached")).toBeUndefined();
  expect(cacheBudget.usedBytes).toBe(0);
  expect(queryBudget.usedBytes).toBe(8);
  expect(pool.usedBytes).toBe(8);
  query.release();
  expect(pool.usedBytes).toBe(0);
});

it("reuses cached owned backing across jobs while enforcing each reader's cap", () => {
  const pool = new MemoryBudget(8);
  const producer = new MemoryScope(pool.child(8));
  const cache = new SharedMemoryCache({ maxBytes: 8, memoryBudget: pool });
  const original = producer.allocate(8, "decoded backing", () => new Uint8Array(8).fill(7));
  expect(cache.setOwned("data", original)).toBe(true);
  producer.dispose();
  expect(producer.budget.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(8);
  const tooSmall = pool.child(7);
  expect(() => cache.acquireOwned("data", tooSmall)).toThrow("Allocation exceeds memory budget");
  expect(tooSmall.usedBytes).toBe(0);
  const consumer = new MemoryScope(pool.child(8));
  const hit = cache.acquireOwned<Uint8Array>("data", consumer.budget);
  if (hit === undefined) throw new Error("Missing owned cache hit");
  const adopted = consumer.adopt(hit);
  expect(adopted.value[0]).toBe(7);
  expect(consumer.budget.usedBytes).toBe(8);
  expect(pool.usedBytes).toBe(8);
  expect(pool.peakBytes).toBe(8);
  cache.delete("data");
  expect(pool.usedBytes).toBe(8);
  consumer.dispose();
  expect(pool.usedBytes).toBe(0);
});

it.each([64, 128])("bounds real shared buffers at %i MiB across two reader jobs", (mib) => {
  const capacity = mib * 1024 * 1024;
  const backingBytes = capacity / 2;
  const outputBytes = capacity / 4;
  const pool = new MemoryBudget(capacity);
  const producer = new MemoryScope(pool.child(backingBytes));
  const cache = new SharedMemoryCache({ maxBytes: backingBytes, memoryBudget: pool });
  const backing = producer.allocate(backingBytes, "shared input", () =>
    new Uint8Array(backingBytes).fill(17),
  );
  expect(cache.setOwned("input", backing)).toBe(true);
  producer.dispose();
  expect(producer.budget.usedBytes).toBe(0);
  const readers = [
    new MemoryScope(pool.child(backingBytes + outputBytes)),
    new MemoryScope(pool.child(backingBytes + outputBytes)),
  ];
  for (const [index, reader] of readers.entries()) {
    const hit = cache.acquireOwned<Uint8Array>("input", reader.budget);
    if (hit === undefined) throw new Error("Missing shared input");
    reader.adopt(hit);
    expect(hit.value.at(-1)).toBe(17);
    const output = reader.allocate(outputBytes, "reader output", () =>
      new Uint8Array(outputBytes).fill(index + 1),
    );
    expect(output.value.at(-1)).toBe(index + 1);
    expect(reader.budget.usedBytes).toBe(backingBytes + outputBytes);
  }
  expect(pool.usedBytes).toBe(capacity);
  expect(pool.peakBytes).toBe(capacity);
  expect(() => cache.acquireOwned("input", pool.child(backingBytes - 1))).toThrow(
    "Allocation exceeds memory budget",
  );
  let allocated = false;
  const extra = new MemoryScope(pool.child());
  expect(() =>
    extra.allocate(1, "overflow", () => {
      allocated = true;
      return new Uint8Array(1);
    }),
  ).toThrow("Allocation exceeds memory budget");
  expect(allocated).toBe(false);
  // Pressure can evict cache ownership, but cannot release either reader's pin.
  expect(cache.acquireOwned("input")).toBeUndefined();
  expect(pool.usedBytes).toBe(capacity);
  readers[0]?.dispose();
  expect(pool.usedBytes).toBe(backingBytes + outputBytes);
  readers[1]?.dispose();
  extra.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("forecasts speculative capacity by priority without allocating or evicting live entries", () => {
  const root = new MemoryBudget(32);
  const cache = new SharedMemoryCache({ maxBytes: 16, memoryBudget: root });
  cache.set("high", "decoded", 8, { priority: 4 });
  cache.set("peer", "range", 4, { priority: 1 });
  expect(cache.availableAdmissionBytes()).toBe(8);
  expect(cache.availableAdmissionBytes(4)).toBe(16);
  expect(root.usedBytes).toBe(12);
  expect(cache.get("high")?.value).toBe("decoded");
  expect(cache.get("peer")?.value).toBe("range");
  cache.deleteWhere(() => true);
  expect(root.usedBytes).toBe(0);
});

it("drops expired backing when forecasting admission capacity", () => {
  const root = new MemoryBudget(8);
  const cache = new SharedMemoryCache({ maxBytes: 8, memoryBudget: root, ttlMs: 0 });
  cache.set("expired", "decoded", 8, { priority: 4 });
  expect(cache.availableAdmissionBytes()).toBe(8);
  expect(root.usedBytes).toBe(0);
  expect(cache.get("expired")).toBeUndefined();
});

it("uses owned entry eviction cost without misreporting its physical backing charge", () => {
  const root = new MemoryBudget(100);
  const scope = new MemoryScope(root);
  const owner = scope.allocate(8, "backing", () => new Uint8Array(8));
  const cache = new SharedMemoryCache({ maxBytes: 32, memoryBudget: root });
  expect(cache.setOwned("metadata", owner, { priority: 4, cacheBytes: 24 })).toBe(true);
  expect(cache.get("metadata")?.bytes).toBe(24);
  expect(cache.availableAdmissionBytes(1)).toBe(8);
  expect(root.usedBytes).toBe(8);
  const pin = cache.acquireOwned("metadata");
  expect(pin?.bytes).toBe(8);
  expect(cache.setOwned("too-large", owner, { cacheBytes: 33 })).toBe(false);
  expect(() => cache.setOwned("invalid", owner, { cacheBytes: 7 })).toThrow();
  expect(() => cache.setOwned("invalid", owner, { cacheBytes: NaN })).toThrow();
  scope.dispose();
  cache.deleteWhere(() => true);
  expect(root.usedBytes).toBe(8);
  pin?.release();
  expect(root.usedBytes).toBe(0);
});

it("forecasts cold cache copies and preserves exact-buffer layout", async () => {
  const inner = memoryStore();
  await inner.put("file", Uint8Array.of(1, 2, 3, 4));
  const pool = new MemoryBudget(2048);
  const cache = new SharedMemoryCache({ maxBytes: 1024, memoryBudget: pool });
  const store = cachedObjectStore(inner, { maxBytes: 1024 }, cache);
  const forecast = store.getRangeOwnedAllocation?.("file", { offset: 0, length: 4 });
  expect(forecast?.output).toBe("exact-buffer");
  expect(forecast?.bytes).toBeGreaterThan(8);
  for (let pass = 0; pass < 2; pass++) {
    const scope = new MemoryScope(pool.child(2048));
    const owner = await store.getRangeOwned?.("file", { offset: 0, length: 4 }, scope);
    expect(owner?.value).toEqual(Uint8Array.of(1, 2, 3, 4));
    expect(owner?.value.buffer.byteLength).toBe(4);
    expect(pool.usedBytes).toBe(forecast?.bytes);
    owner?.release();
    scope.dispose();
    expect(pool.usedBytes).toBe((forecast?.bytes ?? 0) - 4);
  }
  cache.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(0);
  expect(pool.peakBytes).toBe(forecast?.bytes);
});

it("bounds cached HEAD metadata including negative results and transport hints", async () => {
  const backing = memoryStore();
  let heads = 0;
  backing.head = async (path) => {
    heads++;
    return path === "missing" ? null : { size: 10, rangeReadMode: "emulated" };
  };
  const pool = new MemoryBudget(65536);
  const shared = new SharedMemoryCache({ memoryBudget: pool });
  const store = cachedObjectStore(backing, {}, shared);
  expect(await store.head("file")).toMatchObject({ rangeReadMode: "emulated" });
  const charged = pool.usedBytes;
  expect(charged).toBeGreaterThan(0);
  expect(await store.head("file")).toMatchObject({ rangeReadMode: "emulated" });
  expect(heads).toBe(1);
  await store.head("missing");
  expect(pool.usedBytes).toBeGreaterThan(charged);
  shared.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(0);
  for (const maxBytes of [0, charged - 1, charged]) {
    heads = 0;
    const limited = cachedObjectStore(backing, { maxBytes });
    await limited.head("file");
    await limited.head("file");
    expect(heads).toBe(maxBytes === charged ? 1 : 2);
  }
});

it("bounds missing objects, empty objects and empty ranges by metadata capacity", async () => {
  const backing = memoryStore();
  await backing.put("empty", new Uint8Array());
  const pool = new MemoryBudget(65536);
  const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 2048 });
  const counted = countingStore(backing);
  const store = cachedObjectStore(counted, {}, shared);
  for (let i = 0; i < 100; i++) await store.get(`missing-${i}`);
  expect(pool.usedBytes).toBeGreaterThan(0);
  expect(pool.usedBytes).toBeLessThanOrEqual(2048);
  counted.counters.get = 0;
  await store.get("missing-0");
  expect(counted.counters.get).toBe(1);
  shared.deleteWhere(() => true);
  await store.get("empty");
  expect(pool.usedBytes).toBeGreaterThan(0);
  shared.deleteWhere(() => true);
  await store.getRange("empty", { offset: 0, length: 0 });
  expect(pool.usedBytes).toBeGreaterThan(0);
  shared.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(0);
  const disabled = cachedObjectStore(counted, { maxBytes: 0 });
  counted.counters.get = 0;
  await disabled.get("missing");
  await disabled.get("missing");
  expect(counted.counters.get).toBe(2);
});

it("invalidates rejected replacements while preserving already acquired pins", () => {
  const pool = new MemoryBudget(64);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 8 });
  cache.set("key", "old", 4);
  const pin = cache.acquire<string>("key");
  let allocated = false;
  expect(
    cache.setAllocated("key", 9, () => {
      allocated = true;
      return "new";
    }),
  ).toBe(false);
  expect(allocated).toBe(false);
  expect(cache.get("key")).toBeUndefined();
  expect(pin?.value).toBe("old");
  expect(pool.usedBytes).toBe(4);
  pin?.release();
  expect(pool.usedBytes).toBe(0);
  const scope = new MemoryScope(pool);
  const small = scope.allocate(4, "small", () => "old");
  const large = scope.allocate(9, "large", () => "new");
  expect(cache.setOwned("key", small)).toBe(true);
  expect(cache.setOwned("key", large)).toBe(false);
  expect(cache.acquireOwned("key")).toBeUndefined();
  expect(large.value).toBe("new");
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
});
