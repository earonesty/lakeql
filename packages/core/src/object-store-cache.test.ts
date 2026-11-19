import { describe, expect, it, vi } from "vitest";
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

  it("revalidates object heads and returns defensive copies", async () => {
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
    expect(counted.counters).toEqual({ get: 0, getRange: 0, head: 4, bytes: 0 });
  });

  it("revalidates external same-size overwrites and deletion before reusing cached bytes", async () => {
    const backing = memoryStore();
    await backing.put("data", Uint8Array.from([1, 2, 3]));
    const cache = cachedObjectStore(backing);
    const first = await cache.head("data");
    expect(await cache.getRange("data", { offset: 0, length: 3 })).toEqual(
      Uint8Array.from([1, 2, 3]),
    );
    await backing.put("data", Uint8Array.from([4, 5, 6]));
    expect((await cache.head("data"))?.etag).not.toBe(first?.etag);
    expect(await cache.getRange("data", { offset: 0, length: 3 })).toEqual(
      Uint8Array.from([4, 5, 6]),
    );
    await backing.delete("data");
    expect(await cache.head("data")).toBeNull();
    expect(await cache.get("data")).toBeNull();
  });

  it("invalidates old byte entries when a fresh listing observes a new version", async () => {
    const backing = memoryStore();
    await backing.put("data", Uint8Array.from([1, 2]));
    const cache = cachedObjectStore(backing);
    for await (const _object of cache.list("")) {
      /* establish identity */
    }
    await cache.getRange("data", { offset: 0, length: 2 });
    await backing.put("data", Uint8Array.from([3, 4]));
    for await (const _object of cache.list("")) {
      /* refresh identity */
    }
    expect(await cache.getRange("data", { offset: 0, length: 2 })).toEqual(Uint8Array.from([3, 4]));
  });

  it("does not reuse unvalidated byte entries when their earlier head expired", async () => {
    vi.useFakeTimers();
    try {
      const backing = memoryStore();
      await backing.put("data", Uint8Array.from([1, 2]));
      const cache = cachedObjectStore(backing, { ttlMs: 10 });
      await cache.head("data");
      vi.advanceTimersByTime(9);
      await cache.getRange("data", { offset: 0, length: 2 });
      vi.advanceTimersByTime(2);
      await backing.put("data", Uint8Array.from([3, 4]));
      await cache.head("data");
      expect(await cache.getRange("data", { offset: 0, length: 2 })).toEqual(
        Uint8Array.from([3, 4]),
      );
    } finally {
      vi.useRealTimers();
    }
  });

  it("charges observed validator records to the same byte budget", async () => {
    const backing = memoryStore();
    for (let index = 0; index < 50; index++)
      await backing.put(`data/${index}`, Uint8Array.from([index]));
    const shared = new SharedMemoryCache({ maxBytes: 1024 });
    const cache = cachedObjectStore(backing, { maxBytes: 1024 }, shared);
    for await (const _object of cache.list("data/")) {
      /* observe validators */
    }
    expect(
      shared.find(
        (value) =>
          typeof value === "object" && value !== null && "kind" in value && value.kind === "head",
      ),
    ).toBeDefined();
    shared.set("full-budget", new Uint8Array(1024), 1024, { priority: 4 });
    expect(
      shared.find(
        (value) =>
          typeof value === "object" && value !== null && "kind" in value && value.kind === "head",
      ),
    ).toBeUndefined();
  });

  it.each([
    { method: "get", separateObserver: false },
    { method: "getRange", separateObserver: false },
    { method: "get", separateObserver: true },
    { method: "getRange", separateObserver: true },
  ] as const)("fences late $method bytes across invalidation (separate observer=$separateObserver)", async ({
    method,
    separateObserver,
  }) => {
    const backing = memoryStore();
    await backing.put("data", Uint8Array.from([1, 2]));
    const store = countingStore(backing);
    let release: (() => void) | undefined;
    let captured: (() => void) | undefined;
    const capturedRead = new Promise<void>((resolve) => {
      captured = resolve;
    });
    let delayed = true;
    const read =
      method === "get"
        ? backing.get.bind(backing)
        : (path: string) => backing.getRange(path, { offset: 0, length: 2 });
    async function source(path: string) {
      const value = await read(path);
      if (delayed) {
        delayed = false;
        captured?.();
        await new Promise<void>((resolve) => {
          release = resolve;
        });
      }
      return value;
    }
    if (method === "get") store.get = source;
    else
      store.getRange = async (path) => {
        const value = await source(path);
        if (value === null) throw Error("missing");
        return value;
      };
    const shared = new SharedMemoryCache({ maxBytes: 1024 });
    const cache = cachedObjectStore(store, {}, shared);
    const observer = separateObserver ? cachedObjectStore(backing, {}, shared) : cache;
    await cache.head("data");
    const fetch = () =>
      method === "get" ? cache.get("data") : cache.getRange("data", { offset: 0, length: 2 });
    const old = fetch();
    await capturedRead;
    await backing.put("data", Uint8Array.from([3, 4]));
    await observer.head("data");
    expect(await fetch()).toEqual(Uint8Array.from([3, 4]));
    release?.();
    expect(await old).toEqual(Uint8Array.from([1, 2]));
    expect(await fetch()).toEqual(Uint8Array.from([3, 4]));
  });

  it("does not treat an unchanged timestamp as a strong byte validator", async () => {
    const backing = memoryStore();
    await backing.put("data", Uint8Array.from([1, 2]));
    const source = countingStore(backing);
    const original = source.head.bind(source);
    source.head = async (path) => {
      const head = await original(path);
      if (head !== null) {
        delete head.etag;
        head.lastModified = new Date(0);
      }
      return head;
    };
    const cache = cachedObjectStore(source);
    await cache.head("data");
    await cache.getRange("data", { offset: 0, length: 2 });
    await backing.put("data", Uint8Array.from([3, 4]));
    await cache.head("data");
    expect(await cache.getRange("data", { offset: 0, length: 2 })).toEqual(Uint8Array.from([3, 4]));
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

describe("SharedMemoryCache.find", () => {
  it("touches the matched entry for LRU eviction without retaining another index", () => {
    const cache = new SharedMemoryCache({ maxBytes: 4 });
    cache.set("first", "one", 2);
    cache.set("second", "two", 2);
    expect(cache.find<string>((value, key) => key === "first" && value === "one")?.value).toBe(
      "one",
    );
    cache.set("third", "three", 2);
    expect(cache.get("second")).toBeUndefined();
    expect(cache.get("first")?.value).toBe("one");
    cache.delete("first");
    cache.delete("third");
    expect(cache.find(() => true)).toBeUndefined();
  });

  it("never returns expired entries", () => {
    vi.useFakeTimers();
    try {
      const cache = new SharedMemoryCache({ maxBytes: 4, ttlMs: 10 });
      cache.set("expired", "old", 2);
      vi.advanceTimersByTime(11);
      cache.set("live", "new", 2);
      const visited: string[] = [];
      expect(
        cache.find<string>((_value, key) => {
          visited.push(key);
          return true;
        })?.value,
      ).toBe("new");
      expect(visited).toEqual(["live"]);
      expect(cache.get("expired")).toBeUndefined();
    } finally {
      vi.useRealTimers();
    }
  });
});
