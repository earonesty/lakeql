import { describe, expect, it } from "vitest";
import { memoryStore } from "./memory-store.js";
import { cachedObjectStore } from "./object-store-cache.js";
import type { ObjectStore } from "./store.js";

function countingStore(inner: ObjectStore): ObjectStore & {
  counters: { getRange: number; bytes: number };
} {
  const counters = { getRange: 0, bytes: 0 };
  return {
    counters,
    get(path) {
      return inner.get(path);
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
    head(path) {
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
    expect(counted.counters).toEqual({ getRange: 1, bytes: 2 });
  });

  it("evicts least-recently-used byte entries by maxBytes", async () => {
    const backing = memoryStore();
    await backing.put("data.bin", new Uint8Array([1, 2, 3, 4]));
    const counted = countingStore(backing);
    const cached = cachedObjectStore(counted, { maxBytes: 3 });

    await cached.getRange("data.bin", { offset: 0, length: 2 });
    await cached.getRange("data.bin", { offset: 2, length: 2 });
    await cached.getRange("data.bin", { offset: 0, length: 2 });

    expect(counted.counters).toEqual({ getRange: 3, bytes: 6 });
  });

  it("expires entries when ttlMs is set", async () => {
    const backing = memoryStore();
    await backing.put("data.bin", new Uint8Array([1, 2, 3, 4]));
    const counted = countingStore(backing);
    const cached = cachedObjectStore(counted, { ttlMs: 1 });

    await cached.getRange("data.bin", { offset: 0, length: 2 });
    await new Promise((resolve) => setTimeout(resolve, 5));
    await cached.getRange("data.bin", { offset: 0, length: 2 });

    expect(counted.counters).toEqual({ getRange: 2, bytes: 4 });
  });
});
