import { MemoryBudget, MemoryScope, SharedMemoryCache } from "lakeql-core";
import { describe, expect, it, vi } from "vitest";
import { cachedRangeBuffer } from "./range-cache.js";
import type { StoreAsyncBuffer } from "./types.js";

describe("cachedRangeBuffer", () => {
  it.each([false, true])("bounds tiny-range read-ahead callbacks, owned=%s", async (owned) => {
    const gate = Promise.withResolvers<void>();
    let started = 0;
    let active = 0;
    let peak = 0;
    const read = async (start: number) => {
      started++;
      active++;
      peak = Math.max(peak, active);
      await gate.promise;
      active--;
      return Uint8Array.of(start).buffer;
    };
    const file: StoreAsyncBuffer = {
      byteLength: 128,
      slice: async (start) => await read(start),
      ...(owned
        ? ({
            sliceOwned: async (start, _end, scope) =>
              await scope.allocateAsync(1, "test range", async () => await read(start)),
          } satisfies Partial<StoreAsyncBuffer>)
        : {}),
    };
    const cache = cachedRangeBuffer(
      file,
      {
        maxBytes: 64,
        maxEntryBytes: 1,
        coalesceBytes: 0,
        maxConcurrentPrefetchReads: 2,
      },
      "tiny",
    );
    const pool = new MemoryBudget(128);
    const scope = new MemoryScope(pool);
    const ranges = Array.from({ length: 64 }, (_, index) => ({
      start: index * 2,
      end: index * 2 + 1,
    }));
    const prefetch = owned ? cache.prefetchOwned?.(ranges, scope) : cache.prefetch?.(ranges);
    await expect.poll(() => started).toBe(2);
    expect(active).toBe(2);
    gate.resolve();
    await prefetch;
    expect(started).toBe(64);
    expect(peak).toBe(2);
    if (owned) {
      const hit = await cache.sliceOwned?.(42, 43, scope);
      expect(new Uint8Array(hit?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(42));
      hit?.release();
    } else {
      expect(new Uint8Array(await cache.slice(42, 43))).toEqual(Uint8Array.of(42));
    }
    expect(started).toBe(64);
    scope.dispose();
  });

  it.each([0, -1, 0.5, Infinity, NaN])("rejects invalid prefetch concurrency %s", (limit) => {
    expect(() =>
      cachedRangeBuffer(
        { byteLength: 0, slice: async () => new ArrayBuffer(0) },
        { maxBytes: 0, maxConcurrentPrefetchReads: limit },
        "invalid",
      ),
    ).toThrow();
  });

  it("pins shared cache hits across async handoff and releases after copying", async () => {
    const pool = new MemoryBudget(8);
    const sharedCache = new SharedMemoryCache({ maxBytes: 8, memoryBudget: pool });
    const cache = cachedRangeBuffer(
      {
        byteLength: 8,
        async slice() {
          return new ArrayBuffer(8);
        },
      },
      { maxBytes: 8, coalesceBytes: 8, sharedCache },
      "pins",
    );
    await cache.prefetch?.([{ start: 0, end: 2 }]);
    const output = cache.slice(0, 2);
    sharedCache.deleteWhere(() => true);
    expect(pool.usedBytes).toBe(8);
    await expect(output).resolves.toHaveProperty("byteLength", 2);
    expect(pool.usedBytes).toBe(0);
  });

  it.each([false, true])("prefetches without discarded copies (shared=%s)", async (shared) => {
    const payloads: ArrayBuffer[] = [];
    const copies = vi.fn();
    const file: StoreAsyncBuffer = {
      byteLength: 16,
      async slice(start, end = 16) {
        const payload = new Uint8Array(end - start).fill(start).buffer;
        const slice = payload.slice.bind(payload);
        vi.spyOn(payload, "slice").mockImplementation((a, b) => {
          copies();
          return slice(a, b);
        });
        payloads.push(payload);
        return payload;
      },
    };
    const cache = cachedRangeBuffer(
      file,
      {
        maxBytes: 16,
        coalesceBytes: 8,
        ...(shared ? { sharedCache: new SharedMemoryCache({ maxBytes: 16 }) } : {}),
      },
      "copy-test",
    );
    await cache.prefetch?.([
      { start: 0, end: 2 },
      { start: 3, end: 4 },
      { start: 8, end: 10 },
    ]);
    expect(payloads).toHaveLength(2);
    expect(copies).not.toHaveBeenCalled();
    const output = new Uint8Array(await cache.slice(8, 10));
    expect([...output]).toEqual([8, 8]);
    output[0] = 99;
    expect([...new Uint8Array(await cache.slice(8, 10))]).toEqual([8, 8]);
    expect(copies).toHaveBeenCalledTimes(2);
  });

  it.each([
    false,
    true,
  ])("retries failed shared prefetch and demand reads (shared=%s)", async (shared) => {
    let calls = 0;
    const file: StoreAsyncBuffer = {
      byteLength: 8,
      async slice() {
        calls += 1;
        if (calls === 1) throw new Error("network failure");
        return new ArrayBuffer(8);
      },
    };
    const cache = cachedRangeBuffer(
      file,
      {
        maxBytes: 8,
        coalesceBytes: 8,
        ...(shared ? { sharedCache: new SharedMemoryCache({ maxBytes: 8 }) } : {}),
      },
      "failure-test",
    );
    const results = await Promise.allSettled([
      cache.prefetch?.([{ start: 0, end: 4 }]),
      cache.slice(2, 4),
    ]);
    expect(results.map((result) => result.status)).toEqual(["rejected", "rejected"]);
    await expect(cache.slice(0, 4)).resolves.toHaveProperty("byteLength", 4);
    expect(calls).toBe(2);
  });

  it("reuses exact byte ranges within a scan", async () => {
    let slices = 0;
    const source = new Uint8Array([1, 2, 3, 4, 5, 6]);
    const file: StoreAsyncBuffer = {
      byteLength: source.byteLength,
      async slice(start, end) {
        slices += 1;
        return source.buffer.slice(start, end);
      },
    };
    const cached = cachedRangeBuffer(file, { maxBytes: 6, coalesceBytes: 0 }, "local");

    await expect(bytes(cached.slice(1, 4))).resolves.toEqual([2, 3, 4]);
    await expect(bytes(cached.slice(1, 4))).resolves.toEqual([2, 3, 4]);
    await expect(bytes(cached.slice(2, 5))).resolves.toEqual([3, 4, 5]);

    expect(slices).toBe(2);
  });

  it("coalesces adjacent small ranges into one cached window", async () => {
    let slices = 0;
    const source = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
    const file: StoreAsyncBuffer = {
      byteLength: source.byteLength,
      async slice(start, end) {
        slices += 1;
        return source.buffer.slice(start, end);
      },
    };
    const cached = cachedRangeBuffer(file, { maxBytes: 8, coalesceBytes: 4 }, "local");

    await expect(bytes(cached.slice(1, 2))).resolves.toEqual([2]);
    await expect(bytes(cached.slice(2, 4))).resolves.toEqual([3, 4]);
    await expect(bytes(cached.slice(5, 6))).resolves.toEqual([6]);

    expect(slices).toBe(2);
  });

  it("shares an in-flight coalesced range read", async () => {
    let slices = 0;
    const source = new Uint8Array([1, 2, 3, 4]);
    let resolveRead: (() => void) | undefined;
    const file: StoreAsyncBuffer = {
      byteLength: source.byteLength,
      async slice(start, end) {
        slices += 1;
        await new Promise<void>((resolve) => {
          resolveRead = resolve;
        });
        return source.buffer.slice(start, end);
      },
    };
    const cached = cachedRangeBuffer(file, { maxBytes: 4, coalesceBytes: 4 }, "local");

    const first = bytes(cached.slice(0, 1));
    const second = bytes(cached.slice(1, 3));
    resolveRead?.();

    await expect(first).resolves.toEqual([1]);
    await expect(second).resolves.toEqual([2, 3]);
    expect(slices).toBe(1);
  });

  it("prefetches retainable coalesced ranges concurrently and serves later slices from cache", async () => {
    const source = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
    let slices = 0;
    let active = 0;
    let peakActive = 0;
    const file: StoreAsyncBuffer = {
      byteLength: source.byteLength,
      async slice(start, end) {
        slices += 1;
        active += 1;
        peakActive = Math.max(peakActive, active);
        await Promise.resolve();
        active -= 1;
        return source.buffer.slice(start, end);
      },
    };
    const cached = cachedRangeBuffer(file, { maxBytes: 8, coalesceBytes: 4 }, "prefetch");

    await cached.prefetch?.([
      { start: 1, end: 2 },
      { start: 2, end: 3 },
      { start: 5, end: 6 },
    ]);
    await expect(bytes(cached.slice(2, 4))).resolves.toEqual([3, 4]);
    await expect(bytes(cached.slice(6, 8))).resolves.toEqual([7, 8]);

    expect(slices).toBe(2);
    expect(peakActive).toBe(2);
  });

  it("does not prefetch ranges that cannot be retained within the cache budget", async () => {
    let slices = 0;
    const file = storeBuffer([1, 2, 3, 4, 5, 6, 7, 8], () => {
      slices += 1;
    });
    const cached = cachedRangeBuffer(file, { maxBytes: 4, coalesceBytes: 4 }, "bounded-prefetch");

    await cached.prefetch?.([
      { start: 0, end: 1 },
      { start: 4, end: 5 },
    ]);
    expect(slices).toBe(1);

    await cached.slice(4, 5);
    expect(slices).toBe(2);
  });

  it("does not cache ranges larger than the entry budget", async () => {
    let slices = 0;
    const source = new Uint8Array([1, 2, 3, 4, 5, 6]);
    const file: StoreAsyncBuffer = {
      byteLength: source.byteLength,
      async slice(start, end) {
        slices += 1;
        return source.buffer.slice(start, end);
      },
    };
    const cached = cachedRangeBuffer(file, { maxBytes: 6, maxEntryBytes: 2 }, "local");

    await cached.slice(0, 3);
    await cached.slice(0, 3);

    expect(slices).toBe(2);
  });

  it("bypasses caching when either range budget is disabled", async () => {
    let slices = 0;
    const file = storeBuffer([1, 2, 3, 4], () => {
      slices += 1;
    });

    const noCache = cachedRangeBuffer(file, { maxBytes: 0 }, "disabled");
    await noCache.slice(0, 2);
    await noCache.slice(0, 2);

    const noEntries = cachedRangeBuffer(file, { maxBytes: 4, maxEntryBytes: 0 }, "disabled");
    await noEntries.slice(0, 2);
    await noEntries.slice(0, 2);

    expect(slices).toBe(4);
  });

  it("uses file identity and normalized end offsets in shared range cache keys", async () => {
    let slices = 0;
    const file = storeBuffer([1, 2, 3, 4], () => {
      slices += 1;
    });
    file.etag = "etag-a";
    const sharedCache = new SharedMemoryCache({ maxBytes: 8 });
    const first = cachedRangeBuffer(file, { maxBytes: 8, sharedCache, coalesceBytes: 0 }, "path-a");
    const sameIdentity = cachedRangeBuffer(
      file,
      { maxBytes: 8, sharedCache, coalesceBytes: 0 },
      "path-a",
    );
    const otherPath = cachedRangeBuffer(
      file,
      { maxBytes: 8, sharedCache, coalesceBytes: 0 },
      "path-b",
    );

    await expect(bytes(first.slice(1))).resolves.toEqual([2, 3, 4]);
    await expect(bytes(sameIdentity.slice(1, 4))).resolves.toEqual([2, 3, 4]);
    await expect(bytes(otherPath.slice(1, 4))).resolves.toEqual([2, 3, 4]);

    expect(slices).toBe(2);
  });

  it("shares cache budget with policy-specific range priorities", async () => {
    const ioFile = countedBuffer([1, 2, 3, 4]);
    const ioShared = new SharedMemoryCache({ maxBytes: 4 });
    const ioCached = cachedRangeBuffer(
      ioFile,
      { maxBytes: 4, sharedCache: ioShared, cacheOptions: { policy: "io" }, coalesceBytes: 0 },
      "io",
    );
    await ioCached.slice(0, 2);
    ioShared.set("decoded", "decoded", 4, { priority: 2 });
    await ioCached.slice(0, 2);
    expect(ioFile.slices).toBe(1);

    const latencyFile = countedBuffer([1, 2, 3, 4]);
    const latencyShared = new SharedMemoryCache({ maxBytes: 4 });
    const latencyCached = cachedRangeBuffer(
      latencyFile,
      {
        maxBytes: 4,
        sharedCache: latencyShared,
        cacheOptions: { policy: "latency" },
        coalesceBytes: 0,
      },
      "latency",
    );
    await latencyCached.slice(0, 2);
    latencyShared.set("decoded", "decoded", 4, { priority: 2 });
    await latencyCached.slice(0, 2);
    expect(latencyFile.slices).toBe(2);
  });
});

async function bytes(input: Promise<ArrayBuffer>): Promise<number[]> {
  return [...new Uint8Array(await input)];
}

function storeBuffer(values: number[], onSlice: () => void): StoreAsyncBuffer {
  const source = new Uint8Array(values);
  return {
    byteLength: source.byteLength,
    async slice(start, end) {
      onSlice();
      return source.buffer.slice(start, end);
    },
  };
}

function countedBuffer(values: number[]): StoreAsyncBuffer & { readonly slices: number } {
  let slices = 0;
  return {
    ...storeBuffer(values, () => {
      slices += 1;
    }),
    get slices() {
      return slices;
    },
  };
}
