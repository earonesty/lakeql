import { SharedMemoryCache } from "lakeql-core";
import { describe, expect, it, vi } from "vitest";
import { cachedRangeBuffer } from "./range-cache.js";
import type { StoreAsyncBuffer } from "./types.js";

describe("cachedRangeBuffer", () => {
  it.each([false, true])("reuses a covering unaligned range (shared=%s)", async (shared) => {
    const file = countedBuffer([1, 2, 3, 4, 5, 6, 7, 8]);
    const sharedCache = shared ? new SharedMemoryCache({ maxBytes: 8 }) : undefined;
    const options = { maxBytes: 8, coalesceBytes: 4, ...(sharedCache ? { sharedCache } : {}) };
    const cached = cachedRangeBuffer(file, options, "column");
    await cached.prefetch?.([{ start: 1, end: 7 }]);
    await expect(bytes(cached.slice(2, 5))).resolves.toEqual([3, 4, 5]);
    await expect(bytes(cached.slice(3, 7))).resolves.toEqual([4, 5, 6, 7]);
    if (shared) {
      const otherScan = cachedRangeBuffer(file, options, "column");
      await expect(bytes(otherScan.slice(2, 6))).resolves.toEqual([3, 4, 5, 6]);
    }
    expect(file.slices).toBe(1);
  });

  it.each([
    false,
    true,
  ])("shares covering pending reads and retries failures (shared=%s)", async (shared) => {
    const source = Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]);
    let finish: (() => void) | undefined;
    let fail = true;
    let slices = 0;
    const file: StoreAsyncBuffer = {
      byteLength: 8,
      async slice(start, end) {
        slices++;
        await new Promise<void>((resolve) => {
          finish = resolve;
        });
        if (fail) throw new Error("read failed");
        return source.buffer.slice(start, end);
      },
    };
    const cached = cachedRangeBuffer(
      file,
      {
        maxBytes: 8,
        coalesceBytes: 0,
        ...(shared ? { sharedCache: new SharedMemoryCache({ maxBytes: 8 }) } : {}),
      },
      "pending",
    );
    const first = cached.slice(0, 8);
    const second = cached.slice(2, 5);
    const errors = Promise.allSettled([first, second]);
    finish?.();
    expect((await errors).every((r) => r.status === "rejected")).toBe(true);
    expect(slices).toBe(1);
    fail = false;
    const retry = bytes(cached.slice(2, 5));
    finish?.();
    await expect(retry).resolves.toEqual([3, 4, 5]);
    expect(slices).toBe(2);
  });

  it("does not round metadata tail probes into unrelated preceding data", async () => {
    const reads: [number, number | undefined][] = [];
    const file: StoreAsyncBuffer = {
      byteLength: 16,
      async slice(start, end) {
        reads.push([start, end]);
        return new ArrayBuffer((end ?? 16) - start);
      },
    };
    const cached = cachedRangeBuffer(file, { maxBytes: 16, coalesceBytes: 16 }, "tail");
    await cached.slice(12, 16);
    expect(reads).toEqual([[12, 16]]);
  });

  it("expires covering shared entries and isolates etags", async () => {
    vi.useFakeTimers();
    try {
      const file = countedBuffer([1, 2, 3, 4, 5, 6, 7, 8]);
      file.etag = "first";
      const sharedCache = new SharedMemoryCache({ maxBytes: 16, ttlMs: 10 });
      const options = { maxBytes: 8, coalesceBytes: 0, sharedCache };
      const first = cachedRangeBuffer(file, options, "path");
      await first.slice(0, 8);
      await first.slice(1, 3);
      expect(file.slices).toBe(1);
      vi.advanceTimersByTime(11);
      await expect(bytes(first.slice(1, 3))).resolves.toEqual([2, 3]);
      expect(file.slices).toBe(2);
      file.etag = "second";
      const changed = cachedRangeBuffer(file, options, "path");
      await changed.slice(1, 3);
      expect(file.slices).toBe(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it.each([
    false,
    true,
  ])("does not keep an evicted covering entry alive (shared=%s)", async (shared) => {
    const file = countedBuffer([1, 2, 3, 4, 5, 6, 7, 8]);
    const cache = cachedRangeBuffer(
      file,
      {
        maxBytes: 4,
        coalesceBytes: 0,
        ...(shared ? { sharedCache: new SharedMemoryCache({ maxBytes: 4 }) } : {}),
      },
      "eviction",
    );
    await cache.slice(0, 4);
    await cache.slice(4, 8);
    await expect(bytes(cache.slice(1, 3))).resolves.toEqual([2, 3]);
    expect(file.slices).toBe(3);
  });

  it("does not retain a truncated response as a complete cached range", async () => {
    let reads = 0;
    const data = Uint8Array.from([1, 2, 3, 4]);
    const file: StoreAsyncBuffer = {
      byteLength: 4,
      async slice(start, end) {
        reads++;
        return reads === 1 ? data.buffer.slice(0, 1) : data.buffer.slice(start, end);
      },
    };
    const cached = cachedRangeBuffer(file, { maxBytes: 4, coalesceBytes: 0 }, "short");
    await expect(cached.slice(0, 4)).rejects.toMatchObject({
      code: "LAKEQL_PARQUET_READ_ERROR",
    });
    await expect(bytes(cached.slice(0, 4))).resolves.toEqual([1, 2, 3, 4]);
    expect(reads).toBe(2);
  });

  it.each([
    false,
    true,
  ])("rejects a truncated in-flight response for owner and all waiters (shared=%s)", async (shared) => {
    let finish: (() => void) | undefined;
    let reads = 0;
    const source = Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]);
    const file: StoreAsyncBuffer = {
      byteLength: 8,
      async slice(start, end) {
        reads++;
        if (reads === 1) {
          await new Promise<void>((resolve) => {
            finish = resolve;
          });
          return source.buffer.slice(0, 1);
        }
        return source.buffer.slice(start, end);
      },
    };
    const cached = cachedRangeBuffer(
      file,
      {
        maxBytes: 8,
        coalesceBytes: 0,
        ...(shared ? { sharedCache: new SharedMemoryCache({ maxBytes: 8 }) } : {}),
      },
      "truncated-pending",
    );
    const results = Promise.allSettled([
      cached.slice(0, 8),
      cached.slice(4, 6),
      cached.slice(0, 1),
    ]);
    finish?.();
    for (const result of await results) {
      expect(result).toMatchObject({
        status: "rejected",
        reason: {
          code: "LAKEQL_PARQUET_READ_ERROR",
          details: { expectedBytes: 8, actualBytes: 1 },
        },
      });
    }
    await expect(bytes(cached.slice(4, 6))).resolves.toEqual([5, 6]);
    expect(reads).toBe(2);
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

  it("prefetches known column chunks concurrently and serves later slices from cache", async () => {
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
      { start: 0, end: 3 },
      { start: 1, end: 3 },
      { start: 5, end: 8 },
    ]);
    await expect(bytes(cached.slice(1, 3))).resolves.toEqual([2, 3]);
    await expect(bytes(cached.slice(6, 8))).resolves.toEqual([7, 8]);

    expect(slices).toBe(2);
    expect(peakActive).toBe(2);
  });

  it("merges touching prefetched chunks without padding or double charging overlaps", async () => {
    const reads: [number, number | undefined][] = [];
    const source = Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]);
    const file: StoreAsyncBuffer = {
      byteLength: 8,
      async slice(start, end) {
        reads.push([start, end]);
        return source.buffer.slice(start, end);
      },
    };
    const cached = cachedRangeBuffer(file, { maxBytes: 7, coalesceBytes: 8 }, "touching");
    await cached.prefetch?.([
      { start: 4, end: 7 },
      { start: 1, end: 5 },
      { start: 1, end: 2 },
    ]);
    expect(reads).toEqual([[1, 7]]);
    await expect(bytes(cached.slice(2, 6))).resolves.toEqual([3, 4, 5, 6]);
    expect(reads).toHaveLength(1);
  });

  it("does not prefetch ranges that cannot be retained within the cache budget", async () => {
    let slices = 0;
    const file = storeBuffer([1, 2, 3, 4, 5, 6, 7, 8], () => {
      slices += 1;
    });
    const cached = cachedRangeBuffer(file, { maxBytes: 4, coalesceBytes: 4 }, "bounded-prefetch");

    await cached.prefetch?.([
      { start: 0, end: 4 },
      { start: 4, end: 8 },
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
