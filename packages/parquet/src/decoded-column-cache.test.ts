import { MemoryBudget, MemoryScope, SharedMemoryCache, type Vector } from "lakeql-core";
import { describe, expect, it } from "vitest";
import {
  DecodedColumnCache,
  decodedColumnCacheKey,
  decodedColumnPageCacheKey,
  decodedDictionaryPageCacheKey,
} from "./decoded-column-cache.js";

describe("DecodedColumnCache", () => {
  const vector: Vector = { type: "f64", values: new Float64Array([1, 2, 3]) };

  it("admits small decoded pages in constrained balanced caches", () => {
    const cache = new DecodedColumnCache(new SharedMemoryCache({ maxBytes: 32 * 1024 * 1024 }), {
      maxBytes: 32 * 1024 * 1024,
      policy: "balanced",
    });

    expect(cache.setVector("page", vector)).toBe(true);
    expect(cache.getVector("page")).toBe(vector);
  });

  it("admits decoded vectors when balanced caches have enough headroom", () => {
    const cache = new DecodedColumnCache(new SharedMemoryCache({ maxBytes: 256 * 1024 * 1024 }), {
      maxBytes: 256 * 1024 * 1024,
      policy: "balanced",
    });

    expect(cache.setVector("page", vector)).toBe(true);
    expect(cache.getVector("page")).toBe(vector);
  });

  it("admits decoded vectors under latency policy within the cache budget", () => {
    const cache = new DecodedColumnCache(new SharedMemoryCache({ maxBytes: 1024 }), {
      maxBytes: 1024,
      policy: "latency",
    });

    expect(cache.setVector("page", vector)).toBe(true);
    expect(cache.getVector("page")).toBe(vector);
  });

  it("does not admit decoded vectors that exceed a latency cache budget", () => {
    const cache = new DecodedColumnCache(new SharedMemoryCache({ maxBytes: 8 }), {
      maxBytes: 8,
      policy: "latency",
    });

    expect(cache.setVector("page", vector)).toBe(false);
    expect(cache.getVector("page")).toBeUndefined();
  });

  it("keeps decoded batches and work products under the same shared budget", () => {
    const shared = new SharedMemoryCache({ maxBytes: 256 * 1024 * 1024 });
    const cache = new DecodedColumnCache(shared, {
      maxBytes: 256 * 1024 * 1024,
      policy: "balanced",
    });

    const batch = {
      rowCount: 3,
      columns: {
        f64: vector,
        i64: { type: "i64", values: new BigInt64Array([1n, 2n]) },
        timestamp: { type: "timestamp", values: new BigInt64Array([1n]) },
        bool: { type: "bool", values: new Uint8Array([1, 0, 1]), valid: new Uint8Array([1]) },
        utf8: { type: "utf8", values: ["a", "bb"] },
        dict: {
          type: "dict",
          indices: new Uint32Array([0, 1, 0]),
          dictionary: { type: "utf8", values: ["x", "yy"] },
        },
        list: {
          type: "list",
          offsets: new Int32Array([0, 1, 2]),
          child: { type: "f64", values: new Float64Array([1, 2]) },
        },
        struct: {
          type: "struct",
          fields: { a: { type: "f64", values: new Float64Array([1]) } },
        },
        map: {
          type: "map",
          offsets: new Int32Array([0, 1]),
          keys: { type: "utf8", values: ["k"] },
          values: { type: "f64", values: new Float64Array([1]) },
        },
        nulls: { type: "null", rowCount: 3, valid: new Uint8Array([0]) },
      },
    } as const;

    cache.set("batch", batch);
    cache.setValue("rows", [1, 2, 3], 24);

    expect(cache.get("batch")).toBe(batch);
    expect(cache.getValue<number[]>("rows")).toEqual([1, 2, 3]);
  });

  it("skips decoded work products when balanced cache headroom is constrained", () => {
    const cache = new DecodedColumnCache(new SharedMemoryCache({ maxBytes: 128 }), {
      maxBytes: 128,
      policy: "balanced",
    });

    cache.setValue("rows", [1, 2, 3], 24);

    expect(cache.getValue("rows")).toBeUndefined();
  });

  it("admits decoded work products for io policy only when the cache is large enough", () => {
    const small = new DecodedColumnCache(new SharedMemoryCache({ maxBytes: 128 }), {
      maxBytes: 128,
      policy: "io",
    });
    const large = new DecodedColumnCache(new SharedMemoryCache({ maxBytes: 256 * 1024 * 1024 }), {
      maxBytes: 256 * 1024 * 1024,
      policy: "io",
    });

    small.setValue("rows", "small", 24);
    large.setValue("rows", "large", 24);

    expect(small.getValue("rows")).toBeUndefined();
    expect(large.getValue("rows")).toBe("large");
  });

  it("builds stable cache keys from file identity and vector window", () => {
    expect(
      decodedColumnCacheKey({
        path: "flights.parquet",
        byteLength: 10,
        etag: "abc",
        columns: ["a", "b"],
        rowStart: 2,
        rowEnd: 4,
      }),
    ).toBe("flights.parquet\u001f10\u001fabc\u001f2\u001f4\u001fa\u001fb");
    expect(
      decodedColumnCacheKey({
        path: "flights.parquet",
        byteLength: 10,
        columns: ["a"],
        rowStart: 2,
        rowEnd: 4,
      }),
    ).toBe("flights.parquet\u001f10\u001f\u001f2\u001f4\u001fa");
    expect(
      decodedColumnPageCacheKey({
        path: "flights.parquet",
        byteLength: 10,
        etag: "abc",
        column: "delay",
        rowGroupStart: 5,
        pageRowStart: 6,
        pageRowEnd: 7,
        pageOffset: 8,
        compressedPageSize: 9,
      }),
    ).toBe("flights.parquet\u001f10\u001fabc\u001fdelay\u001f5\u001f6\u001f7\u001f8\u001f9");
    expect(
      decodedDictionaryPageCacheKey({
        path: "flights.parquet",
        byteLength: 10,
        column: "delay",
        rowGroupStart: 5,
        pageOffset: 8,
        compressedPageSize: 9,
        values: 3,
      }),
    ).toBe("flights.parquet\u001f10\u001f\u001fdelay\u001f5\u001f8\u001f9\u001f3");
  });
});

it("rejects a short decoded view retaining an oversized backing buffer", () => {
  const cache = new DecodedColumnCache(new SharedMemoryCache({ maxBytes: 8 }), {
    maxBytes: 8,
    policy: "latency",
  });
  const backing = new Uint8Array(32);
  expect(cache.setVector("view", { type: "u8", values: backing.subarray(0, 1) })).toBe(false);
  expect(cache.getVector("view")).toBeUndefined();
});

it("charges shared batch backing once and reports failed pool admission", () => {
  const budget = new MemoryBudget(16);
  const shared = new SharedMemoryCache({ maxBytes: 32, memoryBudget: budget });
  const cache = new DecodedColumnCache(shared, { maxBytes: 32, policy: "latency" });
  const backing = new Uint8Array(16);
  cache.set("batch", {
    rowCount: 1,
    columns: {
      a: { type: "u8", values: backing.subarray(0, 1) },
      b: { type: "u8", values: backing.subarray(1, 2) },
    },
  });
  expect(cache.get("batch")).toBeDefined();
  expect(budget.usedBytes).toBe(16);
  expect(cache.setVector("too-large", { type: "u8", values: new Uint8Array(17) })).toBe(false);
  expect(cache.getVector("too-large")).toBeUndefined();
  shared.deleteWhere(() => true);
  expect(budget.usedBytes).toBe(0);
});

it("retains decoded vector and dictionary ownership across producer disposal and eviction", () => {
  const budget = new MemoryBudget(12);
  const producer = new MemoryScope(budget);
  const shared = new SharedMemoryCache({ memoryBudget: budget, maxBytes: 12 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 12, policy: "latency" });
  const indices = producer.allocate(8, "indices", () => Uint32Array.of(0, 1));
  const dictionary = producer.allocate<Vector>(4, "dictionary", () => ({
    type: "u8",
    values: Uint8Array.of(3, 4, 5, 6),
  }));
  const vector = producer.retainBacking<Vector>(
    { type: "dict", indices: indices.value, dictionary: dictionary.value },
    [indices, dictionary],
  );
  expect(cache.setVectorOwned("page", vector)).toBe(true);
  producer.dispose();
  expect(budget.usedBytes).toBe(12);
  expect(budget.peakBytes).toBe(12);
  const pinned = cache.acquireVectorOwned("page");
  shared.deleteWhere(() => true);
  expect(budget.usedBytes).toBe(12);
  expect(pinned?.value.type).toBe("dict");
  pinned?.release();
  expect(budget.usedBytes).toBe(0);
});

it("preserves admission policy and requires real ownership for decoded cache hits", () => {
  const budget = new MemoryBudget(8);
  const scope = new MemoryScope(budget);
  const shared = new SharedMemoryCache({ memoryBudget: budget, maxBytes: 8 });
  const balanced = new DecodedColumnCache(shared, { maxBytes: 8 });
  const latency = new DecodedColumnCache(shared, { maxBytes: 8, policy: "latency" });
  const vector = scope.allocate<Vector>(8, "decoded", () => ({
    type: "u8",
    values: new Uint8Array(8),
  }));
  expect(balanced.setVectorOwned("page", vector)).toBe(false);
  expect(latency.setValueOwned("dictionary", vector)).toBe(true);
  const dictionary = latency.acquireValueOwned<Vector>("dictionary");
  shared.deleteWhere(() => true);
  dictionary?.release();
  const batch = scope.retainBacking({ rowCount: 8, columns: { value: vector.value } }, [vector]);
  expect(latency.setOwned("batch", batch)).toBe(true);
  const cachedBatch = latency.acquireOwned("batch");
  scope.dispose();
  shared.deleteWhere(() => true);
  expect(budget.usedBytes).toBe(8);
  cachedBatch?.release();
  expect(budget.usedBytes).toBe(0);
  latency.setVector("ordinary", { type: "u8", values: new Uint8Array(1) });
  expect(latency.acquireVectorOwned("ordinary")).toBeUndefined();
  shared.deleteWhere(() => true);
});

it("releases an acquired pin if the receiving decode scope is already closed", () => {
  const root = new MemoryBudget(8);
  const producer = new MemoryScope(root.child());
  const shared = new SharedMemoryCache({ memoryBudget: root, maxBytes: 8 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 8, policy: "latency" });
  const value = producer.allocate<Vector>(8, "decoded", () => ({
    type: "u8",
    values: new Uint8Array(8),
  }));
  cache.setVector("page", value.value, producer);
  producer.dispose();
  const readerBudget = root.child();
  const closed = new MemoryScope(readerBudget);
  closed.dispose();
  expect(() => cache.getVector("page", closed)).toThrow();
  expect(readerBudget.usedBytes).toBe(0);
  expect(root.usedBytes).toBe(8);
  shared.deleteWhere(() => true);
  expect(root.usedBytes).toBe(0);
});

it("declines oversized cache pins so a smaller fresh decode can proceed", () => {
  const root = new MemoryBudget(128);
  const producer = new MemoryScope(root.child());
  const shared = new SharedMemoryCache({ memoryBudget: root, maxBytes: 1024 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 1024, policy: "latency" });
  const input = producer.allocate(64, "retained input", () => new Uint8Array(64));
  const vector: Vector = { type: "u8", values: input.value.subarray(0, 4) };
  cache.setVector("page", vector, producer);
  cache.setValue("dictionary", vector, 4, producer);
  cache.set("batch", { rowCount: 4, columns: { value: vector } }, producer);
  producer.dispose();
  expect(cache.getVector("page")).toBe(vector);
  expect(cache.getValue("dictionary")).toBe(vector);
  expect(cache.get("batch")?.columns.value).toBe(vector);
  const child = root.child(4);
  const reader = new MemoryScope(child);
  expect(cache.getVector("page", reader)).toBeUndefined();
  expect(cache.getValue("dictionary", reader)).toBeUndefined();
  expect(cache.get("batch", reader)).toBeUndefined();
  expect(child.usedBytes).toBe(0);
  expect(reader.allocate(4, "fresh output", () => new Uint8Array(4)).value.byteLength).toBe(4);
  reader.dispose();
  shared.deleteWhere(() => true);
  expect(root.usedBytes).toBe(0);
});

it("protects higher-priority cache entries while admitting small decoded pages when space returns", () => {
  const shared = new SharedMemoryCache({ maxBytes: 1024 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 1024 });
  shared.set("encoded", "protected", 1008, { priority: 10 });
  const vector: Vector = { type: "f64", values: Float64Array.of(1, 2, 3) };
  expect(cache.setVector("page", vector)).toBe(false);
  expect(shared.get("encoded")?.value).toBe("protected");
  shared.delete("encoded");
  expect(cache.setVector("page", vector)).toBe(true);
  expect(cache.getVector("page")).toBe(vector);
});

it.each([64, 128])("keeps owned decoded cache reuse within a shared %i MiB pool", (mib) => {
  const pool = new MemoryBudget(mib * 1024 * 1024);
  const scope = new MemoryScope(pool.child());
  const shared = new SharedMemoryCache({ maxBytes: mib * 1024 * 1024, memoryBudget: pool });
  const cache = new DecodedColumnCache(shared, { maxBytes: mib * 1024 * 1024 });
  const page = scope.allocate<Vector>(1024, "page", () => ({
    type: "u8",
    values: new Uint8Array(1024),
  }));
  expect(cache.setVectorOwned("page", page)).toBe(true);
  scope.dispose();
  expect(pool.usedBytes).toBe(1024);
  const job = pool.child(2048);
  const retained = cache.acquireVectorOwned("page", job);
  expect(retained?.value.type).toBe("u8");
  shared.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(1024);
  retained?.release();
  expect(job.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(0);
});
