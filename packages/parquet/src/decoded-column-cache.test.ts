import { SharedMemoryCache, type Vector } from "lakeql-core";
import { describe, expect, it } from "vitest";
import {
  DecodedColumnCache,
  decodedColumnCacheKey,
  decodedColumnPageCacheKey,
  decodedDictionaryPageCacheKey,
} from "./decoded-column-cache.js";

describe("DecodedColumnCache", () => {
  const vector: Vector = { type: "f64", values: new Float64Array([1, 2, 3]) };

  it("keeps balanced constrained caches focused on encoded bytes", () => {
    const cache = new DecodedColumnCache(new SharedMemoryCache({ maxBytes: 32 * 1024 * 1024 }), {
      maxBytes: 32 * 1024 * 1024,
      policy: "balanced",
    });

    expect(cache.setVector("page", vector)).toBe(false);
    expect(cache.getVector("page")).toBeUndefined();
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
    const cache = new DecodedColumnCache(new SharedMemoryCache({ maxBytes: 64 * 1024 * 1024 }), {
      maxBytes: 64 * 1024 * 1024,
      policy: "balanced",
    });

    cache.setValue("rows", [1, 2, 3], 24);

    expect(cache.getValue("rows")).toBeUndefined();
  });

  it("admits decoded work products for io policy only when the cache is large enough", () => {
    const small = new DecodedColumnCache(new SharedMemoryCache({ maxBytes: 64 * 1024 * 1024 }), {
      maxBytes: 64 * 1024 * 1024,
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
