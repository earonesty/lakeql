import { SharedMemoryCache, type Vector } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { DecodedColumnCache } from "./decoded-column-cache.js";

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
});
