import { describe, expect, it } from "vitest";
import { LakeqlError } from "./errors.js";
import { memoryStore } from "./memory-store.js";
import { objectStoreJsonCache } from "./object-store-json-cache.js";

describe("objectStoreJsonCache", () => {
  it("persists JSON cache entries in an object store namespace", async () => {
    const store = memoryStore();
    const cache = objectStoreJsonCache<{ rows: number }>({ store, prefix: "cache/catalog" });

    await cache.set("iceberg:table:manifest:path", { value: { rows: 20 } });

    expect(await cache.get("iceberg:table:manifest:path")).toEqual({
      value: { rows: 20 },
    });
    const objects = [];
    for await (const object of store.list("cache/catalog/")) objects.push(object.path);
    expect(objects).toEqual(["cache/catalog/iceberg%3Atable%3Amanifest%3Apath.json"]);
  });

  it("expires entries using cache ttl", async () => {
    let now = 100;
    const cache = objectStoreJsonCache<string>({
      store: memoryStore(),
      prefix: "cache",
      ttlMs: 50,
      now: () => now,
    });

    await cache.set("key", { value: "value" });
    expect(await cache.get("key")).toEqual({ value: "value", expiresAt: 150 });

    now = 151;
    expect(await cache.get("key")).toBeUndefined();
  });

  it("honors per-entry expiration over cache ttl", async () => {
    let now = 100;
    const cache = objectStoreJsonCache<string>({
      store: memoryStore(),
      prefix: "cache",
      ttlMs: 50,
      now: () => now,
    });

    await cache.set("key", { value: "value", expiresAt: 300 });
    now = 151;

    expect(await cache.get("key")).toEqual({ value: "value", expiresAt: 300 });
  });

  it("normalizes prefixes and misses absent entries", async () => {
    const store = memoryStore();
    const cache = objectStoreJsonCache<string>({ store, prefix: "/cache/catalog/" });

    expect(await cache.get("missing")).toBeUndefined();
    await cache.set("key", { value: "value" });

    const objects = [];
    for await (const object of store.list("cache/catalog/")) objects.push(object.path);
    expect(objects).toEqual(["cache/catalog/key.json"]);
  });

  it("rejects empty prefixes", () => {
    expect(() => objectStoreJsonCache({ store: memoryStore(), prefix: "///" })).toThrow(
      LakeqlError,
    );
  });

  it("rejects undefined and circular values", async () => {
    const cache = objectStoreJsonCache<unknown>({ store: memoryStore(), prefix: "cache" });
    const circular: Record<string, unknown> = {};
    circular.self = circular;

    await expect(cache.set("undefined", { value: undefined })).rejects.toThrow(LakeqlError);
    await expect(cache.set("circular", { value: circular })).rejects.toThrow(LakeqlError);
  });

  it("rejects invalid stored entries", async () => {
    const store = memoryStore();
    const cache = objectStoreJsonCache<unknown>({ store, prefix: "cache" });

    await store.put("cache/not-json.json", new TextEncoder().encode("{"), {});
    await expect(cache.get("not-json")).rejects.toThrow(LakeqlError);

    await store.put("cache/not-entry.json", new TextEncoder().encode("[]"), {});
    await expect(cache.get("not-entry")).rejects.toThrow(LakeqlError);
  });
});
