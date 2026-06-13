import { describe, expect, it } from "vitest";
import { LaQLError } from "./errors.js";
import { memoryStore } from "./memory-store.js";

const enc = new TextEncoder();

describe("MemoryObjectStore", () => {
  it("round-trips put/get/head/delete", async () => {
    const store = memoryStore();
    await store.put("a/b.txt", enc.encode("hello"), { contentType: "text/plain" });

    expect(await store.get("a/b.txt")).toEqual(enc.encode("hello"));

    const head = await store.head("a/b.txt");
    expect(head?.size).toBe(5);
    expect(head?.contentType).toBe("text/plain");
    expect(head?.etag).toBeTruthy();

    await store.delete("a/b.txt");
    expect(await store.get("a/b.txt")).toBeNull();
    expect(await store.head("a/b.txt")).toBeNull();
  });

  it("getRange returns exact slices and rejects out-of-bounds", async () => {
    const store = memoryStore();
    await store.put("x", enc.encode("0123456789"));

    expect(await store.getRange("x", { offset: 2, length: 3 })).toEqual(enc.encode("234"));
    expect(await store.getRange("x", { offset: 0, length: 10 })).toEqual(enc.encode("0123456789"));

    await expect(store.getRange("x", { offset: 8, length: 5 })).rejects.toThrowError(LaQLError);
    await expect(store.getRange("missing", { offset: 0, length: 1 })).rejects.toMatchObject({
      code: "LAQL_OBJECT_NOT_FOUND",
    });
  });

  it("accepts a ReadableStream body", async () => {
    const store = memoryStore();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(enc.encode("ab"));
        controller.enqueue(enc.encode("cd"));
        controller.close();
      },
    });
    await store.put("streamed", body);
    expect(await store.get("streamed")).toEqual(enc.encode("abcd"));
  });

  it("lists by prefix in sorted order with limit", async () => {
    const store = memoryStore();
    await store.put("data/b.parquet", enc.encode("b"));
    await store.put("data/a.parquet", enc.encode("a"));
    await store.put("other/c.parquet", enc.encode("c"));

    const all = [];
    for await (const info of store.list("data/")) all.push(info.path);
    expect(all).toEqual(["data/a.parquet", "data/b.parquet"]);

    const limited = [];
    for await (const info of store.list("data/", { limit: 1 })) limited.push(info.path);
    expect(limited).toEqual(["data/a.parquet"]);
  });

  it("changes etag on overwrite", async () => {
    const store = memoryStore();
    await store.put("k", enc.encode("1"));
    const first = (await store.head("k"))?.etag;
    await store.put("k", enc.encode("2"));
    const second = (await store.head("k"))?.etag;
    expect(first).not.toBe(second);
  });
});
