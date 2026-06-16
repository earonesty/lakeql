import { describe, expect, it } from "vitest";
import { LaQLError } from "./errors.js";
import { memoryStore } from "./memory-store.js";
import type { ObjectStore } from "./store.js";
import { throwIfAborted, withObjectStoreReadControls } from "./store.js";

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

  it("conditionally creates and updates objects", async () => {
    const store = memoryStore();

    await expect(
      store.conditionalPut("cas", enc.encode("1"), {
        expectedEtag: null,
        contentType: "text/plain",
      }),
    ).resolves.toBe(true);
    const first = await store.head("cas");
    expect(first).toMatchObject({ size: 1, contentType: "text/plain" });

    await expect(
      store.conditionalPut("cas", enc.encode("2"), { expectedEtag: first?.etag ?? "" }),
    ).resolves.toBe(true);
    expect(await store.get("cas")).toEqual(enc.encode("2"));
  });

  it("rejects conditional writes when existence or etag does not match", async () => {
    const store = memoryStore();
    await store.put("cas", enc.encode("1"));

    await expect(
      store.conditionalPut("cas", enc.encode("2"), { expectedEtag: null }),
    ).resolves.toBe(false);
    await expect(
      store.conditionalPut("cas", enc.encode("3"), { expectedEtag: "stale" }),
    ).resolves.toBe(false);
    expect(await store.get("cas")).toEqual(enc.encode("1"));
  });

  it("limits concurrent object reads and aborts queued reads", async () => {
    let active = 0;
    let peak = 0;
    let releaseRead: (() => void) | undefined;
    let resolveFirstStarted: () => void = () => {};
    const firstReadStarted = new Promise<void>((resolve) => {
      resolveFirstStarted = resolve;
    });
    const slowStore: ObjectStore = {
      get: async () => {
        active += 1;
        peak = Math.max(peak, active);
        resolveFirstStarted();
        await new Promise<void>((release) => {
          releaseRead = release;
        });
        active -= 1;
        return enc.encode("ok");
      },
      getRange: async () => enc.encode("ok"),
      put: async () => {},
      delete: async () => {},
      list: async function* () {},
      head: async () => ({ size: 2 }),
    };
    const controller = new AbortController();
    const controlled = withObjectStoreReadControls(slowStore, {
      maxConcurrentReads: 1,
      signal: controller.signal,
    });

    const first = controlled.get("first");
    await firstReadStarted;
    const second = controlled.get("second");
    controller.abort("stop");

    await expect(second).rejects.toMatchObject({ code: "LAQL_ABORTED" });
    releaseRead?.();
    await expect(first).rejects.toMatchObject({ code: "LAQL_ABORTED" });
    expect(peak).toBe(1);
  });

  it("returns the original store when no read controls are set", () => {
    const store = memoryStore();
    expect(withObjectStoreReadControls(store, {})).toBe(store);
  });

  it("validates maxConcurrentReads", () => {
    expect(() => withObjectStoreReadControls(memoryStore(), { maxConcurrentReads: 0 })).toThrow(
      LaQLError,
    );
  });

  it("allows queued reads to resume when a read slot opens", async () => {
    let active = 0;
    let peak = 0;
    let releaseRead: (() => void) | undefined;
    let resolveFirstStarted: () => void = () => {};
    const firstReadStarted = new Promise<void>((resolve) => {
      resolveFirstStarted = resolve;
    });
    const slowStore: ObjectStore = {
      get: async (path) => {
        active += 1;
        peak = Math.max(peak, active);
        if (path === "first") {
          resolveFirstStarted();
          await new Promise<void>((release) => {
            releaseRead = release;
          });
        }
        active -= 1;
        return enc.encode(path);
      },
      getRange: async (_path, range) => enc.encode(`${range.offset}:${range.length}`),
      put: async () => {},
      delete: async () => {},
      list: async function* () {},
      head: async () => ({ size: 2 }),
    };
    const controlled = withObjectStoreReadControls(slowStore, { maxConcurrentReads: 1 });

    const first = controlled.get("first");
    await firstReadStarted;
    const second = controlled.get("second");
    releaseRead?.();

    await expect(first).resolves.toEqual(enc.encode("first"));
    await expect(second).resolves.toEqual(enc.encode("second"));
    await expect(controlled.getRange("range", { offset: 1, length: 2 })).resolves.toEqual(
      enc.encode("1:2"),
    );
    expect(peak).toBe(1);
  });

  it("checks abort signals around non-read store operations", async () => {
    const store = memoryStore();
    await store.put("a", enc.encode("a"));
    const controller = new AbortController();
    const controlled = withObjectStoreReadControls(store, { signal: controller.signal });

    expect(await controlled.head("a")).toMatchObject({ size: 1 });
    const listed = [];
    for await (const object of controlled.list("")) listed.push(object.path);
    expect(listed).toEqual(["a"]);

    controller.abort(new Error("cancelled"));
    expect(() => throwIfAborted(controller.signal)).toThrow("Query aborted");
    expect(() => controlled.put("b", enc.encode("b"))).toThrow(
      expect.objectContaining({
        code: "LAQL_ABORTED",
        details: { reason: "cancelled" },
      }),
    );
    expect(() => controlled.delete("a")).toThrow(
      expect.objectContaining({
        code: "LAQL_ABORTED",
      }),
    );
    await expect(async () => {
      for await (const _object of controlled.list("")) {
        // The abort check happens before iteration starts.
      }
    }).rejects.toMatchObject({ code: "LAQL_ABORTED" });
  });
});
