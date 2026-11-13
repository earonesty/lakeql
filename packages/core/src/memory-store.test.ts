import { describe, expect, it } from "vitest";
import { LakeqlError } from "./errors.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import type { ObjectStore } from "./store.js";
import { readControlSignal, throwIfAborted, withObjectStoreReadControls } from "./store.js";

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

    await expect(store.getRange("x", { offset: 8, length: 5 })).rejects.toThrowError(LakeqlError);
    await expect(store.getRange("missing", { offset: 0, length: 1 })).rejects.toMatchObject({
      code: "LAKEQL_OBJECT_NOT_FOUND",
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

    await expect(second).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    releaseRead?.();
    await expect(first).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    expect(peak).toBe(1);
  });

  it("returns the original store when no read controls are set", () => {
    const store = memoryStore();
    expect(withObjectStoreReadControls(store, {})).toBe(store);
  });

  it("builds timeout-aware read control signals", () => {
    const timeoutOnly = readControlSignal({ maxElapsedMs: 1_000 });
    expect(timeoutOnly).toBeInstanceOf(AbortSignal);

    const alreadyAborted = new AbortController();
    alreadyAborted.abort("done");
    expect(readControlSignal({ signal: alreadyAborted.signal, maxElapsedMs: 1_000 })).toBe(
      alreadyAborted.signal,
    );

    const caller = new AbortController();
    const combined = readControlSignal({ signal: caller.signal, maxElapsedMs: 1_000 });
    caller.abort(new Error("caller aborted"));
    expect(combined?.aborted).toBe(true);
    expect(combined?.reason).toBeInstanceOf(Error);
  });

  it("validates maxConcurrentReads", () => {
    expect(() => withObjectStoreReadControls(memoryStore(), { maxConcurrentReads: 0 })).toThrow(
      LakeqlError,
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

    await expect(controlled.get("a")).resolves.toEqual(enc.encode("a"));
    await expect(controlled.getRange("a", { offset: 0, length: 1 })).resolves.toEqual(
      enc.encode("a"),
    );
    expect(await controlled.head("a")).toMatchObject({ size: 1 });
    const listed = [];
    for await (const object of controlled.list("")) listed.push(object.path);
    expect(listed).toEqual(["a"]);

    controller.abort(new Error("cancelled"));
    expect(() => throwIfAborted(controller.signal)).toThrow("Query aborted");
    expect(() => controlled.put("b", enc.encode("b"))).toThrow(
      expect.objectContaining({
        code: "LAKEQL_ABORTED",
        details: { reason: "cancelled" },
      }),
    );
    expect(() => controlled.delete("a")).toThrow(
      expect.objectContaining({
        code: "LAKEQL_ABORTED",
      }),
    );
    await expect(async () => {
      for await (const _object of controlled.list("")) {
        // The abort check happens before iteration starts.
      }
    }).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  });
});

it("reserves memory-store range copies and isolates subclass-backed input", async () => {
  class ViewSliceBytes extends Uint8Array {
    override slice(start?: number, end?: number): Uint8Array {
      return this.subarray(start, end);
    }
  }
  const store = memoryStore();
  const input = new ViewSliceBytes([1, 2, 3, 4]);
  await store.put("input", input);
  const scope = new MemoryScope(new MemoryBudget(2));
  const owner = await store.getRangeOwned?.("input", { offset: 1, length: 2 }, scope);
  expect(owner?.value).toEqual(Uint8Array.of(2, 3));
  owner?.value.fill(9);
  expect(input).toEqual(new ViewSliceBytes([1, 2, 3, 4]));
  await expect(
    store.getRangeOwned?.("input", { offset: 0, length: 1 }, scope),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(scope.budget.peakBytes).toBe(2);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const ordinary = await store.getRange("input", { offset: 1, length: 2 });
  ordinary.fill(8);
  expect(input[1]).toBe(2);
});

it("rejects malformed range shapes before allocating owned or ordinary outputs", async () => {
  const store = memoryStore();
  await store.put("input", Uint8Array.of(1, 2));
  const scope = new MemoryScope(new MemoryBudget(2));
  for (const range of [
    { offset: NaN, length: 1 },
    { offset: 0, length: Infinity },
    { offset: 0.5, length: 1 },
    { offset: 0, length: -1 },
    { offset: 2, length: 1 },
  ]) {
    await expect(store.getRange("input", range)).rejects.toThrow();
    await expect(store.getRangeOwned?.("input", range, scope)).rejects.toThrow();
  }
  const empty = await store.getRangeOwned?.("input", { offset: 2, length: 0 }, scope);
  expect(empty?.value.length).toBe(0);
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});

it("preserves precise owned allocation forecasts through path and read-control wrappers", async () => {
  const { uriObjectStore, withObjectStoreReadControls } = await import("./store.js");
  const store = memoryStore();
  await store.put("input", Uint8Array.of(1, 2, 3));
  const controlled = withObjectStoreReadControls(
    uriObjectStore(store, { scheme: "s3", authority: "bucket" }),
    { maxConcurrentReads: 1 },
  );
  expect(controlled.getRangeOwnedAllocation?.("input", { offset: 1, length: 2 })).toEqual({
    bytes: 2,
    output: "exact-buffer",
  });
});

it("shares discovery slots with payload reads and cancels queued discovery", async () => {
  const controller = new AbortController();
  let release!: () => void;
  let markStarted!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const started = new Promise<void>((resolve) => {
    markStarted = resolve;
  });
  let heads = 0;
  let ranges = 0;
  const base = memoryStore();
  const controlled = withObjectStoreReadControls(
    {
      ...base,
      async head() {
        heads++;
        markStarted();
        await gate;
        return { size: 1 };
      },
      async getRange() {
        ranges++;
        return Uint8Array.of(1);
      },
    },
    { maxConcurrentReads: 1, signal: controller.signal },
  );
  const first = controlled.head("first");
  await started;
  const queuedHead = controlled.head("second");
  const queuedRange = controlled.getRange("first", { offset: 0, length: 1 });
  const failures = [first, queuedHead, queuedRange].map((promise) =>
    expect(promise).rejects.toMatchObject({ code: "LAKEQL_ABORTED" }),
  );
  controller.abort("cancel queued discovery");
  release();
  await Promise.all(failures);
  expect(heads).toBe(1);
  expect(ranges).toBe(0);
});
