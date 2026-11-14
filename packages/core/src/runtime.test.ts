import { describe, expect, it } from "vitest";
import {
  cacheApiCache,
  memoryCache,
  memorySpillAdapter,
  type RuntimeSubstrate,
} from "./runtime.js";

class FakeCache implements Cache {
  private readonly entries = new Map<string, Response>();

  async add(_request: RequestInfo | URL): Promise<void> {
    throw new Error("not implemented");
  }

  async addAll(_requests: RequestInfo[] | URL[]): Promise<void> {
    throw new Error("not implemented");
  }

  async delete(request: RequestInfo | URL): Promise<boolean> {
    return this.entries.delete(cacheKey(request));
  }

  async keys(_request?: RequestInfo | URL): Promise<readonly Request[]> {
    return [...this.entries.keys()].map((url) => new Request(url));
  }

  async match(request: RequestInfo | URL): Promise<Response | undefined> {
    return this.entries.get(cacheKey(request))?.clone();
  }

  async matchAll(_request?: RequestInfo | URL): Promise<readonly Response[]> {
    return [...this.entries.values()].map((response) => response.clone());
  }

  async put(request: RequestInfo | URL, response: Response): Promise<void> {
    this.entries.set(cacheKey(request), response.clone());
  }
}

function cacheKey(request: RequestInfo | URL): string {
  if (request instanceof Request) return request.url;
  return String(request);
}

describe("runtime substrate helpers", () => {
  it("stores, expires, and deletes memory cache entries", async () => {
    const cache = memoryCache<string>();
    await cache.set("fresh", { value: "ok", expiresAt: Date.now() + 10_000 });
    await cache.set("expired", { value: "old", expiresAt: Date.now() - 1 });

    await expect(cache.get("fresh")).resolves.toEqual({
      value: "ok",
      expiresAt: expect.any(Number),
    });
    await expect(cache.get("expired")).resolves.toBeUndefined();
    await cache.delete("fresh");
    await expect(cache.get("fresh")).resolves.toBeUndefined();
  });

  it("adapts the runtime Cache API with expiration metadata", async () => {
    let now = 1_000;
    const cache = cacheApiCache(new FakeCache(), {
      namespace: "test",
      now: () => now,
    });
    await cache.set("fresh/key", { value: new Uint8Array([1, 2, 3]), expiresAt: 2_000 });
    await cache.set("expired", { value: new Uint8Array([9]), expiresAt: 900 });

    await expect(cache.get("fresh/key")).resolves.toEqual({
      value: new Uint8Array([1, 2, 3]),
      expiresAt: 2_000,
    });
    await expect(cache.get("expired")).resolves.toBeUndefined();
    now = 2_001;
    await expect(cache.get("fresh/key")).resolves.toBeUndefined();

    await cache.set("delete-me", { value: new Uint8Array([4]) });
    await cache.delete("delete-me");
    await expect(cache.get("delete-me")).resolves.toBeUndefined();
  });

  it("adapts Cache API entries without expiration metadata", async () => {
    const cache = cacheApiCache(new FakeCache());
    await cache.set("plain", { value: new Uint8Array([5]) });

    await expect(cache.get("plain")).resolves.toEqual({ value: new Uint8Array([5]) });
  });

  it("spills bytes defensively and tracks memory usage", async () => {
    const spill = memorySpillAdapter();
    const source = new Uint8Array([1, 2, 3]);
    const ref = await spill.write("operator-1", source);
    source[0] = 9;

    expect(ref).toEqual({ id: "operator-1", byteSize: 3 });
    await expect(spill.read(ref)).resolves.toEqual(new Uint8Array([1, 2, 3]));
    const read = await spill.read("operator-1");
    read[1] = 9;
    await expect(spill.read("operator-1")).resolves.toEqual(new Uint8Array([1, 2, 3]));
    await expect(spill.usage()).resolves.toEqual({ entries: 1, byteSize: 3 });

    await spill.write("operator-1", new Uint8Array([4]));
    await expect(spill.usage()).resolves.toEqual({ entries: 1, byteSize: 1 });
    await spill.delete(ref);
    await expect(spill.usage()).resolves.toEqual({ entries: 0, byteSize: 0 });
    await expect(spill.read(ref)).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });
  });

  it("enforces spill byte budgets with typed failures", async () => {
    const spill = memorySpillAdapter({ maxBytes: 4 });
    await spill.write("a", new Uint8Array([1, 2]));
    await spill.write("b", new Uint8Array([3, 4]));

    await expect(spill.write("c", new Uint8Array([5]))).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "spill bytes", limit: 4, actual: 5 },
    });
    await expect(spill.write("", new Uint8Array([1]))).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
    });
    await spill.write("b", new Uint8Array([3]));
    await expect(spill.usage()).resolves.toEqual({ entries: 2, byteSize: 3 });
  });

  it("accepts caller-supplied substrate interfaces", async () => {
    const substrate: RuntimeSubstrate = {
      clock: { now: () => 1 },
      ids: { id: (prefix = "id") => `${prefix}-1` },
      lock: { withLock: async (_key, fn) => fn() },
      spill: memorySpillAdapter(),
      metrics: { count() {}, timing() {} },
      log: { debug() {}, info() {}, warn() {}, error() {} },
    };

    await expect(substrate.lock?.withLock("x", async () => substrate.ids?.id("job"))).resolves.toBe(
      "job-1",
    );
  });
});

it("keeps spill usage exact across replacement, rejection and repeated deletion", async () => {
  const spill = memorySpillAdapter({ maxBytes: 10 });
  await spill.write("a", Uint8Array.of(1, 2, 3));
  await spill.write("b", new Uint8Array(7));
  expect(await spill.usage()).toEqual({ entries: 2, byteSize: 10 });
  await expect(spill.write("a", new Uint8Array(4))).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(await spill.read("a")).toEqual(Uint8Array.of(1, 2, 3));
  expect(await spill.usage()).toEqual({ entries: 2, byteSize: 10 });
  await spill.write("a", new Uint8Array(1));
  expect(await spill.usage()).toEqual({ entries: 2, byteSize: 8 });
  await spill.write("a", new Uint8Array(3));
  await spill.delete("b");
  await spill.delete("b");
  expect(await spill.usage()).toEqual({ entries: 1, byteSize: 3 });
  await spill.write("a", new Uint8Array(0));
  expect(await spill.usage()).toEqual({ entries: 1, byteSize: 0 });
  await spill.delete("a");
  expect(await spill.usage()).toEqual({ entries: 0, byteSize: 0 });
});

it("stages spill replacement without publishing it and rolls back idempotently", async () => {
  const { MemorySpillAdapter } = await import("./runtime.js");
  const spill = new MemorySpillAdapter({ maxBytes: 6 });
  await spill.write("state", Uint8Array.of(1, 2, 3));
  await expect(spill.stageWrite("state", new Uint8Array(4))).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(await spill.usage()).toEqual({ entries: 1, byteSize: 3 });
  const input = Uint8Array.of(4, 5, 6);
  const staged = await spill.stageWrite("state", input);
  input.fill(9);
  expect(await spill.read("state")).toEqual(Uint8Array.of(1, 2, 3));
  expect(await spill.usage()).toEqual({ entries: 2, byteSize: 6 });
  await expect(spill.write("other", Uint8Array.of(1))).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  await staged.rollback();
  await staged.rollback();
  await expect(staged.commit()).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
  expect(await spill.usage()).toEqual({ entries: 1, byteSize: 3 });
  const replacement = await spill.stageWrite("state", Uint8Array.of(4, 5));
  expect(await replacement.commit()).toEqual({ id: "state", byteSize: 2 });
  await replacement.commit();
  await replacement.rollback();
  expect(await spill.read("state")).toEqual(Uint8Array.of(4, 5));
  expect(await spill.usage()).toEqual({ entries: 1, byteSize: 2 });
});

it("does not overwrite a competing spill writer when a staged commit conflicts", async () => {
  const { MemorySpillAdapter } = await import("./runtime.js");
  const spill = new MemorySpillAdapter({ maxBytes: 8 });
  await spill.write("state", Uint8Array.of(1));
  const first = await spill.stageWrite("state", Uint8Array.of(2));
  const second = await spill.stageWrite("state", Uint8Array.of(3));
  await second.commit();
  await expect(first.commit()).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
  await first.rollback();
  expect(await spill.read("state")).toEqual(Uint8Array.of(3));
  expect(await spill.usage()).toEqual({ entries: 1, byteSize: 1 });
  const absent = await spill.stageWrite("new", new Uint8Array(0));
  await expect(spill.read("new")).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });
  await absent.commit();
  expect(await spill.usage()).toEqual({ entries: 2, byteSize: 1 });
});

it("commits a spill batch only after validating every target version", async () => {
  const { MemorySpillAdapter } = await import("./runtime.js");
  const spill = new MemorySpillAdapter({ maxBytes: 10 });
  await spill.write("a", Uint8Array.of(1));
  await spill.write("b", Uint8Array.of(2));
  const conflict = spill.beginWriteBatch();
  await conflict.write("a", Uint8Array.of(3));
  await conflict.write("b", Uint8Array.of(4));
  await spill.write("b", Uint8Array.of(9));
  await expect(conflict.commit()).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
  expect(await spill.read("a")).toEqual(Uint8Array.of(1));
  expect(await spill.read("b")).toEqual(Uint8Array.of(9));
  await conflict.rollback();
  await conflict.rollback();
  expect(await spill.usage()).toEqual({ entries: 2, byteSize: 2 });
  const batch = spill.beginWriteBatch();
  await batch.write("a", Uint8Array.of(5));
  await batch.write("b", Uint8Array.of(6));
  await batch.write("new", Uint8Array.of(7));
  await expect(batch.write("a", Uint8Array.of(8))).rejects.toMatchObject({
    code: "LAKEQL_VALIDATION_ERROR",
  });
  expect(await spill.usage()).toEqual({ entries: 5, byteSize: 5 });
  await batch.commit();
  await batch.commit();
  await batch.rollback();
  expect(await spill.read("a")).toEqual(Uint8Array.of(5));
  expect(await spill.read("b")).toEqual(Uint8Array.of(6));
  expect(await spill.read("new")).toEqual(Uint8Array.of(7));
  expect(await spill.usage()).toEqual({ entries: 3, byteSize: 3 });
  await expect(batch.write("later", Uint8Array.of(0))).rejects.toMatchObject({
    code: "LAKEQL_VALIDATION_ERROR",
  });
});

it("releases all pending spill-batch bytes after an overlap budget rejection", async () => {
  const { MemorySpillAdapter } = await import("./runtime.js");
  const spill = new MemorySpillAdapter({ maxBytes: 4 });
  await spill.write("prior", Uint8Array.of(1, 2));
  const batch = spill.beginWriteBatch();
  await batch.write("run", Uint8Array.of(3, 4));
  await expect(batch.write("checkpoint", Uint8Array.of(5))).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  await batch.rollback();
  expect(await spill.read("prior")).toEqual(Uint8Array.of(1, 2));
  expect(await spill.usage()).toEqual({ entries: 1, byteSize: 2 });
  await expect(batch.commit()).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
});
