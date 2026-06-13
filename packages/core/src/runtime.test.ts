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
    await expect(spill.read(ref)).rejects.toMatchObject({ code: "LAQL_OBJECT_NOT_FOUND" });
  });

  it("enforces spill byte budgets with typed failures", async () => {
    const spill = memorySpillAdapter({ maxBytes: 4 });
    await spill.write("a", new Uint8Array([1, 2]));
    await spill.write("b", new Uint8Array([3, 4]));

    await expect(spill.write("c", new Uint8Array([5]))).rejects.toMatchObject({
      code: "LAQL_BUDGET_EXCEEDED",
      details: { metric: "spill bytes", limit: 4, actual: 5 },
    });
    await expect(spill.write("", new Uint8Array([1]))).rejects.toMatchObject({
      code: "LAQL_TYPE_ERROR",
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
