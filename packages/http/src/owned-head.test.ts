import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "./index.js";

it.each([206, 200])("admits HTTP %s metadata probe overlap at exact capacity", async (status) => {
  const body = new Uint8Array(status === 206 ? 2 : 4096);
  const store = new HttpObjectStore({
    baseUrl: "https://example.test",
    fetch: async () =>
      new Response(body, {
        status,
        headers: {
          "content-length": String(body.length),
          ...(status === 206 ? { "content-range": "bytes 0-1/4096" } : {}),
          etag: "version",
          "last-modified": "Sat, 14 Nov 2026 00:00:00 GMT",
          "content-type": "application/octet-stream",
        },
      }),
  });
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const owner = await store.headOwned("file", scope);
  expect(owner?.value).toMatchObject({
    size: 4096,
    etag: "version",
    rangeReadMode: status === 206 ? "native" : "emulated",
  });
  const peak = pool.peakBytes;
  scope.dispose();
  expect(pool.usedBytes).toBe(owner?.bytes);
  owner?.release();
  expect(pool.usedBytes).toBe(0);
  for (const capacity of [peak - 1, peak]) {
    const target = new MemoryScope(new MemoryBudget(capacity));
    if (capacity < peak)
      await expect(store.headOwned("file", target)).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
      });
    else (await store.headOwned("file", target))?.release();
    target.dispose();
    expect(target.budget.usedBytes).toBe(0);
  }
});

it("uses an admitted full response for compressed range probes", async () => {
  const ranges: (string | null)[] = [];
  const store = new HttpObjectStore({
    baseUrl: "https://example.test",
    fetch: async (_input, init) => {
      const range = new Headers(init?.headers).get("range");
      ranges.push(range);
      return range
        ? new Response(new Uint8Array([0x1f, 0x8b]), {
            status: 206,
            headers: { "content-range": "bytes 0-1/20" },
          })
        : new Response(new Uint8Array(512), { headers: { etag: "full" } });
    },
  });
  const scope = new MemoryScope(new MemoryBudget(65536));
  const owner = await store.headOwned("file", scope);
  expect(owner?.value).toMatchObject({ size: 512, rangeReadMode: "emulated", etag: "full" });
  expect(ranges).toEqual(["bytes=0-1", null]);
  owner?.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("cancels oversized probe bodies and leaves no reservations", async () => {
  let canceled = false;
  const store = new HttpObjectStore({
    baseUrl: "https://example.test",
    fetch: async () =>
      new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new Uint8Array(4096));
          },
          cancel() {
            canceled = true;
          },
        }),
      ),
  });
  const scope = new MemoryScope(new MemoryBudget(1024));
  await expect(store.headOwned("file", scope)).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  expect(canceled).toBe(true);
});

it("reuses admitted full probes and isolates range output from cache mutation", async () => {
  const pool = new MemoryBudget(65536);
  let requests = 0;
  const store = new HttpObjectStore({
    baseUrl: "https://example.test",
    memoryBudget: pool,
    fetch: async () => {
      requests++;
      return new Response(new Uint8Array([1, 2, 3, 4]));
    },
  });
  const headScope = new MemoryScope(pool);
  (await store.headOwned("file", headScope))?.release();
  headScope.dispose();
  expect(pool.usedBytes).toBeGreaterThan(4);
  const readScope = new MemoryScope(pool);
  const first = await store.getRangeOwned("file", { offset: 1, length: 2 }, readScope);
  expect([...first.value]).toEqual([2, 3]);
  first.value[0] = 99;
  const second = await store.getRangeOwned("file", { offset: 1, length: 2 }, readScope);
  expect([...second.value]).toEqual([2, 3]);
  expect(requests).toBe(1);
  store.clearFullObjectCache();
  expect([...second.value]).toEqual([2, 3]);
  readScope.dispose();
  expect(pool.usedBytes).toBe(0);
});

it.each([
  0, 100,
])("declines full-probe retention above cache limit %s without failing metadata", async (maxFullObjectCacheBytes) => {
  const pool = new MemoryBudget(65536);
  const store = new HttpObjectStore({
    baseUrl: "https://example.test",
    memoryBudget: pool,
    maxFullObjectCacheBytes,
    fetch: async () => new Response(new Uint8Array(200)),
  });
  const scope = new MemoryScope(pool);
  const owner = await store.headOwned("file", scope);
  expect(owner?.value.size).toBe(200);
  owner?.release();
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("reclaims full probes for sibling allocations and invalidates after writes", async () => {
  const pool = new MemoryBudget(4096);
  const store = new HttpObjectStore({
    baseUrl: "https://example.test",
    memoryBudget: pool,
    fetch: async (_url, init) => new Response(init?.method === "PUT" ? null : new Uint8Array(200)),
  });
  for (const action of ["pressure", "write"] as const) {
    const scope = new MemoryScope(pool);
    (await store.headOwned("file", scope))?.release();
    scope.dispose();
    expect(pool.usedBytes).toBeGreaterThan(0);
    if (action === "pressure") {
      const sibling = pool.reserve(4096, "sibling");
      sibling.release();
    } else await store.put("file", new Uint8Array(1));
    expect(pool.usedBytes).toBe(0);
  }
});

it("falls back to bounded transport when a child cannot pin the full cached backing", async () => {
  const pool = new MemoryBudget(65536);
  let requests = 0;
  const store = new HttpObjectStore({
    baseUrl: "https://example.test",
    memoryBudget: pool,
    fetch: async () => {
      requests++;
      return new Response(new Uint8Array(4096).fill(7));
    },
  });
  const head = new MemoryScope(pool);
  (await store.headOwned("file", head))?.release();
  head.dispose();
  const child = new MemoryScope(pool.child(512));
  const range = await store.getRangeOwned("file", { offset: 2, length: 2 }, child);
  expect([...range.value]).toEqual([7, 7]);
  expect(requests).toBe(2);
  child.dispose();
  store.clearFullObjectCache();
  expect(pool.usedBytes).toBe(0);
});

it("binds default pools immutably and preserves explicit pool and cache limits", async () => {
  const pool = new MemoryBudget(65536);
  const original = new HttpObjectStore({
    baseUrl: "https://example.test",
    maxFullObjectCacheBytes: 0,
    fetch: async () => new Response(new Uint8Array(200)),
  });
  const bound = original.withDefaultMemoryBudget(pool, 4096);
  expect(bound).not.toBe(original);
  expect(bound.withDefaultMemoryBudget(new MemoryBudget(65536))).toBe(bound);
  const scope = new MemoryScope(pool);
  (await bound.headOwned("file", scope))?.release();
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
});
