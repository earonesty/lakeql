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
