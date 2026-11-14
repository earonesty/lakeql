import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "./index.js";

it("admits only the requested write view before sending at exact capacity", async () => {
  let requests = 0;
  const store = new HttpObjectStore({
    baseUrl: "https://example.test",
    fetch: async (_url, init) => {
      requests++;
      expect([...new Uint8Array(init?.body as ArrayBuffer)]).toEqual([7, 7, 7, 7]);
      expect(new Headers(init?.headers).get("content-type")).toBe("application/json");
      return new Response(null, { status: 204 });
    },
  });
  const run = async (capacity: number) => {
    const scope = new MemoryScope(new MemoryBudget(capacity));
    const body = scope.allocate(1024, "input backing", () =>
      new Uint8Array(1024).fill(7).subarray(10, 14),
    );
    try {
      await store.putOwned("file", body, { contentType: "application/json" });
      expect(scope.budget.usedBytes).toBe(1024);
      return scope.budget.peakBytes;
    } finally {
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
    }
  };
  const peak = await run(65536);
  expect(peak).toBeLessThan(2048);
  await run(peak);
  const before = requests;
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(requests).toBe(before);
});

it.each([
  "success",
  "http-error",
  "network-error",
] as const)("holds write copy and input through delayed %s settlement", async (outcome) => {
  let finish = () => {};
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let started = () => {};
  const entered = new Promise<void>((resolve) => {
    started = resolve;
  });
  let canceled = 0;
  const store = new HttpObjectStore({
    baseUrl: "https://example.test",
    fetch: async (_url, init) => {
      started();
      await pending;
      expect([...new Uint8Array(init?.body as ArrayBuffer)]).toEqual([1, 2, 3]);
      if (outcome === "network-error") throw new Error("network failed");
      return new Response(
        new ReadableStream({
          cancel() {
            canceled++;
          },
        }),
        { status: outcome === "success" ? 200 : 503 },
      );
    },
  });
  const scope = new MemoryScope(new MemoryBudget(65536));
  const body = scope.allocate(256, "input", () => Uint8Array.of(1, 2, 3));
  const writing = store.putOwned("file", body);
  await entered;
  scope.dispose();
  expect(scope.budget.usedBytes).toBeGreaterThan(256);
  finish();
  if (outcome === "success") await writing;
  else await expect(writing).rejects.toThrow();
  expect(scope.budget.usedBytes).toBe(0);
  expect(canceled).toBe(outcome === "network-error" ? 0 : 1);
});
