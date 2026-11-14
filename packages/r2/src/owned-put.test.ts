import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { type R2BucketLike, R2ObjectStore } from "./index.js";

function bucketWithPut(put: R2BucketLike["put"]): R2BucketLike {
  return {
    get: async () => null,
    head: async () => null,
    put,
    delete: async () => {},
    list: async () => ({ objects: [], truncated: false }),
  };
}

it("admits R2 write options before invoking the binding at exact capacity", async () => {
  let requests = 0;
  const bytes = new Uint8Array(4096).subarray(10, 14);
  const store = new R2ObjectStore(
    bucketWithPut(async (_path, body, options) => {
      requests++;
      expect(body).toBe(bytes);
      expect(options).toEqual({
        httpMetadata: { contentType: "application/json" },
        customMetadata: { source: "fixture" },
      });
    }),
  );
  const run = async (capacity: number) => {
    const scope = new MemoryScope(new MemoryBudget(capacity));
    const owner = scope.allocate(4096, "fixture input", () => bytes);
    try {
      await store.putOwned("file", owner, {
        contentType: "application/json",
        metadata: { source: "fixture" },
      });
      expect(scope.budget.usedBytes).toBe(4096);
      return scope.budget.peakBytes;
    } finally {
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
    }
  };
  const peak = await run(65536);
  await run(peak);
  const before = requests;
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(requests).toBe(before);
});

it.each([
  false,
  true,
])("retains the original R2 write view through delayed settlement (failure=%s)", async (fail) => {
  let finish = () => {};
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let started = () => {};
  const entered = new Promise<void>((resolve) => {
    started = resolve;
  });
  const bytes = Uint8Array.of(1, 2, 3);
  const store = new R2ObjectStore(
    bucketWithPut(async (_path, body) => {
      expect(body).toBe(bytes);
      started();
      await pending;
      expect([...(body as Uint8Array)]).toEqual([1, 2, 3]);
      if (fail) throw new Error("upload failed");
    }),
  );
  const scope = new MemoryScope(new MemoryBudget(65536));
  const owner = scope.allocate(256, "input", () => bytes);
  const writing = store.putOwned("file", owner);
  await entered;
  scope.dispose();
  expect(scope.budget.usedBytes).toBeGreaterThan(256);
  finish();
  if (fail) await expect(writing).rejects.toThrow("upload failed");
  else await writing;
  expect(scope.budget.usedBytes).toBe(0);
});
