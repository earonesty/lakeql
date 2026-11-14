import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { type R2BucketLike, R2ObjectStore } from "./index.js";

function bucketWithHead(head: R2BucketLike["head"]): R2BucketLike {
  return {
    head,
    get: async () => null,
    put: async () => {},
    delete: async () => {},
    list: async () => ({ objects: [], truncated: false }),
  };
}

it("admits retained R2 metadata at exact capacity and isolates mutable dates", async () => {
  const uploaded = new Date("2026-11-14T00:00:00Z");
  const store = new R2ObjectStore(
    bucketWithHead(async () => ({
      key: "file",
      size: 4096,
      etag: "version",
      uploaded,
      httpMetadata: { contentType: "application/json" },
    })),
  );
  const scope = new MemoryScope(new MemoryBudget(65536));
  const result = await store.headOwned("file", scope);
  expect(result?.value).toEqual({
    size: 4096,
    etag: "version",
    lastModified: uploaded,
    contentType: "application/json",
    rangeReadMode: "native",
  });
  expect(result?.value.lastModified).not.toBe(uploaded);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(result?.bytes);
  uploaded.setTime(0);
  expect(result?.value.lastModified?.getUTCFullYear()).toBe(2026);
  result?.release();
  expect(scope.budget.usedBytes).toBe(0);
  for (const capacity of [peak - 1, peak]) {
    const exact = new MemoryScope(new MemoryBudget(capacity));
    if (capacity < peak)
      await expect(store.headOwned("file", exact)).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
      });
    else (await store.headOwned("file", exact))?.release();
    exact.dispose();
    expect(exact.budget.usedBytes).toBe(0);
  }
});

it("returns missing R2 metadata without charging a result", async () => {
  const scope = new MemoryScope(new MemoryBudget(0));
  const store = new R2ObjectStore(bucketWithHead(async () => null));
  expect(await store.headOwned("missing", scope)).toBeNull();
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("rejects a late metadata result after scope disposal", async () => {
  let finish = () => {};
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const store = new R2ObjectStore(
    bucketWithHead(async () => {
      await pending;
      return { key: "file", size: 10 };
    }),
  );
  const scope = new MemoryScope(new MemoryBudget(4096));
  const result = store.headOwned("file", scope);
  scope.dispose();
  finish();
  await expect(result).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(scope.budget.usedBytes).toBe(0);
});
