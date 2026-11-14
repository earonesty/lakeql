import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { cachedObjectStore } from "./object-store-cache.js";
import { type ObjectStore, uriObjectStore, withObjectStoreReadControls } from "./store.js";

it("forwards owned writes and invalidates cache entries populated during a pending write", async () => {
  const backing = memoryStore();
  await backing.put("file", Uint8Array.of(1));
  let finish = () => {};
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const store: ObjectStore = backing;
  store.putOwned = async (path, body) => {
    expect(path).toBe("file");
    await pending;
    await backing.put(path, body.value);
  };
  const wrapped = withObjectStoreReadControls(
    cachedObjectStore(
      uriObjectStore(store, {
        scheme: "s3",
        authority: "bucket",
      }),
      { maxBytes: 4096 },
    ),
    { maxConcurrentReads: 1 },
  );
  expect([...((await wrapped.get("s3://bucket/file")) as Uint8Array)]).toEqual([1]);
  const scope = new MemoryScope(new MemoryBudget(4096));
  const body = scope.allocate(256, "input", () => Uint8Array.of(2));
  const writing = wrapped.putOwned?.("s3://bucket/file", body);
  expect([...((await wrapped.get("s3://bucket/file")) as Uint8Array)]).toEqual([1]);
  finish();
  await writing;
  expect([...((await wrapped.get("s3://bucket/file")) as Uint8Array)]).toEqual([2]);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("does not send owned writes through already-aborted controls", () => {
  const backing: ObjectStore = memoryStore();
  let writes = 0;
  backing.putOwned = async () => {
    writes++;
  };
  const controller = new AbortController();
  const wrapped = withObjectStoreReadControls(backing, { signal: controller.signal });
  controller.abort();
  const scope = new MemoryScope(new MemoryBudget(4096));
  const body = scope.allocate(256, "input", () => Uint8Array.of(2));
  expect(() => wrapped.putOwned?.("file", body)).toThrow();
  expect(writes).toBe(0);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
