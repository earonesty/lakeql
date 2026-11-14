import {
  MemoryBudget,
  MemoryScope,
  memoryStore,
  type ScanOptions,
  SharedMemoryCache,
} from "lakeql-core";
import { expect, it } from "vitest";
import { cachedRangeBuffer } from "./range-cache.js";
import { asyncBufferFromObjectInfo } from "./store-buffer.js";

function options(signal: AbortSignal): ScanOptions {
  return {
    budget: { signal, maxConcurrentReads: 1 },
    now: () => 0,
    startedAt: 0,
    stats: {
      queryId: "wrapper",
      elapsedMs: 0,
      manifestsRead: 0,
      manifestsSkipped: 0,
      filesPlanned: 0,
      filesRead: 0,
      filesSkipped: 0,
      rowGroupsRead: 0,
      rowGroupsSkipped: 0,
      columnsRead: [],
      bytesRequested: 0,
      rangeRequests: 0,
      rowsDecoded: 0,
      rowsMatched: 0,
      rowsReturned: 0,
      cacheHits: 0,
      cacheMisses: 0,
    },
  };
}

it.each([
  { cancelAll: false, admitted: false },
  { cancelAll: true, admitted: false },
  { cancelAll: false, admitted: true },
  { cancelAll: true, admitted: true },
])("shares reads across scanner wrappers without inheriting first-query cancellation (%j)", async ({
  cancelAll,
  admitted,
}) => {
  const root = new MemoryBudget(admitted ? 20 : 100);
  const cache = new SharedMemoryCache({ maxBytes: 16, memoryBudget: root });
  let finish: () => void = () => {};
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let reads = 0;
  let physicalSignal: AbortSignal | undefined;
  const store = {
    ...memoryStore(),
    async getRangeOwned(
      _path: string,
      range: { offset: number; length: number },
      scope: MemoryScope,
    ) {
      reads++;
      physicalSignal = scope.signal;
      return scope.allocateAsync(range.length, "physical read", async () => {
        await gate;
        return new Uint8Array(range.length).fill(7);
      });
    },
  };
  const controllers = [new AbortController(), new AbortController()];
  const scopes = controllers.map(
    (controller) => new MemoryScope(root.child(), [controller.signal]),
  );
  if (admitted) for (const scope of scopes) scope.admit(10, "transport and output");
  const files = controllers.map((controller) =>
    cachedRangeBuffer(
      asyncBufferFromObjectInfo(
        store,
        { path: "file", size: 16, etag: "v1" },
        options(controller.signal),
      ),
      { maxBytes: 16, coalesceBytes: 8, sharedCache: cache },
      "file",
    ),
  );
  const readsPromise = Promise.allSettled(
    files.map((file, index) => file.sliceOwned?.(1, 3, scopes[index] as MemoryScope)),
  );
  // Read controls acquire asynchronously. Let both wrapper requests join.
  for (let i = 0; i < 8; i++) await Promise.resolve();
  expect(reads).toBe(1);
  controllers[0]?.abort();
  if (cancelAll) controllers[1]?.abort();
  expect(physicalSignal?.aborted).toBe(cancelAll);
  finish();
  const results = await readsPromise;
  expect(results[0]?.status).toBe("rejected");
  if (cancelAll) expect(results[1]?.status).toBe("rejected");
  else {
    const result = results[1];
    if (result?.status !== "fulfilled" || result.value === undefined)
      throw new Error("Missing survivor");
    expect(new Uint8Array(result.value.value)).toEqual(Uint8Array.of(7, 7));
    result.value.release();
  }
  for (const scope of scopes) scope.dispose();
  cache.deleteWhere(() => true);
  expect(root.usedBytes).toBe(0);
});

it("clears shared pending failure so another wrapper can retry", async () => {
  const root = new MemoryBudget(100);
  const cache = new SharedMemoryCache({ maxBytes: 16, memoryBudget: root });
  let reads = 0;
  let rejectFirst = true;
  const store = {
    ...memoryStore(),
    async getRangeOwned(
      _path: string,
      range: { offset: number; length: number },
      scope: MemoryScope,
    ) {
      reads++;
      return scope.allocateAsync(range.length, "physical read", async () => {
        await Promise.resolve();
        if (rejectFirst) throw new Error("transport failed");
        return new Uint8Array(range.length).fill(9);
      });
    },
  };
  const makeFile = (etag: string) =>
    cachedRangeBuffer(
      asyncBufferFromObjectInfo(store, { path: "file", size: 16, etag }),
      { maxBytes: 16, coalesceBytes: 8, sharedCache: cache },
      "file",
    );
  const first = new MemoryScope(root.child());
  const second = new MemoryScope(root.child());
  const failed = await Promise.allSettled([
    makeFile("v1").sliceOwned?.(0, 2, first),
    makeFile("v1").sliceOwned?.(0, 2, second),
  ]);
  expect(failed.map((result) => result.status)).toEqual(["rejected", "rejected"]);
  expect(reads).toBe(1);
  expect(root.usedBytes).toBe(0);
  rejectFirst = false;
  const outputs = await Promise.all([
    makeFile("v1").sliceOwned?.(0, 2, first),
    makeFile("v2").sliceOwned?.(0, 2, second),
  ]);
  expect(reads).toBe(3);
  for (const output of outputs) {
    expect(new Uint8Array(output?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(9, 9));
    output?.release();
  }
  first.dispose();
  second.dispose();
  cache.deleteWhere(() => true);
  expect(root.usedBytes).toBe(0);
});
