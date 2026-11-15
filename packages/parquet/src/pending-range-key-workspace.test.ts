import {
  MATERIALIZATION_CHARGES as c,
  MemoryBudget,
  MemoryScope,
  SharedMemoryCache,
  sharedCacheMetadataBytes,
} from "lakeql-core";
import { expect, it, vi } from "vitest";
import { fileCacheKey, pendingFileCacheKeyBytes } from "./file-cache-key.js";
import { cachedRangeBuffer } from "./range-cache.js";

it("declines oversized optional range identities before serializing them", async () => {
  const pool = new MemoryBudget(64);
  const scope = new MemoryScope(pool);
  const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 1024 });
  const file = cachedRangeBuffer(
    {
      byteLength: 4,
      async slice() {
        throw new Error("Unexpected unowned read");
      },
      async sliceOwned(start, end, target) {
        return target.allocate(end - start, "fixture", () =>
          Uint8Array.of(1, 2, 3, 4).buffer.slice(start, end),
        );
      },
    },
    { maxBytes: 1024, sharedCache: shared },
    "x".repeat(4096),
  );
  const stringify = vi.spyOn(JSON, "stringify");
  try {
    await file.prefetchOwned?.([{ start: 0, end: 4 }], scope);
    const owner = await file.sliceOwned?.(1, 3, scope);
    expect(new Uint8Array(owner?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(2, 3));
    owner?.release();
    expect(
      stringify.mock.calls.filter(
        ([value]) => Array.isArray(value) && value[0] === "owned-scan-range-v2",
      ),
    ).toHaveLength(0);
  } finally {
    stringify.mockRestore();
    scope.dispose();
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});

it("keeps pending key workspace charged through cancellation and non-abortable settlement", async () => {
  const workspace = pendingFileCacheKeyBytes("fixture", undefined);
  const pool = new MemoryBudget(workspace + 8);
  const scope = new MemoryScope(pool.child(8));
  let finish!: () => void;
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let started!: () => void;
  const active = new Promise<void>((resolve) => {
    started = resolve;
  });
  const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 1024 });
  const file = cachedRangeBuffer(
    {
      byteLength: 4,
      async slice() {
        throw new Error("Unexpected unowned read");
      },
      async sliceOwned(start, end, target) {
        return target.allocateAsync(end - start, "non-abortable fixture", async () => {
          started();
          await gate;
          return new ArrayBuffer(end - start);
        });
      },
    },
    { maxBytes: 1024, coalesceBytes: 4, sharedCache: shared },
    "fixture",
  );
  const read = file.sliceOwned?.(0, 4, scope);
  const rejected = expect(read).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  await active;
  expect(pool.usedBytes).toBe(workspace + 8);
  scope.dispose();
  expect(pool.usedBytes).toBeGreaterThanOrEqual(workspace);
  finish();
  await rejected;
  shared.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("admits prefetch planning, keys, metadata and payload at the exact root boundary (stage=%s)", async (stage) => {
  const workspace = pendingFileCacheKeyBytes("fixture", undefined);
  const metadata = sharedCacheMetadataBytes(
    fileCacheKey("owned-scan-range-v2", "fixture", 8, undefined, 0, 8),
  );
  const bytes = c.object + 8 * c.property + 2 * workspace + metadata + 8;
  for (const shortage of [0, 1]) {
    const pool = new MemoryBudget(bytes - shortage);
    const root = new MemoryScope(pool);
    const scope = stage ? root.childStage(8, "payload") : new MemoryScope(pool.child(8));
    const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: metadata + 8 });
    let reads = 0;
    const file = cachedRangeBuffer(
      {
        byteLength: 8,
        async slice() {
          throw new Error("Unexpected unowned read");
        },
        async sliceOwned(start, end, target) {
          reads++;
          return target.allocate(
            end - start,
            "fixture payload",
            () => new ArrayBuffer(end - start),
          );
        },
      },
      { maxBytes: metadata + 8, coalesceBytes: 8, sharedCache: shared },
      "fixture",
    );
    try {
      await file.prefetchOwned?.([{ start: 0, end: 1 }], scope);
      expect(reads).toBe(shortage ? 0 : 1);
      expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
    } finally {
      scope.dispose();
      root.dispose();
      shared.deleteWhere(() => true);
    }
    expect(pool.usedBytes).toBe(0);
  }
});

it("deduplicates admitted readers and preserves a survivor after the initiator cancels", async () => {
  const workspace = pendingFileCacheKeyBytes("fixture", undefined);
  const pool = new MemoryBudget(2 * workspace + 12);
  const first = new MemoryScope(pool.child(8));
  const second = new MemoryScope(pool.child(8));
  const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 0 });
  let finish!: () => void;
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let reads = 0;
  const file = cachedRangeBuffer(
    {
      byteLength: 4,
      async slice() {
        throw new Error("Unexpected unowned read");
      },
      async sliceOwned(start, end, target) {
        reads++;
        return target.allocateAsync(end - start, "fixture payload", async () => {
          await gate;
          return new Uint8Array(end - start).fill(7).buffer;
        });
      },
    },
    { maxBytes: 4, coalesceBytes: 4, sharedCache: shared },
    "fixture",
  );
  const canceled = file.sliceOwned?.(0, 4, first);
  const rejection = expect(canceled).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  const survivor = file.sliceOwned?.(0, 4, second);
  expect(reads).toBe(1);
  first.dispose();
  expect(pool.usedBytes).toBeGreaterThanOrEqual(2 * workspace);
  finish();
  await rejection;
  const owner = await survivor;
  expect(new Uint8Array(owner?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(7, 7, 7, 7));
  owner?.release();
  second.dispose();
  shared.deleteWhere(() => true);
  expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
  expect(pool.usedBytes).toBe(0);
});

it("does not treat an admitted query payload as reserved in a sibling cache budget", async () => {
  const metadata = sharedCacheMetadataBytes(
    fileCacheKey("owned-scan-range-v2", "fixture", 8, undefined, 0, 8),
  );
  const pool = new MemoryBudget(65536);
  const root = new MemoryScope(pool);
  const scope = root.childStage(8, "query payload");
  const cacheBudget = pool.child(metadata);
  const shared = new SharedMemoryCache({ memoryBudget: cacheBudget, maxBytes: metadata + 8 });
  let reads = 0;
  const file = cachedRangeBuffer(
    {
      byteLength: 8,
      async slice() {
        throw new Error("Unexpected unowned read");
      },
      async sliceOwned(start, end, target) {
        reads++;
        return target.allocate(end - start, "fixture payload", () => new ArrayBuffer(end - start));
      },
    },
    { maxBytes: metadata + 8, coalesceBytes: 8, sharedCache: shared },
    "fixture",
  );
  try {
    await file.prefetchOwned?.([{ start: 0, end: 1 }], scope);
    expect(reads).toBe(0);
    expect(scope.availableAllocationBytes).toBe(8);
  } finally {
    root.dispose();
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});

it("reduces optional selection under a small total pool while demanded bytes remain correct", async () => {
  const pool = new MemoryBudget(8192);
  const scope = new MemoryScope(pool);
  const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 8192 });
  let reads = 0;
  const file = cachedRangeBuffer(
    {
      byteLength: 64,
      async slice() {
        throw new Error("Unexpected unowned read");
      },
      async sliceOwned(start, end, target) {
        reads++;
        return target.allocate(
          end - start,
          "fixture payload",
          () => Uint8Array.from({ length: end - start }, (_, i) => start + i).buffer,
        );
      },
    },
    { maxBytes: 8192, coalesceBytes: 0, sharedCache: shared, maxConcurrentPrefetchReads: 2 },
    "fixture",
  );
  try {
    await file.prefetchOwned?.(
      Array.from({ length: 32 }, (_, i) => ({ start: 2 * i, end: 2 * i + 1 })),
      scope,
    );
    expect(reads).toBeGreaterThan(0);
    expect(reads).toBeLessThan(32);
    for (let i = 0; i < 32; i++) {
      const owner = await file.sliceOwned?.(2 * i, 2 * i + 1, scope);
      try {
        expect(new Uint8Array(owner?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(2 * i));
      } finally {
        owner?.release();
      }
    }
    expect(pool.peakBytes).toBeLessThanOrEqual(8192);
  } finally {
    scope.dispose();
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});
