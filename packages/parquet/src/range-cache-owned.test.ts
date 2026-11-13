import { LakeqlError, MemoryBudget, MemoryScope, SharedMemoryCache } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { cachedRangeBuffer } from "./range-cache.js";
import type { StoreAsyncBuffer } from "./types.js";

function source() {
  let reads = 0;
  let allocations = 0;
  const file: StoreAsyncBuffer = {
    byteLength: 16,
    async slice() {
      throw new Error("Untracked read");
    },
    async sliceOwned(start, end, scope) {
      reads += 1;
      return scope.allocateAsync(end - start, "source", async () => {
        allocations++;
        await Promise.resolve();
        return Uint8Array.from({ length: end - start }, (_, index) => start + index).buffer;
      });
    },
  };
  return { file, reads: () => reads, allocations: () => allocations };
}

describe("owned range cache", () => {
  it("admits delayed adapter scratch after the initiating consumer cancels", async () => {
    const root = new MemoryBudget(10);
    const first = new MemoryScope(root.child());
    const second = new MemoryScope(root.child());
    first.admit(8, "copy plus range plus adapter scratch");
    second.admit(2, "copy of shared range");
    const cache = new SharedMemoryCache({ maxBytes: 4, memoryBudget: root });
    let finish!: () => void;
    const gate = new Promise<void>((resolve) => {
      finish = resolve;
    });
    let reads = 0;
    let signal: AbortSignal | undefined;
    const file = cachedRangeBuffer(
      {
        byteLength: 8,
        async slice() {
          throw new Error("Untracked read");
        },
        async sliceOwned(start, end, stage) {
          reads++;
          signal = stage.signal;
          await gate;
          const scratch = stage.allocate(2, "adapter scratch", () => new Uint8Array(2));
          scratch.release();
          return stage.allocate(
            end - start,
            "transport",
            () => new Uint8Array(end - start).fill(7).buffer,
          );
        },
      },
      { maxBytes: 4, coalesceBytes: 4, sharedCache: cache },
      "late-scratch",
    );
    const canceled = file.sliceOwned?.(0, 2, first);
    const rejected = expect(canceled).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    const survivor = file.sliceOwned?.(1, 3, second);
    expect(reads).toBe(1);
    first.dispose();
    expect(signal?.aborted).toBe(false);
    expect(root.usedBytes).toBe(10);
    finish();
    await rejected;
    const output = await survivor;
    expect(new Uint8Array(output?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(7, 7));
    expect(root.peakBytes).toBe(10);
    output?.release();
    second.dispose();
    cache.deleteWhere(() => true);
    expect(root.usedBytes).toBe(0);
  });

  it("consumes one admitted extent for coalesced transport, copies and a warm hit", async () => {
    const budget = new MemoryBudget(14);
    const scope = new MemoryScope(budget);
    scope.admit(14, "eight-byte transport plus three two-byte outputs");
    const cache = new SharedMemoryCache({ maxBytes: 8, memoryBudget: budget });
    const input = source();
    const file = cachedRangeBuffer(
      input.file,
      { maxBytes: 8, coalesceBytes: 8, sharedCache: cache },
      "admitted",
    );
    const outputs = await Promise.all([
      file.sliceOwned?.(1, 3, scope),
      file.sliceOwned?.(2, 4, scope),
    ]);
    expect(input.reads()).toBe(1);
    expect(scope.availableAllocationBytes).toBe(2);
    for (const output of outputs) output?.release();
    const warm = await file.sliceOwned?.(1, 3, scope);
    expect(new Uint8Array(warm?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(1, 2));
    expect(input.reads()).toBe(1);
    expect(scope.availableAllocationBytes).toBe(0);
    expect(budget.peakBytes).toBe(14);
    warm?.release();
    scope.dispose();
    expect(budget.usedBytes).toBe(8);
    cache.deleteWhere(() => true);
    expect(budget.usedBytes).toBe(0);
  });

  it.each([
    4, 3,
  ])("uses exact demand when coalescing exceeds admission at extent %s", async (extent) => {
    const budget = new MemoryBudget(extent);
    const scope = new MemoryScope(budget);
    scope.admit(extent, "demand");
    const cache = new SharedMemoryCache({ maxBytes: 8, memoryBudget: budget });
    const input = source();
    const file = cachedRangeBuffer(
      input.file,
      { maxBytes: 8, coalesceBytes: 8, sharedCache: cache },
      "tight-admission",
    );
    try {
      if (extent === 3) {
        await expect(file.sliceOwned?.(1, 3, scope)).rejects.toMatchObject({
          code: "LAKEQL_BUDGET_EXCEEDED",
        });
        expect(input.allocations()).toBe(0);
      } else {
        const output = await file.sliceOwned?.(1, 3, scope);
        expect(new Uint8Array(output?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(1, 2));
        expect(scope.availableAllocationBytes).toBe(0);
        output?.release();
      }
    } finally {
      scope.dispose();
      cache.deleteWhere(() => true);
    }
    expect(budget.usedBytes).toBe(0);
  });

  it("deduplicates coalesced prefetch and demand, pins copies, and reuses cached bytes", async () => {
    const budget = new MemoryBudget(24);
    const scope = new MemoryScope(budget);
    const cache = new SharedMemoryCache({ maxBytes: 16, memoryBudget: budget });
    const input = source();
    const file = cachedRangeBuffer(
      input.file,
      { maxBytes: 16, coalesceBytes: 8, sharedCache: cache },
      "file",
    );
    const [, first, second] = await Promise.all([
      file.prefetchOwned?.(
        [
          { start: 0, end: 2 },
          { start: 2, end: 4 },
        ],
        scope,
      ),
      file.sliceOwned?.(1, 3, scope),
      file.sliceOwned?.(2, 4, scope),
    ]);
    expect(input.reads()).toBe(1);
    expect(first?.value.byteLength).toBe(2);
    expect(new Uint8Array(second?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(2, 3));
    expect(budget.usedBytes).toBe(12);
    new Uint8Array(first?.value ?? new ArrayBuffer(0)).fill(99);
    first?.release();
    second?.release();
    const warm = await file.sliceOwned?.(1, 3, scope);
    expect(new Uint8Array(warm?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(1, 2));
    expect(input.reads()).toBe(1);
    scope.dispose();
    expect(budget.usedBytes).toBe(8);
    cache.deleteWhere(() => true);
    expect(budget.usedBytes).toBe(0);
  });

  it("releases pending ownership when even exact input plus output cannot fit", async () => {
    const budget = new MemoryBudget(8);
    const scope = new MemoryScope(budget);
    const input = source();
    const file = cachedRangeBuffer(input.file, { maxBytes: 8, coalesceBytes: 8 }, "file");
    await expect(file.sliceOwned?.(0, 5, scope)).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
    expect(budget.usedBytes).toBe(0);
    await file.prefetchOwned?.([{ start: 0, end: 1 }], scope);
    expect(input.reads()).toBe(2);
    expect(budget.usedBytes).toBe(8);
    const reservation = budget.reserve(8, "reclaim cache");
    expect(budget.usedBytes).toBe(8);
    reservation.release();
    expect(budget.usedBytes).toBe(0);
  });

  it("closing one waiter does not invalidate another waiter sharing its read", async () => {
    const budget = new MemoryBudget(16);
    const first = new MemoryScope(budget);
    const second = new MemoryScope(budget);
    const input = source();
    const cache = new SharedMemoryCache({ memoryBudget: budget });
    const file = cachedRangeBuffer(
      input.file,
      { maxBytes: 8, coalesceBytes: 8, sharedCache: cache },
      "file",
    );
    const cancelled = file.sliceOwned?.(0, 1, first);
    const active = file.sliceOwned?.(1, 2, second);
    first.dispose();
    await expect(cancelled).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    const output = await active;
    expect(new Uint8Array(output?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(1));
    expect(input.reads()).toBe(1);
    second.dispose();
    cache.deleteWhere(() => true);
    expect(budget.usedBytes).toBe(0);
  });
});

it("shrinks read-ahead to exact demand using current shared-pool headroom", async () => {
  const pool = new MemoryBudget(12);
  const budget = pool.child(16);
  const scope = new MemoryScope(budget);
  const competing = pool.reserve(8, "other job");
  const input = source();
  const file = cachedRangeBuffer(input.file, { maxBytes: 8, coalesceBytes: 8 }, "dynamic");
  const output = await file.sliceOwned?.(2, 4, scope);
  expect(new Uint8Array(output?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(2, 3));
  expect(input.reads()).toBe(1);
  expect(pool.peakBytes).toBe(12);
  expect(budget.peakBytes).toBe(4);
  output?.release();
  competing.release();
  const reclaim = budget.reserve(12, "evict");
  reclaim.release();
  expect(pool.usedBytes).toBe(0);
});

it("does not issue an I/O for an empty demand or an invalid owned range", async () => {
  const scope = new MemoryScope(new MemoryBudget(0));
  const input = source();
  const file = cachedRangeBuffer(input.file, { maxBytes: 8 }, "empty");
  const empty = await file.sliceOwned?.(16, 16, scope);
  expect(empty?.value.byteLength).toBe(0);
  for (const [start, end] of [
    [-1, 1],
    [2, 1],
    [0, 17],
    [0, NaN],
  ]) {
    await expect(file.sliceOwned?.(start, end, scope)).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
  }
  expect(input.reads()).toBe(0);
  scope.dispose();
});

it("settles every in-flight prefetch before reporting an error", async () => {
  const budget = new MemoryBudget(16);
  const scope = new MemoryScope(budget);
  const cache = new SharedMemoryCache({ memoryBudget: budget });
  let finish: (() => void) | undefined;
  let started: (() => void) | undefined;
  const ready = new Promise<void>((resolve) => {
    started = resolve;
  });
  const source: StoreAsyncBuffer = {
    byteLength: 16,
    async slice() {
      throw new Error("untracked");
    },
    async sliceOwned(start, end, stage) {
      if (start === 0) throw new Error("first request failed");
      return stage.allocateAsync(
        end - start,
        "pending network",
        () =>
          new Promise<ArrayBuffer>((resolve) => {
            finish = () => resolve(new ArrayBuffer(end - start));
            started?.();
          }),
      );
    },
  };
  const file = cachedRangeBuffer(
    source,
    { maxBytes: 16, coalesceBytes: 8, sharedCache: cache },
    "failure",
  );
  let settled = false;
  const read = file.prefetchOwned?.(
    [
      { start: 0, end: 1 },
      { start: 8, end: 9 },
    ],
    scope,
  );
  const observed = Promise.resolve(read).then(
    () => {
      settled = true;
      return undefined;
    },
    (error: unknown) => {
      settled = true;
      return error;
    },
  );
  await ready;
  await Promise.resolve();
  await Promise.resolve();
  expect(settled).toBe(false);
  expect(budget.usedBytes).toBe(8);
  scope.dispose();
  expect(budget.usedBytes).toBe(8);
  finish?.();
  expect(await observed).toMatchObject({ message: "first request failed" });
  cache.deleteWhere(() => true);
  expect(budget.usedBytes).toBe(0);
});

it("aborts the shared I/O only after its last active waiter leaves and permits a new read", async () => {
  const budget = new MemoryBudget(32);
  const first = new MemoryScope(budget);
  const second = new MemoryScope(budget);
  const retry = new MemoryScope(budget);
  const cache = new SharedMemoryCache({ memoryBudget: budget });
  let reads = 0;
  let aborts = 0;
  const file = cachedRangeBuffer(
    {
      byteLength: 16,
      async slice() {
        throw new Error("untracked");
      },
      async sliceOwned(start, end, stage) {
        reads += 1;
        if (reads > 1)
          return stage.allocate(end - start, "retry", () => new ArrayBuffer(end - start));
        return stage.allocateAsync(
          end - start,
          "pending HTTP",
          () =>
            new Promise<ArrayBuffer>((_resolve, reject) => {
              stage.signal.addEventListener(
                "abort",
                () => {
                  aborts += 1;
                  reject(new Error("transport cancelled"));
                },
                { once: true },
              );
            }),
        );
      },
    },
    { maxBytes: 16, coalesceBytes: 8, sharedCache: cache },
    "cancel",
  );
  const one = file.sliceOwned?.(0, 1, first);
  const two = file.sliceOwned?.(1, 2, second);
  const stopped = Promise.allSettled([one, two]);
  expect(reads).toBe(1);
  first.dispose();
  expect(aborts).toBe(0);
  second.dispose();
  expect(aborts).toBe(1);
  // Start before the cancelled read has finished unwinding.
  const fresh = await file.sliceOwned?.(0, 1, retry);
  expect(reads).toBe(2);
  expect(fresh?.value.byteLength).toBe(1);
  const outcomes = await stopped;
  expect(outcomes.every((outcome) => outcome.status === "rejected")).toBe(true);
  retry.dispose();
  cache.deleteWhere(() => true);
  expect(budget.usedBytes).toBe(0);
});

it("declines speculative allocation under pressure while allowing small demand", async () => {
  const budget = new MemoryBudget(4);
  const scope = new MemoryScope(budget);
  const input = source();
  const cache = new SharedMemoryCache({ memoryBudget: budget });
  const file = cachedRangeBuffer(
    input.file,
    { maxBytes: 16, coalesceBytes: 8, sharedCache: cache },
    "pressure",
  );
  await file.prefetchOwned?.(
    [
      { start: 0, end: 1 },
      { start: 8, end: 9 },
    ],
    scope,
  );
  expect(budget.usedBytes).toBe(0);
  expect(budget.peakBytes).toBe(0);
  expect(input.reads()).toBe(0);
  const demanded = await file.sliceOwned?.(2, 4, scope);
  expect(new Uint8Array(demanded?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(2, 3));
  expect(budget.peakBytes).toBe(4);
  scope.dispose();
  cache.deleteWhere(() => true);
  expect(budget.usedBytes).toBe(0);
});

it("does not suppress physical-read budgets or transport failures during prefetch", async () => {
  for (const error of [
    new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "too many reads", { metric: "range requests" }),
    new Error("network failure"),
  ]) {
    const scope = new MemoryScope(new MemoryBudget(16));
    const file = cachedRangeBuffer(
      {
        byteLength: 16,
        async slice() {
          throw new Error("untracked");
        },
        async sliceOwned() {
          throw error;
        },
      },
      { maxBytes: 16, coalesceBytes: 8 },
      "failure",
    );
    await expect(file.prefetchOwned?.([{ start: 0, end: 1 }], scope)).rejects.toBe(error);
    expect(scope.budget.usedBytes).toBe(0);
    scope.dispose();
  }
});

it("tolerates allocation pressure arriving after prefetch admission", async () => {
  const budget = new MemoryBudget(8);
  const scope = new MemoryScope(budget);
  let attempted = false;
  const file = cachedRangeBuffer(
    {
      byteLength: 8,
      async slice() {
        throw new Error("untracked");
      },
      async sliceOwned(start, end, stage) {
        attempted = true;
        const competitor = budget.reserve(1, "concurrent job");
        try {
          return stage.allocate(end - start, "response", () => new ArrayBuffer(end - start));
        } finally {
          competitor.release();
        }
      },
    },
    { maxBytes: 8, coalesceBytes: 8 },
    "racing-admission",
  );
  await file.prefetchOwned?.([{ start: 0, end: 1 }], scope);
  expect(attempted).toBe(true);
  expect(budget.peakBytes).toBe(1);
  expect(budget.usedBytes).toBe(0);
});

it("reuses owned ranges across job budgets and shrinks oversized cached windows for small readers", async () => {
  const pool = new MemoryBudget(32);
  const cache = new SharedMemoryCache({ maxBytes: 16, memoryBudget: pool });
  const input = source();
  const file = cachedRangeBuffer(
    input.file,
    { maxBytes: 16, coalesceBytes: 8, sharedCache: cache },
    "jobs",
  );
  const producer = new MemoryScope(pool.child(16));
  await file.prefetchOwned?.([{ start: 0, end: 1 }], producer);
  producer.dispose();
  expect(producer.budget.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(8);
  const next = new MemoryScope(pool.child(16));
  const hit = await file.sliceOwned?.(1, 3, next);
  expect(input.reads()).toBe(1);
  expect(hit?.bytes).toBe(2);
  expect(next.budget.usedBytes).toBe(2);
  expect(pool.usedBytes).toBe(10);
  next.dispose();
  const small = new MemoryScope(pool.child(4));
  const exact = await file.sliceOwned?.(1, 3, small);
  expect(new Uint8Array(exact?.value ?? new ArrayBuffer(0))).toEqual(Uint8Array.of(1, 2));
  expect(input.reads()).toBe(2);
  expect(small.budget.peakBytes).toBe(4);
  small.dispose();
  cache.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(0);
});

it("deduplicates in-flight reads across separate job budgets in a shared pool", async () => {
  const pool = new MemoryBudget(16);
  const first = new MemoryScope(pool.child(10));
  const second = new MemoryScope(pool.child(10));
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 8 });
  const input = source();
  const file = cachedRangeBuffer(
    input.file,
    { maxBytes: 8, coalesceBytes: 8, sharedCache: cache },
    "concurrent-jobs",
  );
  const outputs = await Promise.all([
    file.sliceOwned?.(0, 2, first),
    file.sliceOwned?.(2, 4, second),
  ]);
  expect(input.reads()).toBe(1);
  expect(first.budget.usedBytes).toBe(2);
  expect(second.budget.usedBytes).toBe(2);
  expect(pool.usedBytes).toBe(12);
  expect(outputs.map((owner) => [...new Uint8Array(owner?.value ?? new ArrayBuffer(0))])).toEqual([
    [0, 1],
    [2, 3],
  ]);
  first.dispose();
  second.dispose();
  cache.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(0);
});

it.each([
  8, 32,
])("preserves higher-priority decoded backing during speculative reads (pool=%s)", async (poolBytes) => {
  const root = new MemoryBudget(poolBytes);
  const shared = new SharedMemoryCache({ memoryBudget: root, maxBytes: 8 });
  const decoded = new DecodedColumnCache(shared, { maxBytes: 8, policy: "latency" });
  const producer = new MemoryScope(root.child());
  const values = producer.allocate(8, "decoded values", () =>
    Uint8Array.of(1, 2, 3, 4, 5, 6, 7, 8),
  );
  decoded.setVector("hot-page", { type: "u8", values: values.value }, producer);
  producer.dispose();
  const input = source();
  const ranges = cachedRangeBuffer(
    input.file,
    { maxBytes: 8, coalesceBytes: 8, sharedCache: shared },
    "speculation",
  );
  const job = new MemoryScope(root.child());
  await ranges.prefetchOwned?.([{ start: 0, end: 8 }], job);
  expect(input.reads()).toBe(0);
  if (poolBytes === 32) {
    const demanded = await ranges.sliceOwned?.(0, 8, job);
    expect(new Uint8Array(demanded?.value ?? new ArrayBuffer(0))).toEqual(
      Uint8Array.of(0, 1, 2, 3, 4, 5, 6, 7),
    );
    expect(input.reads()).toBe(1);
    demanded?.release();
  }
  const hit = decoded.getVector("hot-page", job);
  expect(hit).toMatchObject({ type: "u8", values: Uint8Array.of(1, 2, 3, 4, 5, 6, 7, 8) });
  job.dispose();
  expect(root.usedBytes).toBe(8);
  shared.deleteWhere(() => true);
  expect(root.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("uses post-expiry allocation headroom without ignoring live pins (pinned=%s)", async (pinned) => {
  const root = new MemoryBudget(8);
  const shared = new SharedMemoryCache({ memoryBudget: root, maxBytes: 8, ttlMs: 0 });
  const producer = new MemoryScope(root);
  const payload = producer.allocate(8, "old cache data", () => new Uint8Array(8));
  const pin = pinned ? payload.retain() : undefined;
  expect(shared.setOwned("expired", payload, { priority: 4 })).toBe(true);
  producer.dispose();
  expect(root.usedBytes).toBe(8);
  const input = source();
  const ranges = cachedRangeBuffer(
    input.file,
    { maxBytes: 8, coalesceBytes: 8, sharedCache: shared },
    "expiry",
  );
  const job = new MemoryScope(root.child());
  await ranges.prefetchOwned?.([{ start: 0, end: 8 }], job);
  expect(input.reads()).toBe(pinned ? 0 : 1);
  expect(root.peakBytes).toBe(8);
  job.dispose();
  shared.deleteWhere(() => true);
  expect(root.usedBytes).toBe(pinned ? 8 : 0);
  pin?.release();
  expect(root.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("avoids unusable read-ahead for incompatible owned caches (unrelated=%s)", async (unrelated) => {
  const budget = new MemoryBudget(32);
  const scope = new MemoryScope(budget);
  const cacheBudget = new MemoryBudget(32);
  const cache = new SharedMemoryCache({
    maxBytes: 16,
    ...(unrelated ? { memoryBudget: cacheBudget } : {}),
  });
  const ranges: number[][] = [];
  const input = source();
  const file = cachedRangeBuffer(
    {
      ...input.file,
      async sliceOwned(start, end, target) {
        ranges.push([start, end]);
        const read = input.file.sliceOwned;
        if (read === undefined) throw new Error("missing owned reader");
        return read(start, end, target);
      },
    },
    { maxBytes: 16, coalesceBytes: 8, sharedCache: cache },
    "incompatible",
  );
  await file.prefetchOwned?.([{ start: 0, end: 8 }], scope);
  expect(ranges).toEqual([]);
  for (const [start, end] of [
    [1, 3],
    [3, 5],
  ] as const) {
    const owner = await file.sliceOwned?.(start, end, scope);
    if (owner === undefined) throw new Error("missing owned output");
    expect(new Uint8Array(owner.value)).toEqual(Uint8Array.of(start, start + 1));
    owner.release();
  }
  expect(ranges).toEqual([
    [1, 3],
    [3, 5],
  ]);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
  expect(cacheBudget.usedBytes).toBe(0);
});
