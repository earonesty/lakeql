import { MemoryBudget, memoryStore } from "lakeql-core";
import { describe, expect, it, vi } from "vitest";
import {
  LanceReadContext,
  type MutableLanceReadStats,
  mergeRanges,
  RangeLease,
  totalBytes,
} from "./io.js";

describe("Lance physical range planning", () => {
  it("merges overlap and nearby ranges without exceeding the physical cap", () => {
    const planned = mergeRanges(
      [
        { offset: 20, length: 5 },
        { offset: 0, length: 10 },
        { offset: 8, length: 4 },
        { offset: 14, length: 4 },
        { offset: 40, length: 0 },
      ],
      2,
      18,
    );

    expect(planned).toEqual([
      { offset: 0, length: 18 },
      { offset: 20, length: 5 },
    ]);
    expect(totalBytes(planned)).toBe(23);
    expect(mergeRanges([], 0, 1)).toEqual([]);
  });

  it.each([
    { offset: -1, length: 1 },
    { offset: 0.5, length: 1 },
    { offset: 0, length: -1 },
    { offset: Number.MAX_SAFE_INTEGER, length: 2 },
  ])("rejects invalid byte range %#", (range) => {
    expect(() => mergeRanges([range], 0, 100)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
  });

  it("leases slices and releases retained physical memory exactly once", () => {
    const released = vi.fn();
    const lease = new RangeLease(
      [{ offset: 10, length: 3, bytes: Uint8Array.of(4, 5, 6) }],
      released,
    );

    expect(lease.slice({ offset: 11, length: 2 })).toEqual(Uint8Array.of(5, 6));
    expect(() => lease.slice({ offset: 9, length: 1 })).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
    lease.release();
    lease.release();
    expect(released).toHaveBeenCalledOnce();
  });

  it("accounts metadata categories and rejects truncated or whole-file data reads", async () => {
    const store = memoryStore();
    await store.put("a", Uint8Array.of(1, 2, 3, 4));
    const stats = emptyStats();
    const context = new LanceReadContext(store, {}, stats, 0, () => 0, {
      coalesceGapBytes: 0,
      maxCoalescedRangeBytes: 100,
    });
    const metadata = await context.readRange("a", { offset: 1, length: 2 }, "file_metadata", 4);
    metadata.release();
    expect(stats.dataMetadataBytes).toBe(2);

    await expect(context.readRange("a", { offset: 0, length: 4 }, "data", 4)).rejects.toMatchObject(
      { code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE" },
    );

    const truncated = new LanceReadContext(
      {
        ...store,
        getRange: async () => Uint8Array.of(1),
      },
      {},
      emptyStats(),
      0,
      () => 0,
      { coalesceGapBytes: 0, maxCoalescedRangeBytes: 100 },
    );
    await expect(
      truncated.readRange("a", { offset: 0, length: 2 }, "snapshot"),
    ).rejects.toMatchObject({ code: "LAKEQL_LANCE_READ_ERROR" });
  });

  it.each([
    "reject",
    "truncate",
  ] as const)("keeps pending sibling reads charged after a %s failure", async (failure) => {
    const pending = Promise.withResolvers<Uint8Array>();
    const failed = Promise.withResolvers<Uint8Array>();
    const started = vi.fn();
    const cause = new Error("range failed");
    const context = new LanceReadContext(
      {
        ...memoryStore(),
        getRange: async (_path, range) => {
          started(range.offset);
          return await (range.offset === 0 ? failed.promise : pending.promise);
        },
      },
      { maxMemoryBytes: 4, maxConcurrentReads: 2 },
      emptyStats(),
      0,
      () => 0,
      { coalesceGapBytes: 0, maxCoalescedRangeBytes: 2 },
    );
    const read = context.readRanges(
      "a",
      [
        { offset: 0, length: 2 },
        { offset: 4, length: 2 },
      ],
      "snapshot",
    );
    let completed = false;
    const checked = read.then(
      () => {
        throw new Error("expected range failure");
      },
      (error) => {
        completed = true;
        return error;
      },
    );
    await expect.poll(() => started.mock.calls.length).toBe(2);
    if (failure === "reject") failed.reject(cause);
    else failed.resolve(new Uint8Array(1));
    // Allow the failed read and its control wrapper to finish.
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(completed).toBe(false);
    expect(() => context.leaseDecodedMemory(1)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
    );
    pending.resolve(new Uint8Array(2));
    const error = await checked;
    if (failure === "reject") expect(error).toBe(cause);
    else expect(error).toMatchObject({ code: "LAKEQL_LANCE_READ_ERROR" });
    context.leaseDecodedMemory(4).release();
  });

  it("admits sibling jobs against a shared pool and individual allocation limits", async () => {
    const pool = new MemoryBudget(6);
    const pending = Promise.withResolvers<Uint8Array>();
    const getRange = vi.fn(async () => await pending.promise);
    const makeContext = () =>
      new LanceReadContext(
        { ...memoryStore(), getRange },
        { maxAllocatedMemoryBytes: 4 },
        emptyStats(),
        0,
        () => 0,
        { coalesceGapBytes: 0, maxCoalescedRangeBytes: 4 },
        pool,
      );
    const first = makeContext();
    const second = makeContext();
    const read = first.readRange("a", { offset: 0, length: 4 }, "snapshot");
    await expect.poll(() => getRange.mock.calls.length).toBe(1);
    expect(pool.usedBytes).toBe(4);
    const sibling = second.leaseDecodedMemory(2);
    expect(pool.usedBytes).toBe(6);
    expect(() => second.leaseDecodedMemory(1)).toThrow();
    sibling.release();
    expect(() => first.leaseDecodedMemory(1)).toThrow();
    pending.resolve(new Uint8Array(4));
    const lease = await read;
    expect(pool.usedBytes).toBe(4);
    lease.release();
    expect(pool.usedBytes).toBe(0);
    second.leaseDecodedMemory(4).release();
    expect(pool.usedBytes).toBe(0);
    expect(pool.peakBytes).toBe(6);
  });

  it.each([false, true])("keeps owned pending storage charged, abort=%s", async (abort) => {
    const pool = new MemoryBudget(4);
    const gate = Promise.withResolvers<Uint8Array>();
    const controller = new AbortController();
    const allocate = vi.fn(async () => await gate.promise);
    const borrowed = vi.fn(async () => new Uint8Array(4));
    const context = new LanceReadContext(
      {
        ...memoryStore(),
        getRange: borrowed,
        getRangeOwned: async (_path, range, scope) =>
          await scope.allocateAsync(range.length, "gated owned transport", allocate),
      },
      { maxAllocatedMemoryBytes: 4, signal: controller.signal },
      emptyStats(),
      0,
      () => 0,
      { coalesceGapBytes: 0, maxCoalescedRangeBytes: 4 },
      pool,
    );
    const read = context.readRange("a", { offset: 0, length: 4 }, "snapshot");
    const outcome = read.then(
      (lease) => ({ lease }),
      (error: unknown) => ({ error }),
    );
    await expect.poll(() => allocate.mock.calls.length).toBe(1);
    expect(pool.usedBytes).toBe(4);
    if (abort) {
      controller.abort();
      expect(pool.usedBytes).toBe(4);
    }
    gate.resolve(Uint8Array.of(1, 2, 3, 4));
    const result = await outcome;
    if ("lease" in result) {
      expect(abort).toBe(false);
      expect(result.lease.slice({ offset: 0, length: 4 })).toEqual(Uint8Array.of(1, 2, 3, 4));
      expect(pool.usedBytes).toBe(4);
      result.lease.release();
    } else {
      expect(abort).toBe(true);
      expect(result.error).toMatchObject({ code: "LAKEQL_ABORTED" });
    }
    expect(borrowed).not.toHaveBeenCalled();
    expect(pool.usedBytes).toBe(0);
    expect(pool.peakBytes).toBe(4);
  });

  it.each([
    false,
    true,
  ])("declines coalescing gaps under shared pressure, owned=%s", async (owned) => {
    const store = memoryStore();
    await store.put("a", Uint8Array.of(10, 11, 12, 13, 14, 15));
    const pool = new MemoryBudget(6);
    const cache = pool.reserve(2, "pinned sibling cache");
    const stats = emptyStats();
    const reads: number[] = [];
    const context = new LanceReadContext(
      {
        ...store,
        getRange: async (path, range) => {
          reads.push(range.length);
          return await store.getRange(path, range);
        },
        ...(owned
          ? ({
              getRangeOwned: async (path, range, scope) => {
                reads.push(range.length);
                return await store.getRangeOwned(path, range, scope);
              },
            } satisfies Partial<typeof store>)
          : {}),
      },
      { maxConcurrentReads: 2 },
      stats,
      0,
      () => 0,
      { coalesceGapBytes: 2, maxCoalescedRangeBytes: 6 },
      pool,
    );
    const ranges = [
      { offset: 0, length: 2 },
      { offset: 4, length: 2 },
    ];
    const lease = await context.readRanges("a", ranges, "snapshot");
    expect(reads).toEqual([2, 2]);
    expect(stats.physicalBytesRequested).toBe(4);
    expect(pool.usedBytes).toBe(6);
    expect(lease.slice({ offset: 4, length: 2 })).toEqual(Uint8Array.of(14, 15));
    lease.release();
    cache.release();
    expect(pool.usedBytes).toBe(0);
    reads.length = 0;
    const coalesced = await context.readRanges("a", ranges, "snapshot");
    expect(reads).toEqual([6]);
    coalesced.release();
    expect(pool.usedBytes).toBe(0);
  });

  it.each([
    "bytes",
    "memory",
    "whole-file",
  ] as const)("uses exact ranges when optional gap reads violate %s constraints", async (constraint) => {
    const store = memoryStore();
    await store.put("a", new Uint8Array(6));
    const stats = emptyStats();
    const context = new LanceReadContext(
      store,
      constraint === "bytes"
        ? { maxBytes: 4 }
        : constraint === "memory"
          ? { maxMemoryBytes: 4 }
          : {},
      stats,
      0,
      () => 0,
      { coalesceGapBytes: 2, maxCoalescedRangeBytes: 6 },
    );
    const lease = await context.readRanges(
      "a",
      [
        { offset: 0, length: 2 },
        { offset: 4, length: 2 },
      ],
      constraint === "whole-file" ? "data" : "snapshot",
      6,
    );
    expect(stats.physicalBytesRequested).toBe(4);
    expect(stats.rangeRequests).toBe(2);
    lease.release();
    expect(context.memoryBudget.usedBytes).toBe(0);
  });

  it("does not start the remaining range queue after failure", async () => {
    const getRange = vi.fn(async () => {
      throw new Error("transport failure");
    });
    const pool = new MemoryBudget(6);
    const context = new LanceReadContext(
      { ...memoryStore(), getRange },
      { maxConcurrentReads: 1 },
      emptyStats(),
      0,
      () => 0,
      { coalesceGapBytes: 0, maxCoalescedRangeBytes: 2 },
      pool,
    );
    await expect(
      context.readRanges(
        "a",
        [
          { offset: 0, length: 2 },
          { offset: 4, length: 2 },
          { offset: 8, length: 2 },
        ],
        "snapshot",
      ),
    ).rejects.toThrow("transport failure");
    expect(getRange).toHaveBeenCalledTimes(1);
    expect(pool.usedBytes).toBe(0);
  });

  it("enforces request, byte, memory, and elapsed budgets", async () => {
    const store = memoryStore();
    await store.put("a", Uint8Array.of(1, 2, 3, 4));
    for (const budget of [{ maxRangeRequests: 0 }, { maxBytes: 1 }, { maxMemoryBytes: 1 }]) {
      const context = new LanceReadContext(store, budget, emptyStats(), 0, () => 0, {
        coalesceGapBytes: 0,
        maxCoalescedRangeBytes: 100,
      });
      await expect(
        context.readRange("a", { offset: 0, length: 2 }, "snapshot"),
      ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    }
    const elapsed = new LanceReadContext(store, { maxElapsedMs: 1 }, emptyStats(), 0, () => 2, {
      coalesceGapBytes: 0,
      maxCoalescedRangeBytes: 100,
    });
    expect(() => elapsed.check()).toThrowError(
      expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
    );
  });

  it("accounts decoded rows cumulatively and validates reservations", () => {
    const stats = emptyStats();
    const context = new LanceReadContext(memoryStore(), { maxRowsDecoded: 3 }, stats, 0, () => 0, {
      coalesceGapBytes: 0,
      maxCoalescedRangeBytes: 100,
    });

    context.reserveDecodedRows(1);
    context.reserveDecodedRows(2);
    expect(stats.rowsDecoded).toBe(3);
    expect(() => context.reserveDecodedRows(1)).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: expect.objectContaining({ metric: "rows decoded" }),
      }),
    );
    for (const count of [-1, 0.5]) {
      expect(() => context.reserveDecodedRows(count)).toThrowError(
        expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
      );
    }
  });

  it("retains decoded-memory reservations until the operation releases them", () => {
    const stats = emptyStats();
    const context = new LanceReadContext(memoryStore(), { maxMemoryBytes: 6 }, stats, 0, () => 0, {
      coalesceGapBytes: 0,
      maxCoalescedRangeBytes: 100,
    });

    context.accountDecodedMemory(4);
    expect(stats.peakMemoryBytes).toBe(4);
    const temporary = context.leaseDecodedMemory(2);
    expect(stats.peakMemoryBytes).toBe(6);
    expect(() => context.accountDecodedMemory(1)).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: expect.objectContaining({ metric: "memory bytes" }),
      }),
    );
    temporary.release();
    temporary.release();
    context.accountDecodedMemory(2);
    context.releaseDecodedMemory();
    context.accountDecodedMemory(6);
    context.releaseDecodedMemory();
    expect(stats.peakMemoryBytes).toBe(6);
    expect(() => context.accountDecodedMemory(-1)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
  });
});

function emptyStats(): MutableLanceReadStats {
  return {
    snapshotMetadataBytes: 0,
    dataMetadataBytes: 0,
    logicalBytesRequested: 0,
    physicalBytesRequested: 0,
    rangeRequests: 0,
    cacheHits: 0,
    cacheMisses: 0,
    peakMemoryBytes: 0,
    rowsDecoded: 0,
    fragments: new Set(),
    pages: new Set(),
  };
}
