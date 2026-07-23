import { memoryStore } from "lakeql-core";
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
    fragments: new Set(),
    pages: new Set(),
  };
}
