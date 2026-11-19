import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { memoryStore, type QueryBudget, SharedMemoryCache } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { networkWaveStore } from "../../../tests/support/network-waves.js";
import { openLanceDataset } from "./index.js";

const ROOT = resolve(import.meta.dirname, "../fixtures/spatial-default");
const select = ["serial", "label", "note", "score", "category"];
const box = (west: number, south: number, east: number, north: number) => ({
  predicate: "intersects" as const,
  geometry: {
    type: "Polygon" as const,
    coordinates: [
      [
        [west, south],
        [east, south],
        [east, north],
        [west, north],
        [west, south],
      ],
    ],
  },
});

// Each event-loop turn represents a network round trip. All queued reads finish
// together. Assertions measure dependency waves, never machine-dependent time.
async function fixture(
  concurrency: number,
  cache: number | SharedMemoryCache = 8 * 1024 * 1024,
  budgetOverrides: Partial<QueryBudget> = {},
) {
  const expected = JSON.parse(await readFile(resolve(ROOT, "expected.json"), "utf8"));
  const inner = memoryStore();
  for (const path of Object.keys(expected.sha256).filter((p) => p.startsWith("dataset.lance/")))
    await inner.put(path, new Uint8Array(await readFile(resolve(ROOT, path))));
  const { store, trace } = networkWaveStore(
    {
      head: inner.head.bind(inner),
      getRange: inner.getRange.bind(inner),
      put: inner.put.bind(inner),
      delete: inner.delete.bind(inner),
      get: async () => {
        throw new Error("Whole-object read forbidden");
      },
      list: () => {
        throw new Error("Dataset scan forbidden");
      },
    },
    (path) =>
      path.includes("/_indices/") ? "index" : path.includes("/data/") ? "payload" : "metadata",
  );
  const dataset = await openLanceDataset({
    store,
    path: "dataset.lance",
    ...(typeof cache === "number" ? { indexCacheBytes: cache } : { indexCache: cache }),
    coalesceGapBytes: 0,
    budget: {
      maxConcurrentReads: concurrency,
      maxMemoryBytes: 8 * 1024 * 1024,
      maxBytes: 4 * 1024 * 1024,
      maxRangeRequests: 1000,
      maxRowsDecoded: 100_000,
      maxOutputRows: 2000,
      ...budgetOverrides,
    },
  });
  trace.reset();
  return { dataset, trace, expected };
}

function query(dataset: Awaited<ReturnType<typeof fixture>>["dataset"], wide = false) {
  return dataset.spatialRows({
    snapshotId: dataset.snapshotId,
    index: "geometry_idx",
    select,
    query: wide ? box(10, 10, 14, 200) : box(10, 10, 14, 14),
  });
}

describe("Lance spatial network work contracts", () => {
  it.each([
    1, 2, 8, 32,
  ])("bounds cold reads, pages, bytes, memory and waves at concurrency %i", async (concurrency) => {
    const { dataset, trace, expected } = await fixture(concurrency);
    const result = await query(dataset);
    expect(
      result.rows
        .map((row, i) => ({ ...row, _rowid: Number(result.rowIds[i]) }))
        .sort((a, b) => Number(a.serial) - Number(b.serial)),
    ).toEqual(expected.queries.intersects);
    expect(trace.heads).toEqual([]);
    expect(trace.peak).toBeLessThanOrEqual(concurrency);
    expect(trace.active).toBe(0);
    expect(result.pagesVisited).toBe(2);
    expect(result.candidatesExamined).toBe(4096);
    expect(trace.requests.length).toBeLessThanOrEqual(32);
    expect(result.stats.rangeRequests).toBeLessThanOrEqual(34);
    expect(result.stats.pagesTouched).toBeLessThanOrEqual(10);
    expect(result.stats.rowsDecoded).toBeLessThanOrEqual(4153);
    expect(trace.waves).toBeLessThanOrEqual({ 1: 32, 2: 20, 8: 10, 32: 9 }[concurrency] as number);
    expect(result.stats.physicalBytesRequested).toBeLessThan(512 * 1024);
    expect(result.stats.peakMemoryBytes).toBeLessThan(512 * 1024);
    if (concurrency > 1) {
      expect(trace.group("index").peak).toBeGreaterThan(1);
      expect(trace.group("payload").peak).toBeGreaterThan(1);
    }
  });

  it("fans out sibling leaves, shares physical directories, and preserves packed result order", async () => {
    const serial = await fixture(1, 0);
    const parallel = await fixture(8, 0);
    const before = await query(serial.dataset, true);
    const after = await query(parallel.dataset, true);
    expect(after.rows).toEqual(before.rows);
    expect(after.rowIds).toEqual(before.rowIds);
    const serials = after.rows.map((r) => Number(r.serial)).sort((a, b) => a - b);
    expect(serials).toEqual(
      Array.from({ length: 191 }, (_, y) =>
        Array.from({ length: 5 }, (_, x) => (y + 10) * 512 + x + 10),
      ).flat(),
    );
    expect(parallel.trace.heads).toEqual([]);
    expect(after.pagesVisited).toBe(6);
    expect(after.candidatesExamined).toBe(20480);
    expect(parallel.trace.requests.length).toBeLessThanOrEqual(242);
    expect(parallel.trace.waves).toBeLessThanOrEqual(35);
    expect(parallel.trace.group("index").waves).toBeLessThanOrEqual(7);
    expect(after.stats.peakMemoryBytes).toBeLessThan(3 * 1024 * 1024);
    expect(parallel.trace.waves).toBeLessThan(serial.trace.waves / 2);
    expect(parallel.trace.peak).toBeLessThanOrEqual(8);
  });

  it("retains decoded tree pages without a byte cache, including nearby viewports", async () => {
    const { dataset, trace } = await fixture(8);
    const first = await query(dataset);
    trace.reset();
    const repeat = await query(dataset);
    expect(repeat.rows).toEqual(first.rows);
    expect(trace.requests.filter((r) => r.path.includes("/_indices/"))).toEqual([]);
    expect(repeat.stats.cacheHits).toBeGreaterThanOrEqual(3);
    trace.reset();
    await dataset.spatialRows({
      snapshotId: dataset.snapshotId,
      index: "geometry_idx",
      select,
      query: box(11, 10, 15, 14),
    });
    expect(trace.requests.filter((r) => r.path.includes("/_indices/"))).toEqual([]);
  });

  it.each([
    { maxMemoryBytes: 100_000 },
    { maxOutputRows: 1 },
    { maxRangeRequests: 3 },
    { maxRowsDecoded: 1 },
    { maxBytes: 100_000 },
  ])("drains concurrent work on budget failure without reading payload: %j", async (overrides) => {
    const { dataset, trace } = await fixture(8, 0, overrides);
    await expect(query(dataset)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    expect(trace.active).toBe(0);
    expect(trace.requests.some((r) => r.path.includes("/data/"))).toBe(false);
    const reads = trace.requests.length;
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(trace.requests).toHaveLength(reads);
  });

  it("keeps zero-capacity and evicted caches correct", async () => {
    const { dataset, trace } = await fixture(8, 0);
    const first = await query(dataset);
    trace.reset();
    expect((await query(dataset)).rows).toEqual(first.rows);
    expect(trace.requests.some((r) => r.path.includes("/_indices/"))).toBe(true);
    const cache = new SharedMemoryCache({ maxBytes: 8 * 1024 * 1024 });
    const shared = await fixture(8, cache);
    const cold = await query(shared.dataset);
    // A higher-priority entry fills the cache and evicts both metadata and vectors.
    cache.set("pressure", new Uint8Array(8 * 1024 * 1024), 8 * 1024 * 1024, { priority: 5 });
    shared.trace.reset();
    expect((await query(shared.dataset)).rows).toEqual(cold.rows);
    expect(shared.trace.requests.some((r) => r.path.includes("/_indices/"))).toBe(true);
  });
});
