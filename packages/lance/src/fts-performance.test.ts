import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { memoryStore, type QueryBudget, type SharedMemoryCache } from "lakeql";
import { describe, expect, it } from "vitest";
import { networkWaveStore } from "../../../tests/support/network-waves.js";
import { type LanceFtsLimits, type LanceFtsQuery, openLanceDataset } from "./index.js";

const ROOT = resolve(import.meta.dirname, "../fixtures/fts-inverted-v2.0.lance");
const ACTIVE_VERSION = 5;
const isIndex = (path: string) => path.includes("/_indices/");

// Each event-loop turn is one network round trip: every queued read completes together, so
// dependent reads necessarily land in later waves. Assertions count waves, never wall time.
async function fixture(
  concurrency: number,
  cache: number | SharedMemoryCache = 8 * 1024 * 1024,
  budgetOverrides: Partial<QueryBudget> = {},
) {
  const expected = JSON.parse(await readFile(resolve(ROOT, "expected.json"), "utf8"));
  const inner = memoryStore();
  const sizes = new Map<string, number>();
  for (const path of Object.keys(expected.sha256)) {
    const bytes = new Uint8Array(await readFile(resolve(ROOT, path)));
    await inner.put(`dataset.lance/${path}`, bytes);
    sizes.set(path, bytes.length);
  }
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
    (path) => (isIndex(path) ? "index" : path.includes("/data/") ? "payload" : "metadata"),
  );
  const dataset = await openLanceDataset({
    store,
    path: "dataset.lance",
    version: ACTIVE_VERSION,
    ...(typeof cache === "number" ? { indexCacheBytes: cache } : { indexCache: cache }),
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
  // The active index is the optimized generation with a second partition.
  const active = [...sizes.keys()].find((path) => path.endsWith("/part_1_invert.lance"));
  const prefix = active?.slice(0, active.lastIndexOf("/") + 1);
  const indexBytes = [...sizes]
    .filter(([path]) => path.startsWith(prefix as string))
    .reduce((sum, [, size]) => sum + size, 0);
  trace.reset();
  return { dataset, trace, indexBytes };
}

type Fixture = Awaited<ReturnType<typeof fixture>>;

function search(
  { dataset }: Fixture,
  query: LanceFtsQuery,
  options: { limit?: number; limits?: Partial<LanceFtsLimits> } = {},
) {
  return dataset.searchRows({
    snapshotId: dataset.snapshotId,
    index: "text_idx",
    query,
    select: ["id"],
    limit: options.limit ?? 10,
    ...(options.limits === undefined ? {} : { limits: options.limits }),
  });
}

const indexRequests = (trace: Fixture["trace"]) => trace.requests.filter((r) => isIndex(r.path));
const indexBytesRead = (trace: Fixture["trace"]) =>
  indexRequests(trace).reduce((sum, r) => sum + r.length, 0);

const COMMON: LanceFtsQuery = { kind: "match", text: "quick brown fox" };
const RARE: LanceFtsQuery = { kind: "match", text: "zebra" };
const PHRASE: LanceFtsQuery = { kind: "phrase", text: "quick brown fox" };

describe("Lance full-text search network work contracts", () => {
  it.each([
    1, 2, 8, 32,
  ])("bounds cold index reads, bytes, memory and dependent waves at concurrency %i", async (concurrency) => {
    const f = await fixture(concurrency);
    const result = await search(f, COMMON);
    expect(result.rows).toHaveLength(10);
    expect(f.trace.heads.filter(isIndex)).toEqual([]);
    expect(f.trace.active).toBe(0);
    expect(f.trace.peak).toBeLessThanOrEqual(Math.min(concurrency, 8));
    // Work counters are exact and independent of scheduling.
    expect(result.search.postingEntriesDecoded).toBe(2093);
    expect(result.search.candidatesScored).toBe(1264);
    expect(result.search.partitions).toBe(2);
    // Cold: footers, token sets and doc lengths for two partitions, then posting waves.
    expect(f.trace.group("index").waves).toBeLessThanOrEqual(
      { 1: 36, 2: 20, 8: 12, 32: 12 }[concurrency] as number,
    );
    expect(indexRequests(f.trace).length).toBeLessThanOrEqual(36);
    expect(indexBytesRead(f.trace)).toBeLessThan(90_000);
    expect(result.stats.rangeRequests).toBeLessThanOrEqual(72);
    expect(result.stats.physicalBytesRequested).toBeLessThan(110_000);
    expect(result.stats.peakMemoryBytes).toBeLessThan(256 * 1024);
  });

  it.each([
    1, 2, 8, 32,
  ])("a warm index needs only the posting dependency chain at concurrency %i", async (concurrency) => {
    const f = await fixture(concurrency);
    const cold = await search(f, COMMON);
    f.trace.reset();
    const warm = await search(f, COMMON);
    expect(warm.rowIds).toEqual(cold.rowIds);
    expect(warm.scores).toEqual(cold.scores);
    // Offsets/lengths -> block offsets -> block bytes -> row ids is the whole index chain.
    expect(f.trace.group("index").waves).toBeLessThanOrEqual(
      { 1: 16, 2: 9, 8: 5, 32: 5 }[concurrency] as number,
    );
    expect(indexRequests(f.trace).length).toBeLessThanOrEqual(16);
    expect(indexBytesRead(f.trace)).toBeLessThan(9_000);
    // The token set and doc-length columns are not fetched again: nothing touches a token set,
    // and docs reads are row-id lookups (at most one coalesced window each), not the 4-byte
    // per-document length column or a footer.
    expect(indexRequests(f.trace).filter((r) => /_tokens\.lance$/.test(r.path))).toEqual([]);
    expect(
      indexRequests(f.trace).filter((r) => /_docs\.lance$/.test(r.path) && r.length > 2048),
    ).toEqual([]);
    expect(warm.stats.cacheHits).toBeGreaterThan(0);
  });

  it("reads far less than the index for a rare term (no scans)", async () => {
    const f = await fixture(8);
    await search(f, COMMON); // warm the cache
    f.trace.reset();
    const result = await search(f, RARE);
    expect(result.rows.length).toBeGreaterThan(0);
    expect(result.search.postingEntriesDecoded).toBe(201);
    expect(indexBytesRead(f.trace)).toBeLessThan(4_500);
    // A whole-file or posting-column scan would be at least the size of the index.
    expect(indexBytesRead(f.trace)).toBeLessThan(f.indexBytes / 50);
    expect(f.trace.group("index").waves).toBeLessThanOrEqual(5);
  });

  it("bounds phrase position reads to the candidate span with a constant chain", async () => {
    const f = await fixture(8);
    await search(f, COMMON); // warm tokens, doc lengths
    f.trace.reset();
    const result = await search(f, PHRASE);
    expect(result.totalMatches).toBe(31);
    expect(result.search.phraseCandidatesChecked).toBe(172);
    // Per-term coalesced offsets rather than one read per candidate document.
    expect(indexRequests(f.trace).length).toBeLessThanOrEqual(32);
    expect(f.trace.group("index").waves).toBeLessThanOrEqual(10);
    expect(indexBytesRead(f.trace)).toBeLessThan(100_000);
    expect(result.stats.peakMemoryBytes).toBeLessThan(256 * 1024);
  });

  it("fans out reads: parallel chains finish in far fewer waves than serial", async () => {
    const serial = await fixture(1, 0);
    const parallel = await fixture(8, 0);
    const before = await search(serial, COMMON);
    const after = await search(parallel, COMMON);
    expect(after.rowIds).toEqual(before.rowIds);
    expect(after.scores).toEqual(before.scores);
    expect(parallel.trace.waves).toBeLessThan(serial.trace.waves / 2);
    expect(parallel.trace.peak).toBeLessThanOrEqual(8);
  });

  it("keeps a nearby query cheap after the first (shared dictionary and statistics)", async () => {
    const f = await fixture(8);
    await search(f, COMMON);
    f.trace.reset();
    const nearby = await search(f, { kind: "match", text: "quick lazy dog" });
    expect(nearby.rows.length).toBeGreaterThan(0);
    expect(indexBytesRead(f.trace)).toBeLessThan(9_000);
    expect(f.trace.group("index").waves).toBeLessThanOrEqual(5);
  });

  it("enforces explicit work limits before they can run away", async () => {
    await expect(
      search(await fixture(8), COMMON, { limits: { maxPostingEntries: 100 } }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(
      search(await fixture(8), COMMON, { limits: { maxCandidates: 100 } }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(
      search(await fixture(8), PHRASE, { limits: { maxPhraseCandidates: 10 } }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(
      search(await fixture(8), COMMON, { limits: { maxTokenSetBytes: 16 } }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(
      search(await fixture(8), COMMON, { limits: { maxDocLengthBytes: 16 } }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  });

  it("honours the operation byte, request and memory budgets", async () => {
    await expect(search(await fixture(8, 0, { maxBytes: 20_000 }), COMMON)).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
    await expect(
      search(await fixture(8, 0, { maxRangeRequests: 10 }), COMMON),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(
      search(await fixture(8, 0, { maxMemoryBytes: 8 * 1024 }), COMMON),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  });
});
