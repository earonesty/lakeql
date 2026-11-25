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
    // Cold: footers, streamed document-length totals and token sets for two partitions, then
    // the posting chain with per-batch document lengths.
    expect(f.trace.group("index").waves).toBeLessThanOrEqual(
      { 1: 40, 2: 22, 8: 13, 32: 13 }[concurrency] as number,
    );
    expect(indexRequests(f.trace).length).toBeLessThanOrEqual(40);
    expect(indexBytesRead(f.trace)).toBeLessThan(90_000);
    expect(result.stats.rangeRequests).toBeLessThanOrEqual(75);
    expect(result.stats.physicalBytesRequested).toBeLessThan(110_000);
    // Resident index structure is the file directories alone (reserved before decoding, about
    // 35 KiB for the six files); the rest of the peak is the largest batch with its columns,
    // read plans and containers, not the index.
    expect(result.search.directoryBytes).toBeLessThan(40 * 1024);
    expect(result.stats.peakMemoryBytes).toBeLessThan(128 * 1024);
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
    // Offsets/lengths -> block offsets -> block bytes -> document lengths -> row ids.
    expect(f.trace.group("index").waves).toBeLessThanOrEqual(
      { 1: 24, 2: 12, 8: 5, 32: 5 }[concurrency] as number,
    );
    expect(indexRequests(f.trace).length).toBeLessThanOrEqual(24);
    // Row ids are read per partition for the candidates that could enter the top ten, so they
    // can be checked against deletions before ranking: at most ten per partition.
    expect(indexBytesRead(f.trace)).toBeLessThan(18_000);
    // Warm, the cache owns the directories: the peak is two partitions' posting batches plus
    // their merge columns (admitted at batch capacity), read plans and containers.
    expect(warm.stats.peakMemoryBytes).toBeLessThan(80 * 1024);
    expect(warm.stats.peakMemoryBytes).toBeLessThan(cold.stats.peakMemoryBytes);
    // Cached token sets are not fetched again, and the docs file is read only for the scored
    // documents' lengths (at most the 6,400-byte length columns for 1,264 of 1,600 documents)
    // plus coalesced row-id windows for each partition's ten best candidates.
    expect(indexRequests(f.trace).filter((r) => /_tokens\.lance$/.test(r.path))).toEqual([]);
    const docsBytes = indexRequests(f.trace)
      .filter((r) => /_docs\.lance$/.test(r.path))
      .reduce((sum, r) => sum + r.length, 0);
    expect(docsBytes).toBeLessThan(13_000);
    expect(warm.stats.cacheHits).toBeGreaterThan(0);
  });

  it("reads far less than the index for a rare term (no scans)", async () => {
    const f = await fixture(8);
    await search(f, COMMON); // warm the cache
    f.trace.reset();
    const result = await search(f, RARE);
    expect(result.rows.length).toBeGreaterThan(0);
    expect(result.search.postingEntriesDecoded).toBe(201);
    expect(indexBytesRead(f.trace)).toBeLessThan(13_000);
    // A whole-file or posting-column scan would be at least the size of the index.
    expect(indexBytesRead(f.trace)).toBeLessThan(f.indexBytes / 30);
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
    expect(indexRequests(f.trace).length).toBeLessThanOrEqual(40);
    expect(f.trace.group("index").waves).toBeLessThanOrEqual(10);
    expect(indexBytesRead(f.trace)).toBeLessThan(100_000);
    // 172 candidates of a three-term phrase are 516 position lists in one batch. Each list's
    // read plan, containers and decoded positions are all accounted (about 600 bytes a list).
    expect(result.stats.peakMemoryBytes).toBeLessThan(352 * 1024);
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
    const query: LanceFtsQuery = { kind: "match", text: "quick lazy dog" };
    const nearby = await search(f, query);
    // Cheap must not mean stale: the answer equals an independent cold execution.
    const independent = await search(await fixture(8, 0), query);
    expect(independent.rows.length).toBeGreaterThan(0);
    expect(nearby.rowIds).toEqual(independent.rowIds);
    expect(nearby.scores).toEqual(independent.scores);
    expect(nearby.rows).toEqual(independent.rows);
    expect(nearby.totalMatches).toBe(independent.totalMatches);
    // The corpus token total came from the cache; the cold run had to sum every length.
    expect(nearby.search.documentLengthsSummed).toBe(0);
    expect(independent.search.documentLengthsSummed).toBe(1600);
    expect(indexBytesRead(f.trace)).toBeLessThan(18_000);
    expect(f.trace.group("index").waves).toBeLessThanOrEqual(5);
  });

  it("keeps memory flat under a fixed budget and small k as the match count grows", async () => {
    // Tiny batches make the streaming visible: one posting block per term and 32 documents
    // per batch. The same 96 KiB budget serves every query, from 201 matches to every
    // document, and results equal an unbatched search. About 35 KiB of it is the index's file
    // directories, which are resident for the whole search and reserved before decoding.
    const small = { postingBlocksPerBatch: 1, documentsPerBatch: 32 };
    const queries: LanceFtsQuery[] = [
      { kind: "match", text: "zebra" },
      { kind: "match", text: "quick" },
      { kind: "match", text: "quick brown fox" },
      { kind: "match", text: "the quick brown fox jumps over the lazy dog" },
    ];
    let previousMatches = 0;
    const peaks: number[] = [];
    for (const query of queries) {
      const reference = await search(await fixture(8, 0), query, { limit: 5 });
      const bounded = await search(await fixture(8, 0, { maxMemoryBytes: 96 * 1024 }), query, {
        limit: 5,
        limits: small,
      });
      expect(bounded.rowIds).toEqual(reference.rowIds);
      expect(bounded.scores).toEqual(reference.scores);
      expect(bounded.totalMatches).toBeGreaterThan(previousMatches);
      previousMatches = bounded.totalMatches;
      expect(bounded.stats.peakMemoryBytes).toBeLessThan(96 * 1024);
      peaks.push(bounded.stats.peakMemoryBytes);
      expect(bounded.search.batches).toBeGreaterThan(reference.search.batches);
    }
    expect(previousMatches).toBeGreaterThan(1400);
    // Flat: seven times the matches (and nine terms instead of one) moves the peak by less
    // than 8 KiB.
    expect(Math.max(...peaks) - Math.min(...peaks)).toBeLessThan(8 * 1024);
  });

  it("reads no document lengths for an absent term, and sums them in parallel otherwise", async () => {
    // Absent term, cold: the dictionary answers it. No length, posting or row id is read.
    const absent = await fixture(8, 0);
    const nothing = await search(absent, { kind: "match", text: "zzzzunknown" });
    expect(nothing.rows).toEqual([]);
    expect(nothing.search.documentLengthsSummed).toBe(0);
    expect(nothing.stats.rowsDecoded).toBe(0);
    // The docs files are touched once each, for their footers when the index is opened.
    expect(indexRequests(absent.trace).filter((r) => /_docs\.lance$/.test(r.path))).toHaveLength(2);
    const absentWaves = absent.trace.group("index").waves;
    // Rare term, cold, with the lengths split into 16 chunks of 100 rows (11 + 5 across the
    // two partitions). Eight readers take them in two or three waves (they share the read
    // limit with the posting-location wave they run beside), so the dependent chain grows by
    // the chunk count over the concurrency; read one after the other, partition 0 alone would
    // add ten waves.
    const whole = await fixture(8, 0);
    const reference = await search(whole, RARE);
    const chunked = await fixture(8, 0);
    const result = await search(chunked, RARE, { limits: { documentLengthChunkRows: 100 } });
    expect(result.rowIds).toEqual(reference.rowIds);
    expect(result.scores).toEqual(reference.scores);
    expect(result.search.documentLengthsSummed).toBe(1600);
    expect(reference.search.documentLengthsSummed).toBe(1600);
    expect(chunked.trace.group("index").waves).toBeLessThanOrEqual(
      whole.trace.group("index").waves + 3,
    );
    expect(absentWaves).toBeLessThan(whole.trace.group("index").waves);
    expect(chunked.trace.peak).toBeLessThanOrEqual(8);
    // Chunks bound what is resident: 100 lengths per reader instead of a partition's worth.
    expect(result.stats.peakMemoryBytes).toBeLessThanOrEqual(reference.stats.peakMemoryBytes);
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
    for (const limits of [{ postingBlocksPerBatch: 0 }, { documentsPerBatch: -1 }]) {
      await expect(search(await fixture(8), COMMON, { limits })).rejects.toMatchObject({
        code: "LAKEQL_VALIDATION_ERROR",
      });
    }
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
