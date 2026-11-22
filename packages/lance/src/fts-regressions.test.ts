import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { memoryStore, SharedMemoryCache } from "lakeql";
import { describe, expect, it } from "vitest";
import { networkWaveStore } from "../../../tests/support/network-waves.js";
import { readBinaryRanges, readFixedRanges, readU32Ranges, resolveColumn } from "./column.js";
import { inspectLanceFile } from "./file.js";
import { decodePositions, decodePosting, inGroups, phraseMatches } from "./fts.js";
import { type LanceFtsQuery, openLanceDataset } from "./index.js";
import { LanceReadContext } from "./io.js";

const FIXTURES = resolve(import.meta.dirname, "../fixtures");

async function open(
  name: string,
  options: { version?: number; budget?: Record<string, number>; cache?: SharedMemoryCache } = {},
) {
  const dir = resolve(FIXTURES, name);
  const expected = JSON.parse(await readFile(resolve(dir, "expected.json"), "utf8"));
  const inner = memoryStore();
  for (const path of Object.keys(expected.sha256)) {
    await inner.put(`dataset.lance/${path}`, new Uint8Array(await readFile(resolve(dir, path))));
  }
  const { store, trace } = networkWaveStore(inner, () => "all");
  const dataset = await openLanceDataset({
    store,
    path: "dataset.lance",
    version: options.version ?? expected.datasetVersion,
    ...(options.cache === undefined ? {} : { indexCache: options.cache }),
    budget: { maxMemoryBytes: 64 * 1024 * 1024, ...options.budget },
  });
  trace.reset();
  return { dataset, trace, expected };
}

const search = (
  dataset: Awaited<ReturnType<typeof open>>["dataset"],
  query: LanceFtsQuery,
  extra: { index?: string; limit?: number; limits?: Record<string, number> } = {},
) =>
  dataset.searchRows({
    snapshotId: dataset.snapshotId,
    index: extra.index ?? "text_idx",
    query,
    select: ["id"],
    limit: extra.limit ?? 10,
    ...(extra.limits === undefined ? {} : { limits: extra.limits }),
  });

const BUDGET = { code: "LAKEQL_BUDGET_EXCEEDED" };
const VALIDATION = { code: "LAKEQL_VALIDATION_ERROR" };

describe("full-text search regressions", () => {
  it("applies maxCandidates to every query shape", async () => {
    const { dataset } = await open("fts-inverted-v2.0.lance");
    await expect(
      search(dataset, { kind: "phrase", text: "quick brown" }, { limits: { maxCandidates: 1 } }),
    ).rejects.toMatchObject(BUDGET);
    await expect(
      search(
        dataset,
        {
          kind: "boolean",
          should: [
            { kind: "match", text: "term1" },
            { kind: "match", text: "term2" },
          ],
        },
        { limits: { maxCandidates: 8 } },
      ),
    ).rejects.toMatchObject(BUDGET);
  });

  it("bounds ranking to the requested rows and checks maxBufferedRows before allocating", async () => {
    // Ranking keeps only the top `limit` (or a refill window), so a small buffered-row budget
    // serves a query with over a thousand matches...
    const small = await open("fts-inverted-v2.0.lance", { budget: { maxBufferedRows: 10 } });
    const ok = await search(
      small.dataset,
      { kind: "match", text: "quick brown fox" },
      { limit: 10 },
    );
    expect(ok.totalMatches).toBeGreaterThan(1000);
    expect(ok.rows).toHaveLength(10);
    // ...and a limit beyond it fails before the heap exists.
    await expect(
      search(small.dataset, { kind: "match", text: "quick brown fox" }, { limit: 11 }),
    ).rejects.toMatchObject(BUDGET);
  });

  it("matches repeated phrase terms and rare/common phrases exactly like pylance", async () => {
    const { dataset, expected } = await open("fts-inverted-v2.0.lance");
    for (const name of [
      "phrase_repeated_term",
      "phrase_rare_and_common",
      "phrase_slop_three_terms",
    ]) {
      const recorded = expected.queries[name];
      const result = await search(
        dataset,
        { kind: "phrase", text: recorded.spec.query, slop: recorded.spec.slop ?? 0 },
        { limit: recorded.limit },
      );
      expect(
        result.scores.every((score) => score > 0),
        name,
      ).toBe(true);
      expect(result.rows.length, name).toBe(recorded.hits.length);
      recorded.hits.forEach((hit: { score: number }, i: number) => {
        expect(Math.abs((result.scores[i] as number) - hit.score), `${name} ${i}`).toBeLessThan(
          1e-4,
        );
      });
    }
  });

  it("applies AND and expansion cutoffs to deduplicated fuzzy expansions like pylance", async () => {
    const { dataset, expected } = await open("fts-inverted-v2.0.lance");
    for (const name of [
      "match_fuzzy_and_shared",
      "match_and_expansion_cutoff",
      "match_fuzzy_multi_cutoff",
    ]) {
      const recorded = expected.queries[name];
      const spec = recorded.spec;
      const result = await search(
        dataset,
        {
          kind: "match",
          text: spec.query,
          ...(spec.operator === undefined
            ? {}
            : { operator: String(spec.operator).toLowerCase() as "and" }),
          fuzziness: spec.fuzziness,
          ...(spec.max_expansions === undefined ? {} : { maxExpansions: spec.max_expansions }),
        },
        { limit: recorded.limit },
      );
      expect(result.rows.length, name).toBe(recorded.hits.length);
      if (name !== "match_and_expansion_cutoff")
        expect(result.rows.length, name).toBeGreaterThan(0);
      recorded.hits.forEach((hit: { score: number }, i: number) => {
        expect(Math.abs((result.scores[i] as number) - hit.score), `${name} ${i}`).toBeLessThan(
          1e-4,
        );
      });
    }
  });

  it("measures fuzzy prefixes in UTF-8 bytes and rejects a length that splits a character", async () => {
    const { dataset } = await open("fts-variants-v2.0.lance");
    const query = (prefixLength: number): LanceFtsQuery => ({
      kind: "match",
      text: "ÉxOLE",
      fuzziness: 1,
      prefixLength,
    });
    const ok = await search(dataset, query(2), { index: "cased_idx" });
    expect(ok.rows.length).toBeGreaterThan(0);
    await expect(search(dataset, query(1), { index: "cased_idx" })).rejects.toMatchObject(
      VALIDATION,
    );
  });

  it("accepts a valid index whose analyzer removed every token", async () => {
    const { dataset } = await open("fts-edge-v2.0.lance");
    const [index] = await dataset.ftsIndexes();
    expect(index).toMatchObject({ name: "text_idx", supported: true });
    const result = await search(dataset, { kind: "match", text: "the and" });
    expect(result.rows).toEqual([]);
    expect(result.totalMatches).toBe(0);
    expect((await search(dataset, { kind: "phrase", text: "the and" })).rows).toEqual([]);
  });

  it("stops issuing requests once a search is rejected", async () => {
    const { dataset, trace } = await open("fts-inverted-v2.0.lance", {
      budget: { maxConcurrentReads: 2 },
    });
    await search(dataset, { kind: "match", text: "fox" }); // open once
    trace.reset();
    await expect(
      search(dataset, { kind: "match", text: "quick" }, { limits: { maxPostingEntries: 600 } }),
    ).rejects.toMatchObject(BUDGET);
    const atRejection = trace.requests.length;
    await new Promise((done) => setTimeout(done, 20));
    expect(trace.requests.length).toBe(atRejection);
    expect(trace.active).toBe(0);
    // The aggregate plan is checked before any posting payload is read: only offsets/lengths.
    expect(trace.requests.every((r) => r.length <= 64)).toBe(true);
  });

  it("charges decoded index rows and records the pages it reads", async () => {
    const { dataset } = await open("fts-inverted-v2.0.lance", { budget: { maxRowsDecoded: 0 } });
    await expect(search(dataset, { kind: "match", text: "zzzzunknown" })).rejects.toMatchObject(
      BUDGET,
    );
    const ok = await open("fts-inverted-v2.0.lance");
    const result = await search(ok.dataset, { kind: "match", text: "fox" });
    expect(result.stats.pagesTouched).toBeGreaterThan(4);
    expect(result.stats.rowsDecoded).toBeGreaterThan(1500);
  });

  it("charges the retained index against the cache", async () => {
    const tiny = new SharedMemoryCache({ maxBytes: 1000 });
    const { dataset } = await open("fts-inverted-v2.0.lance", { cache: tiny });
    await search(dataset, { kind: "match", text: "fox" });
    const second = await search(dataset, { kind: "match", text: "fox" });
    expect(second.stats.cacheHits).toBe(0);
    const roomy = new SharedMemoryCache({ maxBytes: 8 * 1024 * 1024 });
    const warm = await open("fts-inverted-v2.0.lance", { cache: roomy });
    await search(warm.dataset, { kind: "match", text: "fox" });
    expect(
      (await search(warm.dataset, { kind: "match", text: "fox" })).stats.cacheHits,
    ).toBeGreaterThan(0);
  });

  it("literal capital Y survives case-preserving stemming (matches pylance)", async () => {
    const { dataset, expected } = await open("fts-variants-v2.0.lance");
    for (const text of ["YELL", "Yell", "yell", "SKY", "Running"]) {
      const recorded = expected.queries.find(
        (q: { index: string; text: string }) => q.index === "casestem_idx" && q.text === text,
      );
      const result = await search(
        dataset,
        { kind: "match", text },
        { index: "casestem_idx", limit: 40 },
      );
      expect(result.rows.length, text).toBe(recorded.hits.length);
      expect(result.rows.length, text).toBeGreaterThan(0);
    }
  });

  it("lists an unsupported index without hiding usable ones, and refuses to search it", async () => {
    const { dataset } = await open("fts-variants-v2.0.lance");
    const indexes = await dataset.ftsIndexes();
    const french = indexes.find((i) => i.name === "french_idx");
    expect(french).toMatchObject({ supported: false, tokenizer: { language: "French" } });
    expect(french?.unsupportedReason).toContain("language");
    expect(indexes.find((i) => i.name === "plain_idx")?.supported).toBe(true);
    await expect(
      search(dataset, { kind: "match", text: "x" }, { index: "french_idx" }),
    ).rejects.toMatchObject({
      code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE",
    });
  });

  it("rejects unknown index versions before decoding anything", async () => {
    const { dataset } = await open("fts-inverted-v2.0.lance");
    const internals = (
      dataset as unknown as { options: { indexMetadata: { indexVersion: number }[] } }
    ).options;
    const original = internals.indexMetadata.map((m) => m.indexVersion);
    for (const m of internals.indexMetadata) m.indexVersion = 99;
    try {
      await expect(search(dataset, { kind: "match", text: "fox" })).rejects.toMatchObject({
        code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE",
      });
      expect((await dataset.ftsIndexes())[0]).toMatchObject({ supported: false });
    } finally {
      internals.indexMetadata.forEach((m, i) => {
        m.indexVersion = original[i] as number;
      });
    }
  });

  it("validates the whole query tree before opening the index", async () => {
    const { dataset, trace } = await open("fts-inverted-v2.0.lance");
    await expect(search(dataset, { kind: "typo", text: "quick" } as never)).rejects.toMatchObject(
      VALIDATION,
    );
    await expect(
      search(
        dataset,
        { kind: "boolean", must: [{ kind: "match", text: "x", fuzziness: 3 as never }] },
        { limits: { maxTokenSetBytes: 1 } },
      ),
    ).rejects.toMatchObject(VALIDATION);
    expect(trace.requests).toEqual([]);
  });

  it("reports totals across refill batches", async () => {
    const { dataset } = await open("fts-inverted-v2.0.lance");
    const result = await search(dataset, { kind: "match", text: "quick brown fox" }, { limit: 37 });
    expect(result.rows).toHaveLength(37);
    expect(result.stats.rowsMaterialized).toBe(37);
    expect(result.stats.rowsRequested).toBeGreaterThanOrEqual(37);
  });

  it("processes partitions in groups whose resident token sets fit the shared budget", async () => {
    const sizes = [40, 30, 50, 10, 70];
    let active = 0;
    let resident = 0;
    let peakResident = 0;
    const order: number[] = [];
    await inGroups(
      sizes,
      (size) => size,
      80,
      async (size) => {
        active += 1;
        resident += size;
        peakResident = Math.max(peakResident, resident);
        await new Promise((done) => setTimeout(done, 1));
        order.push(size);
        resident -= size;
        active -= 1;
      },
    );
    // Groups [40,30], [50,10], [70]: concurrent within a group, never above capacity.
    expect(peakResident).toBe(70);
    expect(order.sort((a, b) => a - b)).toEqual([10, 30, 40, 50, 70]);
    expect(active).toBe(0);
    // A single item larger than the capacity still runs (alone); admission then decides.
    let ran = false;
    await inGroups(
      [500],
      (size) => size,
      80,
      async () => {
        ran = true;
      },
    );
    expect(ran).toBe(true);
  });

  it("validates and admits position lists before decoding them", () => {
    const u32 = (...values: number[]) => new Uint8Array(new Uint32Array(values).buffer);
    // A header claiming 2^31 positions with no blocks fails on the layout check.
    expect(() => decodePositions([u32(2 ** 31)], () => undefined)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
    // A consistent list asks for its memory first; a refusal stops it before allocation.
    const reserved: number[] = [];
    expect(() =>
      decodePositions([u32(3), u32(1, 4, 9)], (bytes) => {
        reserved.push(bytes);
        throw new Error("over budget");
      }),
    ).toThrowError("over budget");
    expect(reserved).toEqual([24]);
  });

  it("rejects posting varints and document ids that overflow 32 bits", () => {
    const block = (bytes: number[]) => Uint8Array.of(0, 0, 0, 0, ...bytes);
    // delta 2^32 encoded as five bytes.
    expect(() =>
      decodePosting([block([0x80, 0x80, 0x80, 0x80, 0x10, 1])], 1, "varint_delta"),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }));
    // 0xffffffff then +1 would wrap to 0.
    expect(() =>
      decodePosting([block([0xff, 0xff, 0xff, 0xff, 0x0f, 1, 1, 1])], 2, "varint_delta"),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }));
  });

  it("matches phrases without quadratic position scans", () => {
    let reads = 0;
    const counted = (values: number[]) =>
      new Proxy(values, {
        get(target, key, receiver) {
          if (typeof key === "string" && /^\d+$/.test(key)) reads += 1;
          return Reflect.get(target, key, receiver);
        },
      });
    const quick = Array.from({ length: 1024 }, (_v, i) => 10 * i);
    const adjacent = quick.map((p) => p + 1);
    // A genuine match at the very end must still be found while skipping everything before it.
    expect(phraseMatches([counted(quick), counted(adjacent)], [0, 1], 0)).toBe(true);
    expect(reads).toBeLessThan(100_000);
    // Unaligned data: the scan is near-linear (a quadratic scan reads about 520,000 times).
    for (const slop of [0, 1]) {
      reads = 0;
      expect(phraseMatches([counted(quick), counted(quick.map((p) => p + 5))], [0, 1], slop)).toBe(
        false,
      );
      expect(reads, `slop ${slop}`).toBeLessThan(100_000);
    }
  });
});

describe("review: index-global BM25 statistics survive AND pruning", () => {
  it("scores an AND query with frequencies from every partition, like pylance", async () => {
    // `term4` is absent from one of the two partitions. That partition cannot hold an AND match,
    // but its `quick` postings still count toward the index-global IDF.
    const { dataset } = await open("fts-inverted-v2.0.lance");
    const and = await search(dataset, { kind: "match", text: "quick term4", operator: "and" });
    expect(and.rows.map((row) => Number(row.id))).toEqual([909, 31, 838]);
    // Recorded from pylance 8.0.0: MatchQuery("quick term4", operator=AND), top three rows.
    const pylance = [5.813813209533691, 5.549165725708008, 4.520327568054199];
    and.scores.forEach((score, i) => {
      expect(Math.abs(score - (pylance[i] as number))).toBeLessThan(1e-4);
    });
    // The equivalent boolean must-query uses the same frequencies, so it scores identically.
    const must = await search(dataset, {
      kind: "boolean",
      must: [
        { kind: "match", text: "quick" },
        { kind: "match", text: "term4" },
      ],
    });
    expect(must.rowIds).toEqual(and.rowIds);
    must.scores.forEach((score, i) => {
      expect(Math.abs(score - (and.scores[i] as number))).toBeLessThan(1e-5);
    });
  });
});

describe("review: range readers admit every allocation before making it", () => {
  function contextFor(store: ReturnType<typeof memoryStore>, maxMemoryBytes: number) {
    return new LanceReadContext(
      store,
      { maxMemoryBytes },
      {
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
      },
      0,
      () => 0,
      { coalesceGapBytes: 1024, maxCoalescedRangeBytes: 1 << 20 },
    );
  }
  /** Inspect index files with a roomy context, then read through one with `maxMemoryBytes`. */
  async function docsFile(maxMemoryBytes: number) {
    const dir = resolve(FIXTURES, "fts-inverted-v2.0.lance/_indices");
    const { readdir } = await import("node:fs/promises");
    const store = memoryStore();
    const index = (await readdir(dir)).sort()[0] as string;
    for (const name of await readdir(resolve(dir, index))) {
      await store.put(`i/${name}`, new Uint8Array(await readFile(resolve(dir, index, name))));
    }
    const setup = contextFor(store, 1 << 24);
    const docs = await inspectLanceFile(setup, "i/part_0_docs.lance");
    const tokens = await inspectLanceFile(setup, "i/part_0_tokens.lance");
    const context = contextFor(store, maxMemoryBytes);
    return { context, docs, tokens, column: resolveColumn(docs, { name: "_num_tokens" }).column };
  }
  // 100 overlapping 64-row ranges of a 32-bit column: 25,600 output bytes from a 256-byte read.
  const overlapping = Array.from({ length: 100 }, () => ({ start: 0, end: 64 }));

  it("rejects outputs that exceed the memory budget even when the physical read is tiny", async () => {
    const { context, docs, column } = await docsFile(1024);
    const requestsBefore = context.stats.rangeRequests;
    await expect(readFixedRanges(context, docs, column, overlapping, 4)).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
    await expect(readU32Ranges(context, docs, column, overlapping)).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
    // Admission happens before any read is issued.
    expect(context.stats.rangeRequests).toBe(requestsBefore);
  });

  it("reports retained outputs in peak memory and returns the charge on release", async () => {
    const { context, docs, column } = await docsFile(1 << 20);
    const before = context.availableMemoryBytes;
    const read = await readFixedRanges(context, docs, column, overlapping, 4);
    expect(read.values).toHaveLength(100);
    expect(read.bytes).toBe(25_600);
    expect(context.stats.peakMemoryBytes).toBeGreaterThanOrEqual(25_600);
    expect(context.availableMemoryBytes).toBe(before - 25_600);
    read.release();
    expect(context.availableMemoryBytes).toBe(before);
    // Retaining moves the charge to the operation instead of dropping it.
    const kept = await readU32Ranges(context, docs, column, [{ start: 0, end: 64 }]);
    kept.retainForOperation();
    expect(context.availableMemoryBytes).toBe(before - 256);
  });

  it("admits binary payloads at their exact size", async () => {
    const roomy = await docsFile(1 << 20);
    const fstColumn = resolveColumn(roomy.tokens, { name: "_token_fst_bytes" }).column;
    const read = await readBinaryRanges(roomy.context, roomy.tokens, fstColumn, [
      { start: 0, end: 1 },
    ]);
    const payload = (read.values[0] as Uint8Array[])[0] as Uint8Array;
    expect(read.bytes).toBeGreaterThanOrEqual(payload.length);
    read.release();
    const tight = await docsFile(payload.length + 64);
    await expect(
      readBinaryRanges(tight.context, tight.tokens, fstColumn, [{ start: 0, end: 1 }]),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  });

  it("validates row ranges before allocating or reading", async () => {
    const { context, docs, column } = await docsFile(1 << 20);
    const requestsBefore = context.stats.rangeRequests;
    for (const range of [
      { start: -1, end: 4 },
      { start: 4, end: 2 },
      { start: 0, end: docs.rowCount + 1 },
      { start: 0.5, end: 2 },
    ]) {
      await expect(readU32Ranges(context, docs, column, [range])).rejects.toMatchObject({
        code: "LAKEQL_LANCE_READ_ERROR",
      });
    }
    expect(context.stats.rangeRequests).toBe(requestsBefore);
  });
});

describe("FTS format version 2 (varint tails, shared position streams)", () => {
  it("ranks like pylance for match queries and rejects phrases explicitly", async () => {
    const { dataset, expected } = await open("fts-format2-v2.0.lance");
    expect((await dataset.ftsIndexes())[0]).toMatchObject({ supported: true, indexVersion: 2 });
    for (const recorded of expected.queries) {
      const result = await search(dataset, { kind: "match", text: recorded.text }, { limit: 40 });
      expect(result.rows.length, recorded.text).toBe(recorded.hits.length);
      recorded.hits.forEach((hit: { score: number }, i: number) => {
        expect(
          Math.abs((result.scores[i] as number) - hit.score),
          `${recorded.text} ${i}`,
        ).toBeLessThan(1e-4);
      });
    }
    await expect(search(dataset, { kind: "phrase", text: "quick brown" })).rejects.toMatchObject({
      code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE",
    });
  });
});
