import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { memoryStore, SharedMemoryCache } from "lakeql";
import { describe, expect, it } from "vitest";
import { networkWaveStore } from "../../../tests/support/network-waves.js";
import {
  childField,
  columnOfField,
  readBinaryRanges,
  readFixedRanges,
  readListOffsets,
  readU32Ranges,
  readU32Rows,
  readU64Ranges,
  readU64Rows,
  resolveColumn,
} from "./column.js";
import { inspectLanceFile } from "./file.js";
import {
  decodePositions,
  decodePosting,
  inGroups,
  type LanceRowLiveness,
  PostingCursor,
  phraseMatches,
  searchFtsIndex,
} from "./fts.js";
import { analyzeTokens } from "./fts-tokenizer.js";
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
    // Scoring needs every document length once (the corpus token total), and those rows are
    // charged. A query whose terms are all absent never needs them.
    const { dataset } = await open("fts-inverted-v2.0.lance", { budget: { maxRowsDecoded: 0 } });
    await expect(search(dataset, { kind: "match", text: "fox" })).rejects.toMatchObject(BUDGET);
    const absent = await search(dataset, { kind: "match", text: "zzzzunknown" });
    expect(absent.rows).toEqual([]);
    expect(absent.stats.rowsDecoded).toBe(0);
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

  it("requests only live rows from the take path", async () => {
    const { dataset } = await open("fts-inverted-v2.0.lance");
    const result = await search(dataset, { kind: "match", text: "quick brown fox" }, { limit: 37 });
    expect(result.rows).toHaveLength(37);
    expect(result.stats.rowsMaterialized).toBe(37);
    // Deleted hits are filtered while ranking, so none is requested and then discarded.
    expect(result.stats.rowsRequested).toBe(37);
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
    // 25,600 payload bytes plus the 100 buffers that hold them.
    expect(read.bytes).toBe(25_600 + 100 * 96);
    expect(context.stats.peakMemoryBytes).toBeGreaterThanOrEqual(read.bytes);
    expect(context.availableMemoryBytes).toBe(before - read.bytes);
    read.release();
    expect(context.availableMemoryBytes).toBe(before);
    // Retaining moves the charge to the operation instead of dropping it.
    const kept = await readU32Ranges(context, docs, column, [{ start: 0, end: 64 }]);
    kept.retainForOperation();
    expect(context.availableMemoryBytes).toBe(before - 256 - 96);
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

describe("review: memory accounting gaps and deleted rows", () => {
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
  /** The first index's partition 0 files, inspected with a roomy context. */
  async function indexFiles(maxMemoryBytes: number) {
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
    const invert = await inspectLanceFile(setup, "i/part_0_invert.lance");
    const posting = resolveColumn(invert, { name: "_posting" });
    return {
      store,
      context: contextFor(store, maxMemoryBytes),
      docs,
      tokens,
      invert,
      numTokens: resolveColumn(docs, { name: "_num_tokens" }).column,
      rowId: resolveColumn(docs, { name: "_rowid" }).column,
      fst: resolveColumn(tokens, { name: "_token_fst_bytes" }).column,
      length: resolveColumn(invert, { name: "_length" }).column,
      posting: posting.column,
      postingItems: columnOfField(invert, childField(invert, posting.fieldId)),
    };
  }
  const empties = Array.from({ length: 10_000 }, () => ({ start: 0, end: 0 }));

  it("charges every returned container, so empty ranges are not free", async () => {
    // 10,000 empty ranges are 10,000 buffers: under a 1 KiB budget every reader refuses them
    // before allocating or reading anything.
    const tight = await indexFiles(1024);
    const requestsBefore = tight.context.stats.rangeRequests;
    for (const read of [
      () => readFixedRanges(tight.context, tight.docs, tight.numTokens, empties, 4),
      () => readU32Ranges(tight.context, tight.docs, tight.numTokens, empties),
      () => readU64Ranges(tight.context, tight.docs, tight.rowId, empties),
      () => readListOffsets(tight.context, tight.invert, tight.posting, empties),
      () => readBinaryRanges(tight.context, tight.invert, tight.postingItems, empties),
    ]) {
      await expect(read()).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: { metric: "memory bytes" },
      });
    }
    expect(tight.context.stats.rangeRequests).toBe(requestsBefore);
    expect(tight.context.availableMemoryBytes).toBe(1024);
    // With room, the buffers are reported at their container cost rather than as 0 bytes.
    const roomy = await indexFiles(1 << 24);
    const read = await readU32Ranges(roomy.context, roomy.docs, roomy.numTokens, empties);
    expect(read.values).toHaveLength(10_000);
    expect(read.bytes).toBeGreaterThanOrEqual(10_000 * 64);
    expect(roomy.context.stats.peakMemoryBytes).toBeGreaterThanOrEqual(read.bytes);
    read.release();
    expect(roomy.context.availableMemoryBytes).toBe(1 << 24);
  });

  it("charges read plans while they are live", async () => {
    // 2,000 one-row ranges of a 4-byte column: 8,000 payload bytes, but 2,000 planned requests
    // and 2,000 buffers. The peak has to include all three.
    const f = await indexFiles(1 << 24);
    const ranges = Array.from({ length: 2000 }, (_v, i) => ({ start: i % 64, end: (i % 64) + 1 }));
    const read = await readU32Ranges(f.context, f.docs, f.numTokens, ranges);
    expect(read.bytes).toBeGreaterThanOrEqual(2000 * (4 + 64));
    expect(f.context.stats.peakMemoryBytes).toBeGreaterThanOrEqual(read.bytes + 2000 * 64);
    read.release();
    expect(f.context.availableMemoryBytes).toBe(1 << 24);
  });

  it("decodes binary offsets of a mid-page range in place, within its charge", async () => {
    const f = await indexFiles(1 << 24);
    // Items 5..9 start mid-page, so their first offset is the previous item's end.
    const whole = await readBinaryRanges(f.context, f.invert, f.postingItems, [
      { start: 0, end: 12 },
    ]);
    const expected = (whole.values[0] as Uint8Array[]).slice(5, 9).map((item) => [...item]);
    whole.release();
    const available = f.context.availableMemoryBytes;
    const part = await readBinaryRanges(f.context, f.invert, f.postingItems, [
      { start: 5, end: 9 },
    ]);
    expect((part.values[0] as Uint8Array[]).map((item) => [...item])).toEqual(expected);
    const payload = expected.reduce((sum, item) => sum + item.length, 0);
    expect(part.bytes).toBeGreaterThanOrEqual(payload + 4 * 64);
    part.release();
    expect(f.context.availableMemoryBytes).toBe(available);
    // The four item offsets (8 bytes each, plus their array) are charged on top of the plan
    // and the physical read while they are decoded: a budget with no room for them fails.
    const offsetsOnly = await indexFiles(4 * 8 + 5 * 8 + 128 + 64);
    await expect(
      readBinaryRanges(offsetsOnly.context, offsetsOnly.invert, offsetsOnly.postingItems, [
        { start: 5, end: 9 },
      ]),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  });

  it("gathers scattered rows into one output with a plan sized by requests, not rows", async () => {
    const f = await indexFiles(1 << 24);
    const all = await readU32Ranges(f.context, f.docs, f.numTokens, [
      { start: 0, end: f.docs.rowCount },
    ]);
    const lengths = [...(all.values[0] as Uint32Array)];
    all.release();
    const rows = Array.from({ length: Math.floor(f.docs.rowCount / 3) }, (_v, i) => i * 3);
    const before = f.context.stats.peakMemoryBytes;
    const gathered = await readU32Rows(f.context, f.docs, f.numTokens, rows);
    expect([...gathered.values]).toEqual(rows.map((row) => lengths[row]));
    // One container and four bytes per row.
    expect(gathered.bytes).toBeLessThan(rows.length * 4 + 128);
    expect(Math.max(before, f.context.stats.peakMemoryBytes)).toBeLessThan(
      before + rows.length * 4 + 16_384,
    );
    gathered.release();
    const ids = await readU64Rows(f.context, f.docs, f.rowId, [0, 0, 7]);
    expect(ids.values).toHaveLength(3);
    expect(ids.values[0]).toBe(ids.values[1]);
    ids.release();
    expect(f.context.availableMemoryBytes).toBe(1 << 24);
    for (const bad of [[3, 2], [-1], [f.docs.rowCount], [0.5]]) {
      await expect(readU32Rows(f.context, f.docs, f.numTokens, bad)).rejects.toMatchObject({
        code: "LAKEQL_LANCE_READ_ERROR",
      });
    }
  });

  it("admits a file directory from its encoded size before decoding it", async () => {
    const f = await indexFiles(1 << 24);
    const admitted: number[] = [];
    const file = await inspectLanceFile(f.context, "i/part_0_invert.lance", undefined, (bytes) =>
      admitted.push(bytes),
    );
    expect(file.columns.length).toBeGreaterThan(0);
    expect(admitted).toHaveLength(1);
    expect(admitted[0]).toBeGreaterThan(512);
    // A directory that does not fit is refused, and nothing stays charged.
    const refusal = new Error("no room");
    await expect(
      inspectLanceFile(f.context, "i/part_0_invert.lance", undefined, () => {
        throw refusal;
      }),
    ).rejects.toBe(refusal);
    expect(f.context.availableMemoryBytes).toBe(1 << 24);
  });

  it("reserves index directories and the query plan against the operation", async () => {
    const common: LanceFtsQuery = { kind: "match", text: "quick brown fox" };
    // Cold (no cache): the operation opens the index, so directories and plan are in its peak.
    const cold = await open("fts-inverted-v2.0.lance", { cache: new SharedMemoryCache(0) });
    const first = await search(cold.dataset, common);
    expect(first.search.directoryBytes).toBeGreaterThan(6 * 512);
    expect(first.search.planBytes).toBeGreaterThan(0);
    expect(first.stats.peakMemoryBytes).toBeGreaterThanOrEqual(
      first.search.directoryBytes + first.search.planBytes,
    );
    // The plan holds an entry per term per partition, so it grows with the query's terms.
    const one = await search(cold.dataset, { kind: "match", text: "quick" });
    expect(first.search.planBytes).toBeGreaterThan(one.search.planBytes);
    const exact = await search(cold.dataset, { kind: "match", text: "term1" });
    const fuzzy = await search(cold.dataset, { kind: "match", text: "term1", fuzziness: 1 });
    expect(fuzzy.search.tokensMatched).toBeGreaterThan(exact.search.tokensMatched);
    expect(fuzzy.search.planBytes).toBeGreaterThan(exact.search.planBytes);
    // A budget below the directories refuses to open the index at all.
    const tight = await open("fts-inverted-v2.0.lance", {
      cache: new SharedMemoryCache(0),
      budget: { maxMemoryBytes: first.search.directoryBytes - 1 },
    });
    await expect(search(tight.dataset, common)).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "memory bytes" },
    });
    // Warm: the cache owns the directories, at the same size the operation reserved.
    const cache = new SharedMemoryCache(8 * 1024 * 1024);
    const warm = await open("fts-inverted-v2.0.lance", { cache });
    await search(warm.dataset, common);
    const again = await search(warm.dataset, common);
    expect(again.search.directoryBytes).toBe(first.search.directoryBytes);
    expect(again.stats.peakMemoryBytes).toBeLessThan(first.stats.peakMemoryBytes);
  });

  /** Run `searchFtsIndex` on a context the test owns, so its accounted memory is observable. */
  async function directSearch(
    query: LanceFtsQuery,
    limit: number,
    extra: { maxBufferedRows?: number; isLive?: LanceRowLiveness } = {},
  ) {
    const { maxBufferedRows, isLive } = extra;
    const { dataset, trace } = await open("fts-inverted-v2.0.lance");
    const internals = (dataset as unknown as { options: Record<string, unknown> }).options;
    const maxMemoryBytes = 1 << 24;
    const context = new LanceReadContext(
      internals.store as ReturnType<typeof memoryStore>,
      { maxMemoryBytes, ...(maxBufferedRows === undefined ? {} : { maxBufferedRows }) },
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
    const result = await searchFtsIndex({
      ...(internals as unknown as Parameters<typeof searchFtsIndex>[0]),
      context,
      indexCache: undefined,
      indexName: "text_idx",
      query,
      limit,
      ...(isLive === undefined ? {} : { isLive }),
    });
    return {
      context,
      result,
      trace,
      charged: () => maxMemoryBytes - context.availableMemoryBytes,
    };
  }

  it("keeps exactly the directories, the plan and the current page charged", async () => {
    const { context, result, charged } = await directSearch(
      { kind: "match", text: "quick brown fox" },
      10,
    );
    expect(result.hits).toHaveLength(10);
    // While the first page is held: the page, plus what stays resident for the operation.
    const resident = result.stats.directoryBytes + result.stats.planBytes;
    const withPage = charged();
    expect(withPage).toBeGreaterThan(resident);
    result.close();
    const afterClose = charged();
    expect(afterClose).toBeLessThan(withPage);
    // Everything transient (token sets, posting batches, merge columns, read plans, document
    // lengths) has been returned; directories and plan are what remains, give or take the
    // small operation-lifetime bookkeeping of the manifest's index metadata.
    expect(afterClose).toBeGreaterThanOrEqual(resident);
    expect(afterClose).toBeLessThan(resident + 4096);
    context.releaseDecodedMemory();
    expect(charged()).toBe(0);
  });

  it("ranks live rows in one scan however many of the best matches were deleted", async () => {
    // The fixture deletes under 5% of rows. To make deletion heavy, treat 19 rows in 20 as
    // deleted: the search has to look far down the ranking to find ten live rows.
    const query: LanceFtsQuery = { kind: "match", text: "quick brown fox" };
    const everything = await directSearch(query, 1264);
    expect(everything.result.total).toBe(1264);
    const oneScan = {
      postingEntriesDecoded: everything.result.stats.postingEntriesDecoded,
      candidatesScored: everything.result.stats.candidatesScored,
    };
    expect(oneScan).toEqual({ postingEntriesDecoded: 2093, candidatesScored: 1264 });
    const unfiltered = await directSearch(query, 10);
    const wavesWithoutDeletes = unfiltered.trace.group("all").waves;
    const isLive: LanceRowLiveness = async (rowIds) => rowIds.map((rowId) => rowId % 20n === 0n);
    const live = everything.result.hits.filter(({ rowId }) => rowId % 20n === 0n);
    expect(live.length).toBeGreaterThan(40);
    for (const maxBufferedRows of [undefined, 10]) {
      const { context, result, trace, charged } = await directSearch(query, 10, {
        isLive,
        ...(maxBufferedRows === undefined ? {} : { maxBufferedRows }),
      });
      expect(result.hits).toEqual(live.slice(0, 10));
      // Deleted matches still count as index matches.
      expect(result.total).toBe(1264);
      // Cumulative work is exactly one scan: nothing is decoded or scored a second time.
      expect(result.stats.postingEntriesDecoded).toBe(oneScan.postingEntriesDecoded);
      expect(result.stats.candidatesScored).toBe(oneScan.candidatesScored);
      // Only candidates that could still enter the top ten have their row ids read.
      expect(result.stats.rowIdsResolved).toBeGreaterThan(10);
      expect(result.stats.rowIdsResolved).toBeLessThanOrEqual(1264);
      // Rounds double while unbounded, so the dependent chain grows logarithmically; held to
      // ten rows a round it grows linearly but stays inside the buffered-row bound.
      const extraWaves = trace.group("all").waves - wavesWithoutDeletes;
      expect(extraWaves).toBeLessThanOrEqual(maxBufferedRows === undefined ? 8 : 40);
      result.close();
      context.releaseDecodedMemory();
      expect(charged()).toBe(0);
    }
    // Every match deleted: an empty result after the same single scan, having checked them all.
    const none = await directSearch(query, 10, { isLive: async (ids) => ids.map(() => false) });
    expect(none.result.hits).toEqual([]);
    expect(none.result.total).toBe(1264);
    expect(none.result.stats.candidatesScored).toBe(oneScan.candidatesScored);
    expect(none.result.stats.postingEntriesDecoded).toBe(oneScan.postingEntriesDecoded);
    expect(none.result.stats.rowIdsResolved).toBe(1264);
  });

  it("drops a posting batch before returning its lease", async () => {
    const f = await indexFiles(1 << 24);
    // The longest posting list of the partition.
    const lengths = await readU32Ranges(f.context, f.invert, f.length, [
      { start: 0, end: f.invert.rowCount },
    ]);
    const all = [...(lengths.values[0] as Uint32Array)];
    lengths.release();
    const tokenId = all.indexOf(Math.max(...all));
    const bounds = await readListOffsets(f.context, f.invert, f.posting, [
      { start: tokenId, end: tokenId + 1 },
    ]);
    const [blockStart, blockEnd] = bounds.values[0] as [number, number];
    bounds.release();
    expect(blockEnd - blockStart).toBeGreaterThan(2);
    const available = f.context.availableMemoryBytes;
    // Record how many decoded entries the cursor still holds whenever a lease is returned.
    const heldAtRelease: number[] = [];
    let cursor: PostingCursor | undefined;
    const lease = f.context.leaseDecodedMemory.bind(f.context);
    f.context.leaseDecodedMemory = (bytes: number) => {
      const inner = lease(bytes);
      const wrapped = Object.create(inner) as typeof inner;
      wrapped.release = () => {
        if (bytes % 8 === 0 && bytes >= 8 * 128) heldAtRelease.push(cursor?.residentEntries ?? -1);
        inner.release();
      };
      return wrapped;
    };
    cursor = new PostingCursor(
      {
        context: f.context,
        partition: { invert: f.invert, postingItemColumn: f.postingItems, tailCodec: "fixed32" },
        limits: { postingBlocksPerBatch: 1, maxPostingEntries: 1e9 },
        counters: { postingEntries: 0 },
      } as never,
      { text: "", tokenId, length: all[tokenId] as number, blockStart, blockEnd },
    );
    let seen = 0;
    let previous = -1;
    while (!cursor.exhausted) {
      if (cursor.needsLoad) {
        await cursor.load();
        // The charge always matches exactly what the cursor can reach.
        expect(cursor.leasedBytes).toBe(cursor.residentEntries * 8);
        expect(cursor.residentEntries).toBeLessThanOrEqual(128);
        expect(f.context.availableMemoryBytes).toBe(available - cursor.leasedBytes);
      }
      expect(cursor.entry).toBe(seen);
      expect(cursor.doc).toBeGreaterThan(previous);
      previous = cursor.doc;
      cursor.advance();
      seen += 1;
    }
    expect(seen).toBe(all[tokenId]);
    cursor.release();
    expect(cursor.residentEntries).toBe(0);
    expect(cursor.leasedBytes).toBe(0);
    expect(cursor.doc).toBe(Infinity);
    expect(f.context.availableMemoryBytes).toBe(available);
    expect(heldAtRelease.length).toBeGreaterThan(2);
    expect(heldAtRelease.every((entries) => entries === 0)).toBe(true);
  });

  it("returns live rows past deleted hits within maxBufferedRows and one scan's work", async () => {
    const query: LanceFtsQuery = { kind: "match", text: "quick brown fox" };
    const reference = await open("fts-inverted-v2.0.lance");
    const everything = await search(reference.dataset, query, { limit: 1500 });
    const deletedId = (id: number) => id % 22 === 0;
    expect(everything.rows.some((row) => deletedId(Number(row.id)))).toBe(false);
    expect(everything.totalMatches).toBeGreaterThan(everything.rows.length);
    for (const limit of [20, 3, 50, 400]) {
      // The work limits allow exactly one scan (2,093 postings, 1,264 documents): a search
      // that rescored anything to get past a deleted hit would exceed them.
      const bounded = await open("fts-inverted-v2.0.lance", {
        budget: { maxBufferedRows: limit },
      });
      const result = await search(bounded.dataset, query, {
        limit,
        limits: { maxCandidates: 1264, maxPostingEntries: 2093 },
      });
      expect(result.rowIds).toEqual(everything.rowIds.slice(0, limit));
      expect(result.scores).toEqual(everything.scores.slice(0, limit));
      expect(result.rows).toHaveLength(limit);
      expect(result.totalMatches).toBe(everything.totalMatches);
      expect(result.search.candidatesScored).toBe(1264);
      expect(result.search.postingEntriesDecoded).toBe(2093);
      // Every row handed to the take path is live: none is requested and thrown away.
      expect(result.stats.rowsRequested).toBe(limit);
      expect(result.stats.rowsMaterialized).toBe(limit);
    }
  });

  it("charges query analysis as it runs, for wide and for long queries", async () => {
    // Tokens are produced one at a time, so the planner can admit each before the next exists.
    const params = {
      baseTokenizer: "simple",
      language: "English",
      lowerCase: true,
      stem: false,
      removeStopWords: false,
      asciiFolding: false,
      withPosition: true,
    } as Parameters<typeof analyzeTokens>[1];
    const tokens = analyzeTokens("Quick brown fox", params);
    expect(tokens.next().value).toEqual({ text: "quick", position: 0 });
    expect(tokens.next().value).toEqual({ text: "brown", position: 1 });
    // A 256 KiB budget serves ordinary queries...
    const budget = { maxMemoryBytes: 256 * 1024 };
    const { dataset } = await open("fts-inverted-v2.0.lance", { budget });
    expect((await search(dataset, { kind: "match", text: "quick brown fox" })).rows).toHaveLength(
      10,
    );
    // ...but not twenty thousand clauses, nor one clause of fifty thousand distinct terms.
    const wide: LanceFtsQuery = {
      kind: "boolean",
      should: Array.from({ length: 20_000 }, () => ({ kind: "match", text: "quick" }) as const),
    };
    const long: LanceFtsQuery = {
      kind: "match",
      text: Array.from({ length: 50_000 }, (_v, i) => `w${i}`).join(" "),
    };
    for (const query of [wide, long]) {
      await expect(search(dataset, query)).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: { metric: "memory bytes" },
      });
    }
    // The failure leaves nothing charged: the next ordinary query still fits.
    expect((await search(dataset, { kind: "match", text: "zebra" })).rows).toHaveLength(10);
  });

  it("stops at the first candidate past a work limit, not after the batch is built", async () => {
    const { dataset } = await open("fts-inverted-v2.0.lance");
    await expect(
      search(
        dataset,
        { kind: "phrase", text: "quick brown fox" },
        { limits: { maxPhraseCandidates: 10 } },
      ),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "FTS phrase candidates", limit: 10, actual: 11 },
    });
    await expect(
      search(
        dataset,
        { kind: "match", text: "quick brown fox" },
        { limits: { maxCandidates: 100 } },
      ),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "FTS candidates", limit: 100, actual: 101 },
    });
  });
});
