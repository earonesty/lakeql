import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { memoryStore, SharedMemoryCache } from "lakeql";
import { describe, expect, it } from "vitest";
import { networkWaveStore } from "../../../tests/support/network-waves.js";
import { decodePosting, phraseMatches } from "./fts.js";
import { type LanceFtsQuery, openLanceDataset } from "./index.js";

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

  it("charges ranking memory and the buffered-row ceiling", async () => {
    const warm = await open("fts-inverted-v2.0.lance", { budget: { maxBufferedRows: 100 } });
    await expect(
      search(warm.dataset, { kind: "match", text: "quick brown fox" }, { limit: 1 }),
    ).rejects.toMatchObject(BUDGET);
    const tight = await open("fts-inverted-v2.0.lance", { budget: { maxMemoryBytes: 40_000 } });
    await expect(
      search(tight.dataset, { kind: "match", text: "quick brown fox" }, { limit: 1 }),
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
