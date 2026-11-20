import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { memoryStore } from "lakeql";
import { describe, expect, it } from "vitest";
import { type LanceFtsQuery, openLanceDataset } from "./index.js";

const ROOT = resolve(import.meta.dirname, "../fixtures/fts-inverted-v2.0.lance");

async function open(version?: number) {
  const expected = JSON.parse(await readFile(resolve(ROOT, "expected.json"), "utf8"));
  const store = memoryStore();
  for (const path of Object.keys(expected.sha256)) {
    await store.put(`dataset.lance/${path}`, new Uint8Array(await readFile(resolve(ROOT, path))));
  }
  const dataset = await openLanceDataset({
    store,
    path: "dataset.lance",
    version: version ?? expected.datasetVersion,
    budget: { maxMemoryBytes: 64 * 1024 * 1024 },
  });
  return { dataset, expected };
}

type Dataset = Awaited<ReturnType<typeof open>>["dataset"];

const run = (dataset: Dataset, query: LanceFtsQuery, limit = 10, index = "text_idx") =>
  dataset.searchRows({
    snapshotId: dataset.snapshotId,
    index,
    query,
    select: ["id", "text"],
    limit,
  });

const deleted = (id: number) => id % 11 === 0 && id % 2 === 0;

describe("Lance full-text search semantics", () => {
  it("treats an AND query with an unknown term as matching nothing", async () => {
    // Lance itself ignores unknown terms under AND and returns rows without them; lakeql
    // returns only rows containing every requested term. The recorded pylance result
    // documents the difference.
    const { dataset, expected } = await open();
    expect(expected.queries.match_and_missing_term.hits.length).toBeGreaterThan(0);
    const result = await run(dataset, {
      kind: "match",
      text: "quick zzzzunknown",
      operator: "and",
    });
    expect(result.rows).toEqual([]);
    expect(result.totalMatches).toBe(0);
  });

  it("rejects an index that no longer covers every fragment", async () => {
    const { dataset } = await open((await open()).expected.unindexedAppendVersion);
    await expect(run(dataset, { kind: "match", text: "fox" })).rejects.toMatchObject({
      code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE",
      message: expect.stringContaining("does not cover all active fragments"),
    });
  });

  it("skips rows deleted after indexing and refills from the ranked tail", async () => {
    const { dataset } = await open();
    const everything = await run(dataset, { kind: "match", text: "quick brown fox" }, 1500);
    const ids = everything.rows.map((row) => Number(row.id));
    expect(ids.some(deleted)).toBe(false);
    // Deleted documents still count as index matches but never as returned rows.
    expect(everything.totalMatches).toBeGreaterThan(ids.length);
    expect(everything.rows.length).toBe(everything.rowIds.length);
    for (const limit of [3, 10, 37]) {
      const page = await run(dataset, { kind: "match", text: "quick brown fox" }, limit);
      expect(page.rows.map((row) => Number(row.id))).toEqual(ids.slice(0, limit));
      expect(page.scores).toEqual(everything.scores.slice(0, limit));
    }
    // Scores are descending.
    expect([...everything.scores].sort((a, b) => b - a)).toEqual(everything.scores);
  });

  it("returns the selected columns and string row ids", async () => {
    const { dataset } = await open();
    const result = await run(dataset, { kind: "match", text: "lance" }, 3);
    expect(result.rows).toHaveLength(3);
    for (const [i, row] of result.rows.entries()) {
      expect(String(row.text).toLowerCase()).toContain("lance");
      expect(typeof result.rowIds[i]).toBe("string");
    }
    expect(result.index).toMatchObject({ name: "text_idx", column: "text", withPosition: true });
  });

  it("combines boolean clauses: must narrows, should boosts, mustNot removes", async () => {
    const { dataset } = await open();
    const must = await run(
      dataset,
      { kind: "boolean", must: [{ kind: "match", text: "zebra" }] },
      1500,
    );
    const both = await run(
      dataset,
      {
        kind: "boolean",
        must: [{ kind: "match", text: "zebra" }],
        should: [{ kind: "match", text: "yak" }],
      },
      1500,
    );
    expect(both.rowIds.sort()).toEqual(must.rowIds.sort());
    for (const row of must.rows) expect(String(row.text)).toContain("zebra");
    const without = await run(
      dataset,
      {
        kind: "boolean",
        must: [{ kind: "match", text: "zebra" }],
        mustNot: [{ kind: "match", text: "yak" }],
      },
      1500,
    );
    for (const row of without.rows) expect(String(row.text)).not.toContain("yak");
    expect(without.rows.length).toBeLessThan(must.rows.length);
  });

  it("validates arguments before reading any index data", async () => {
    const { dataset } = await open();
    const bad = (query: LanceFtsQuery, limit = 5, index = "text_idx") =>
      run(dataset, query, limit, index);
    await expect(bad({ kind: "match", text: "x" }, 0)).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(bad({ kind: "match", text: "x" }, 5, "nope")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(bad({ kind: "match", text: "x", fuzziness: 3 })).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(bad({ kind: "match", text: "x", prefixLength: -1 })).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(bad({ kind: "match", text: "x", maxExpansions: 0 })).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(bad({ kind: "match", text: "x", operator: "xor" as never })).rejects.toMatchObject(
      { code: "LAKEQL_VALIDATION_ERROR" },
    );
    await expect(bad({ kind: "phrase", text: "x y", slop: -1 })).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(bad({ kind: "boolean" })).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(
      dataset.searchRows({
        snapshotId: "other",
        index: "text_idx",
        query: { kind: "match", text: "x" },
        select: ["id"],
        limit: 1,
      }),
    ).rejects.toMatchObject({ code: expect.stringContaining("LAKEQL") });
  });

  it("returns no rows for queries made only of stop words or unknown terms", async () => {
    const { dataset } = await open();
    for (const text of ["the of and", "zzzzunknown", ""]) {
      const result = await run(dataset, { kind: "match", text });
      expect(result.rows).toEqual([]);
      expect(result.totalMatches).toBe(0);
    }
  });

  it("caches the opened index per snapshot and shares it between queries", async () => {
    const { dataset } = await open();
    const first = await run(dataset, { kind: "match", text: "fox" });
    const second = await run(dataset, { kind: "match", text: "dog" });
    expect(first.stats.cacheMisses).toBeGreaterThan(0);
    expect(second.stats.cacheHits).toBeGreaterThan(0);
    expect(second.stats.physicalBytesRequested).toBeLessThan(first.stats.physicalBytesRequested);
  });
});
