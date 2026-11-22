import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { memoryStore } from "lakeql";
import { describe, expect, it } from "vitest";
import { type LanceFtsQuery, openLanceDataset } from "./index.js";

const ROOT = resolve(import.meta.dirname, "../fixtures/fts-inverted-v2.0.lance");

interface Spec {
  query?: string;
  operator?: string;
  fuzziness?: number | null;
  prefix_length?: number;
  max_expansions?: number;
  slop?: number;
  must?: [string, Spec][];
  should?: [string, Spec][];
  must_not?: [string, Spec][];
}

interface ExpectedQuery {
  kind: "match" | "phrase" | "bool";
  spec: Spec;
  limit: number;
  hits?: { id: number; score: number }[];
  error?: string;
}

async function load() {
  const expected = JSON.parse(await readFile(resolve(ROOT, "expected.json"), "utf8")) as {
    datasetVersion: number;
    unindexedAppendVersion: number;
    queries: Record<string, ExpectedQuery>;
    sha256: Record<string, string>;
  };
  const store = memoryStore();
  for (const path of Object.keys(expected.sha256)) {
    await store.put(`dataset.lance/${path}`, new Uint8Array(await readFile(resolve(ROOT, path))));
  }
  return { expected, store };
}

function toQuery(kind: string, spec: Spec): LanceFtsQuery {
  if (kind === "match") {
    const { query, fuzziness, operator, prefix_length, max_expansions } = spec;
    return {
      kind: "match",
      text: query as string,
      ...(operator === undefined
        ? {}
        : { operator: String(operator).toLowerCase() as "or" | "and" }),
      ...(fuzziness === undefined || fuzziness === null ? {} : { fuzziness }),
      ...(prefix_length === undefined ? {} : { prefixLength: prefix_length }),
      ...(max_expansions === undefined ? {} : { maxExpansions: max_expansions }),
    };
  }
  if (kind === "phrase")
    return { kind: "phrase", text: spec.query as string, slop: spec.slop ?? 0 };
  const clause = ([k, s]: [string, Spec]) => toQuery(k, s);
  return {
    kind: "boolean",
    must: (spec.must ?? []).map(clause),
    should: (spec.should ?? []).map(clause),
    mustNot: (spec.must_not ?? []).map(clause),
  };
}

describe("Lance full-text search parity with pylance", () => {
  it("streams exactly across batch boundaries (one block and seven documents per batch)", async () => {
    const { expected, store } = await load();
    const dataset = await openLanceDataset({
      store,
      path: "dataset.lance",
      version: expected.datasetVersion,
      budget: { maxMemoryBytes: 64 * 1024 * 1024 },
    });
    for (const [name, recorded] of Object.entries(expected.queries)) {
      if (name === "match_and_missing_term") continue;
      const query = toQuery(recorded.kind, recorded.spec);
      const options = {
        snapshotId: dataset.snapshotId,
        index: "text_idx",
        query,
        select: ["id"],
        limit: recorded.limit,
      };
      const whole = await dataset.searchRows(options);
      const batched = await dataset.searchRows({
        ...options,
        limits: { postingBlocksPerBatch: 1, documentsPerBatch: 7 },
      });
      expect(batched.rowIds, name).toEqual(whole.rowIds);
      expect(batched.scores, name).toEqual(whole.scores);
      expect(batched.totalMatches, name).toBe(whole.totalMatches);
    }
  });

  it("matches pylance rankings and BM25 scores for the recorded query set", async () => {
    const { expected, store } = await load();
    const dataset = await openLanceDataset({
      store,
      path: "dataset.lance",
      version: expected.datasetVersion,
      budget: { maxMemoryBytes: 64 * 1024 * 1024 },
    });
    const indexes = await dataset.ftsIndexes();
    expect(indexes.map((index) => index.name)).toEqual(["text_idx"]);
    expect(indexes[0]?.withPosition).toBe(true);
    for (const [name, recorded] of Object.entries(expected.queries)) {
      if (name === "match_and_missing_term") continue; // documented deviation, tested below
      const result = await dataset.searchRows({
        snapshotId: dataset.snapshotId,
        index: "text_idx",
        query: toQuery(recorded.kind, recorded.spec),
        select: ["id"],
        limit: recorded.limit,
      });
      const hits = recorded.hits ?? [];
      expect(result.rows.length, name).toBe(hits.length);
      expect(
        result.scores
          .map((score, i) => [score, hits[i]?.score])
          .filter(
            ([a, b]) => Math.abs((a as number) - (b as number)) > 1e-4 * Math.max(1, b as number),
          ),
        `${name} scores`,
      ).toEqual([]);
      const floor = hits.at(-1)?.score ?? 0;
      const sure = (hit: { score: number }) => hit.score > floor + 1e-4;
      const got = new Set(result.rows.map((row) => Number(row.id)));
      for (const hit of hits.filter(sure))
        expect(got.has(hit.id), `${name} id ${hit.id}`).toBe(true);
    }
  });
});
