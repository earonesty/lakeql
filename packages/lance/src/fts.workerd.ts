import { memoryStore } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { FTS_WORKERD_EXPECTED, FTS_WORKERD_FIXTURE_BASE64 } from "./fts-fixture.generated.js";
import { type LanceFtsQuery, openLanceDataset } from "./index.js";

function decodeBase64(encoded: string): Uint8Array {
  const binary = atob(encoded);
  return Uint8Array.from(binary, (char) => char.charCodeAt(0));
}

const QUERIES: Record<string, LanceFtsQuery> = {
  match_or_multi: { kind: "match", text: "quick brown fox" },
  match_stem: { kind: "match", text: "running databases" },
  phrase_exact: { kind: "phrase", text: "quick brown fox" },
  bool_must_should: {
    kind: "boolean",
    must: [{ kind: "match", text: "fox" }],
    should: [{ kind: "match", text: "jumps lazy" }],
  },
};

describe("lakeql-lance full-text search on workerd", () => {
  it("ranks like pylance through the real Workers runtime within a byte budget", async () => {
    expect((globalThis as Record<string, unknown>).WebSocketPair).toBeTypeOf("function");
    const store = memoryStore();
    for (const [path, encoded] of Object.entries(FTS_WORKERD_FIXTURE_BASE64)) {
      await store.put(path, decodeBase64(encoded));
    }
    const dataset = await openLanceDataset({
      store,
      path: "fixtures/fts-workerd.lance",
      version: FTS_WORKERD_EXPECTED.datasetVersion,
      budget: {
        maxBytes: 256_000,
        maxRangeRequests: 200,
        maxMemoryBytes: 2 * 1024 * 1024,
        maxOutputRows: 64,
        maxConcurrentReads: 4,
        maxElapsedMs: 5_000,
      },
    });
    expect((await dataset.ftsIndexes()).map((index) => index.name)).toEqual(["text_idx"]);
    for (const [name, recorded] of Object.entries(FTS_WORKERD_EXPECTED.queries)) {
      const result = await dataset.searchRows({
        snapshotId: dataset.snapshotId,
        index: "text_idx",
        query: QUERIES[name] as LanceFtsQuery,
        select: ["id"],
        limit: recorded.limit,
      });
      expect(result.rows.length, name).toBe(recorded.hits.length);
      for (const [i, hit] of recorded.hits.entries()) {
        expect(
          Math.abs((result.scores[i] as number) - hit.score),
          `${name} score ${i}`,
        ).toBeLessThan(1e-4 * Math.max(1, hit.score));
      }
      expect(result.stats.physicalBytesRequested, name).toBeLessThan(256_000);
    }
  });
});
