import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { memoryStore } from "lakeql";
import { describe, expect, it } from "vitest";
import { type LanceFtsQuery, openLanceDataset } from "./index.js";

const ROOT = resolve(import.meta.dirname, "../fixtures/fts-variants-v2.0.lance");

interface Recorded {
  index: string;
  kind: "match" | "phrase";
  text: string;
  fuzziness?: 0 | 1 | 2;
  prefixLength?: number;
  hits: { id: number; score: number }[];
}

async function load() {
  const expected = JSON.parse(await readFile(resolve(ROOT, "expected.json"), "utf8")) as {
    queries: Recorded[];
    sha256: Record<string, string>;
  };
  const store = memoryStore();
  for (const path of Object.keys(expected.sha256)) {
    await store.put(`dataset.lance/${path}`, new Uint8Array(await readFile(resolve(ROOT, path))));
  }
  const dataset = await openLanceDataset({
    store,
    path: "dataset.lance",
    budget: { maxMemoryBytes: 64 * 1024 * 1024 },
  });
  return { expected, dataset };
}

describe("Lance inverted-index analyzer variants match pylance", () => {
  it("describes every analyzer configuration", async () => {
    const { dataset } = await load();
    const indexes = await dataset.ftsIndexes();
    expect(
      Object.fromEntries(indexes.map((i) => [i.name, [i.column, i.withPosition, i.tokenizer]])),
    ).toEqual({
      plain_idx: ["plain", false, expect.objectContaining({ stem: true, removeStopWords: true })],
      cased_idx: [
        "cased",
        true,
        expect.objectContaining({
          baseTokenizer: "whitespace",
          lowerCase: false,
          stem: false,
          asciiFolding: false,
        }),
      ],
      custom_idx: ["custom", true, expect.objectContaining({ stem: false })],
      raw_idx: ["raw", false, expect.objectContaining({ baseTokenizer: "raw" })],
      nostem_idx: ["nostem", true, expect.objectContaining({ stem: false, asciiFolding: false })],
      casestem_idx: [
        "casestem",
        true,
        expect.objectContaining({ lowerCase: false, stem: true, asciiFolding: false }),
      ],
      french_idx: ["french", false, expect.objectContaining({ language: "French", stem: true })],
      tabs_idx: [
        "tabs",
        true,
        expect.objectContaining({ baseTokenizer: "whitespace", stem: true }),
      ],
    });
  });

  it("ranks identically to pylance for each recorded analyzer-sensitive query", async () => {
    const { expected, dataset } = await load();
    const mismatches: string[] = [];
    for (const recorded of expected.queries) {
      const query: LanceFtsQuery =
        recorded.kind === "match"
          ? {
              kind: "match",
              text: recorded.text,
              ...(recorded.fuzziness === undefined ? {} : { fuzziness: recorded.fuzziness }),
              ...(recorded.prefixLength === undefined
                ? {}
                : { prefixLength: recorded.prefixLength }),
            }
          : { kind: "phrase", text: recorded.text };
      const result = await dataset.searchRows({
        snapshotId: dataset.snapshotId,
        index: recorded.index,
        query,
        select: ["id"],
        limit: 40,
      });
      const label = `${recorded.index} ${recorded.kind} "${recorded.text}"`;
      if (result.rows.length !== recorded.hits.length) {
        mismatches.push(`${label}: ${result.rows.length} rows, expected ${recorded.hits.length}`);
        continue;
      }
      const floor = recorded.hits.at(-1)?.score ?? 0;
      for (const [i, hit] of recorded.hits.entries()) {
        if (Math.abs((result.scores[i] as number) - hit.score) > 1e-4 * Math.max(1, hit.score)) {
          mismatches.push(`${label}: score ${i} ${result.scores[i]} != ${hit.score}`);
        }
        if (hit.score > floor + 1e-4 && !result.rows.some((row) => Number(row.id) === hit.id)) {
          mismatches.push(`${label}: missing id ${hit.id}`);
        }
      }
    }
    expect(mismatches).toEqual([]);
    expect(expected.queries.filter((q) => q.hits.length > 0).length).toBeGreaterThan(25);
  });

  it("rejects phrase queries on an index built without positions", async () => {
    const { dataset } = await load();
    await expect(
      dataset.searchRows({
        snapshotId: dataset.snapshotId,
        index: "plain_idx",
        query: { kind: "phrase", text: "quick brown" },
        select: ["id"],
        limit: 5,
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE" });
  });
});
