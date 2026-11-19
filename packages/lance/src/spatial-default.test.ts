import { createHash } from "node:crypto";
import { open, readFile, stat } from "node:fs/promises";
import { resolve } from "node:path";
import type { ObjectStore } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { openLanceDataset } from "./index.js";

const ROOT = resolve(import.meta.dirname, "../fixtures/spatial-default");
const SELECT = ["serial", "label", "note", "score", "category"];
const polygon = {
  type: "Polygon" as const,
  coordinates: [
    [
      [10, 10],
      [14, 10],
      [14, 14],
      [10, 14],
      [10, 10],
    ],
  ] as [number, number][][],
};
const budget = {
  maxBytes: 512 * 1024,
  maxMemoryBytes: 512 * 1024,
  maxRangeRequests: 64,
  maxRowsDecoded: 10000,
  maxOutputRows: 100,
  maxConcurrentReads: 2,
};

async function fixture(overrides = {}, path = "dataset.lance") {
  const expected = JSON.parse(await readFile(resolve(ROOT, "expected.json"), "utf8"));
  const ranges: { path: string; offset: number; length: number }[] = [];
  const store: ObjectStore = {
    head: async (path) => {
      const s = await stat(resolve(ROOT, path));
      return { path, size: s.size };
    },
    getRange: async (path, { offset, length }) => {
      ranges.push({ path, offset, length });
      const file = await open(resolve(ROOT, path), "r");
      try {
        const bytes = new Uint8Array(length);
        const result = await file.read(bytes, 0, length, offset);
        if (result.bytesRead !== length) throw new Error("Truncated fixture range");
        return bytes;
      } finally {
        await file.close();
      }
    },
    get: async () => {
      throw new Error("Whole-object reads are forbidden");
    },
    put: async () => {
      throw new Error("Read-only fixture");
    },
    delete: async () => {
      throw new Error("Read-only fixture");
    },
    list: () => {
      throw new Error("Dataset scans are forbidden");
    },
  };
  const dataset = await openLanceDataset({ store, path, budget: { ...budget, ...overrides } });
  return { dataset, ranges, expected };
}

describe("issue #24 default GeoParquet conversion", () => {
  it.each([
    "intersects",
    "within",
    "hole",
  ] as const)("matches official %s ground truth with default storage and row IDs", async (name) => {
    const { dataset, expected, ranges } = await fixture();
    expect(dataset.storageVersion).toBe("2.1");
    expect(dataset.rowIdMode).toBe("address");
    expect(await dataset.spatialIndexes()).toMatchObject([
      { name: "geometry_idx", indexVersion: 0 },
    ]);
    const geometry =
      name === "hole"
        ? {
            ...polygon,
            coordinates: [
              ...polygon.coordinates,
              [
                [11, 11],
                [11, 13],
                [13, 13],
                [13, 11],
                [11, 11],
              ] as [number, number][],
            ],
          }
        : polygon;
    const result = await dataset.spatialRows({
      snapshotId: dataset.snapshotId,
      index: "geometry_idx",
      query: { geometry, predicate: name === "within" ? "within" : "intersects" },
      select: SELECT,
    });
    const matches = result.rows
      .map((row, i) => ({ ...row, _rowid: Number(result.rowIds[i]) }))
      .sort((a, b) => Number(a.serial) - Number(b.serial));
    expect(matches).toEqual(expected.queries[name]);
    expect(result.pagesVisited).toBeLessThan(5);
    expect(result.stats.peakMemoryBytes).toBeLessThan(budget.maxMemoryBytes);
    expect(result.stats.rangeRequests).toBeLessThanOrEqual(budget.maxRangeRequests);
    for (const range of ranges.filter((range) => range.path.endsWith(".lance")))
      expect(range.length).toBeLessThan((await stat(resolve(ROOT, range.path))).size);
  });
  it("materializes null points, nullable attributes, signed values and dictionary strings", async () => {
    const { dataset, expected } = await fixture();
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: expected.take.map((row: { serial: number }) => row.serial),
      select: [...SELECT, "geometry"],
    });
    expect(result.rows).toEqual(expected.take);
  });
  it("uses physical row addresses across fragments for both take and indexed queries", async () => {
    const { dataset, expected } = await fixture({}, "fragmented.lance");
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: expected.fragmented.map((row: { _rowid: number }) => row._rowid),
      select: [...SELECT, "geometry"],
    });
    expect(result.rows).toEqual(
      expected.fragmented.map(({ _rowid, ...row }: Record<string, unknown>) => row),
    );
    expect(expected.fragmented.some((row: { _rowid: number }) => row._rowid > 2 ** 32)).toBe(true);
    const found = await dataset.spatialRows({
      snapshotId: dataset.snapshotId,
      index: "geometry_idx",
      query: { geometry: polygon, predicate: "within" },
      select: ["serial", "geometry"],
    });
    expect(found.rowIds).toEqual([String(2 ** 32 + 1)]);
    expect(found.rows).toEqual([{ serial: 5643, geometry: { x: 11, y: 11 } }]);
  });
  it("reads default full-zip, all-null, binary, boolean, uint64 and nested struct layouts", async () => {
    const { dataset, expected } = await fixture({}, "layouts.lance");
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: [0, 1, 2, 3],
      select: ["id", "text", "value", "none", "flag", "blob", "large", "left", "right"],
    });
    expect(
      result.rows.map((row) =>
        row === null
          ? null
          : {
              ...row,
              blob: row.blob === null ? null : Array.from(row.blob as Uint8Array),
              large: row.large === null ? null : String(row.large),
            },
      ),
    ).toEqual(expected.layouts);
  });
  it("decodes narrow numeric words and nullable structs at every depth", async () => {
    const { dataset, expected } = await fixture({}, "numeric.lance");
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: expected.numericOffsets,
      select: ["tiny", "small", "f32", "deep"],
    });
    expect(result.rows).toEqual(expected.numeric);
  });
  it("verifies the source GeoParquet and all generated objects against recorded hashes", async () => {
    const { expected } = await fixture();
    for (const [path, hash] of Object.entries(expected.sha256)) {
      expect(
        createHash("sha256")
          .update(await readFile(resolve(ROOT, path)))
          .digest("hex"),
      ).toBe(hash);
    }
    expect(expected.batchSize).toBe(100000);
    expect(expected.rowCount).toBeGreaterThan(expected.batchSize);
  });
  it("enforces the exact observed memory boundary for default R-tree traversal", async () => {
    const run = async (maxMemoryBytes = budget.maxMemoryBytes) => {
      const { dataset } = await fixture({ maxMemoryBytes });
      return dataset.spatialRows({
        snapshotId: dataset.snapshotId,
        index: "geometry_idx",
        query: { geometry: polygon, predicate: "within" },
        select: ["serial"],
      });
    };
    const first = await run();
    const cap = first.stats.peakMemoryBytes;
    expect((await run(cap)).rows).toEqual(first.rows);
    await expect(run(cap - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  });
  it("rejects traversal, decode, output, byte, memory and request overruns", async () => {
    for (const [overrides, limits] of [
      [{ maxMemoryBytes: 256 * 1024 }, {}],
      [{ maxOutputRows: 1 }, {}],
      [{ maxBytes: 32 * 1024 }, {}],
      [{ maxRowsDecoded: 100 }, {}],
      [{ maxRangeRequests: 3 }, {}],
      [{}, { maxPagesVisited: 1 }],
      [{}, { maxCandidatesExamined: 1 }],
    ] as const) {
      const { dataset } = await fixture(overrides);
      await expect(
        dataset.spatialRows({
          snapshotId: dataset.snapshotId,
          index: "geometry_idx",
          query: { geometry: polygon, predicate: "intersects" },
          select: SELECT,
          limits,
        }),
      ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    }
  });
});
