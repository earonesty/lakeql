import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { memoryStore, type ObjectStore } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { openLanceDataset } from "./index.js";

const ROOT = resolve(import.meta.dirname, "../fixtures/spatial-rtree-v2.0.lance");
const PATH = "grid.lance";
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
async function fixture() {
  const inner = memoryStore();
  const ranges: { path: string; offset: number; length: number }[] = [];
  async function load(dir: string, prefix: string) {
    for (const entry of await readdir(dir, { withFileTypes: true })) {
      if (entry.isDirectory()) await load(resolve(dir, entry.name), `${prefix}/${entry.name}`);
      else
        await inner.put(
          `${prefix}/${entry.name}`,
          new Uint8Array(await readFile(resolve(dir, entry.name))),
        );
    }
  }
  await load(ROOT, PATH);
  const store: ObjectStore = {
    head: inner.head.bind(inner),
    put: inner.put.bind(inner),
    delete: inner.delete.bind(inner),
    list: inner.list.bind(inner),
    get: async () => {
      throw new Error("Whole-object get forbidden");
    },
    getRange: async (path, { offset, length }) => {
      ranges.push({ path, offset, length });
      return inner.getRange(path, { offset, length });
    },
  };
  const expected = JSON.parse(await readFile(resolve(ROOT, "expected.json"), "utf8"));
  return { store, ranges, expected, inner };
}
const budget = {
  maxBytes: 1024 * 1024,
  maxMemoryBytes: 512 * 1024,
  maxRangeRequests: 32,
  maxRowsDecoded: 10000,
  maxOutputRows: 100,
  maxConcurrentReads: 2,
};

describe("official Lance R-tree spatial queries", () => {
  it.each([
    "intersects",
    "within",
    "hole",
  ] as const)("matches official %s ground truth using pruned ranges", async (name) => {
    const f = await fixture();
    const ds = await openLanceDataset({ store: f.store, path: PATH, version: 2, budget });
    expect(await ds.spatialIndexes()).toMatchObject([
      { name: "geometry_idx", column: "geometry", indexVersion: 0 },
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
    const result = await ds.spatialRows({
      snapshotId: ds.snapshotId,
      index: "geometry_idx",
      query: { geometry, predicate: name === "within" ? "within" : "intersects" },
      select: ["serial"],
    });
    expect(result.rows.map((row) => row.serial).sort((a, b) => Number(a) - Number(b))).toEqual(
      f.expected.queries[name],
    );
    expect(result.pagesVisited).toBeLessThan(5);
    expect(result.stats.peakMemoryBytes).toBeLessThan(budget.maxMemoryBytes);
    expect(result.stats.physicalBytesRequested).toBeLessThan(300_000);
    expect(result.stats.rangeRequests).toBeLessThan(20);
    expect(result.stats.rowsMaterialized).toBe(f.expected.queries[name].length);
  });
  it("reuses decoded storage 2.0 tree pages without a raw byte cache", async () => {
    const f = await fixture();
    const ds = await openLanceDataset({ store: f.store, path: PATH, version: 2, budget });
    const options = {
      snapshotId: ds.snapshotId,
      index: "geometry_idx",
      query: { geometry: polygon, predicate: "intersects" as const },
      select: ["serial"],
    };
    const cold = await ds.spatialRows(options);
    f.ranges.length = 0;
    const repeat = await ds.spatialRows(options);
    expect(repeat.rows).toEqual(cold.rows);
    expect(repeat.rowIds).toEqual(cold.rowIds);
    expect(f.ranges.some((range) => range.path.includes("/_indices/"))).toBe(false);
    expect(repeat.stats.cacheHits).toBeGreaterThanOrEqual(3);
  });
  it("verifies every official fixture file against its recorded digest", async () => {
    const f = await fixture();
    for (const [path, hash] of Object.entries(f.expected.sha256)) {
      expect(
        createHash("sha256")
          .update(await readFile(resolve(ROOT, path)))
          .digest("hex"),
      ).toBe(hash);
    }
  });
  it("returns no payload rows when the query is outside all indexed bounds", async () => {
    const f = await fixture();
    const ds = await openLanceDataset({ store: f.store, path: PATH, version: 2, budget });
    const result = await ds.spatialRows({
      snapshotId: ds.snapshotId,
      index: "geometry_idx",
      query: {
        predicate: "intersects",
        geometry: { type: "Point", coordinates: [-1, -1] },
      },
      select: ["label"],
    });
    expect(result.rows).toEqual([]);
    expect(result.pagesVisited).toBe(1);
    expect(f.ranges.some((range) => range.path.includes("/data/"))).toBe(false);
  });
  it("supports contains with exact indexed point semantics", async () => {
    const f = await fixture();
    const ds = await openLanceDataset({ store: f.store, path: PATH, version: 2, budget });
    const result = await ds.spatialRows({
      snapshotId: ds.snapshotId,
      index: "geometry_idx",
      query: {
        predicate: "contains",
        geometry: { type: "Point", coordinates: [10, 10] },
      },
      select: ["serial"],
    });
    expect(result.rows).toEqual([{ serial: 1290 }]);
  });
  it("projects native point and ordinary structs with repeated child names", async () => {
    const f = await fixture();
    const ds = await openLanceDataset({ store: f.store, path: PATH, version: 2, budget });
    const result = await ds.takeRows({
      snapshotId: ds.snapshotId,
      rowIds: [1290],
      select: ["geometry", "location"],
    });
    expect(result.rows).toEqual([{ geometry: { x: 10, y: 10 }, location: { x: 10, y: 10 } }]);
  });
  it("rejects an index that omits a newly appended fragment", async () => {
    const f = await fixture();
    const ds = await openLanceDataset({ store: f.store, path: PATH, budget });
    await expect(
      ds.spatialRows({
        snapshotId: ds.snapshotId,
        index: "geometry_idx",
        query: { geometry: polygon, predicate: "within" },
        select: ["serial"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE" });
  });
  it("rejects unsupported indexed geometry shapes without reading payload data", async () => {
    const f = await fixture();
    const manifest = `${PATH}/_versions/18446744073709551613.manifest`;
    const bytes = new Uint8Array(
      await readFile(resolve(ROOT, "_versions/18446744073709551613.manifest")),
    );
    const from = new TextEncoder().encode("geoarrow.point");
    const to = new TextEncoder().encode("geoarrow.other");
    for (let i = 0; i <= bytes.length - from.length; i++) {
      if (from.every((b, j) => bytes[i + j] === b)) bytes.set(to, i);
    }
    await f.inner.put(manifest, bytes);
    const ds = await openLanceDataset({ store: f.store, path: PATH, version: 2, budget });
    await expect(
      ds.spatialRows({
        snapshotId: ds.snapshotId,
        index: "geometry_idx",
        query: { geometry: polygon, predicate: "within" },
        select: ["serial"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE" });
    expect(f.ranges.some((range) => range.path.includes("/data/"))).toBe(false);
  });
  it("enforces traversal, candidate, output, memory and byte budgets", async () => {
    for (const [overrides, limits] of [
      [{ maxOutputRows: 1 }, {}],
      [{ maxMemoryBytes: 100_000 }, {}],
      [{ maxBytes: 20_000 }, {}],
      [{ maxRowsDecoded: 100 }, {}],
      [{ maxRangeRequests: 2 }, {}],
      [{}, { maxPagesVisited: 1 }],
      [{}, { maxCandidatesExamined: 1 }],
    ] as const) {
      const f = await fixture();
      const ds = await openLanceDataset({
        store: f.store,
        path: PATH,
        version: 2,
        budget: { ...budget, ...overrides },
      });
      await expect(
        ds.spatialRows({
          snapshotId: ds.snapshotId,
          index: "geometry_idx",
          query: { geometry: polygon, predicate: "intersects" },
          select: ["serial"],
          limits,
        }),
      ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    }
  });
  it("rejects mismatched snapshots and invalid queries before reading", async () => {
    const f = await fixture();
    const ds = await openLanceDataset({ store: f.store, path: PATH, version: 2, budget });
    const reads = f.ranges.length;
    await expect(
      ds.spatialRows({
        snapshotId: "wrong",
        index: "geometry_idx",
        query: { geometry: polygon, predicate: "within" },
        select: ["serial"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_LANCE_SNAPSHOT_MISMATCH" });
    expect(f.ranges).toHaveLength(reads);
  });
});
