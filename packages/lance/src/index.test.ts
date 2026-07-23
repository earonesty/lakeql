import { readdir, readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { resolve } from "node:path";
import { memoryCache, memoryStore, type ObjectStore } from "lakeql-core";
import { httpStore } from "lakeql-http";
import { afterEach, describe, expect, it } from "vitest";
import { openLanceDataset } from "./index.js";

const FIXTURE_ROOT = resolve(import.meta.dirname, "../fixtures/take-v2.0.lance");
const DATASET_PATH = "fixtures/take-v2.0.lance";
const TYPE_FIXTURE_ROOT = resolve(import.meta.dirname, "../fixtures/types-v2.0.lance");
const TYPE_DATASET_PATH = "fixtures/types-v2.0.lance";
const DELETION_FIXTURE_ROOT = resolve(import.meta.dirname, "../fixtures/deletions-v2.0.lance");
const DELETION_DATASET_PATH = "fixtures/deletions-v2.0.lance";
const SCALAR_FIXTURE_ROOT = resolve(import.meta.dirname, "../fixtures/scalar-btree-v2.0.lance");
const SCALAR_DATASET_PATH = "fixtures/scalar-btree-v2.0.lance";
const SCALAR_MULTIPAGE_FIXTURE_ROOT = resolve(
  import.meta.dirname,
  "../fixtures/scalar-btree-multipage-v2.0.lance",
);
const SCALAR_MULTIPAGE_DATASET_PATH = "fixtures/scalar-btree-multipage-v2.0.lance";
const VECTOR_FIXTURE_ROOT = resolve(import.meta.dirname, "../fixtures/vector-ivf-flat-v2.0.lance");
const VECTOR_DATASET_PATH = "fixtures/vector-ivf-flat-v2.0.lance";
const DICTIONARY_FIXTURE_ROOT = resolve(import.meta.dirname, "../fixtures/dictionary-v2.0.lance");
const DICTIONARY_DATASET_PATH = "fixtures/dictionary-v2.0.lance";
const servers: { close(): Promise<void> }[] = [];

afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => server.close()));
});

describe("lakeql-lance takeRows", () => {
  it("materializes one projected row without a full data-file read", async () => {
    const fixture = await fixtureStore();
    const observed = recordingStore(fixture.store);
    const dataset = await openLanceDataset({
      store: observed.store,
      path: DATASET_PATH,
      budget: generousBudget(),
    });
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: [17n],
      select: ["serial", "mark_text", "active"],
    });

    expect(result.rows).toEqual([{ serial: 10000017, mark_text: "MARK 017", active: false }]);
    expect(result.stats).toMatchObject({
      snapshotVersion: "1",
      rowsRequested: 1,
      rowsMaterialized: 1,
      fragmentsTouched: 1,
      pagesTouched: 3,
      selectedColumns: ["serial", "mark_text", "active"],
    });
    expect(result.stats.physicalBytesRequested).toBeLessThan(4_000);
    expect(observed.fullDataGets).toBe(0);
    expect(observed.dataRanges.every((range) => range.length < range.fileSize)).toBe(true);
  });

  it("preserves caller order, duplicates, projections, and nulls", async () => {
    const fixture = await fixtureStore();
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: generousBudget(),
    });
    const expected = JSON.parse(await readFile(resolve(FIXTURE_ROOT, "expected.json"), "utf8"))
      .sampleProjection as {
      rowIds: string[];
      select: string[];
      rows: Record<string, unknown>[];
    };

    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: expected.rowIds,
      select: expected.select,
    });

    expect(result.rows).toEqual(expected.rows);
    expect(result.stats.rowsRequested).toBe(5);
    expect(result.stats.rowsMaterialized).toBe(5);
    expect(result.stats.fragmentsTouched).toBe(3);
  });

  it("materializes the supported scalar, binary, date, and timestamp type matrix", async () => {
    const fixture = await fixtureStore(TYPE_FIXTURE_ROOT, TYPE_DATASET_PATH);
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: TYPE_DATASET_PATH,
      budget: generousBudget(),
    });
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: [1, 2],
      select: [
        "i8",
        "u8",
        "i16",
        "u16",
        "i32",
        "u32",
        "i64",
        "u64",
        "f32",
        "f64",
        "flag",
        "plain_text",
        "payload",
        "event_date",
        "utc_millis",
        "local_micros",
        "maybe_i32",
      ],
    });

    expect(result.rows[0]).toMatchObject({
      i8: -7,
      u8: 249,
      i16: -1_599,
      u16: 64_999,
      i32: -1_999_999,
      u32: 4_000_000_001,
      i64: -9_007_199_254_740_992n,
      u64: 18_000_000_000_000_000_001n,
      f32: 2.25,
      f64: -4.5,
      flag: false,
      plain_text: "plain-1",
      payload: Uint8Array.of(0, 254, 1),
      event_date: new Date("2026-07-24T00:00:00.000Z"),
      utc_millis: {
        epochNanoseconds: 1_784_768_523_457_000_000n,
        unit: "millis",
        isAdjustedToUTC: true,
      },
      local_micros: {
        epochNanoseconds: 1_784_768_523_456_790_000n,
        unit: "micros",
        isAdjustedToUTC: false,
      },
      maybe_i32: null,
    });
    expect(result.rows[1]).toMatchObject({
      i8: -6,
      plain_text: "plain-2",
      payload: Uint8Array.of(0, 253, 2),
      event_date: new Date("2026-07-25T00:00:00.000Z"),
      maybe_i32: 2,
    });
  });

  it("materializes optimized string dictionaries with nulls and duplicate row IDs", async () => {
    const fixture = await fixtureStore(DICTIONARY_FIXTURE_ROOT, DICTIONARY_DATASET_PATH);
    const expected = JSON.parse(
      await readFile(resolve(DICTIONARY_FIXTURE_ROOT, "expected.json"), "utf8"),
    ).sampleProjection as {
      rowIds: string[];
      rows: Record<string, unknown>[];
    };
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: DICTIONARY_DATASET_PATH,
      budget: generousBudget(),
    });

    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: expected.rowIds,
      select: ["serial", "status"],
    });

    expect(result.rows).toEqual(expected.rows);
    expect(result.stats.rowsMaterialized).toBe(expected.rows.length);
  });

  it("treats snapshot deletions as explicit missing rows without hiding live rows", async () => {
    const fixture = await fixtureStore(DELETION_FIXTURE_ROOT, DELETION_DATASET_PATH);
    const expected = JSON.parse(
      await readFile(resolve(DELETION_FIXTURE_ROOT, "expected.json"), "utf8"),
    ) as { originalRowIds: string[]; deletedRowIds: string[] };
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: DELETION_DATASET_PATH,
      budget: generousBudget(),
    });
    const requested = [2, 3, 7, 13].map((offset) => expected.originalRowIds[offset] ?? "");

    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: requested,
        select: ["serial", "label"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_OBJECT_NOT_FOUND",
      details: {
        missingRowIds: expected.deletedRowIds,
        deletedRowIds: expected.deletedRowIds,
      },
    });

    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: requested,
        select: ["serial", "label"],
        onMissing: "null",
      }),
    ).resolves.toMatchObject({
      rows: [null, { serial: 103, label: "row-3" }, null, null],
      missingRowIds: expected.deletedRowIds,
      deletedRowIds: expected.deletedRowIds,
    });
  });

  it("uses an official BTree index for duplicate and missing exact keys before takeRows", async () => {
    const fixture = await fixtureStore(SCALAR_FIXTURE_ROOT, SCALAR_DATASET_PATH);
    const observed = recordingStore(fixture.store);
    const expected = JSON.parse(
      await readFile(resolve(SCALAR_FIXTURE_ROOT, "expected.json"), "utf8"),
    ) as {
      index: { name: string; uuid: string; version: number };
      lookups: Record<string, { rowIds: string[]; labels: string[] }>;
    };
    const dataset = await openLanceDataset({
      store: observed.store,
      path: SCALAR_DATASET_PATH,
      budget: generousBudget(),
    });

    await expect(dataset.scalarIndexes()).resolves.toEqual([
      {
        name: expected.index.name,
        uuid: expected.index.uuid,
        column: "serial",
        indexVersion: 0,
      },
    ]);
    const result = await dataset.lookupRows({
      snapshotId: dataset.snapshotId,
      index: "serial_btree",
      values: [1005, 9999, 1031, 1005],
      select: ["label", "status"],
    });

    expect(result.groups).toEqual([
      {
        value: 1005,
        rowIds: expected.lookups["1005"]?.rowIds,
        rows: [
          { label: "indexed-10", status: "LIVE" },
          { label: "indexed-11", status: "DEAD" },
        ],
      },
      { value: 9999, rowIds: [], rows: [] },
      {
        value: 1031,
        rowIds: expected.lookups["1031"]?.rowIds,
        rows: [
          { label: "indexed-62", status: "LIVE" },
          { label: "indexed-63", status: "DEAD" },
        ],
      },
      {
        value: 1005,
        rowIds: expected.lookups["1005"]?.rowIds,
        rows: [
          { label: "indexed-10", status: "LIVE" },
          { label: "indexed-11", status: "DEAD" },
        ],
      },
    ]);
    expect(result.stats.rowsRequested).toBe(6);
    expect(result.stats.rowsMaterialized).toBe(6);
    expect(observed.fullDataGets).toBe(0);
    expect(observed.dataRanges.every((range) => range.length < range.fileSize)).toBe(true);
  });

  it("searches both sides of an official multi-page BTree boundary with logarithmic reads", async () => {
    const fixture = await fixtureStore(
      SCALAR_MULTIPAGE_FIXTURE_ROOT,
      SCALAR_MULTIPAGE_DATASET_PATH,
    );
    const observed = recordingStore(fixture.store);
    const dataset = await openLanceDataset({
      store: observed.store,
      path: SCALAR_MULTIPAGE_DATASET_PATH,
      budget: {
        ...generousBudget(),
        maxBytes: 128_000,
        maxRangeRequests: 128,
        maxRowsDecoded: 256,
      },
    });
    const result = await dataset.lookupRows({
      snapshotId: dataset.snapshotId,
      index: "serial_btree",
      values: [24_095, 24_096, 24_999],
      select: ["serial", "label"],
    });

    expect(result.groups).toEqual([
      {
        value: 24_095,
        rowIds: ["4095"],
        rows: [{ serial: 24_095, label: "multi-4095" }],
      },
      {
        value: 24_096,
        rowIds: ["4096"],
        rows: [{ serial: 24_096, label: "multi-4096" }],
      },
      {
        value: 24_999,
        rowIds: ["4999"],
        rows: [{ serial: 24_999, label: "multi-4999" }],
      },
    ]);
    expect(result.stats.physicalBytesRequested).toBeLessThan(64_000);
    expect(result.stats.rangeRequests).toBeLessThan(100);
    expect(observed.fullDataGets).toBe(0);
  });

  it("reads inclusive, exclusive, and one-sided BTree ranges in index order", async () => {
    const fixture = await fixtureStore(
      SCALAR_MULTIPAGE_FIXTURE_ROOT,
      SCALAR_MULTIPAGE_DATASET_PATH,
    );
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: SCALAR_MULTIPAGE_DATASET_PATH,
      budget: { ...generousBudget(), maxRowsDecoded: 256 },
    });

    await expect(
      dataset.rangeRows({
        snapshotId: dataset.snapshotId,
        index: "serial_btree",
        range: { lower: 24_095, upper: 24_097 },
        select: ["serial", "label"],
      }),
    ).resolves.toMatchObject({
      rowIds: ["4095", "4096", "4097"],
      rows: [
        { serial: 24_095, label: "multi-4095" },
        { serial: 24_096, label: "multi-4096" },
        { serial: 24_097, label: "multi-4097" },
      ],
    });
    await expect(
      dataset.rangeRows({
        snapshotId: dataset.snapshotId,
        index: "serial_btree",
        range: {
          lower: 24_095,
          lowerInclusive: false,
          upper: 24_098,
          upperInclusive: false,
        },
        select: ["serial"],
      }),
    ).resolves.toMatchObject({
      rowIds: ["4096", "4097"],
      rows: [{ serial: 24_096 }, { serial: 24_097 }],
    });
    await expect(
      dataset.rangeRows({
        snapshotId: dataset.snapshotId,
        index: "serial_btree",
        range: { upper: 20_002 },
        select: ["serial"],
      }),
    ).resolves.toMatchObject({
      rowIds: ["0", "1", "2"],
      rows: [{ serial: 20_000 }, { serial: 20_001 }, { serial: 20_002 }],
    });
    await expect(
      dataset.rangeRows({
        snapshotId: dataset.snapshotId,
        index: "serial_btree",
        range: { lower: 24_998 },
        select: ["serial"],
      }),
    ).resolves.toMatchObject({
      rowIds: ["4998", "4999"],
      rows: [{ serial: 24_998 }, { serial: 24_999 }],
    });
  });

  it("validates BTree range shape and applies output budgets before ID reads", async () => {
    const fixture = await fixtureStore(SCALAR_FIXTURE_ROOT, SCALAR_DATASET_PATH);
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: SCALAR_DATASET_PATH,
      budget: generousBudget(),
    });
    for (const range of [{}, { lower: 10, upper: 1 }]) {
      await expect(
        dataset.rangeRows({
          snapshotId: dataset.snapshotId,
          index: "serial_btree",
          range,
          select: ["label"],
        }),
      ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    }
    await expect(
      dataset.rangeRows({
        snapshotId: dataset.snapshotId,
        index: "serial_btree",
        range: { lower: 1005, lowerInclusive: false, upper: 1005 },
        select: ["label"],
      }),
    ).resolves.toMatchObject({ rowIds: [], rows: [] });

    const bounded = await openLanceDataset({
      store: fixture.store,
      path: SCALAR_DATASET_PATH,
      budget: { ...generousBudget(), maxOutputRows: 1 },
    });
    await expect(
      bounded.rangeRows({
        snapshotId: bounded.snapshotId,
        index: "serial_btree",
        range: { lower: 1005, upper: 1005 },
        select: ["label"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "output rows" },
    });
  });

  it("validates scalar lookup identity, index names, key types, and row budgets", async () => {
    const fixture = await fixtureStore(SCALAR_FIXTURE_ROOT, SCALAR_DATASET_PATH);
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: SCALAR_DATASET_PATH,
      budget: generousBudget(),
    });

    await expect(
      dataset.lookupRows({
        snapshotId: `${dataset.snapshotId}-stale`,
        index: "serial_btree",
        values: [1005],
        select: ["label"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_LANCE_SNAPSHOT_MISMATCH" });
    await expect(
      dataset.lookupRows({
        snapshotId: dataset.snapshotId,
        index: "serial_btree",
        values: [],
        select: ["label"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      dataset.lookupRows({
        snapshotId: dataset.snapshotId,
        index: "absent",
        values: [1005],
        select: ["label"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });
    await expect(
      dataset.lookupRows({
        snapshotId: dataset.snapshotId,
        index: "serial_btree",
        values: ["1005"],
        select: ["label"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      dataset.lookupRows({
        snapshotId: dataset.snapshotId,
        index: "serial_btree",
        values: [9999],
        select: ["label"],
      }),
    ).resolves.toMatchObject({
      groups: [{ value: 9999, rowIds: [], rows: [] }],
      stats: { rowsRequested: 0, rowsMaterialized: 0 },
    });

    const decodedBudget = await openLanceDataset({
      store: fixture.store,
      path: SCALAR_DATASET_PATH,
      budget: { ...generousBudget(), maxRowsDecoded: 1 },
    });
    await expect(
      decodedBudget.lookupRows({
        snapshotId: decodedBudget.snapshotId,
        index: "serial_btree",
        values: [1005],
        select: ["label"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "rows decoded" },
    });

    const unindexed = await openLanceDataset({
      store: (await fixtureStore()).store,
      path: DATASET_PATH,
      budget: generousBudget(),
    });
    await expect(unindexed.scalarIndexes()).resolves.toEqual([]);
  });

  it("matches official IVF_FLAT results and materializes projections in distance order", async () => {
    const fixture = await fixtureStore(VECTOR_FIXTURE_ROOT, VECTOR_DATASET_PATH);
    const observed = recordingStore(fixture.store);
    const expected = JSON.parse(
      await readFile(resolve(VECTOR_FIXTURE_ROOT, "expected.json"), "utf8"),
    ) as {
      indices: { name: string; uuid: string; version: number }[];
      query: number[];
      k: number;
      nprobes: number;
      expected: Record<string, { id: number; label: string; distance: number }[]>;
    };
    const dataset = await openLanceDataset({
      store: observed.store,
      path: VECTOR_DATASET_PATH,
      budget: {
        ...generousBudget(),
        maxBytes: 128_000,
        maxRangeRequests: 256,
        maxRowsDecoded: 512,
        maxOutputRows: expected.k,
      },
      vectorLimits: {
        maxDimension: 16,
        maxPartitionsSearched: 4,
        maxCandidatesScored: 256,
      },
    });

    const indexes = await dataset.vectorIndexes();
    expect(indexes).toHaveLength(3);
    expect(indexes.map(({ metric }) => metric).sort()).toEqual(["cosine", "dot", "l2"]);
    expect(indexes).toEqual(
      expect.arrayContaining(
        expected.indices.map((index) =>
          expect.objectContaining({
            name: index.name,
            uuid: index.uuid,
            column: "vector",
            type: "IVF_FLAT",
            dimension: 4,
            partitions: 4,
          }),
        ),
      ),
    );
    for (const metric of ["l2", "cosine", "dot"] as const) {
      const result = await dataset.nearest({
        snapshotId: dataset.snapshotId,
        index: `vector_ivf_flat_${metric}`,
        vector: expected.query,
        k: expected.k,
        nprobes: expected.nprobes,
        select: ["id", "label"],
      });
      const groundTruth = expected.expected[metric] ?? [];
      expect(result.matches.map(({ rowId, row }) => ({ rowId, row }))).toEqual(
        groundTruth.map((match) => ({
          rowId: String(match.id),
          row: { id: match.id, label: match.label },
        })),
      );
      for (const [index, match] of result.matches.entries()) {
        expect(match.distance).toBeCloseTo(groundTruth[index]?.distance ?? Number.NaN, 5);
      }
      expect(result.metric).toBe(metric);
      expect(result.partitionsSearched).toHaveLength(4);
      expect(result.candidatesScored).toBeGreaterThanOrEqual(255);
      expect(result.candidatesScored).toBeLessThanOrEqual(256);
      expect(result.stats.rowsDecoded).toBe(result.candidatesScored + expected.k);
    }
    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0],
        select: ["vector"],
      }),
    ).resolves.toMatchObject({
      rows: [{ vector: Float32Array.of(0, 0, 0, 0) }],
    });
    expect(observed.fullDataGets).toBe(0);
    expect(observed.dataRanges.every((range) => range.length < range.fileSize)).toBe(true);
  });

  it("bounds IVF_FLAT partitions, candidates, vector shape, and result buffering", async () => {
    const fixture = await fixtureStore(VECTOR_FIXTURE_ROOT, VECTOR_DATASET_PATH);
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: VECTOR_DATASET_PATH,
      budget: { ...generousBudget(), maxRowsDecoded: 128 },
      vectorLimits: {
        maxDimension: 4,
        maxPartitionsSearched: 1,
        maxCandidatesScored: 128,
      },
    });
    const result = await dataset.nearest({
      snapshotId: dataset.snapshotId,
      index: "vector_ivf_flat_l2",
      vector: [0, 0, 0, 0],
      k: 4,
      nprobes: 1,
      select: ["id"],
    });
    expect(result.partitionsSearched).toHaveLength(1);
    expect(result.candidatesScored).toBeLessThanOrEqual(128);
    expect(result.matches).toHaveLength(4);

    for (const options of [
      { vector: [], k: 1, nprobes: 1 },
      { vector: [0, 0, 0], k: 1, nprobes: 1 },
      { vector: [0, 0, 0, Number.NaN], k: 1, nprobes: 1 },
      { vector: [0, 0, 0, 1e100], k: 1, nprobes: 1 },
      { vector: [0, 0, 0, 0], k: 0, nprobes: 1 },
      { vector: [0, 0, 0, 0], k: 1.5, nprobes: 1 },
      { vector: [0, 0, 0, 0], k: 1, nprobes: 0 },
      { vector: [0, 0, 0, 0], k: 1, nprobes: 1.5 },
      { vector: [0, 0, 0, 0], k: 1, nprobes: 2 },
    ]) {
      await expect(
        dataset.nearest({
          snapshotId: dataset.snapshotId,
          index: "vector_ivf_flat_l2",
          ...options,
          select: ["id"],
        }),
      ).rejects.toMatchObject({
        code: options.nprobes === 2 ? "LAKEQL_BUDGET_EXCEEDED" : "LAKEQL_VALIDATION_ERROR",
      });
    }

    await expect(
      dataset.nearest({
        snapshotId: `${dataset.snapshotId}-stale`,
        index: "vector_ivf_flat_l2",
        vector: [0, 0, 0, 0],
        k: 1,
        nprobes: 1,
        select: ["id"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_LANCE_SNAPSHOT_MISMATCH" });
    await expect(
      dataset.nearest({
        snapshotId: dataset.snapshotId,
        index: "absent",
        vector: [0, 0, 0, 0],
        k: 1,
        nprobes: 1,
        select: ["id"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });

    const broader = await openLanceDataset({
      store: fixture.store,
      path: VECTOR_DATASET_PATH,
      budget: { ...generousBudget(), maxBufferedRows: 1, maxRowsDecoded: 512 },
      vectorLimits: {
        maxDimension: 4,
        maxPartitionsSearched: 4,
        maxCandidatesScored: 256,
      },
    });
    await expect(
      broader.nearest({
        snapshotId: broader.snapshotId,
        index: "vector_ivf_flat_l2",
        vector: [0, 0, 0, 0],
        k: 2,
        nprobes: 1,
        select: ["id"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "vector result rows" },
    });
    await expect(
      broader.nearest({
        snapshotId: broader.snapshotId,
        index: "vector_ivf_flat_l2",
        vector: [0, 0, 0, 0],
        k: 1,
        nprobes: 5,
        select: ["id"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(
      broader.nearest({
        snapshotId: broader.snapshotId,
        index: "vector_ivf_flat_cosine",
        vector: [0, 0, 0, 0],
        k: 1,
        nprobes: 4,
        select: ["id"],
      }),
    ).resolves.toMatchObject({
      metric: "cosine",
      matches: [{ distance: 1 }],
    });

    const partitionValidated = await openLanceDataset({
      store: fixture.store,
      path: VECTOR_DATASET_PATH,
      budget: { ...generousBudget(), maxRowsDecoded: 512 },
      vectorLimits: {
        maxDimension: 4,
        maxPartitionsSearched: 8,
        maxCandidatesScored: 256,
      },
    });
    await expect(
      partitionValidated.nearest({
        snapshotId: partitionValidated.snapshotId,
        index: "vector_ivf_flat_l2",
        vector: [0, 0, 0, 0],
        k: 1,
        nprobes: 5,
        select: ["id"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });

    const dimensionBounded = await openLanceDataset({
      store: fixture.store,
      path: VECTOR_DATASET_PATH,
      vectorLimits: { maxDimension: 3 },
    });
    await expect(dimensionBounded.vectorIndexes()).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "vector dimension" },
    });
    const unindexed = await openLanceDataset({
      store: (await fixtureStore()).store,
      path: DATASET_PATH,
    });
    await expect(unindexed.vectorIndexes()).resolves.toEqual([]);

    const candidateBounded = await openLanceDataset({
      store: fixture.store,
      path: VECTOR_DATASET_PATH,
      budget: { ...generousBudget(), maxRowsDecoded: 512 },
      vectorLimits: {
        maxDimension: 4,
        maxPartitionsSearched: 4,
        maxCandidatesScored: 1,
      },
    });
    await expect(
      candidateBounded.nearest({
        snapshotId: candidateBounded.snapshotId,
        index: "vector_ivf_flat_l2",
        vector: [0, 0, 0, 0],
        k: 1,
        nprobes: 1,
        select: ["id"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "vector candidates" },
    });

    const defaultMemory = await openLanceDataset({
      store: fixture.store,
      path: VECTOR_DATASET_PATH,
      budget: {
        maxBytes: 128_000,
        maxRangeRequests: 256,
        maxRowsDecoded: 128,
        maxOutputRows: 1,
      },
      vectorLimits: {
        maxDimension: 4,
        maxPartitionsSearched: 1,
        maxCandidatesScored: 128,
      },
    });
    await expect(
      defaultMemory.nearest({
        snapshotId: defaultMemory.snapshotId,
        index: "vector_ivf_flat_l2",
        vector: [0, 0, 0, 0],
        k: 1,
        nprobes: 1,
        select: ["id"],
      }),
    ).resolves.toMatchObject({ matches: [{ rowId: "0" }] });

    for (const vectorLimits of [
      { maxDimension: 0 },
      { maxPartitionsSearched: 0.5 },
      { maxCandidatesScored: -1 },
    ]) {
      await expect(
        openLanceDataset({
          store: fixture.store,
          path: VECTOR_DATASET_PATH,
          vectorLimits,
        }),
      ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    }
  });

  it("materializes 32 scattered rows with bounded reads and concurrency", async () => {
    const fixture = await fixtureStore();
    let active = 0;
    let peak = 0;
    const store = wrapStore(fixture.store, {
      async getRange(path, range) {
        active += 1;
        peak = Math.max(peak, active);
        try {
          await Promise.resolve();
          return await fixture.store.getRange(path, range);
        } finally {
          active -= 1;
        }
      },
    });
    const dataset = await openLanceDataset({
      store,
      path: DATASET_PATH,
      budget: { ...generousBudget(), maxConcurrentReads: 2 },
    });
    const rowIds = Array.from({ length: 32 }, (_value, index) => (index * 17) % 64);
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds,
      select: ["serial", "status", "score"],
    });

    expect(result.rows).toHaveLength(32);
    expect(result.rows.map((row) => row?.serial)).toEqual(
      rowIds.map((rowId) => 10_000_000 + rowId),
    );
    expect(peak).toBeLessThanOrEqual(2);
    expect(result.stats.fragmentsTouched).toBe(4);
    expect(result.stats.physicalBytesRequested).toBeLessThan(16_000);
  });

  it("defines missing and invalid row-ID behavior", async () => {
    const fixture = await fixtureStore();
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: generousBudget(),
    });

    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0, 999],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_OBJECT_NOT_FOUND",
      details: { missingRowIds: ["999"] },
    });
    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0, 999, 0],
        select: ["serial"],
        onMissing: "null",
      }),
    ).resolves.toMatchObject({
      rows: [{ serial: 10000000 }, null, { serial: 10000000 }],
      missingRowIds: ["999"],
    });
    for (const invalid of [-1, Number.MAX_SAFE_INTEGER + 1, "1.5", "-1"]) {
      await expect(
        dataset.takeRows({
          snapshotId: dataset.snapshotId,
          rowIds: [invalid],
          select: ["serial"],
        }),
      ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    }
  });

  it("validates dataset paths, range planning, projections, and row-shape budgets", async () => {
    const fixture = await fixtureStore();
    for (const path of ["", ".", "a/../b"]) {
      await expect(openLanceDataset({ store: fixture.store, path })).rejects.toMatchObject({
        code: "LAKEQL_VALIDATION_ERROR",
      });
    }
    for (const options of [
      { coalesceGapBytes: -1 },
      { coalesceGapBytes: 0.5 },
      { maxCoalescedRangeBytes: 0 },
      { maxCoalescedRangeBytes: 0.5 },
    ]) {
      await expect(
        openLanceDataset({ store: fixture.store, path: DATASET_PATH, ...options }),
      ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    }
    await expect(
      openLanceDataset({ store: fixture.store, path: DATASET_PATH, version: 0 }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });

    const dataset = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: { ...generousBudget(), maxOutputRows: 1, maxRowsDecoded: 1 },
    });
    for (const select of [[], [""], ["serial", "serial"]]) {
      await expect(
        dataset.takeRows({
          snapshotId: dataset.snapshotId,
          rowIds: [0],
          select,
        }),
      ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    }
    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0, 0],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0, 1],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "output rows" },
    });
    const decodedBudget = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: { ...generousBudget(), maxRowsDecoded: 1 },
    });
    await expect(
      decodedBudget.takeRows({
        snapshotId: decodedBudget.snapshotId,
        rowIds: [0, 1],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "rows decoded" },
    });
  });

  it("rejects row IDs coupled to another snapshot before data reads", async () => {
    const fixture = await fixtureStore();
    const observed = recordingStore(fixture.store);
    const dataset = await openLanceDataset({
      store: observed.store,
      path: DATASET_PATH,
      budget: generousBudget(),
    });
    const requestsAfterOpen = observed.rangeRequests;

    await expect(
      dataset.takeRows({
        snapshotId: `${dataset.snapshotId}-stale`,
        rowIds: [0],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_LANCE_SNAPSHOT_MISMATCH",
      details: {
        expectedSnapshotId: dataset.snapshotId,
        actualSnapshotId: `${dataset.snapshotId}-stale`,
      },
    });
    expect(observed.rangeRequests).toBe(requestsAfterOpen);
  });

  it("starts a fresh elapsed-time budget for each dataset operation", async () => {
    const fixture = await fixtureStore();
    let now = 0;
    let advanceDuringReads = false;
    const store = wrapStore(fixture.store, {
      async getRange(path, range) {
        if (advanceDuringReads) now += 2;
        return await fixture.store.getRange(path, range);
      },
    });
    const dataset = await openLanceDataset({
      store,
      path: DATASET_PATH,
      budget: { ...generousBudget(), maxElapsedMs: 1 },
      now: () => now,
    });

    now = 10_000;
    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0],
        select: ["serial"],
      }),
    ).resolves.toMatchObject({
      rows: [{ serial: 10_000_000 }],
      stats: { totalElapsedMs: 0 },
    });

    advanceDuringReads = true;
    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "elapsed milliseconds" },
    });
  });

  it("uses a byte metadata cache without weakening snapshot identity", async () => {
    const fixture = await fixtureStore();
    const cache = memoryCache<Uint8Array>();
    const first = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: generousBudget(),
      metadataCache: cache,
    });
    const second = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: generousBudget(),
      metadataCache: cache,
    });
    const result = await second.takeRows({
      snapshotId: second.snapshotId,
      rowIds: [0],
      select: ["serial"],
    });

    expect(second.snapshotId).toBe(first.snapshotId);
    expect(result.stats.cacheHits).toBe(1);
    expect(result.stats.cacheMisses).toBe(0);
    expect(result.stats.snapshotMetadataBytes).toBe(13);
  });

  it("enforces byte, request, output, memory, elapsed, and cancellation budgets", async () => {
    const fixture = await fixtureStore();
    await expect(
      openLanceDataset({
        store: fixture.store,
        path: DATASET_PATH,
        budget: { maxBytes: 12 },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    const limitedRequests = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: { ...generousBudget(), maxRangeRequests: 3 },
    });
    await expect(
      limitedRequests.takeRows({
        snapshotId: limitedRequests.snapshotId,
        rowIds: [0],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    const limitedRows = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: { ...generousBudget(), maxOutputRows: 1 },
    });
    await expect(
      limitedRows.takeRows({
        snapshotId: limitedRows.snapshotId,
        rowIds: [0, 1],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    await expect(
      openLanceDataset({
        store: fixture.store,
        path: DATASET_PATH,
        budget: { ...generousBudget(), maxMemoryBytes: 8 },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    let now = 0;
    const timedStore = wrapStore(fixture.store, {
      async getRange(path, range) {
        now += 2;
        return await fixture.store.getRange(path, range);
      },
    });
    await expect(
      openLanceDataset({
        store: timedStore,
        path: DATASET_PATH,
        budget: { ...generousBudget(), maxElapsedMs: 1 },
        now: () => now,
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    const controller = new AbortController();
    controller.abort("test");
    await expect(
      openLanceDataset({
        store: fixture.store,
        path: DATASET_PATH,
        budget: { signal: controller.signal },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  });

  it("rejects corrupt manifests and data-file offsets with typed errors", async () => {
    const manifestCorrupt = await fixtureStore();
    const manifestPath = `${DATASET_PATH}/_versions/18446744073709551614.manifest`;
    const manifest = await manifestCorrupt.store.get(manifestPath);
    if (manifest === null) throw new Error("fixture manifest missing");
    manifest[manifest.byteLength - 1] ^= 0xff;
    await manifestCorrupt.store.put(manifestPath, manifest);
    await expect(
      openLanceDataset({
        store: manifestCorrupt.store,
        path: DATASET_PATH,
        budget: generousBudget(),
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_LANCE_READ_ERROR" });

    const dataCorrupt = await fixtureStore();
    for (const dataPath of [...dataCorrupt.files.keys()].filter((path) =>
      path.endsWith(".lance"),
    )) {
      const data = await dataCorrupt.store.get(dataPath);
      if (data === null) throw new Error("fixture data file missing");
      const view = new DataView(data.buffer, data.byteOffset + data.byteLength - 40, 40);
      view.setBigUint64(8, BigInt(data.byteLength + 1), true);
      await dataCorrupt.store.put(dataPath, data);
    }
    const dataset = await openLanceDataset({
      store: dataCorrupt.store,
      path: DATASET_PATH,
      budget: generousBudget(),
    });
    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_LANCE_READ_ERROR" });
  });

  it("reads the official fixture through HTTP byte ranges", async () => {
    const fixture = await fixtureStore();
    const requests: string[] = [];
    const server = createServer(async (request, response) => {
      const key = decodeURIComponent(
        new URL(request.url ?? "/", "http://localhost").pathname.slice(1),
      );
      const bytes = fixture.files.get(key);
      if (bytes === undefined) {
        response.writeHead(404).end();
        return;
      }
      const range = /^bytes=(\d+)-(\d+)$/u.exec(request.headers.range ?? "");
      requests.push(request.headers.range ?? "full");
      if (range === null) {
        response.writeHead(200, { "content-length": bytes.byteLength }).end(bytes);
        return;
      }
      const start = Number(range[1]);
      const end = Number(range[2]);
      const body = bytes.subarray(start, end + 1);
      response
        .writeHead(206, {
          "content-length": body.byteLength,
          "content-range": `bytes ${start}-${end}/${bytes.byteLength}`,
        })
        .end(body);
    });
    await new Promise<void>((resolveListen) => server.listen(0, "127.0.0.1", resolveListen));
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("HTTP server not bound");
    servers.push({
      close: () => new Promise((resolveClose) => server.close(() => resolveClose())),
    });

    const dataset = await openLanceDataset({
      store: httpStore({ baseUrl: `http://127.0.0.1:${address.port}/` }),
      path: DATASET_PATH,
      budget: generousBudget(),
    });
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: [0, 63],
      select: ["serial", "mark_text"],
    });

    expect(result.rows).toEqual([
      { serial: 10000000, mark_text: "MARK 000" },
      { serial: 10000063, mark_text: "MARK 063" },
    ]);
    expect(requests.length).toBeGreaterThan(0);
    expect(requests.every((range) => range.startsWith("bytes="))).toBe(true);
  });
});

function generousBudget() {
  return {
    maxBytes: 1024 * 1024,
    maxRangeRequests: 128,
    maxElapsedMs: 5_000,
    maxMemoryBytes: 1024 * 1024,
    maxConcurrentReads: 4,
    maxOutputRows: 64,
    maxRowsDecoded: 64,
  };
}

async function fixtureStore(
  root = FIXTURE_ROOT,
  datasetPath = DATASET_PATH,
): Promise<{
  store: ObjectStore;
  files: Map<string, Uint8Array>;
}> {
  const store = memoryStore();
  const files = new Map<string, Uint8Array>();
  for (const relative of await fixtureFiles(root)) {
    if (relative === "expected.json") continue;
    const bytes = new Uint8Array(await readFile(resolve(root, relative)));
    const path = `${datasetPath}/${relative}`;
    files.set(path, bytes);
    await store.put(path, bytes);
  }
  return { store, files };
}

async function fixtureFiles(root: string, prefix = ""): Promise<string[]> {
  const output: string[] = [];
  for (const entry of await readdir(resolve(root, prefix), { withFileTypes: true })) {
    const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
    if (entry.isDirectory()) output.push(...(await fixtureFiles(root, relative)));
    else output.push(relative);
  }
  return output;
}

function recordingStore(inner: ObjectStore): {
  store: ObjectStore;
  rangeRequests: number;
  fullDataGets: number;
  dataRanges: { length: number; fileSize: number }[];
} {
  const state = {
    rangeRequests: 0,
    fullDataGets: 0,
    dataRanges: [] as { length: number; fileSize: number }[],
  };
  return {
    ...state,
    get rangeRequests() {
      return state.rangeRequests;
    },
    get fullDataGets() {
      return state.fullDataGets;
    },
    get dataRanges() {
      return state.dataRanges;
    },
    store: wrapStore(inner, {
      async get(path) {
        if (path.endsWith(".lance")) state.fullDataGets += 1;
        return await inner.get(path);
      },
      async getRange(path, range) {
        state.rangeRequests += 1;
        if (path.endsWith(".lance")) {
          const head = await inner.head(path);
          if (head === null) throw new Error("missing fixture object");
          state.dataRanges.push({ length: range.length, fileSize: head.size });
        }
        return await inner.getRange(path, range);
      },
    }),
  };
}

function wrapStore(
  inner: ObjectStore,
  overrides: Partial<Pick<ObjectStore, "get" | "getRange">>,
): ObjectStore {
  return {
    get: overrides.get ?? ((path) => inner.get(path)),
    getRange: overrides.getRange ?? ((path, range) => inner.getRange(path, range)),
    put: (path, body, options) => inner.put(path, body, options),
    delete: (path) => inner.delete(path),
    async *list(prefix, options) {
      yield* inner.list(prefix, options);
    },
    head: (path) => inner.head(path),
  };
}
