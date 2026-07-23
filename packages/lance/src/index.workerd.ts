import { memoryStore } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { WORKERD_FIXTURE_BASE64 } from "./fixture.generated.js";
import { openLanceDataset } from "./index.js";

const DATASET_PATH = "fixtures/take-v2.0.lance";

describe("lakeql-lance workerd runtime", () => {
  it("materializes scattered projected rows through the real Workers runtime", async () => {
    expect((globalThis as Record<string, unknown>).WebSocketPair).toBeTypeOf("function");
    const store = memoryStore();
    for (const [path, encoded] of Object.entries(WORKERD_FIXTURE_BASE64)) {
      await store.put(path, decodeBase64(encoded));
    }

    const dataset = await openLanceDataset({
      store,
      path: DATASET_PATH,
      budget: {
        maxBytes: 32_000,
        maxRangeRequests: 64,
        maxMemoryBytes: 32_000,
        maxOutputRows: 32,
        maxConcurrentReads: 2,
        maxElapsedMs: 3_000,
      },
    });
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: [31n, 0n, 47n, 9n],
      select: ["serial", "mark_text", "active"],
    });

    expect(result.rows).toEqual([
      { serial: 10_000_031, mark_text: "MARK 031", active: false },
      { serial: 10_000_000, mark_text: "MARK 000", active: true },
      { serial: 10_000_047, mark_text: "MARK 047", active: false },
      { serial: 10_000_009, mark_text: null, active: false },
    ]);
    expect(result.stats).toMatchObject({
      fragmentsTouched: 3,
      rowsRequested: 4,
      rowsMaterialized: 4,
      selectedColumns: ["serial", "mark_text", "active"],
    });
    expect(result.stats.physicalBytesRequested).toBeLessThan(16_000);

    const typed = await openLanceDataset({
      store,
      path: "fixtures/types-v2.0.lance",
      budget: {
        maxBytes: 32_000,
        maxRangeRequests: 64,
        maxMemoryBytes: 32_000,
        maxOutputRows: 4,
        maxConcurrentReads: 2,
        maxElapsedMs: 3_000,
      },
    });
    await expect(
      typed.takeRows({
        snapshotId: typed.snapshotId,
        rowIds: [1n],
        select: ["i8", "u64", "plain_text", "payload", "event_date", "utc_millis"],
      }),
    ).resolves.toMatchObject({
      rows: [
        {
          i8: -7,
          u64: 18_000_000_000_000_000_001n,
          plain_text: "plain-1",
          payload: Uint8Array.of(0, 254, 1),
          event_date: new Date("2026-07-24T00:00:00.000Z"),
          utc_millis: {
            epochNanoseconds: 1_784_768_523_457_000_000n,
            unit: "millis",
            isAdjustedToUTC: true,
          },
        },
      ],
    });

    const dictionary = await openLanceDataset({
      store,
      path: "fixtures/dictionary-v2.0.lance",
      budget: {
        maxBytes: 32_000,
        maxRangeRequests: 64,
        maxMemoryBytes: 32_000,
        maxOutputRows: 8,
        maxConcurrentReads: 2,
        maxElapsedMs: 3_000,
      },
    });
    await expect(
      dictionary.takeRows({
        snapshotId: dictionary.snapshotId,
        rowIds: [0n, 1n, 2n, 3n, 4095n, 1n],
        select: ["serial", "status"],
      }),
    ).resolves.toMatchObject({
      rows: [
        { serial: 0, status: null },
        { serial: 1, status: "LIVE" },
        { serial: 2, status: "PENDING" },
        { serial: 3, status: "DEAD" },
        { serial: 4095, status: "DEAD" },
        { serial: 1, status: "LIVE" },
      ],
    });

    const deleted = await openLanceDataset({
      store,
      path: "fixtures/deletions-v2.0.lance",
      budget: {
        maxBytes: 32_000,
        maxRangeRequests: 64,
        maxMemoryBytes: 32_000,
        maxOutputRows: 4,
        maxConcurrentReads: 2,
        maxElapsedMs: 3_000,
      },
    });
    await expect(
      deleted.takeRows({
        snapshotId: deleted.snapshotId,
        rowIds: [2n, 3n],
        select: ["serial", "label"],
        onMissing: "null",
      }),
    ).resolves.toMatchObject({
      rows: [null, { serial: 103, label: "row-3" }],
      deletedRowIds: ["2"],
    });

    const indexed = await openLanceDataset({
      store,
      path: "fixtures/scalar-btree-v2.0.lance",
      budget: {
        maxBytes: 64_000,
        maxRangeRequests: 128,
        maxMemoryBytes: 64_000,
        maxOutputRows: 8,
        maxConcurrentReads: 2,
        maxElapsedMs: 3_000,
      },
    });
    await expect(
      indexed.lookupRows({
        snapshotId: indexed.snapshotId,
        index: "serial_btree",
        values: [1005, 9999],
        select: ["label", "status"],
      }),
    ).resolves.toMatchObject({
      groups: [
        {
          value: 1005,
          rowIds: ["10", "11"],
          rows: [
            { label: "indexed-10", status: "LIVE" },
            { label: "indexed-11", status: "DEAD" },
          ],
        },
        { value: 9999, rowIds: [], rows: [] },
      ],
    });
    await expect(
      indexed.rangeRows({
        snapshotId: indexed.snapshotId,
        index: "serial_btree",
        range: { lower: 1005, upper: 1006 },
        select: ["label"],
      }),
    ).resolves.toMatchObject({
      rowIds: ["10", "11", "12", "13"],
      rows: [
        { label: "indexed-10" },
        { label: "indexed-11" },
        { label: "indexed-12" },
        { label: "indexed-13" },
      ],
    });

    const vectors = await openLanceDataset({
      store,
      path: "fixtures/vector-ivf-flat-v2.0.lance",
      budget: {
        maxBytes: 128_000,
        maxRangeRequests: 256,
        maxMemoryBytes: 64_000,
        maxRowsDecoded: 512,
        maxOutputRows: 8,
        maxConcurrentReads: 2,
        maxElapsedMs: 3_000,
      },
      vectorLimits: {
        maxDimension: 4,
        maxPartitionsSearched: 4,
        maxCandidatesScored: 256,
      },
    });
    await expect(
      vectors.nearest({
        snapshotId: vectors.snapshotId,
        index: "vector_ivf_flat_l2",
        vector: [1, 2, 3, 4],
        k: 2,
        nprobes: 4,
        select: ["id", "label"],
      }),
    ).resolves.toMatchObject({
      metric: "l2",
      matches: [
        { rowId: "53", distance: 27, row: { id: 53, label: "vector-53" } },
        { rowId: "122", distance: 29, row: { id: 122, label: "vector-122" } },
      ],
      candidatesScored: 256,
    });
  });
});

function decodeBase64(value: string): Uint8Array {
  const binary = atob(value);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}
