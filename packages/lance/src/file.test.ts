import { memoryStore, type ObjectStore } from "lakeql";
import { describe, expect, it } from "vitest";
import { type InspectedLanceFile, materializeInspectedLanceFileRows } from "./file.js";
import { LanceReadContext, type MutableLanceReadStats } from "./io.js";
import type { LanceArrayEncoding, LanceField, LancePage } from "./proto.js";

const PATH = "index/page_data.lance";
const VALUES: LanceField = {
  id: 0,
  name: "values",
  parentId: -1,
  logicalType: "int64",
  nullable: true,
};
const IDS: LanceField = {
  id: 1,
  name: "ids",
  parentId: -1,
  logicalType: "uint64",
  nullable: false,
};
const INT64_ENCODING: LanceArrayEncoding = {
  kind: "nullable",
  mode: "no_nulls",
  values: {
    kind: "flat",
    bitsPerValue: 64,
    bufferIndex: 0,
    bufferType: 0,
  },
};

describe("Lance physical page materialization", () => {
  it("maps ordered 2.0 pages by cumulative length across independent column boundaries", async () => {
    const bytes = new Uint8Array(128);
    const view = new DataView(bytes.buffer);
    [10n, 11n, 12n, 13n, 14n, 15n].forEach((value, index) => {
      view.setBigInt64(index * 8, value, true);
    });
    [100n, 101n, 102n, 103n, 104n, 105n].forEach((value, index) => {
      view.setBigUint64(48 + index * 8, value, true);
    });

    const inner = memoryStore();
    await inner.put(PATH, bytes);
    let fullObjectGets = 0;
    const store: ObjectStore = {
      async get(path) {
        fullObjectGets += 1;
        return await inner.get(path);
      },
      getRange: inner.getRange.bind(inner),
      put: inner.put.bind(inner),
      delete: inner.delete.bind(inner),
      list: inner.list.bind(inner),
      head: inner.head.bind(inner),
    };
    const stats = emptyStats();
    const context = new LanceReadContext(
      store,
      {
        maxBytes: 96,
        maxRangeRequests: 12,
        maxMemoryBytes: 512,
        maxRowsDecoded: 12,
        maxConcurrentReads: 2,
        maxElapsedMs: 1_000,
      },
      stats,
      performance.now(),
      performance.now.bind(performance),
      { coalesceGapBytes: 0, maxCoalescedRangeBytes: 32 },
    );
    const file: InspectedLanceFile = {
      path: PATH,
      fileSize: bytes.byteLength,
      fields: [VALUES, IDS],
      schemaMetadata: {},
      globalBuffers: [],
      columns: [
        {
          pages: [page(0, 2, 0, 16), page(2, 3, 16, 24), page(5, 1, 40, 8)],
          bufferOffsets: [],
          bufferSizes: [],
        },
        {
          pages: [page(0, 4, 48, 32), page(4, 2, 80, 16)],
          bufferOffsets: [],
          bufferSizes: [],
        },
      ],
      rowCount: 6,
    };

    const rows = await materializeInspectedLanceFileRows({
      context,
      file,
      selections: [
        { field: VALUES, columnIndex: 0 },
        { field: IDS, columnIndex: 1 },
      ],
      rowOffsets: [0, 1, 2, 3, 4, 5],
    });

    expect([...rows.values()]).toEqual([
      { values: 10, ids: 100 },
      { values: 11, ids: 101 },
      { values: 12, ids: 102 },
      { values: 13, ids: 103 },
      { values: 14, ids: 104 },
      { values: 15, ids: 105 },
    ]);
    expect(stats.physicalBytesRequested).toBe(96);
    expect(stats.rangeRequests).toBeLessThanOrEqual(12);
    expect(stats.pages).toEqual(
      new Set([`${PATH}:0:0`, `${PATH}:0:1`, `${PATH}:0:2`, `${PATH}:1:0`, `${PATH}:1:1`]),
    );
    expect(fullObjectGets).toBe(0);
  });
});

function page(rowStart: number, length: number, offset: number, size: number): LancePage {
  return {
    bufferOffsets: [offset],
    bufferSizes: [size],
    length,
    priority: 0,
    rowStart,
    encoding: INT64_ENCODING,
  };
}

function emptyStats(): MutableLanceReadStats {
  return {
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
  };
}
