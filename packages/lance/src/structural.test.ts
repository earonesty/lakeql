import { memoryStore } from "lakeql";
import { expect, it } from "vitest";
import type { Layout21 } from "./encoding21.js";
import { LanceReadContext, type MutableLanceReadStats } from "./io.js";
import type { LancePage } from "./proto.js";
import { readStructuralColumn } from "./structural.js";

async function setup(data: Uint8Array, layout: Layout21, maxMemoryBytes = 4096) {
  const store = memoryStore();
  const file = new Uint8Array(128);
  new DataView(file.buffer).setUint16(0, (data.length / 8 - 1) << 4, true);
  file.set(data, 16);
  await store.put("column", file);
  const stats: MutableLanceReadStats = {
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
  const context = new LanceReadContext(
    store,
    { maxMemoryBytes },
    stats,
    performance.now(),
    performance.now.bind(performance),
    { coalesceGapBytes: 0, maxCoalescedRangeBytes: 64 },
  );
  const page: LancePage = {
    bufferOffsets: [0, 16],
    bufferSizes: [2, data.length],
    length: layout.items,
    priority: 0,
    rowStart: 0,
    encoding: layout,
  };
  const values: unknown[] = [];
  const run = async () => {
    try {
      await readStructuralColumn(
        {
          context,
          path: "column",
          fileSize: 128,
          page,
          layout,
          rows: Array.from({ length: layout.items }, (_, i) => i),
          logicalType: layout.values?.kind === "fsst" ? "string" : "int32",
          retainValues: true,
          decodeFixed: (bytes) =>
            new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getInt32(0, true),
        },
        (row, value) => {
          values[row] = value;
        },
      );
      return values;
    } finally {
      context.releaseDecodedMemory();
    }
  };
  return { context, page, run, stats };
}
const flat: Layout21 = {
  kind: "structural",
  layout: "mini",
  layers: [1],
  values: { kind: "flat", bits: 32 },
  dictionaryItems: 0,
  buffers: 1,
  items: 2,
  bitsDef: 0,
  bitsValue: 0,
  bitsOffset: 0,
};
function flatChunk() {
  const chunk = new Uint8Array(16);
  const view = new DataView(chunk.buffer);
  view.setUint16(2, 8, true);
  view.setInt32(8, 7, true);
  view.setInt32(12, -9, true);
  return chunk;
}
it("admits exact mini-block memory boundaries and releases leases on failure", async () => {
  const first = await setup(flatChunk(), flat);
  expect(await first.run()).toEqual([7, -9]);
  const peak = first.stats.peakMemoryBytes;
  const exact = await setup(flatChunk(), flat, peak);
  expect(await exact.run()).toEqual([7, -9]);
  const short = await setup(flatChunk(), flat, peak - 1);
  await expect(short.run()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  short.context.leaseDecodedMemory(peak - 1).release();
});
it("rejects malformed mini-block directories and buffer lengths", async () => {
  const directory = await setup(flatChunk(), flat);
  directory.page.bufferSizes[0] = 1;
  await expect(directory.run()).rejects.toMatchObject({ code: "LAKEQL_LANCE_READ_ERROR" });
  const chunk = flatChunk();
  new DataView(chunk.buffer).setUint16(2, 100, true);
  const broken = await setup(chunk, flat);
  await expect(broken.run()).rejects.toThrow(/outside chunk/);
  broken.context.leaseDecodedMemory(4096).release();
});
it("decodes FSST symbols and rejects truncated escapes before retaining output", async () => {
  const symbols = new Uint8Array(2312);
  new DataView(symbols.buffer).setUint32(4, 0x46535354, true);
  symbols[0] = 1;
  symbols[3] = 1;
  symbols[8] = 97;
  symbols[9] = 98;
  symbols[16] = 2;
  const layout: Layout21 = {
    ...flat,
    items: 1,
    values: {
      kind: "fsst",
      symbols,
      values: { kind: "variable", offsets: { kind: "flat", bits: 32 } },
    },
  };
  const chunk = new Uint8Array(24);
  const view = new DataView(chunk.buffer);
  view.setUint16(2, 11, true);
  view.setUint32(8, 8, true);
  view.setUint32(12, 11, true);
  chunk.set([0, 255, 33], 16);
  expect(await (await setup(chunk, layout)).run()).toEqual(["ab!"]);
  chunk[17] = 0;
  chunk[18] = 255;
  const broken = await setup(chunk, layout);
  await expect(broken.run()).rejects.toThrow(/Truncated FSST escape/);
  broken.context.leaseDecodedMemory(4096).release();
});
