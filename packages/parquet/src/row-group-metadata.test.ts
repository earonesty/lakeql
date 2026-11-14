import { expect, it } from "vitest";
import { convertRowGroupMetadata } from "./row-group-metadata.js";

it("preserves row-group structure and rejects statistics without a schema leaf", () => {
  const raw = {
    field_1: [
      {
        field_2: 0n,
        field_3: {
          field_1: 1,
          field_2: [0],
          field_3: [new TextEncoder().encode("x")],
          field_4: 0,
          field_5: 1n,
          field_6: 4n,
          field_7: 4n,
          field_9: 4n,
          field_12: {
            field_1: Uint8Array.of(7, 0, 0, 0),
            field_2: Uint8Array.of(7, 0, 0, 0),
            field_3: 0n,
          },
        },
      },
    ],
    field_2: 4n,
    field_3: 1n,
  };
  const converted = convertRowGroupMetadata(raw, [{ name: "x", type: "INT32" }]);
  expect(converted.num_rows).toBe(1n);
  expect(converted.columns[0]?.meta_data?.statistics?.min).toBe(7);
  expect(converted.columns[0]?.meta_data?.path_in_schema).toEqual(["x"]);
  expect(() => convertRowGroupMetadata(raw, [])).toThrow(/schema leaf/);
});

it("checks raw decimal statistics and drops unsafe bounds", async () => {
  const { convertColumnChunkMetadata } = await import("./row-group-metadata.js");
  const decimal = {
    name: "amount",
    type: "FIXED_LEN_BYTE_ARRAY" as const,
    converted_type: "DECIMAL" as const,
    precision: 15,
    scale: 3,
  };
  const chunk = {
    field_3: {
      field_1: 7,
      field_2: [0],
      field_3: [new TextEncoder().encode("amount")],
      field_4: 0,
      field_12: {
        field_1: Uint8Array.of(0, 15, 81, 82),
        field_2: Uint8Array.of(0, 15, 81, 82),
        field_3: 0n,
      },
    },
  };
  const result = convertColumnChunkMetadata(chunk, decimal, true);
  expect(result.meta_data?.statistics?.max).toBe(1003.858);
  chunk.field_3.field_12.field_1 = Uint8Array.of(0x20, 0, 0, 0, 0, 0, 0);
  const invalid = convertColumnChunkMetadata(chunk, decimal, true);
  expect(invalid.meta_data?.statistics).not.toHaveProperty("max");
  expect(invalid.meta_data?.statistics?.min).toBe(1003.858);
  expect(invalid.meta_data?.statistics?.null_count).toBe(0n);
});

it("projects unused column-chunk payloads without allocating their lists", async () => {
  const { inspectCompactAllocations, readCompactHeader } = await import("./compact-header.js");
  const { COLUMN_CHUNK_PROJECTION, convertColumnChunkMetadata } = await import(
    "./row-group-metadata.js"
  );
  const bytes = Uint8Array.from([0x26, 0, 0x89, 0xf5, 0x80, 8, ...Array(1024).fill(0), 0]);
  const reader = () => ({ view: new DataView(bytes.buffer), offset: 0 });
  const raw = readCompactHeader(reader());
  const projected = readCompactHeader(reader(), COLUMN_CHUNK_PROJECTION);
  expect(projected).toEqual({ field_2: 0n });
  expect(convertColumnChunkMetadata(projected, undefined)).toEqual(
    convertColumnChunkMetadata(raw, undefined),
  );
  expect(inspectCompactAllocations(reader(), COLUMN_CHUNK_PROJECTION)).toEqual({
    structs: 1,
    fields: 1,
    lists: 0,
    listSlots: 0,
    binaryViews: 0,
    borrowedBytes: 0,
  });
  expect(inspectCompactAllocations(reader()).listSlots).toBe(1024);
  // Statistics.min/max are binary, not lists: reject before reading a length.
  expect(() =>
    readCompactHeader(
      { view: new DataView(Uint8Array.of(0x3c, 0xcc, 0x19).buffer), offset: 0 },
      COLUMN_CHUNK_PROJECTION,
    ),
  ).toThrow(/Unexpected compact type/);
});

it("preserves legacy non-scalar column metadata field 15", async () => {
  const { readCompactHeader } = await import("./compact-header.js");
  const { COLUMN_CHUNK_PROJECTION } = await import("./row-group-metadata.js");
  const bytes = Uint8Array.of(0x3c, 0xf9, 0x15, 2, 0, 0);
  expect(
    readCompactHeader({ view: new DataView(bytes.buffer), offset: 0 }, COLUMN_CHUNK_PROJECTION),
  ).toEqual({ field_3: { field_15: [1] } });
});

it("bounds raw scratch by one column while retaining real wide-schema metadata", async () => {
  const { parquetWriteBuffer } = await import("hyparquet-writer");
  const { parquetMetadata } = await import("hyparquet");
  const { MemoryBudget, MemoryScope } = await import("lakeql-core");
  const { indexFooterRowGroups, footerRowGroupView } = await import("./footer-directory.js");
  const { decodeRowGroupMetadata } = await import("./row-group-metadata.js");
  const { lakeqlParquetParsers } = await import("./parsers.js");
  const scratchPeaks: number[] = [];
  for (const width of [1, 8, 32]) {
    const file = parquetWriteBuffer({
      rowGroupSize: [2, 2],
      columnData: Array.from({ length: width }, (_, index) => ({
        name: `c${index.toString().padStart(2, "0")}`,
        type: "INT32" as const,
        data: [null, index, -index, index + 1],
      })),
    });
    const expected = parquetMetadata(file, { parsers: lakeqlParquetParsers });
    const leaves = expected.schema.filter((element) => element.type);
    const footerLength = new DataView(file).getUint32(file.byteLength - 8, true);
    const view = new DataView(file, file.byteLength - footerLength - 8, footerLength);
    const directoryScope = new MemoryScope(new MemoryBudget(16));
    const directory = indexFooterRowGroups(view, directoryScope);
    const group = footerRowGroupView(view, directory, 1);
    directoryScope.dispose();
    const run = (budget: InstanceType<typeof MemoryBudget>) => {
      const scope = new MemoryScope(budget);
      try {
        const result = decodeRowGroupMetadata(group, leaves, true, scope);
        expect(result).toEqual(expected.row_groups[1]);
        return budget.usedBytes;
      } finally {
        scope.dispose();
      }
    };
    const budget = new MemoryBudget(1024 * 1024);
    const retained = run(budget);
    scratchPeaks.push(budget.peakBytes - retained);
    expect(budget.usedBytes).toBe(0);
    expect(run(new MemoryBudget(budget.peakBytes))).toBe(retained);
    const tight = new MemoryBudget(budget.peakBytes - 1);
    expect(() => run(tight)).toThrow(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
    expect(tight.usedBytes).toBe(0);
    const invalid = new MemoryBudget(1024 * 1024);
    const scope = new MemoryScope(invalid);
    try {
      expect(() =>
        decodeRowGroupMetadata(
          new DataView(group.buffer, group.byteOffset, group.byteLength - 1),
          leaves,
          true,
          scope,
        ),
      ).toThrow();
    } finally {
      scope.dispose();
    }
    expect(invalid.usedBytes).toBe(0);
  }
  // Equal-shaped columns must not grow temporary raw-tree storage with width.
  expect(scratchPeaks[1]).toBe(scratchPeaks[0]);
  expect(scratchPeaks[2]).toBe(scratchPeaks[0]);
});
