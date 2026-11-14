import { parquetMetadata } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { incrementalParquetMetadata, readParquetFooterEnvelope } from "./incremental-metadata.js";
import { lakeqlParquetParsers } from "./parsers.js";

it.each([1, 20])("matches full metadata for %i generated row groups", (groups) => {
  const bytes = parquetWriteBuffer({
    rowGroupSize: Array.from({ length: groups }, () => 2),
    columnData: [
      { name: "id", type: "INT32", data: Array.from({ length: groups * 2 }, (_, i) => i) },
      {
        name: "text",
        type: "BYTE_ARRAY",
        data: Array.from({ length: groups * 2 }, (_, i) => `value-${i}`),
      },
    ],
    kvMetadata: [{ key: "custom", value: "metadata ✓" }],
  });
  expect(incrementalParquetMetadata(bytes)).toEqual(
    parquetMetadata(bytes, { parsers: lakeqlParquetParsers }),
  );
});

it("rejects invalid footer bounds and trailing compact bytes", () => {
  for (const bytes of [
    new ArrayBuffer(0),
    Uint8Array.of(255, 255, 255, 255, 80, 65, 82, 49).buffer,
    Uint8Array.of(0, 0, 2, 0, 0, 0, 80, 65, 82, 49).buffer,
  ]) {
    expect(() => incrementalParquetMetadata(bytes)).toThrow();
  }
});

it("matches a wide row group when converting columns incrementally", () => {
  const bytes = parquetWriteBuffer({
    columnData: Array.from({ length: 1024 }, (_, index) => ({
      name: `column.${index}`,
      type: "INT32" as const,
      data: [index, index + 1],
    })),
  });
  const actual = incrementalParquetMetadata(bytes);
  expect(actual).toEqual(parquetMetadata(bytes, { parsers: lakeqlParquetParsers }));
  expect(actual.row_groups[0]?.columns).toHaveLength(1024);
});

it.each([
  false,
  true,
])("preserves out-of-order envelope fields and empty key/value presence=%s", (hasKeyValues) => {
  // Empty row groups precede schema; explicit field IDs then move backward.
  const payload = Uint8Array.from([
    0x49,
    0x0c,
    0x09,
    4,
    0x1c,
    0x48,
    1,
    114,
    0x15,
    0,
    0,
    0x05,
    2,
    2,
    0x26,
    0,
    ...(hasKeyValues ? [0x29, 0x0c] : []),
    0,
  ]);
  const footer = new Uint8Array(payload.length + 8);
  footer.set(payload);
  new DataView(footer.buffer).setUint32(payload.length, payload.length, true);
  footer.set([80, 65, 82, 49], payload.length + 4);
  const actual = incrementalParquetMetadata(footer.buffer);
  expect(actual).toEqual(parquetMetadata(footer.buffer, { parsers: lakeqlParquetParsers }));
  expect(actual.key_value_metadata).toEqual(hasKeyValues ? [] : undefined);
  expect(actual.row_groups).toEqual([]);
});

it("rejects omitted required lists/counts and wrong integer wire types", () => {
  const schema = [0x09, 4, 0x1c, 0x48, 1, 114, 0x15, 0, 0];
  const version = [0x05, 2, 2];
  const rows = [0x06, 6, 0];
  for (const payload of [
    [...schema, ...version, ...rows, 0],
    [0x49, 0x0c, ...schema, ...rows, 0],
    [0x49, 0x0c, ...schema, ...version, 0],
    [0x49, 0x0c, ...schema, ...version, 0x05, 6, 0, 0],
    [0x49, 0x1c, 0x19, 0x0c, 0, ...schema, ...version, ...rows, 0],
  ]) {
    const footer = new Uint8Array(payload.length + 8);
    footer.set(payload);
    new DataView(footer.buffer).setUint32(payload.length, payload.length, true);
    footer.set([80, 65, 82, 49], payload.length + 4);
    expect(() => incrementalParquetMetadata(footer.buffer)).toThrow(
      /required|row-group|compact type/,
    );
  }
});

it("skips unknown schema payloads before allocation while preserving known fields", async () => {
  const { readParquetFooterEnvelope } = await import("./incremental-metadata.js");
  const footer = (unknown: number[]) => {
    // Root SchemaElement name=r, num_children=0; unknown field 11 is a list.
    const payload = [
      0x15,
      2,
      0x19,
      0x1c,
      0x48,
      1,
      114,
      0x15,
      0,
      ...unknown,
      0,
      0x16,
      0,
      0x19,
      0x0c,
      0,
    ];
    const bytes = new Uint8Array(payload.length + 8);
    bytes.set(payload);
    new DataView(bytes.buffer).setUint32(payload.length, payload.length, true);
    bytes.set([80, 65, 82, 49], payload.length + 4);
    return bytes.buffer;
  };
  const plainEvents: unknown[] = [];
  const unknownEvents: unknown[] = [];
  const plain = readParquetFooterEnvelope(footer([]), (event) => plainEvents.push(event));
  const extended = readParquetFooterEnvelope(
    footer([0x69, 0xf5, 0x80, 0x08, ...Array(1024).fill(0)]),
    (event) => unknownEvents.push(event),
  );
  expect(extended.metadata.schema).toEqual(plain.metadata.schema);
  expect(unknownEvents).toEqual(plainEvents);
  expect(() => readParquetFooterEnvelope(footer([0x69, 0xf5, 0x80, 0x08]))).toThrow(/Truncated/);
});

it("matches allocation preflight to actual parser events on a generated multi-group footer", async () => {
  const { inspectCompactAllocations, readCompactHeader } = await import("./compact-header.js");
  const bytes = parquetWriteBuffer({
    rowGroupSize: [2, 2],
    columnData: [
      { name: "id", type: "INT32", data: [1, 2, 3, 4] },
      { name: "label", type: "STRING", data: ["a", "β", "longer", ""] },
    ],
    kvMetadata: [{ key: "custom", value: "unicode ✓" }],
  });
  const length = new DataView(bytes).getUint32(bytes.byteLength - 8, true);
  const view = new DataView(bytes, bytes.byteLength - length - 8, length);
  const expected = {
    structs: 0,
    fields: 0,
    lists: 0,
    listSlots: 0,
    binaryViews: 0,
    borrowedBytes: 0,
  };
  readCompactHeader({ view, offset: 0 }, undefined, undefined, (event) => {
    switch (event.kind) {
      case "struct":
        expected.structs++;
        break;
      case "field":
        expected.fields++;
        break;
      case "list":
        expected.lists++;
        expected.listSlots += event.length;
        break;
      case "binary-view":
        expected.binaryViews++;
        expected.borrowedBytes += event.borrowedBytes;
        break;
    }
  });
  expect(inspectCompactAllocations({ view, offset: 0 })).toEqual(expected);
  expect(expected.structs).toBeGreaterThan(4);
  expect(expected.binaryViews).toBeGreaterThan(4);
});

it("retains admitted envelope structures and text with their owner", () => {
  const bytes = parquetWriteBuffer({
    columnData: [{ name: "café", type: "INT32", data: [1, 2] }],
    kvMetadata: [{ key: "custom", value: "value ✓" }],
  });
  const measure = new MemoryScope(new MemoryBudget(Number.MAX_SAFE_INTEGER));
  const expected = readParquetFooterEnvelope(bytes, undefined, measure);
  const capacity = measure.budget.usedBytes;
  measure.dispose();
  const scope = new MemoryScope(new MemoryBudget(capacity));
  const envelope = readParquetFooterEnvelope(bytes, undefined, scope);
  expect(envelope).toEqual(expected);
  const owner = scope.retainAllocations(envelope);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(capacity);
  expect(owner.value.metadata.key_value_metadata).toContainEqual({
    key: "custom",
    value: "value ✓",
  });
  owner.release();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(capacity - 1));
  try {
    expect(() => readParquetFooterEnvelope(bytes, undefined, tight)).toThrow(/budget/);
  } finally {
    tight.dispose();
  }
  expect(tight.budget.usedBytes).toBe(0);
});
