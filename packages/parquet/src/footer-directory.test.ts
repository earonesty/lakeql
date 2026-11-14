import { parquetWriteBuffer } from "hyparquet-writer";
import { MATERIALIZATION_CHARGES as c, MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { readCompactHeader } from "./compact-header.js";
import {
  footerDirectoryBytes,
  footerRowGroupView,
  indexFooterRowGroups,
} from "./footer-directory.js";

it.each([1, 20])("admits directory view plus eight bytes per row group (%i)", (groups) => {
  const bytes = parquetWriteBuffer({
    rowGroupSize: Array(groups).fill(2),
    columnData: [
      { name: "id", type: "INT32", data: Array.from({ length: groups * 2 }, (_, i) => i) },
    ],
  });
  const length = new DataView(bytes).getUint32(bytes.byteLength - 8, true);
  const view = new DataView(bytes, bytes.byteLength - 8 - length, length);
  const budget = new MemoryBudget(c.view + groups * 8),
    scope = new MemoryScope(budget);
  const directory = indexFooterRowGroups(view, scope);
  expect(budget.peakBytes).toBe(c.view + groups * 8);
  expect(directory).toHaveLength(groups * 2);
  for (let index = 0; index < groups; index++) {
    const group = footerRowGroupView(view, directory, index);
    expect(group.buffer).toBe(bytes);
    const reader = { view: group, offset: 0 };
    expect(readCompactHeader(reader).field_3).toBe(2n);
    expect(reader.offset).toBe(group.byteLength);
  }
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(c.view + groups * 8 - 1));
  expect(() => indexFooterRowGroups(view, tight)).toThrow(/budget/);
  expect(tight.budget.peakBytes).toBe(0);
  tight.dispose();
});

it("rejects invalid directories and malformed footer before admission", () => {
  const view = new DataView(Uint8Array.of(0x49, 0x1c, 0).buffer);
  const scope = new MemoryScope(new MemoryBudget(8));
  expect(() => indexFooterRowGroups(view, scope)).toThrow(/Truncated/);
  expect(scope.budget.peakBytes).toBe(0);
  for (const index of [-1, 0.5, NaN, 1])
    expect(() => footerRowGroupView(view, Uint32Array.of(0, 1), index)).toThrow();
  expect(() => footerRowGroupView(view, Uint32Array.of(0, 99), 0)).toThrow(/extent/);
  scope.dispose();
});

it("decodes random-access groups independently without reading peer structs", async () => {
  const { parquetMetadata } = await import("hyparquet");
  const { decodeRowGroupMetadata } = await import("./row-group-metadata.js");
  const { lakeqlParquetParsers } = await import("./parsers.js");
  const bytes = parquetWriteBuffer({
    rowGroupSize: Array(20).fill(2),
    columnData: [{ name: "id", type: "INT32", data: Array.from({ length: 40 }, (_, i) => i) }],
  });
  const expected = parquetMetadata(bytes, { parsers: lakeqlParquetParsers });
  const leaves = expected.schema.filter((element) => element.type);
  const length = new DataView(bytes).getUint32(bytes.byteLength - 8, true);
  const view = new DataView(bytes, bytes.byteLength - 8 - length, length);
  const scope = new MemoryScope(new MemoryBudget(c.view + 160));
  const directory = indexFooterRowGroups(view, scope);
  for (const index of [19, 0, 7, 7, 1])
    expect(decodeRowGroupMetadata(footerRowGroupView(view, directory, index), leaves)).toEqual(
      expected.row_groups[index],
    );
  const first = footerRowGroupView(view, directory, 0);
  const original = first.getUint8(0);
  first.setUint8(0, 0);
  expect(decodeRowGroupMetadata(footerRowGroupView(view, directory, 19), leaves)).toEqual(
    expected.row_groups[19],
  );
  expect(() => decodeRowGroupMetadata(first, leaves)).toThrow(/Trailing/);
  first.setUint8(0, original);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects mismatched precomputed counts and releases failed directory allocations", () => {
  const bytes = parquetWriteBuffer({
    rowGroupSize: [2, 2],
    columnData: [{ name: "id", type: "INT32", data: [0, 1, 2, 3] }],
  });
  const length = new DataView(bytes).getUint32(bytes.byteLength - 8, true);
  const view = new DataView(bytes, bytes.byteLength - 8 - length, length);
  for (const count of [-1, NaN, 0.5, view.byteLength + 1, 0, 1, 3]) {
    const scope = new MemoryScope(new MemoryBudget(c.view + 64));
    expect(() => indexFooterRowGroups(view, scope, count)).toThrow(/count|changed/);
    expect(scope.budget.usedBytes).toBe(0);
    scope.dispose();
  }
  const scope = new MemoryScope(new MemoryBudget(c.view + 16));
  expect(indexFooterRowGroups(view, scope, 2)).toHaveLength(4);
  expect(scope.budget.usedBytes).toBe(c.view + 16);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("charges empty directory views and validates forecast capacities", () => {
  const view = new DataView(Uint8Array.of(0x49, 0x0c, 0).buffer);
  expect(footerDirectoryBytes(0)).toBe(c.view);
  const scope = new MemoryScope(new MemoryBudget(c.view));
  expect(indexFooterRowGroups(view, scope)).toHaveLength(0);
  expect(scope.budget.usedBytes).toBe(c.view);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  for (const count of [-1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER])
    expect(() => footerDirectoryBytes(count)).toThrow(/capacity/);
  const tight = new MemoryScope(new MemoryBudget(c.view - 1));
  expect(() => indexFooterRowGroups(view, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});
