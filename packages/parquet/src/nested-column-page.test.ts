import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { getSchemaPath } from "hyparquet/src/schema.js";
import type { ColumnDecoder, PageHeader } from "hyparquet/src/types.js";
import { MemoryBudget, MemoryScope, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { decodeCompactNestedColumnPage, decodeNestedColumnPage } from "./nested-column-page.js";

const schemaPath = getSchemaPath(
  [
    { name: "root", num_children: 1 },
    { name: "items", repetition_type: "REPEATED", num_children: 1 },
    { name: "value", repetition_type: "OPTIONAL", type: "INT32" },
  ],
  ["items", "value"],
);
const decoder: ColumnDecoder = {
  type: "INT32",
  codec: "UNCOMPRESSED",
  element: schemaPath[2]?.element ?? { name: "value" },
  schemaPath,
  pathInSchema: ["items", "value"],
  parsers: DEFAULT_PARSERS,
};
function page(reps: number[], defs: number[], values: number[]) {
  const payload = new Uint8Array(values.length * 4);
  values.forEach((value, i) => {
    new DataView(payload.buffer).setInt32(i * 4, value, true);
  });
  const bytes = Uint8Array.from([
    ...reps.flatMap((r) => [2, r]),
    ...defs.flatMap((d) => [2, d]),
    ...payload,
  ]);
  const header: PageHeader = {
    type: "DATA_PAGE_V2",
    compressed_page_size: bytes.length,
    uncompressed_page_size: bytes.length,
    data_page_header_v2: {
      num_values: reps.length,
      num_rows: reps.filter((r) => r === 0).length,
      num_nulls: defs.filter((d) => d !== 2).length,
      encoding: "PLAIN",
      repetition_levels_byte_length: reps.length * 2,
      definition_levels_byte_length: defs.length * 2,
      is_compressed: false,
    },
  };
  return { bytes, header };
}

it("commits continuation state only after a complete page succeeds", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const first = page([0], [2], [7]);
  const a = decodeNestedColumnPage(first.bytes, first.header, decoder, undefined, scope);
  if (!a) throw Error("Missing first page");
  const retained = scope.budget.usedBytes;
  const invalid = page([1], [0], []);
  expect(() =>
    decodeNestedColumnPage(invalid.bytes, invalid.header, decoder, a.nextDefinition, scope),
  ).toThrow(/transition/);
  expect(scope.budget.usedBytes).toBe(retained);
  expect(vectorValue(a.values, 0)).toBe(7);
  const next = page([1, 0], [2, 2], [9, 11]);
  const b = decodeNestedColumnPage(next.bytes, next.header, decoder, a.nextDefinition, scope);
  expect(b?.rows.continuesPrevious).toBe(true);
  expect(b?.nextDefinition).toBe(2);
  expect(b?.rows.valueOffsets).toEqual(Uint32Array.of(0, 1, 2));
  if (!b) throw Error("Missing next page");
  expect([vectorValue(b.values, 0), vectorValue(b.values, 1)]).toEqual([9, 11]);
  const nullable = page([1], [1], []);
  const c = decodeNestedColumnPage(
    nullable.bytes,
    nullable.header,
    decoder,
    b.nextDefinition,
    scope,
  );
  if (!c) throw Error("Missing nullable continuation");
  expect(vectorValue(c.slots, 0)).toBeNull();
  expect(c.physical.levels.presentCount).toBe(0);
  expect(c.rows.continuesPrevious).toBe(true);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects orphan continuations and rolls back unsupported logical pages", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const orphan = page([1], [2], [7]);
  expect(() =>
    decodeNestedColumnPage(orphan.bytes, orphan.header, decoder, undefined, scope),
  ).toThrow();
  expect(scope.budget.usedBytes).toBe(0);
  const binary = page([0], [2], [0]); // PLAIN BYTE_ARRAY length zero.
  const binaryDecoder = {
    ...decoder,
    type: "BYTE_ARRAY",
    element: {
      ...decoder.element,
      type: "BYTE_ARRAY",
      converted_type: "INTERVAL",
    },
  } as ColumnDecoder;
  expect(
    decodeNestedColumnPage(binary.bytes, binary.header, binaryDecoder, undefined, scope),
  ).toBeUndefined();
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("admits compact pages without allocating unused nullable leaf-slot expansion", () => {
  const count = 1024;
  const input = page(Array(count).fill(0), Array(count).fill(1), []);
  const compact = new MemoryScope(new MemoryBudget(65536));
  const value = decodeCompactNestedColumnPage(
    input.bytes,
    input.header,
    decoder,
    undefined,
    compact,
  );
  expect(value).toBeDefined();
  expect(value).not.toHaveProperty("slots");
  expect(value?.rows.eventOffsets.length).toBe(count + 1);
  const compactPeak = compact.budget.peakBytes;
  compact.dispose();
  const expanded = new MemoryScope(new MemoryBudget(65536));
  const full = decodeNestedColumnPage(input.bytes, input.header, decoder, undefined, expanded);
  if (!full) throw Error("Missing expanded page");
  expect(vectorValue(full.slots, count - 1)).toBeNull();
  expect(expanded.budget.peakBytes - compactPeak).toBeGreaterThanOrEqual(count * 5);
  expanded.dispose();
  for (const limit of [compactPeak, compactPeak - 1]) {
    const scope = new MemoryScope(new MemoryBudget(limit));
    if (limit === compactPeak)
      expect(
        decodeCompactNestedColumnPage(input.bytes, input.header, decoder, undefined, scope),
      ).toBeDefined();
    else {
      expect(() =>
        decodeCompactNestedColumnPage(input.bytes, input.header, decoder, undefined, scope),
      ).toThrow(/budget/);
      expect(scope.budget.usedBytes).toBe(0);
    }
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});
