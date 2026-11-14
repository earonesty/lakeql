import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import type { ColumnDecoder, PageHeader } from "hyparquet/src/types.js";
import { MemoryBudget, MemoryScope, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { decodeNestedDictionaryPage } from "./nested-dictionary-page.js";

const decoder = {
  type: "BYTE_ARRAY",
  codec: "UNCOMPRESSED",
  element: {
    name: "value",
    type: "BYTE_ARRAY",
    converted_type: "UTF8",
  },
  schemaPath: [],
  pathInSchema: ["value"],
  parsers: DEFAULT_PARSERS,
} as ColumnDecoder;
const bytes = Uint8Array.of(1, 0, 0, 0, 97, 0, 0, 0, 0);
const header = (encoding: "PLAIN" | "PLAIN_DICTIONARY"): PageHeader => ({
  type: "DICTIONARY_PAGE",
  compressed_page_size: bytes.length,
  uncompressed_page_size: bytes.length,
  dictionary_page_header: { num_values: 2, encoding },
});
it.each([
  "PLAIN",
  "PLAIN_DICTIONARY",
] as const)("admits packed %s dictionary output atomically", (encoding) => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  const result = decodeNestedDictionaryPage(bytes, header(encoding), decoder, scope);
  if (result?.type !== "utf8-view") throw Error("Expected packed dictionary");
  expect([vectorValue(result, 0), vectorValue(result, 1)]).toEqual(["a", ""]);
  expect(result.data.buffer).toBe(bytes.buffer);
  const peak = scope.budget.peakBytes;
  expect(scope.budget.usedBytes).toBeLessThan(peak);
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(decodeNestedDictionaryPage(bytes, header(encoding), decoder, exact)).toBeDefined();
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => decodeNestedDictionaryPage(bytes, header(encoding), decoder, tight)).toThrow(
    /budget/,
  );
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});
it("rolls back malformed extents and unsupported logical dictionaries", () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  expect(() =>
    decodeNestedDictionaryPage(bytes.subarray(1), header("PLAIN"), decoder, scope),
  ).toThrow(/Malformed/);
  const malformed = {
    ...header("PLAIN"),
    dictionary_page_header: { num_values: 1, encoding: "PLAIN" as const },
  };
  expect(() => decodeNestedDictionaryPage(bytes, malformed, decoder, scope)).toThrow(/Malformed/);
  expect(scope.budget.usedBytes).toBe(0);
  expect(
    decodeNestedDictionaryPage(
      bytes,
      header("PLAIN"),
      {
        ...decoder,
        element: { ...decoder.element, converted_type: "INTERVAL" },
      },
      scope,
    ),
  ).toBeUndefined();
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
