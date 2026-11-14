import type { ColumnDecoder, PageHeader } from "hyparquet/src/types.js";
import { MemoryBudget, MemoryScope, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { convertBsonValues } from "./bson-conversion.js";
import { decodeNestedDictionaryPage } from "./nested-dictionary-page.js";
import { lakeqlParquetParsers } from "./parsers.js";

const doc = Uint8Array.from([12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0]);
it("rolls back a whole BSON page when a later document is invalid", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(() => convertBsonValues([doc, new Uint8Array([0])], scope)).toThrow(/BSON/);
  expect(scope.budget.usedBytes).toBe(0);
  expect(convertBsonValues([doc], scope)).toEqual(convertBsonValues([doc]));
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it.each([
  "PLAIN",
  "PLAIN_DICTIONARY",
] as const)("decodes a BSON %s logical dictionary with exact admission", (encoding) => {
  const bytes = new Uint8Array(doc.length + 4);
  new DataView(bytes.buffer).setInt32(0, doc.length, true);
  bytes.set(doc, 4);
  const header: PageHeader = {
    type: "DICTIONARY_PAGE",
    compressed_page_size: bytes.length,
    uncompressed_page_size: bytes.length,
    dictionary_page_header: { num_values: 1, encoding },
  };
  const decoder = {
    type: "BYTE_ARRAY",
    codec: "UNCOMPRESSED",
    element: { name: "document", type: "BYTE_ARRAY", converted_type: "BSON" },
    schemaPath: [],
    pathInSchema: ["document"],
    parsers: lakeqlParquetParsers,
  } as ColumnDecoder;
  const baseline = new MemoryScope(new MemoryBudget(65536));
  const vector = decodeNestedDictionaryPage(bytes, header, decoder, baseline);
  if (!vector) throw Error("Missing BSON dictionary");
  expect(vectorValue(vector, 0)).toEqual({ a: 1 });
  const peak = baseline.budget.peakBytes;
  baseline.dispose();
  for (const cap of [peak, peak - 1]) {
    const scope = new MemoryScope(new MemoryBudget(cap));
    if (cap === peak)
      expect(decodeNestedDictionaryPage(bytes, header, decoder, scope)).toBeDefined();
    else {
      expect(() => decodeNestedDictionaryPage(bytes, header, decoder, scope)).toThrow(/budget/i);
      expect(scope.budget.usedBytes).toBe(0);
    }
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});
