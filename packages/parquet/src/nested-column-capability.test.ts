import type { ColumnDecoder, ColumnMetaData } from "hyparquet/src/types.js";
import { expect, it } from "vitest";
import { canReadNestedColumn } from "./nested-column-capability.js";

const column = {
  type: "INT32",
  codec: "SNAPPY",
  encodings: ["PLAIN", "RLE", "RLE_DICTIONARY"],
} as ColumnMetaData;
const decoder = {
  type: "INT32",
  codec: "SNAPPY",
  element: { name: "v", type: "INT32" },
} as ColumnDecoder;
it("distinguishes level metadata from supported value encodings", () => {
  expect(canReadNestedColumn(column, decoder)).toBe(true);
  expect(canReadNestedColumn({ ...column, encodings: ["RLE"] }, decoder)).toBe(false);
  expect(canReadNestedColumn({ ...column, encodings: [] }, decoder)).toBe(false);
  expect(canReadNestedColumn({ ...column, encodings: ["PLAIN", "BIT_PACKED"] }, decoder)).toBe(
    true,
  );
  expect(canReadNestedColumn({ ...column, encodings: ["DELTA_BYTE_ARRAY"] }, decoder)).toBe(false);
  expect(canReadNestedColumn({ ...column, codec: "BROTLI" }, { ...decoder, codec: "BROTLI" })).toBe(
    false,
  );
  expect(canReadNestedColumn(column, { ...decoder, type: "INT64" })).toBe(false);
});
it("declines unsupported packed annotations before reading", () => {
  const binary = { ...column, type: "BYTE_ARRAY" as const };
  const leaf = {
    ...decoder,
    type: "BYTE_ARRAY" as const,
    element: { name: "v", type: "BYTE_ARRAY" as const },
  };
  expect(canReadNestedColumn(binary, leaf)).toBe(true);
  expect(
    canReadNestedColumn(binary, {
      ...leaf,
      element: { ...leaf.element, converted_type: "INTERVAL" },
    }),
  ).toBe(false);
});
