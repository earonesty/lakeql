import { expect, it } from "vitest";
import { supportsNestedValueEncoding } from "./nested-value-capability.js";

it("declares the complete supported physical type/encoding matrix", () => {
  const types = [
    "BOOLEAN",
    "INT32",
    "INT64",
    "INT96",
    "FLOAT",
    "DOUBLE",
    "BYTE_ARRAY",
    "FIXED_LEN_BYTE_ARRAY",
  ] as const;
  for (const type of types) {
    expect(supportsNestedValueEncoding(type, "PLAIN")).toBe(true);
    for (const encoding of ["PLAIN_DICTIONARY", "RLE_DICTIONARY"] as const)
      expect(supportsNestedValueEncoding(type, encoding)).toBe(type !== "BOOLEAN");
    expect(supportsNestedValueEncoding(type, "DELTA_BINARY_PACKED")).toBe(
      type === "INT32" || type === "INT64",
    );
    for (const encoding of ["DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY"] as const)
      expect(supportsNestedValueEncoding(type, encoding)).toBe(type === "BYTE_ARRAY");
    expect(supportsNestedValueEncoding(type, "BYTE_STREAM_SPLIT")).toBe(
      ["INT32", "INT64", "FLOAT", "DOUBLE"].includes(type),
    );
    for (const encoding of ["RLE", "BIT_PACKED", undefined] as const)
      expect(supportsNestedValueEncoding(type, encoding)).toBe(false);
  }
});
