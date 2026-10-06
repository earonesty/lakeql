import type { SchemaElement } from "hyparquet";
import { describe, expect, it } from "vitest";
import { convertIntegerDecimalValues, isIntegerDecimal } from "./decimal.js";

const navDecimal: SchemaElement = {
  name: "nav",
  type: "INT64",
  converted_type: "DECIMAL",
  precision: 18,
  scale: 4,
};

describe("integer decimal conversion", () => {
  it("converts safe INT64 values using their declared scale", () => {
    expect(
      convertIntegerDecimalValues(new BigInt64Array([101234n, 25402019508n]), navDecimal, "nav"),
    ).toEqual([10.1234, 2540201.9508]);
  });

  it("rejects values whose unscaled integer cannot be represented exactly", () => {
    expect(() =>
      convertIntegerDecimalValues(new BigInt64Array([9007199254740992n]), navDecimal, "nav"),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
        details: expect.objectContaining({
          column: "nav",
          feature: "decimal-value-precision",
          value: "9007199254740992",
        }),
      }),
    );
  });

  it("supports logical DECIMAL metadata and safe INT32 number values", () => {
    const logicalDecimal: SchemaElement = {
      name: "amount",
      type: "INT32",
      logical_type: { type: "DECIMAL", precision: 9, scale: 2 },
    };

    expect(isIntegerDecimal(logicalDecimal)).toBe(true);
    expect(convertIntegerDecimalValues([1234, -567], logicalDecimal, "amount")).toEqual([
      12.34, -5.67,
    ]);
    expect(isIntegerDecimal({ name: "id", type: "INT64" })).toBe(false);
  });

  it("rejects non-integer decoded values", () => {
    expect(() => convertIntegerDecimalValues([1.5], navDecimal, "nav")).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
        details: expect.objectContaining({ value: 1.5 }),
      }),
    );
  });
});
