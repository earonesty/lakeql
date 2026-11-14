import { convert } from "hyparquet/src/convert.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import { decodeVariantColumn } from "hyparquet/src/variant.js";
import { DecimalValue, MemoryBudget, MemoryScope, timestampFromEpoch } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { normalizeDecodedRows } from "./decoded-rows.js";
import { lakeqlParquetParsers, parquetParsersForMetadata } from "./parsers.js";

const metadata = new Uint8Array([1, 0, 0]);

it.each([
  "logical",
  "converted",
])("converts nullable shredded decimal %s annotations", (annotation) => {
  const element = {
    name: "typed_value",
    type: "BYTE_ARRAY" as const,
    ...(annotation === "logical"
      ? { logical_type: { type: "DECIMAL" as const, precision: 38, scale: 9 } }
      : { converted_type: "DECIMAL" as const, precision: 38, scale: 9 }),
  };
  const result = convert(
    [null, Uint8Array.of(1, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef), undefined],
    {
      element,
      parsers: lakeqlParquetParsers,
      schemaPath: [{ element: { name: "var", logical_type: { type: "VARIANT" } } }],
    } as ColumnDecoder,
  );
  expect(result).toEqual([null, new DecimalValue(0x123456789abcdefn, 9), undefined]);
});

it("preserves decoded scalar classes in shredded wrappers and arrays", () => {
  for (const value of [
    new DecimalValue(123456789987654321n, 9),
    timestampFromEpoch(123n, "nanos"),
  ]) {
    expect(decodeVariantColumn({ metadata, typed_value: value }, lakeqlParquetParsers)).toBe(value);
    expect(
      decodeVariantColumn(
        { metadata, typed_value: [{ typed_value: value }] },
        lakeqlParquetParsers,
      ),
    ).toEqual([value]);
  }
});

it("bounds signed shredded byte conversion independently of sign extension", () => {
  for (const coefficient of [0n, 127n, 128n, -128n, -129n, (1n << 127n) - 1n, -(1n << 127n)]) {
    for (const length of [16, 65536]) {
      const bytes = new Uint8Array(length).fill(coefficient < 0n ? 255 : 0);
      for (let i = 0; i < 16; i++)
        bytes[length - 1 - i] = Number((coefficient >> BigInt(i * 8)) & 255n);
      const scope = new MemoryScope(new MemoryBudget(4096));
      const parsers = parquetParsersForMetadata({ key_value_metadata: [] }, scope);
      const actual = parsers.variantDecimal(bytes, 9);
      expect(actual).toEqual(parsers.decimalFromUnscaled(coefficient, 9));
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
    }
  }
});

it("admits shredded coefficient scratch and releases it on success or rejection", () => {
  const bytes = Uint8Array.of(1, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef);
  const run = (limit: number) => {
    const scope = new MemoryScope(new MemoryBudget(limit));
    const parsers = parquetParsersForMetadata({ key_value_metadata: [] }, scope);
    return { scope, parsers };
  };
  const { scope, parsers } = run(4096);
  const table = scope.budget.usedBytes;
  expect(parsers.variantDecimal(bytes, 9)).toBeInstanceOf(DecimalValue);
  const peak = scope.budget.peakBytes;
  expect(scope.budget.usedBytes).toBeLessThan(peak);
  scope.dispose();
  const exact = run(peak);
  expect(exact.parsers.variantDecimal(bytes, 9)).toBeInstanceOf(DecimalValue);
  exact.scope.dispose();
  const tight = run(peak - 1);
  expect(() => tight.parsers.variantDecimal(bytes, 9)).toThrow(/budget/i);
  expect(tight.scope.budget.usedBytes).toBe(table);
  for (const value of [new Uint8Array(), new Uint8Array(17).fill(127), NaN, 1.5])
    expect(() => tight.parsers.variantDecimal(value, 9)).toThrow(/Invalid variant decimal/);
  expect(() => tight.parsers.variantDecimal(bytes, 39)).toThrow(/scale/);
  expect(tight.scope.budget.usedBytes).toBe(table);
  tight.scope.dispose();
});
function encoded(coefficient: bigint, scale: number, width: 4 | 8 | 16): Uint8Array {
  const output = new Uint8Array(2 + width);
  output[0] = (width === 4 ? 8 : width === 8 ? 9 : 10) << 2;
  output[1] = scale;
  for (let i = 0; i < width; i++) output[2 + i] = Number((coefficient >> BigInt(i * 8)) & 255n);
  return output;
}

it.each([4, 8, 16] as const)("decodes signed %i-byte decimal boundaries and scales", (width) => {
  const high = (1n << BigInt(width * 8 - 1)) - 1n;
  for (const coefficient of [0n, 1n, -1n, high, -high, -high - 1n])
    for (const scale of [0, 9, 23, 255]) {
      const result = decodeVariantColumn(
        { metadata, value: encoded(coefficient, scale, width) },
        lakeqlParquetParsers,
      );
      if (Number.isSafeInteger(Number(coefficient))) {
        expect(typeof result).toBe("number");
        expect(result).toBe(Number(`${coefficient}e-${scale}`));
      } else {
        expect(result).toBeInstanceOf(DecimalValue);
        expect(result.coefficient).toBe(coefficient);
        expect(result.scale).toBe(scale);
      }
    }
});

it.each([
  9007199254740991n,
  9007199254740992n,
  -9007199254740991n,
  -9007199254740992n,
])("selects exact representation at coefficient boundary %s", (coefficient) => {
  const result = decodeVariantColumn(
    { metadata, value: encoded(coefficient, 9, 8) },
    lakeqlParquetParsers,
  );
  expect(result instanceof DecimalValue).toBe(!Number.isSafeInteger(Number(coefficient)));
});

it("preserves nested exact values through public row normalization", () => {
  const value = encoded(123456789987654321n, 9, 8);
  const decimal = decodeVariantColumn({ metadata, value }, lakeqlParquetParsers);
  const nested = decodeVariantColumn(
    { metadata, typed_value: [{ value }, { typed_value: null }] },
    lakeqlParquetParsers,
  );
  expect(String(decimal)).toBe("123456789.987654321");
  const rows = normalizeDecodedRows([{ decimal, nested }]);
  expect(rows[0]?.decimal).toBe(decimal);
  expect(rows[0]?.nested).toEqual([decimal, null]);
  expect((rows[0]?.nested as unknown[])[0]).toBeInstanceOf(DecimalValue);
});

it.each([
  4, 8, 16,
] as const)("rejects truncated %i-byte carriers before invoking the parser", (width) => {
  const parse = vi.fn(lakeqlParquetParsers.decimalFromUnscaled);
  const value = encoded(1n, 0, width).subarray(0, width + 1);
  expect(() =>
    decodeVariantColumn(
      { metadata, value },
      { ...lakeqlParquetParsers, decimalFromUnscaled: parse },
    ),
  ).toThrow();
  expect(parse).not.toHaveBeenCalled();
});

it("admits variant parser storage and exact scalars before construction", () => {
  const metadata = {
    key_value_metadata: [],
    schema: [{ name: "var", logical_type: { type: "VARIANT" as const } }],
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const parsers = parquetParsersForMetadata(metadata, scope);
  const tableBytes = scope.budget.usedBytes;
  expect(tableBytes).toBeGreaterThan(0);
  expect(parsers.decimalFromUnscaled(1n, 255)).toBe(1e-255);
  expect(scope.budget.usedBytes).toBe(tableBytes);
  const value = parsers.decimalFromUnscaled(123456789987654321n, 9);
  expect(String(value)).toBe("123456789.987654321");
  const total = scope.budget.usedBytes;
  expect(total).toBeGreaterThan(tableBytes);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  expect(() => parsers.decimalFromUnscaled(123456789987654321n, 9)).toThrow();

  const exact = new MemoryScope(new MemoryBudget(total));
  const admitted = parquetParsersForMetadata(metadata, exact);
  expect(String(admitted.decimalFromUnscaled(123456789987654321n, 9))).toBe("123456789.987654321");
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(total - 1));
  const rejected = parquetParsersForMetadata(metadata, tight);
  expect(() => rejected.decimalFromUnscaled(123456789987654321n, 9)).toThrow(/budget/i);
  expect(tight.budget.usedBytes).toBe(tableBytes);
  tight.dispose();
  expect(tight.budget.usedBytes).toBe(0);
});

it("does not allocate a variant parser table for non-variant schemas", () => {
  const scope = new MemoryScope(new MemoryBudget(0));
  expect(
    parquetParsersForMetadata(
      { key_value_metadata: [], schema: [{ name: "value", type: "INT32" }] },
      scope,
    ),
  ).toBe(lakeqlParquetParsers);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
