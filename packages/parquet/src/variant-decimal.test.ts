import { decodeVariantColumn } from "hyparquet/src/variant.js";
import { DecimalValue, MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { normalizeDecodedRows } from "./decoded-rows.js";
import { lakeqlParquetParsers, parquetParsersForMetadata } from "./parsers.js";

const metadata = new Uint8Array([1, 0, 0]);
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
