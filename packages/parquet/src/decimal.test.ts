import type { SchemaElement } from "hyparquet";
import { MATERIALIZATION_CHARGES as c, MemoryBudget, MemoryScope } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { convertDecimalValue, convertDecimalValues, isIntegerDecimal } from "./decimal.js";

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
      convertDecimalValues(new BigInt64Array([101234n, 25402019508n]), navDecimal, "nav"),
    ).toEqual([10.1234, 2540201.9508]);
  });

  it("rejects values whose unscaled integer cannot be represented exactly", () => {
    expect(() =>
      convertDecimalValues(new BigInt64Array([9007199254740992n]), navDecimal, "nav"),
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
    expect(convertDecimalValues([1234, -567], logicalDecimal, "amount")).toEqual([12.34, -5.67]);
    expect(isIntegerDecimal({ name: "id", type: "INT64" })).toBe(false);
  });

  it("rejects non-integer decoded values", () => {
    expect(() => convertDecimalValues([1.5], navDecimal, "nav")).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
        details: expect.objectContaining({ value: 1.5 }),
      }),
    );
  });
});

it("charges dense decimal output before conversion and releases failed allocations", () => {
  const input = BigInt64Array.of(-1234n, 0n, 9007199254740991n);
  const expected = convertDecimalValues(input, navDecimal, "nav");
  const budget = new MemoryBudget(24);
  const scope = new MemoryScope(budget);
  scope.admit(24, "decimal conversion");
  expect(convertDecimalValues(input, navDecimal, "nav", scope)).toEqual(
    Float64Array.from(expected as number[]),
  );
  expect(budget.usedBytes).toBe(24);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(23));
  expect(() => convertDecimalValues(input, navDecimal, "nav", tight)).toThrow(/budget/);
  expect(tight.budget.peakBytes).toBe(0);
  tight.dispose();
  const invalid = new MemoryScope(new MemoryBudget(8));
  expect(() =>
    convertDecimalValues(BigInt64Array.of(9007199254740992n), navDecimal, "nav", invalid),
  ).toThrow(/precision|safely|represent/i);
  expect(invalid.budget.usedBytes).toBe(0);
  invalid.dispose();
});

it("rounds exact unscaled integers once across decimal scales", () => {
  for (const scale of [0, 1, 3, 4, 22, 23, 100, 308, 324, Number.MAX_SAFE_INTEGER]) {
    const element = { ...navDecimal, scale };
    const integers = [0, 1, -1, 1003858, -1003858, Number.MAX_SAFE_INTEGER];
    const expected = integers.map((value) => Number(`${value}e-${scale}`));
    expect(convertDecimalValues(integers, element, "nav")).toEqual(expected);
    expect(convertDecimalValues(BigInt64Array.from(integers.map(BigInt)), element, "nav")).toEqual(
      expected,
    );
  }
  expect(convertDecimalValues([1003858], { ...navDecimal, scale: 3 }, "nav")).toEqual([1003.858]);
});

it("rejects malformed scales before allocating decimal output", () => {
  for (const scale of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    const scope = new MemoryScope(new MemoryBudget(8));
    expect(() =>
      convertDecimalValues(BigInt64Array.of(1n), { ...navDecimal, scale }, "nav", scope),
    ).toThrow(/scale/);
    expect(scope.budget.peakBytes).toBe(0);
    scope.dispose();
  }
});

it("converts signed byte decimals without input-sized integer allocation", () => {
  for (const type of ["BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"] as const) {
    const element = { ...navDecimal, type, scale: 3 };
    for (const value of [0n, 1n, -1n, 1003858n, -1003858n, 9007199254740991n, -9007199254740991n]) {
      const bytes = new Uint8Array(32);
      let remaining = value;
      for (let i = bytes.length - 1; i >= 0; i--) {
        bytes[i] = Number(remaining & 255n);
        remaining >>= 8n;
      }
      expect(convertDecimalValues([bytes], element, "amount")).toEqual([Number(`${value}e-3`)]);
    }
    expect(convertDecimalValues([null, undefined], element, "amount")).toEqual([null, undefined]);
    for (const bytes of [new Uint8Array(), Uint8Array.of(0x20, 0, 0, 0, 0, 0, 0)]) {
      expect(() => convertDecimalValues([bytes], element, "amount")).toThrow(/precision/);
    }
  }
});

it("admits byte decimal output before conversion at exact capacity", () => {
  const values = [Uint8Array.of(1), Uint8Array.of(255)];
  const element = { ...navDecimal, type: "BYTE_ARRAY" as const, scale: 0 };
  const scope = new MemoryScope(new MemoryBudget(16));
  expect(convertDecimalValues(values, element, "amount", scope)).toEqual(Float64Array.of(1, -1));
  expect(scope.budget.usedBytes).toBe(16);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(15));
  expect(() => convertDecimalValues(values, element, "amount", tight)).toThrow(/budget/);
  expect(tight.budget.peakBytes).toBe(0);
  tight.dispose();
});

it("normalizes selected top-level decimals once and preserves no-op identity", async () => {
  const { rawDecimalMetadata, normalizeDecimalColumns, normalizeDecimalRows } = await import(
    "./decimal.js"
  );
  const metadata = {
    version: 1,
    num_rows: 1n,
    row_groups: [],
    schema: [
      { name: "root", num_children: 3 },
      { name: "nested", num_children: 1 },
      { ...navDecimal, name: "amount" },
      { ...navDecimal, name: "amount", scale: 2 },
      { name: "id", type: "INT32" as const },
    ],
  };
  const values = { amount: [1234], id: [7] };
  const rows = [{ amount: 1234, id: 7 }];
  expect(rawDecimalMetadata(metadata, ["id"])).toBe(metadata);
  expect(normalizeDecimalColumns(metadata, ["id"], values)).toBe(values);
  expect(normalizeDecimalRows(metadata, ["id"], rows)).toBe(rows);
  expect(normalizeDecimalColumns(metadata, ["amount", "amount"], values)).toEqual({
    amount: [12.34],
    id: [7],
  });
  expect(normalizeDecimalRows(metadata, ["amount", "amount"], rows)).toEqual([
    { amount: 12.34, id: 7 },
  ]);
  expect(values.amount).toEqual([1234]);
  const raw = rawDecimalMetadata(metadata, ["amount"]);
  expect(raw.schema[2]).toBe(metadata.schema[2]);
  expect(raw.schema[3]?.converted_type).toBeUndefined();
  expect(metadata.schema[3]?.converted_type).toBe("DECIMAL");
});

it("admits scalar decimal output independently of sign-extension length", () => {
  const scope = new MemoryScope(new MemoryBudget(16));
  const bytes = new Uint8Array(65536);
  bytes[bytes.length - 1] = 12;
  try {
    expect(
      convertDecimalValue(
        bytes,
        { ...navDecimal, type: "FIXED_LEN_BYTE_ARRAY", scale: 1 },
        "nav",
        scope,
      ),
    ).toBe(1.2);
    expect(scope.budget.usedBytes).toBe(16);
  } finally {
    scope.dispose();
  }
});

it("releases bounded exponent-formatting scratch after scalar conversion", () => {
  const scope = new MemoryScope(new MemoryBudget(344));
  try {
    expect(convertDecimalValue(123n, { ...navDecimal, scale: 23 }, "nav", scope)).toBe(
      Number("123e-23"),
    );
    expect(scope.budget.peakBytes).toBe(344);
    expect(scope.budget.usedBytes).toBe(16);
  } finally {
    scope.dispose();
  }
  const tight = new MemoryScope(new MemoryBudget(343));
  try {
    expect(() => convertDecimalValue(123n, { ...navDecimal, scale: 23 }, "nav", tight)).toThrow(
      /budget/,
    );
    expect(tight.budget.peakBytes).toBe(0);
  } finally {
    tight.dispose();
  }
});

it("admits nullable decimal output and rolls back late precision failures", () => {
  const values = [1234n, null, undefined, -567n];
  const bytes = c.array + values.length * (c.arraySlot + c.number);
  const budget = new MemoryBudget(bytes);
  const scope = new MemoryScope(budget);
  expect(convertDecimalValues(values, navDecimal, "nested.nav", scope)).toEqual([
    0.1234,
    null,
    undefined,
    -0.0567,
  ]);
  expect(budget.usedBytes).toBe(bytes);
  expect(values).toEqual([1234n, null, undefined, -567n]);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);

  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => convertDecimalValues(values, navDecimal, "nested.nav", tight)).toThrow(/budget/);
  expect(tight.budget.peakBytes).toBe(0);
  tight.dispose();

  const invalid = new MemoryScope(new MemoryBudget(bytes));
  expect(() =>
    convertDecimalValues(
      [1234n, null, undefined, 9007199254740992n],
      navDecimal,
      "nested.nav",
      invalid,
    ),
  ).toThrowError(
    expect.objectContaining({
      details: expect.objectContaining({
        column: "nested.nav",
        feature: "decimal-value-precision",
      }),
    }),
  );
  expect(invalid.budget.usedBytes).toBe(0);
  invalid.dispose();
});

it.each([false, true])("admits reusable large-scale decimal scratch (nullable=%s)", (nullable) => {
  const values = nullable ? [1n, null, -1n] : [1n, 0n, -1n];
  const bytes = nullable ? c.array + values.length * (c.arraySlot + c.number) : values.length * 8;
  const scratch = 3 * c.string + (17 + 16 + 35) * c.stringCodeUnit;
  const element = { ...navDecimal, scale: 23 };
  const scope = new MemoryScope(new MemoryBudget(bytes + scratch));
  const output = convertDecimalValues(values, element, "nav", scope);
  expect(Array.from(output)).toEqual(nullable ? [1e-23, null, -1e-23] : [1e-23, 0, -1e-23]);
  expect(scope.budget.peakBytes).toBe(bytes + scratch);
  expect(scope.budget.usedBytes).toBe(bytes);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(bytes + scratch - 1));
  expect(() => convertDecimalValues(values, element, "nav", tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  const invalid = new MemoryScope(new MemoryBudget(bytes + scratch));
  expect(() =>
    convertDecimalValues(
      nullable ? [1n, null, 9007199254740992n] : [1n, 0n, 9007199254740992n],
      element,
      "nav",
      invalid,
    ),
  ).toThrow(/precision|safely|represent/i);
  expect(invalid.budget.usedBytes).toBe(0);
  invalid.dispose();
});
