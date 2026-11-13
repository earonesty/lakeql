import type { SchemaElement } from "hyparquet";
import { expect, it } from "vitest";
import { rejectUnsupportedParquetSchema } from "./schema.js";

function deepSchema(depth: number, precision = 9): SchemaElement[] {
  return [
    { name: "root", num_children: 1 },
    ...Array.from({ length: depth }, (_, index) => ({ name: `g${index}`, num_children: 1 })),
    { name: "amount", type: "INT64", converted_type: "DECIMAL", precision, scale: 2 },
  ];
}

it("validates deeply nested schemas without recursive path copying or call-stack exhaustion", () => {
  expect(() => rejectUnsupportedParquetSchema({ schema: deepSchema(20000) })).not.toThrow();
  expect(() =>
    rejectUnsupportedParquetSchema({ schema: deepSchema(20000, 20) }, { columns: [] }),
  ).not.toThrow();
});

it("reports the exact selected path and preserves top-level-only decimal permission", () => {
  expect(() =>
    rejectUnsupportedParquetSchema(
      { schema: deepSchema(3, 20) },
      {
        columns: ["g0"],
        allowValueCheckedScalarDecimals: true,
      },
    ),
  ).toThrowError(
    expect.objectContaining({
      code: "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
      details: { column: "g0.g1.g2.amount", feature: "decimal-precision", precision: 20 },
    }),
  );
  expect(() =>
    rejectUnsupportedParquetSchema(
      { schema: deepSchema(0, 20) },
      {
        allowValueCheckedScalarDecimals: true,
      },
    ),
  ).not.toThrow();
});

it("skips an unselected deep subtree and validates the following selected sibling", () => {
  const schema = deepSchema(20000, 20);
  schema[0] = { name: "root", num_children: 2 };
  schema.push({ name: "selected", type: "INT64", converted_type: "DECIMAL", precision: 20 });
  expect(() => rejectUnsupportedParquetSchema({ schema }, { columns: ["selected"] })).toThrowError(
    expect.objectContaining({
      details: {
        column: "selected",
        feature: "decimal-precision",
        precision: 20,
      },
    }),
  );
});

it("restores the parent path after completing a nested sibling", () => {
  const schema: SchemaElement[] = [
    { name: "root", num_children: 1 },
    { name: "group", num_children: 2 },
    { name: "nested", num_children: 1 },
    { name: "safe", type: "INT32" },
    { name: "amount", type: "INT64", converted_type: "DECIMAL", precision: 20 },
  ];
  expect(() => rejectUnsupportedParquetSchema({ schema })).toThrowError(
    expect.objectContaining({
      details: { column: "group.amount", feature: "decimal-precision", precision: 20 },
    }),
  );
});
