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

it("admits traversal growth and releases it on success, rejection, and budget failure", async () => {
  const { MemoryBudget, MemoryScope } = await import("lakeql-core");
  const schema = deepSchema(128);
  const pool = new MemoryBudget(65536);
  const sibling = new MemoryScope(pool);
  sibling.allocate(7, "independent sibling", () => new Uint8Array(7));
  rejectUnsupportedParquetSchema({ schema }, { memoryBudget: pool, columns: ["g0"] });
  expect(pool.usedBytes).toBe(7);
  const peak = pool.peakBytes;
  expect(peak).toBeGreaterThan(7);
  const exact = new MemoryBudget(peak - 7);
  rejectUnsupportedParquetSchema({ schema }, { memoryBudget: exact, columns: ["g0"] });
  expect(exact.usedBytes).toBe(0);
  const tight = new MemoryBudget(peak - 8);
  expect(() =>
    rejectUnsupportedParquetSchema({ schema }, { memoryBudget: tight, columns: ["g0"] }),
  ).toThrowError(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
  expect(tight.usedBytes).toBe(0);
  expect(() =>
    rejectUnsupportedParquetSchema({ schema: deepSchema(128, 20) }, { memoryBudget: pool }),
  ).toThrowError(expect.objectContaining({ code: "LAKEQL_UNSUPPORTED_PARQUET_FEATURE" }));
  expect(pool.usedBytes).toBe(7);
  sibling.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("bounds traversal workspace by depth rather than schema width", async () => {
  const { MemoryBudget } = await import("lakeql-core");
  const peaks = [1, 10000].map((width) => {
    const schema: SchemaElement[] = [
      { name: "root", num_children: width },
      ...Array.from({ length: width }, (_, index) => ({
        name: `c${index}`,
        type: "INT32" as const,
      })),
    ];
    const pool = new MemoryBudget(1024);
    rejectUnsupportedParquetSchema({ schema }, { memoryBudget: pool });
    expect(pool.usedBytes).toBe(0);
    return pool.peakBytes;
  });
  // The minimum capacity is capped by schema size; large widths stop at 16 slots.
  expect(peaks[1]).toBeLessThanOrEqual(1024);
  expect(peaks[1]).toBeGreaterThanOrEqual(peaks[0] ?? 0);
});

it("rejects selection allocation before enumerating its elements", async () => {
  const { MemoryBudget } = await import("lakeql-core");
  let reads = 0;
  const columns = ["g0"];
  Object.defineProperty(columns, "0", {
    get() {
      reads++;
      return "g0";
    },
  });
  expect(() =>
    rejectUnsupportedParquetSchema(
      { schema: deepSchema(1) },
      {
        columns,
        memoryBudget: new MemoryBudget(0),
      },
    ),
  ).toThrowError(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
  expect(reads).toBe(0);
});
