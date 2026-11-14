import { parquetSchema } from "hyparquet";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { parquetColumnShape } from "./schema-shape.js";

it("preserves nested shape fields and includes names only below the column root", () => {
  const tree = parquetSchema({
    schema: [
      { name: "schema", num_children: 1 },
      { name: "column", num_children: 2, repetition_type: "OPTIONAL" },
      { name: "left", type: "INT32", repetition_type: "REQUIRED" },
      { name: "right", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
    ],
  } as Parameters<typeof parquetSchema>[0]);
  const column = tree.children[0];
  if (column === undefined) throw Error("Missing column");
  const shape = parquetColumnShape(column);
  expect(Object.hasOwn(shape, "name")).toBe(false);
  expect(shape).toMatchObject({
    repetition_type: "OPTIONAL",
    children: [
      { name: "left", type: "INT32", children: [] },
      { name: "right", type: "BYTE_ARRAY", converted_type: "UTF8", children: [] },
    ],
  });
  expect(Object.keys(shape)).toEqual([
    "type",
    "converted_type",
    "logical_type",
    "repetition_type",
    "type_length",
    "precision",
    "scale",
    "children",
  ]);
});

it("builds deeply nested shapes without recursive stack overflow", () => {
  type Node = ReturnType<typeof parquetSchema>;
  let node = { element: { name: "leaf", type: "INT32" }, children: [] } as unknown as Node;
  for (let depth = 0; depth < 20000; depth++)
    node = { element: { name: "group" }, children: [node] } as unknown as Node;
  let shape = parquetColumnShape(node);
  let visited = 0;
  while ((shape.children as Record<string, unknown>[]).length > 0) {
    const child = (shape.children as Record<string, unknown>[])[0];
    if (child === undefined) throw Error("Missing child");
    shape = child;
    visited++;
  }
  expect(visited).toBe(20000);
  expect(shape).toMatchObject({ name: "leaf", type: "INT32" });
});

it("admits shape records and live traversal frames at exact capacity with rollback", () => {
  type Node = ReturnType<typeof parquetSchema>;
  const leaf = { element: { name: "leaf", type: "INT32" }, children: [] } as unknown as Node;
  const root = { element: { name: "column" }, children: [leaf, leaf] } as unknown as Node;
  const run = (capacity: number) => {
    const pool = new MemoryBudget(capacity);
    const scope = new MemoryScope(pool);
    scope.allocate(8, "sibling", () => undefined);
    try {
      expect(parquetColumnShape(root, scope)).toEqual(parquetColumnShape(root));
      expect(pool.usedBytes).toBeGreaterThan(8);
      return pool.peakBytes;
    } catch (error) {
      expect(pool.usedBytes).toBe(8);
      throw error;
    } finally {
      scope.dispose();
      expect(pool.usedBytes).toBe(0);
    }
  };
  const peak = run(65536);
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
});
