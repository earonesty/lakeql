import { parquetSchema } from "hyparquet";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { parquetColumnDescriptors } from "./schema-descriptors.js";
import { parquetColumnShape } from "./schema-shape.js";
import { stableSchemaSignature } from "./schema-signature.js";
import { schemaShapeTree } from "./schema-tree.js";

const schema = [
  { name: "root", num_children: 2 },
  { name: "group", num_children: 1, repetition_type: "OPTIONAL" as const },
  { name: "leaf", type: "INT32" as const, repetition_type: "REQUIRED" as const },
  { name: "text", type: "BYTE_ARRAY" as const, converted_type: "UTF8" as const },
];

it("preserves descriptor signatures without copied root-to-node paths", () => {
  const expected = parquetSchema({ schema } as Parameters<typeof parquetSchema>[0]);
  const tree = schemaShapeTree(schema);
  expect(parquetColumnShape(tree)).toEqual(parquetColumnShape(expected));
  const descriptors = parquetColumnDescriptors({ schema }, "file");
  for (const child of expected.children)
    expect(descriptors.get(child.element.name)?.signature).toBe(
      stableSchemaSignature(parquetColumnShape(child)),
    );
});

it("admits descriptors plus topology/shape/serialization overlap at exact capacity", () => {
  const run = (capacity: number) => {
    const pool = new MemoryBudget(capacity);
    const scope = new MemoryScope(pool);
    scope.allocate(8, "sibling", () => undefined);
    try {
      expect(parquetColumnDescriptors({ schema }, "file", scope)).toEqual(
        parquetColumnDescriptors({ schema }, "file"),
      );
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

it.each([
  { schema: [] },
  { schema: [{ name: "root", num_children: -1 }] },
  { schema: [{ name: "root", num_children: 1 }] },
  { schema: [{ name: "root" }, { name: "extra" }] },
])("rejects malformed topology with no retained storage %#", ({ schema }) => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  expect(() => schemaShapeTree(schema, scope)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_VALIDATION_ERROR" }),
  );
  expect(pool.usedBytes).toBe(0);
  scope.dispose();
});
