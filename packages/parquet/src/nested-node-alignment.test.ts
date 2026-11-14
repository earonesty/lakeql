import { getSchemaPath } from "hyparquet/src/schema.js";
import type { SchemaElement } from "hyparquet/src/types.js";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { validateNestedNodeAlignment } from "./nested-node-alignment.js";

function source(reps: number[], deeper = false) {
  const schema: SchemaElement[] = [
    { name: "root", num_children: 1 },
    { name: "outer", repetition_type: "REPEATED", num_children: 1 },
    { name: "inner", repetition_type: "REPEATED", num_children: 1 },
    ...(deeper ? [{ name: "extra", repetition_type: "REPEATED" as const, num_children: 1 }] : []),
    { name: "value", repetition_type: "OPTIONAL", type: "INT32" },
  ];
  return {
    schemaPath: getSchemaPath(
      schema,
      deeper ? ["outer", "inner", "extra", "value"] : ["outer", "inner", "value"],
    ),
    levels: {
      repetition: Uint8Array.from(reps),
      definition: new Uint8Array(reps.length).fill(deeper ? 4 : 3),
      rowsStarted: 1,
      presentCount: reps.length,
      valueOffset: 0,
    },
  };
}
it("rejects equal node counts with different repeated-ancestor placement", () => {
  const left = source([0, 2, 1]),
    right = source([0, 1, 2]);
  const scope = new MemoryScope(new MemoryBudget(4096));
  validateNestedNodeAlignment(left, right, 1, scope);
  expect(() => validateNestedNodeAlignment(left, right, 2, scope)).toThrow(/alignment/);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
it("accepts different descendant event counts with identical projected shape", () => {
  const left = source([0, 2, 1]),
    right = source([0, 3, 2, 1], true);
  const scope = new MemoryScope(new MemoryBudget(4096));
  validateNestedNodeAlignment(left, right, 2, scope);
  const peak = scope.budget.peakBytes;
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  validateNestedNodeAlignment(left, right, 2, exact);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => validateNestedNodeAlignment(left, right, 2, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});
