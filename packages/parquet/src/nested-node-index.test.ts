import { getSchemaPath } from "hyparquet/src/schema.js";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { indexNestedNode } from "./nested-node-index.js";

const path = getSchemaPath(
  [
    { name: "root", num_children: 1 },
    { name: "list", repetition_type: "OPTIONAL", num_children: 1 },
    { name: "items", repetition_type: "REPEATED", num_children: 1 },
    { name: "value", repetition_type: "OPTIONAL", type: "INT32" },
  ],
  ["list", "items", "value"],
);
const levels = {
  repetition: Uint8Array.of(0, 0, 0, 1, 0),
  definition: Uint8Array.of(0, 1, 3, 2, 3),
  rowsStarted: 4,
  presentCount: 2,
  valueOffset: 0,
};
it.each([
  { depth: 1, offsets: [0, 1, 2, 4, 5], valid: [0, 1, 1, 1] },
  { depth: 2, offsets: [2, 3, 4, 5], valid: [1, 1, 1] },
  { depth: 3, offsets: [2, 3, 4, 5], valid: [1, 0, 1] },
])("indexes node depth $depth with null parents, empty lists and null elements", ({
  depth,
  offsets,
  valid,
}) => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  const index = indexNestedNode(levels, path, depth, scope);
  expect(Array.from(index.eventOffsets)).toEqual(offsets);
  expect(Array.from(index.valid)).toEqual(valid);
  expect(index.continuesPrevious).toBe(false);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  indexNestedNode(levels, path, depth, exact);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => indexNestedNode(levels, path, depth, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("marks continued parent occurrences without treating the next repeated element as continued", () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  const continued = {
    ...levels,
    repetition: Uint8Array.of(1),
    definition: Uint8Array.of(3),
    rowsStarted: 0,
    presentCount: 1,
  };
  expect(indexNestedNode(continued, path, 1, scope).continuesPrevious).toBe(true);
  expect(indexNestedNode(continued, path, 2, scope).continuesPrevious).toBe(false);
  scope.dispose();
});

it("assembles a list dimension while preserving null, empty and null-child distinctions", async () => {
  const { nestedListVector } = await import("./nested-list-vector.js");
  const { vectorFromValues, vectorValue } = await import("lakeql-core");
  const inputScope = new MemoryScope(new MemoryBudget(4096));
  const parent = indexNestedNode(levels, path, 1, inputScope);
  const child = indexNestedNode(levels, path, 2, inputScope);
  const values = vectorFromValues([7, null, 9], inputScope);
  const scope = new MemoryScope(new MemoryBudget(4096));
  const list = nestedListVector(parent, child, values, scope);
  expect([0, 1, 2, 3].map((i) => vectorValue(list, i))).toEqual([null, [], [7, null], [9]]);
  if (list.type !== "list") throw Error("Expected list");
  expect(list.child).toBe(values);
  expect(list.valid).toBe(parent.valid);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  nestedListVector(parent, child, values, exact);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => nestedListVector(parent, child, values, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  const invalid = new MemoryScope(new MemoryBudget(4096));
  parent.valid[2] = 0;
  expect(() => nestedListVector(parent, child, values, invalid)).toThrow(/Unaligned/);
  expect(invalid.budget.usedBytes).toBe(0);
  invalid.dispose();
  inputScope.dispose();
});

it("composes repeated dimensions without materializing intermediate row arrays", async () => {
  const { nestedListVector } = await import("./nested-list-vector.js");
  const { nestedLeafSlots } = await import("./nested-leaf-slots.js");
  const { validateNestedTransitions } = await import("./nested-transitions.js");
  const { vectorFromValues, vectorValue } = await import("lakeql-core");
  const matrixPath = getSchemaPath(
    [
      { name: "root", num_children: 1 },
      { name: "outer", repetition_type: "OPTIONAL", num_children: 1 },
      { name: "items", repetition_type: "REPEATED", num_children: 1 },
      { name: "inner", repetition_type: "OPTIONAL", num_children: 1 },
      { name: "elements", repetition_type: "REPEATED", num_children: 1 },
      { name: "value", repetition_type: "OPTIONAL", type: "INT32" },
    ],
    ["outer", "items", "inner", "elements", "value"],
  );
  const matrixLevels = {
    repetition: Uint8Array.of(0, 0, 0, 2, 1, 1, 1, 0),
    definition: Uint8Array.of(0, 1, 5, 4, 2, 3, 5, 5),
    rowsStarted: 4,
    presentCount: 3,
    valueOffset: 0,
  };
  const scope = new MemoryScope(new MemoryBudget(16384));
  validateNestedTransitions(matrixLevels, matrixPath, undefined, scope);
  const compact = vectorFromValues([1, 2, 3], scope);
  const slots = nestedLeafSlots(matrixLevels, matrixPath, compact, scope);
  const inner = nestedListVector(
    indexNestedNode(matrixLevels, matrixPath, 3, scope),
    indexNestedNode(matrixLevels, matrixPath, 4, scope),
    slots,
    scope,
  );
  const outer = nestedListVector(
    indexNestedNode(matrixLevels, matrixPath, 1, scope),
    indexNestedNode(matrixLevels, matrixPath, 2, scope),
    inner,
    scope,
  );
  expect([0, 1, 2, 3].map((i) => vectorValue(outer, i))).toEqual([
    null,
    [],
    [[1, null], null, [], [2]],
    [[3]],
  ]);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("retains only output validity after index workspace is disposed", () => {
  const budget = new MemoryBudget(65536);
  const workspace = new MemoryScope(budget);
  const output = new MemoryScope(budget);
  const index = indexNestedNode(levels, path, 1, workspace, output);
  const both = budget.usedBytes;
  workspace.dispose();
  expect(Array.from(index.valid)).toEqual([0, 1, 1, 1]);
  expect(budget.usedBytes).toBeGreaterThan(0);
  expect(budget.usedBytes).toBeLessThan(both);
  output.dispose();
  expect(budget.usedBytes).toBe(0);
});
