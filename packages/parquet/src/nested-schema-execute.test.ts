import { getSchemaPath } from "hyparquet/src/schema.js";
import {
  MemoryBudget,
  MemoryScope,
  vectorFromValues,
  vectorLength,
  vectorValue,
} from "lakeql-core";
import { expect, it } from "vitest";
import { executeNestedSchema } from "./nested-schema-execute.js";
import { compileNestedSchema } from "./nested-schema-plan.js";

it("executes optional structs containing lists without conflating absent, null and empty slots", () => {
  const schemaPath = getSchemaPath(
    [
      { name: "root", num_children: 1 },
      { name: "record", repetition_type: "OPTIONAL", num_children: 1 },
      { name: "items", repetition_type: "OPTIONAL", converted_type: "LIST", num_children: 1 },
      { name: "list", repetition_type: "REPEATED", num_children: 1 },
      { name: "element", repetition_type: "OPTIONAL", type: "INT32" },
    ],
    ["record", "items", "list", "element"],
  );
  const input = new MemoryScope(new MemoryBudget(65536));
  const node = schemaPath[1];
  if (!node) throw Error("Missing schema");
  const plan = compileNestedSchema(node, [schemaPath], input);
  const inputs = [
    {
      schemaPath,
      values: vectorFromValues([7, 9], input),
      levels: {
        repetition: Uint8Array.of(0, 0, 0, 0, 1, 0),
        definition: Uint8Array.of(0, 1, 2, 4, 3, 4),
        rowsStarted: 5,
        presentCount: 2,
        valueOffset: 0,
      },
    },
  ];
  const run = (scope: MemoryScope) => {
    const output = executeNestedSchema(plan, inputs, scope);
    expect(Array.from({ length: vectorLength(output) }, (_, i) => vectorValue(output, i))).toEqual([
      null,
      { items: null },
      { items: [] },
      { items: [7, null] },
      { items: [9] },
    ]);
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  try {
    run(scope);
    const peak = scope.budget.peakBytes;
    const exact = new MemoryScope(new MemoryBudget(peak));
    try {
      run(exact);
    } finally {
      exact.dispose();
    }
    const tight = new MemoryScope(new MemoryBudget(peak - 1));
    expect(() => run(tight)).toThrow(/budget/);
    expect(tight.budget.usedBytes).toBe(0);
    tight.dispose();
  } finally {
    scope.dispose();
    input.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("aligns legacy repeated fields across absent optional structs", () => {
  const schemaPath = getSchemaPath(
    [
      { name: "root", num_children: 1 },
      { name: "record", repetition_type: "OPTIONAL", num_children: 1 },
      { name: "items", repetition_type: "REPEATED", type: "INT32" },
    ],
    ["record", "items"],
  );
  const scope = new MemoryScope(new MemoryBudget(65536));
  try {
    const node = schemaPath[1];
    if (!node) throw Error("Missing schema");
    const plan = compileNestedSchema(node, [schemaPath], scope);
    const output = executeNestedSchema(
      plan,
      [
        {
          schemaPath,
          values: vectorFromValues([7, 9], scope),
          levels: {
            repetition: Uint8Array.of(0, 0, 0, 1),
            definition: Uint8Array.of(0, 1, 2, 2),
            rowsStarted: 3,
            presentCount: 2,
            valueOffset: 0,
          },
        },
      ],
      scope,
    );
    expect(Array.from({ length: vectorLength(output) }, (_, i) => vectorValue(output, i))).toEqual([
      null,
      { items: [] },
      { items: [7, 9] },
    ]);
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("aligns nullable sibling leaves and atomically rejects conflicting parent validity", () => {
  const schema = [
    { name: "root", num_children: 1 },
    { name: "record", repetition_type: "OPTIONAL" as const, num_children: 2 },
    { name: "a", repetition_type: "OPTIONAL" as const, type: "INT32" as const },
    { name: "b", repetition_type: "OPTIONAL" as const, type: "INT32" as const },
  ];
  const paths = [getSchemaPath(schema, ["record", "a"]), getSchemaPath(schema, ["record", "b"])];
  const input = new MemoryScope(new MemoryBudget(65536));
  const output = new MemoryScope(new MemoryBudget(65536));
  try {
    const node = paths[0]?.[1];
    if (!node) throw Error("Missing node");
    const plan = compileNestedSchema(node, paths, input);
    const inputs = paths.map((schemaPath, i) => ({
      schemaPath,
      values: vectorFromValues(i === 0 ? [7, 9] : [8, 10], input),
      levels: {
        repetition: undefined,
        definition: Uint8Array.from(i === 0 ? [0, 1, 2, 2] : [0, 2, 1, 2]),
        rowsStarted: 4,
        presentCount: 2,
        valueOffset: 0,
      },
    }));
    const vector = executeNestedSchema(plan, inputs, output);
    expect(Array.from({ length: vectorLength(vector) }, (_, i) => vectorValue(vector, i))).toEqual([
      null,
      { a: null, b: 8 },
      { a: 7, b: null },
      { a: 9, b: 10 },
    ]);
    const failed = new MemoryScope(new MemoryBudget(65536));
    const sibling = inputs[1];
    if (!sibling) throw Error("Missing sibling");
    sibling.levels.definition[2] = 0;
    expect(() => executeNestedSchema(plan, inputs, failed)).toThrow(
      /alignment|align|shape|validity/i,
    );
    expect(failed.budget.usedBytes).toBe(0);
    failed.dispose();
    expect(vectorValue(vector, 3)).toEqual({ a: 9, b: 10 });
  } finally {
    output.dispose();
    input.dispose();
  }
  expect(output.budget.usedBytes).toBe(0);
});
