import { getSchemaPath } from "hyparquet/src/schema.js";
import { MATERIALIZATION_CHARGES as c, MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { validateNestedTransitions } from "./nested-transitions.js";

const path = getSchemaPath(
  [
    { name: "root", num_children: 1 },
    { name: "outer", repetition_type: "OPTIONAL", num_children: 1 },
    { name: "items", repetition_type: "REPEATED", num_children: 1 },
    { name: "inner", repetition_type: "REPEATED", num_children: 1 },
    { name: "value", repetition_type: "OPTIONAL", type: "INT32" },
  ],
  ["outer", "items", "inner", "value"],
);
const levels = (reps: number[], defs: number[]) => ({
  repetition: Uint8Array.from(reps),
  definition: Uint8Array.from(defs),
  rowsStarted: reps.filter((value) => value === 0).length,
  presentCount: defs.filter((value) => value === 4).length,
  valueOffset: 0,
});

it("preserves valid transitions at every page split including empty pages", () => {
  const reps = [0, 2, 1, 0, 0, 1];
  const defs = [4, 3, 2, 0, 4, 4];
  for (let split = 0; split <= reps.length; split++) {
    const scope = new MemoryScope(new MemoryBudget(8 + c.view));
    let previous = validateNestedTransitions(
      levels(reps.slice(0, split), defs.slice(0, split)),
      path,
      undefined,
      scope,
    );
    previous = validateNestedTransitions(levels([], []), path, previous, scope);
    expect(
      validateNestedTransitions(
        levels(reps.slice(split), defs.slice(split)),
        path,
        previous,
        scope,
      ),
    ).toBe(4);
    expect(scope.budget.usedBytes).toBe(0);
    expect(scope.budget.peakBytes).toBe(8 + c.view);
    scope.dispose();
  }
});

it.each([
  { reps: [1], defs: [4], previous: undefined },
  { reps: [1], defs: [4], previous: 1 },
  { reps: [2], defs: [2], previous: 4 },
  { reps: [0, 2], defs: [2, 4], previous: undefined },
  { reps: [3], defs: [4], previous: 4 },
  { reps: [0], defs: [5], previous: undefined },
])("rejects absent repeated ancestors: $reps/$defs", ({ reps, defs, previous }) => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  expect(() => validateNestedTransitions(levels(reps, defs), path, previous, scope)).toThrow(
    /transition/,
  );
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("rejects schema workspace below exact capacity and leaves no allocation", () => {
  const scope = new MemoryScope(new MemoryBudget(8 + c.view - 1));
  expect(() => validateNestedTransitions(levels([0], [4]), path, undefined, scope)).toThrow(
    /budget/,
  );
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
