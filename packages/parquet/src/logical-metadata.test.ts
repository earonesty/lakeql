import type { ThriftObject } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES, MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { convertLogicalMetadata } from "./logical-metadata.js";

it.each([
  [{ field_1: {} }, { type: "STRING" }],
  [{ field_5: { field_1: 2, field_2: 9 } }, { type: "DECIMAL", scale: 2, precision: 9 }],
  [
    { field_8: { field_1: true, field_2: { field_3: {} } } },
    { type: "TIMESTAMP", isAdjustedToUTC: true, unit: "NANOS" },
  ],
  [
    { field_10: { field_1: 32, field_2: false } },
    { type: "INTEGER", bitWidth: 32, isSigned: false },
  ],
  [{ field_16: { field_1: 1 } }, { type: "VARIANT", specification_version: 1 }],
] as [ThriftObject, object][])("admits logical shape %# before construction", (raw, expected) => {
  const cost = MATERIALIZATION_CHARGES;
  const bytes = cost.object + Object.keys(expected).length * cost.property;
  const scope = new MemoryScope(new MemoryBudget(bytes));
  try {
    expect(convertLogicalMetadata(raw, scope)).toEqual(expected);
    expect(scope.budget.usedBytes).toBe(bytes);
  } finally {
    scope.dispose();
  }
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  try {
    expect(() => convertLogicalMetadata(raw, tight)).toThrow(/budget/);
    expect(tight.budget.usedBytes).toBe(0);
  } finally {
    tight.dispose();
  }
});

it("preserves unknown raw logical metadata without creating an extra object", () => {
  const raw = { field_99: { field_1: 1 } };
  const scope = new MemoryScope(new MemoryBudget(0));
  expect(convertLogicalMetadata(raw, scope)).toBe(raw);
  expect(convertLogicalMetadata(undefined, scope)).toBeUndefined();
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
