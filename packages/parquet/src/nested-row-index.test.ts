import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { indexNestedRows } from "./nested-row-index.js";

it.each([false, true])("indexes compact row spans with continuation=%s", (continuation) => {
  const levels = {
    repetition: Uint8Array.of(continuation ? 1 : 0, 2, 0, 1, 0),
    definition: Uint8Array.of(3, 2, 0, 3, 3),
    rowsStarted: continuation ? 2 : 3,
    presentCount: 3,
    valueOffset: 0,
  };
  const scope = new MemoryScope(new MemoryBudget(4096));
  const index = indexNestedRows(levels, 3, scope);
  expect(Array.from(index.eventOffsets)).toEqual([0, 2, 4, 5]);
  expect(Array.from(index.valueOffsets)).toEqual([0, 1, 2, 3]);
  expect(index.continuesPrevious).toBe(continuation);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  indexNestedRows(levels, 3, exact);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => indexNestedRows(levels, 3, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it.each([0, 3])("indexes required nonrepeated values with count=%s", (count) => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  const index = indexNestedRows(
    {
      repetition: undefined,
      definition: undefined,
      rowsStarted: count,
      presentCount: count,
      valueOffset: 0,
    },
    0,
    scope,
  );
  expect(Array.from(index.eventOffsets)).toEqual(Array.from({ length: count + 1 }, (_, i) => i));
  expect(index.valueOffsets).toEqual(index.eventOffsets);
  expect(index.continuesPrevious).toBe(false);
  scope.dispose();
});

it("rejects inconsistent physical counts before allocating", () => {
  for (const rowsStarted of [0, 2]) {
    const scope = new MemoryScope(new MemoryBudget(0));
    expect(() =>
      indexNestedRows(
        {
          repetition: Uint8Array.of(0),
          definition: Uint8Array.of(1),
          rowsStarted,
          presentCount: 1,
          valueOffset: 0,
        },
        1,
        scope,
      ),
    ).toThrow(/Invalid/);
    expect(scope.budget.peakBytes).toBe(0);
    scope.dispose();
  }
});
