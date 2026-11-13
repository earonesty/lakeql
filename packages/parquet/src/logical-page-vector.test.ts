import { MemoryBudget, MemoryScope, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { logicalPageVector } from "./logical-page-vector.js";

it.each([
  false,
  true,
])("expands mixed logical values across nulls and repeated inference passes, dictionary=%s", (dictionary) => {
  const values = ["first", [1, false], { nested: [null, "x", 2] }];
  const levels = Uint8Array.of(0, 1, 0, 1, 1, 0);
  const scope = new MemoryScope(new MemoryBudget(65536));
  const input = dictionary ? Uint32Array.of(2, 0, 1) : values;
  const result = logicalPageVector(
    input,
    levels,
    levels.length,
    dictionary ? values : undefined,
    scope,
  );
  const ordered = dictionary ? [values[2], values[0], values[1]] : values;
  expect(Array.from({ length: 6 }, (_, row) => vectorValue(result, row))).toEqual([
    null,
    ordered[0],
    null,
    ordered[1],
    ordered[2],
    null,
  ]);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects malformed logical page counts and dictionary indices", () => {
  for (const run of [
    () => logicalPageVector([1], undefined, 2, undefined),
    () => logicalPageVector([1], Uint8Array.of(2), 1, undefined),
    () => logicalPageVector([1], Uint8Array.of(1, 0), 1, undefined),
    () => logicalPageVector(Uint32Array.of(2), undefined, 1, ["only"]),
  ])
    expect(run).toThrow();
});
