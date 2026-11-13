import { expect, it } from "vitest";
import { vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { buildUnionChildren, type UnionConstruction } from "./union-construction.js";

function materialize(result: UnionConstruction): unknown[] {
  return Array.from(result.routing.tags, (tag, row) => {
    const child = result.children[tag];
    if (child === undefined) throw Error("Missing child");
    return vectorValue(child, result.routing.offsets[row] ?? 0);
  });
}

it("constructs compact scalar/list/struct/null children with atomic budget ownership", () => {
  const values = [{ a: 1 }, [1, null, 3], null, { a: 2, b: null }, "text", [4]];
  const tags = [0, 1, 2, 0, 3, 1];
  const source = { length: values.length, get: (row: number) => values[row] };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const result = buildUnionChildren(source, 5, (row) => tags[row] ?? 0, scope);
  expect(materialize(result)).toEqual([
    { a: 1, b: null },
    [1, null, 3],
    null,
    { a: 2, b: null },
    "text",
    [4],
  ]);
  expect(result.children.map((child) => child.type)).toEqual([
    "struct",
    "list",
    "null",
    "utf8",
    "null",
  ]);
  const peak = scope.budget.peakBytes;
  const retained = scope.retainAllocations(result);
  scope.dispose();
  expect(scope.budget.usedBytes).toBeGreaterThan(0);
  expect(materialize(retained.value)).toEqual(materialize(result));
  retained.release();
  expect(scope.budget.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(materialize(buildUnionChildren(source, 5, (row) => tags[row] ?? 0, exact))).toEqual(
    materialize(result),
  );
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => buildUnionChildren(source, 5, (row) => tags[row] ?? 0, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("rolls back routing and earlier children when a later child rejects its domain", () => {
  const values = ["valid", 3, Symbol("invalid")];
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(() =>
    buildUnionChildren(
      { length: 3, get: (row) => values[row] },
      2,
      (row) => (row === 0 ? 0 : 1),
      scope,
    ),
  ).toThrow(/Unsupported vector/);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("uses grouped child reads with work independent of unused domain count", () => {
  const length = 1024;
  let reads = 0;
  let classifications = 0;
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  const result = buildUnionChildren(
    {
      length,
      get: (row) => {
        reads++;
        return row;
      },
    },
    256,
    (row) => {
      classifications++;
      return row % 2;
    },
    scope,
  );
  expect(classifications).toBe(length);
  expect(reads).toBe(2 * length); // Numeric inference and typed-buffer fill.
  expect(materialize(result)).toEqual(Array.from({ length }, (_, row) => row));
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
