import { MemoryBudget, MemoryScope, vectorFromValues, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { type NestedStructField, nestedStructVector } from "./nested-struct-vector.js";

it("shares null-parent routing across compact fields while preserving aligned lists", () => {
  const source = new MemoryScope(new MemoryBudget(16384));
  const fields: Record<string, NestedStructField> = Object.create(null);
  fields.a = { alignment: "present-parents", values: vectorFromValues([7, 9], source) };
  fields.b = { alignment: "present-parents", values: vectorFromValues(["x", "y"], source) };
  fields.list = {
    alignment: "all-parents",
    values: vectorFromValues([[], null, [1, null]], source),
  };
  Object.defineProperty(fields, "__proto__", {
    enumerable: true,
    value: { alignment: "present-parents", values: vectorFromValues([true, false], source) },
  });
  const valid = Uint8Array.of(1, 0, 1);
  const scope = new MemoryScope(new MemoryBudget(16384));
  const result = nestedStructVector(valid, fields, scope);
  if (result.type !== "struct") throw Error("Expected struct");
  const a = result.fields.a,
    b = result.fields.b;
  if (a?.type !== "dict" || b?.type !== "dict") throw Error("Expected shared routing");
  expect(a.indices).toBe(b.indices);
  expect(a.dictionary).toBe(fields.a.values);
  expect(result.fields.list).toBe(fields.list.values);
  const expected = [
    JSON.parse('{"a":7,"b":"x","list":[],"__proto__":true}'),
    null,
    JSON.parse('{"a":9,"b":"y","list":[1,null],"__proto__":false}'),
  ];
  expect([0, 1, 2].map((i) => vectorValue(result, i))).toEqual(expected);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  nestedStructVector(valid, fields, exact);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => nestedStructVector(valid, fields, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  source.dispose();
});

it("rejects mismatched alignment before allocation and handles empty fields", () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  expect(() =>
    nestedStructVector(
      Uint8Array.of(1, 0),
      {
        bad: { alignment: "all-parents", values: { type: "null", length: 1 } },
      },
      scope,
    ),
  ).toThrow(/Unaligned/);
  expect(scope.budget.peakBytes).toBe(0);
  const result = nestedStructVector(Uint8Array.of(1, 0), {}, scope);
  expect(vectorValue(result, 0)).toEqual({});
  expect(vectorValue(result, 1)).toBeNull();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
