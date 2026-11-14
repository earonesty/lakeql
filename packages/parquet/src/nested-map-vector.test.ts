import { MemoryBudget, MemoryScope, vectorFromValues, vectorValue } from "lakeql-core";
import { expect, it } from "vitest";
import { nestedMapVector } from "./nested-map-vector.js";

it("assembles null/empty maps and nullable values while sharing entry payload", () => {
  const source = new MemoryScope(new MemoryBudget(16384));
  const keys = vectorFromValues(["__proto__", "b", "c"], source);
  const values = vectorFromValues([7, null, 9], source);
  const parent = {
    eventOffsets: Uint32Array.of(0, 1, 2, 4, 5),
    valid: Uint8Array.of(0, 1, 1, 1),
    continuesPrevious: false,
  };
  const entries = {
    eventOffsets: Uint32Array.of(2, 3, 4, 5),
    valid: Uint8Array.of(1, 1, 1),
    continuesPrevious: false,
  };
  const scope = new MemoryScope(new MemoryBudget(4096));
  const map = nestedMapVector(parent, entries, keys, values, scope);
  expect([0, 1, 2, 3].map((i) => vectorValue(map, i))).toEqual([
    null,
    {},
    JSON.parse('{"__proto__":7,"b":null}'),
    { c: 9 },
  ]);
  if (map.type !== "map") throw Error("Expected map");
  expect(map.keys).toBe(keys);
  expect(map.values).toBe(values);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  nestedMapVector(parent, entries, keys, values, exact);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => nestedMapVector(parent, entries, keys, values, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  const invalid = new MemoryScope(new MemoryBudget(4096));
  expect(() =>
    nestedMapVector(parent, entries, keys, { type: "null", length: 2 }, invalid),
  ).toThrow(/Unaligned/);
  expect(invalid.budget.peakBytes).toBe(0);
  parent.valid[2] = 0;
  expect(() => nestedMapVector(parent, entries, keys, values, invalid)).toThrow(/Unaligned/);
  expect(invalid.budget.usedBytes).toBe(0);
  invalid.dispose();
  source.dispose();
});
