import { expect, it } from "vitest";
import { expandDictionaryMask } from "./dictionary-mask.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

const vector = {
  indices: Uint32Array.of(0, 1, 2, 0, 99),
  valid: Uint8Array.of(1, 1, 1, 0, 1),
};

it("consumes dictionary workspace while retaining SQL truth values in independent output", () => {
  const pool = new MemoryBudget(8);
  const scope = new MemoryScope(pool);
  const input = scope.allocate(3, "dictionary mask", () => Uint8Array.of(1, 0, 2)).value;
  const result = expandDictionaryMask(vector, input, scope);
  expect(result).toEqual(Uint8Array.of(1, 0, 2, 2, 2));
  expect(pool.peakBytes).toBe(8);
  expect(pool.usedBytes).toBe(5);
  input.fill(0);
  const owner = scope.retainAllocation(result);
  scope.dispose();
  expect(owner.value).toEqual(Uint8Array.of(1, 0, 2, 2, 2));
  expect(pool.usedBytes).toBe(5);
  owner.release();
  expect(pool.usedBytes).toBe(0);
});

it("releases dictionary workspace when row-mask admission fails", () => {
  const pool = new MemoryBudget(7);
  const scope = new MemoryScope(pool);
  const input = scope.allocate(3, "dictionary mask", () => Uint8Array.of(1, 0, 2)).value;
  expect(() => expandDictionaryMask(vector, input, scope)).toThrow();
  expect(pool.usedBytes).toBe(0);
  scope.dispose();
});

it("preserves borrowed use and empty dictionaries", () => {
  expect(expandDictionaryMask(vector, Uint8Array.of(1, 0, 2))).toEqual(
    Uint8Array.of(1, 0, 2, 2, 2),
  );
  expect(expandDictionaryMask({ indices: new Uint32Array() }, new Uint8Array())).toEqual(
    new Uint8Array(),
  );
});
