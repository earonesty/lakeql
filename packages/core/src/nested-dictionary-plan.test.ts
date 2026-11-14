import { expect, it } from "vitest";
import { type Vector, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { expandNestedDictionary, nestedDictionaryExpansionPlan } from "./nested-dictionary.js";

for (const shape of ["struct", "list", "map"] as const) {
  it(`plans exact ${shape} selection metadata with nested null masks`, () => {
    const child: Vector = { type: "i32", values: Int32Array.of(10, 20, 30) };
    const leaf: Vector =
      shape === "struct"
        ? { type: "struct", length: 2, fields: { x: child } }
        : shape === "list"
          ? { type: "list", offsets: Int32Array.of(0, 1, 3), child }
          : { type: "map", offsets: Int32Array.of(0, 1, 3), keys: child, values: child };
    const input: Extract<Vector, { type: "dict" }> = {
      type: "dict",
      indices: Uint32Array.of(1, 0, 2, 1),
      dictionary: {
        type: "dict",
        indices: Uint32Array.of(0, 1, 99),
        valid: Uint8Array.of(1, 1, 0),
        dictionary: leaf,
      },
    };
    const plan = nestedDictionaryExpansionPlan(input);
    const bytes = shape === "struct" ? 20 : 44;
    expect(plan.bytes).toBe(bytes);
    for (const cap of [bytes - 1, bytes]) {
      const budget = new MemoryBudget(cap);
      const scope = new MemoryScope(budget);
      if (cap < bytes) {
        expect(() => expandNestedDictionary(input, scope)).toThrow(/budget/);
        expect(budget.peakBytes).toBe(0);
      } else {
        scope.admit(bytes, "nested expansion");
        const output = expandNestedDictionary(input, scope);
        expect(budget.usedBytes).toBe(bytes);
        for (let i = 0; i < 4; i++)
          expect(vectorValue(output.vector, i)).toEqual(vectorValue(input, i));
        output.release();
        expect(budget.usedBytes).toBe(0);
      }
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  });
}

it("validates selected offsets without reading unused child payload", () => {
  const values = [1];
  Object.defineProperty(values, 0, {
    get() {
      throw new Error("payload inspected");
    },
  });
  const dictionary: Vector = {
    type: "list",
    offsets: Int32Array.of(0, 1, 99),
    child: { type: "utf8", values: values as unknown as string[] },
  };
  const input: Extract<Vector, { type: "dict" }> = {
    type: "dict",
    indices: Uint32Array.of(0),
    dictionary,
  };
  expect(nestedDictionaryExpansionPlan(input).bytes).toBe(12);
  input.indices[0] = 1;
  expect(() => nestedDictionaryExpansionPlan(input)).toThrow(/offsets/);
});
