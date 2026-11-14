import { expect, it } from "vitest";
import { type Vector, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { concatStructRouting } from "./struct-routing-concat.js";

it("shares struct field payloads, preserves parent nulls, and admits atomically", () => {
  const a: Vector = { type: "i32", values: Int32Array.of(7, 8) };
  const b: Vector = { type: "i32", values: Int32Array.of(9) };
  const inputs = [
    { type: "struct" as const, fields: { value: a }, length: 2, valid: Uint8Array.of(1, 0) },
    { type: "struct" as const, fields: { value: b }, length: 1 },
  ];
  const run = (scope: MemoryScope) => {
    const output = concatStructRouting(inputs, scope);
    expect([0, 1, 2].map((i) => vectorValue(output, i))).toEqual([
      { value: 7 },
      null,
      { value: 9 },
    ]);
    const field = output.fields.value;
    if (field?.type !== "union") throw Error("Missing routed field");
    expect(field.children[0]).toBe(a);
    expect(field.children[1]).toBe(b);
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  run(scope);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  run(exact);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => run(tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("routes more than 256 structs and safely preserves prototype-named fields", () => {
  const inputs = Array.from({ length: 513 }, (_, i) => ({
    type: "struct" as const,
    length: 1,
    fields: Object.assign(Object.create(null), {
      ["__proto__"]: { type: "i32", values: Int32Array.of(i) },
    }) as Record<string, Vector>,
  }));
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  try {
    const output = concatStructRouting(inputs, scope);
    expect(Object.getPrototypeOf(output.fields)).toBeNull();
    expect(Object.hasOwn(output.fields, "__proto__")).toBe(true);
    const projected = Object.getOwnPropertyDescriptor(output.fields, "__proto__")?.value as Vector;
    for (let i = 0; i < inputs.length; i++) expect(vectorValue(projected, i)).toBe(i);
    const stack = [projected];
    while (stack.length) {
      const value = stack.pop();
      if (value?.type === "union") {
        expect(value.children.length).toBeLessThanOrEqual(256);
        stack.push(...value.children);
      }
    }
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects incompatible field sets and extents before allocating output", () => {
  const field: Vector = { type: "i32", values: Int32Array.of(1) };
  const first = { type: "struct" as const, length: 1, fields: { a: field } };
  for (const bad of [
    { type: "struct" as const, length: 1, fields: { b: field } },
    { type: "struct" as const, length: 2, fields: { a: field } },
    { ...first, valid: Uint8Array.of() },
    { ...first, length: -1 },
  ]) {
    const scope = new MemoryScope(new MemoryBudget(0));
    expect(() => concatStructRouting([first, bad], scope)).toThrow(/Incompatible/);
    expect(scope.budget.peakBytes).toBe(0);
    scope.dispose();
  }
});

it("borrows a single validated struct without allocating duplicate metadata", () => {
  const scope = new MemoryScope(new MemoryBudget(0));
  const input = {
    type: "struct" as const,
    length: 2,
    fields: { a: { type: "i32" as const, values: Int32Array.of(1, 2) } },
    valid: Uint8Array.of(1, 0),
  };
  expect(concatStructRouting([input], scope)).toBe(input);
  expect(scope.budget.peakBytes).toBe(0);
  expect(() => concatStructRouting([{ ...input, length: 3 }], scope)).toThrow(/Incompatible/);
  scope.dispose();
});
