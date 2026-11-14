import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { jsonAllocationBytes, parseAdmittedJson } from "./json-allocation.js";

it.each([
  "{}",
  "[]",
  "null",
  "42",
  "[true,false,null,-1.5e2]",
  '{"é😀":"escaped\\"[,]{}\\n","nested":[{},[]]}',
])("admits parsed JSON before construction: %s", (text) => {
  const expected = JSON.parse(text);
  const bytes = jsonAllocationBytes(text);
  const scope = new MemoryScope(new MemoryBudget(bytes));
  expect(parseAdmittedJson(text, scope)).toEqual(expected);
  expect(scope.budget.usedBytes).toBe(bytes);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  const spy = vi.spyOn(JSON, "parse");
  try {
    expect(() => parseAdmittedJson(text, tight)).toThrow(/budget/);
    expect(spy).not.toHaveBeenCalled();
    expect(tight.budget.usedBytes).toBe(0);
  } finally {
    spy.mockRestore();
    tight.dispose();
  }
});

it("rolls back admitted storage when JSON syntax is invalid", () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  expect(() => parseAdmittedJson('{"bad": [}', scope)).toThrow(SyntaxError);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
