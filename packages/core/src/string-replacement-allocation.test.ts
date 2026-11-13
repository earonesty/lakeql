import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { batchExprValues } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { replaceAllWithAdmission } from "./string-replacement-allocation.js";

it("forecasts native replacement tokens, empty search, overlaps and Unicode exactly", () => {
  for (const value of ["", "a", "aaa", "abab", "é😀a", "$$a", "\ud800a"]) {
    for (const search of ["", "a", "aa", "ab", "😀", "missing"]) {
      for (const replacement of [
        "",
        "x",
        "$",
        "$$",
        "$&",
        "$`",
        "$'",
        "$1",
        "$<name>",
        "$$$&$`$'😀",
      ]) {
        const expected = value.replaceAll(search, replacement);
        const count = search === "" ? value.length + 1 : value.split(search).length - 1;
        const bytes =
          count === 0
            ? 0
            : c.string + c.stringCodeUnit * expected.length + c.array + c.arraySlot * count;
        const scope = new MemoryScope(new MemoryBudget(bytes));
        expect(replaceAllWithAdmission(value, search, replacement, scope)).toBe(expected);
        expect(scope.budget.usedBytes).toBe(bytes);
        scope.dispose();
        expect(scope.budget.usedBytes).toBe(0);
        if (bytes !== 0) {
          const rejected = new MemoryScope(new MemoryBudget(bytes + 6));
          rejected.allocate(7, "caller backing", () => new Uint8Array(7));
          expect(() => replaceAllWithAdmission(value, search, replacement, rejected)).toThrow(
            /budget/i,
          );
          expect(rejected.budget.usedBytes).toBe(7);
          rejected.dispose();
        }
      }
    }
  }
});

it("admits replacement expansion through lazy expression evaluation", () => {
  const values = batchExprValues(
    { rowCount: 1, columns: {} },
    {
      kind: "call",
      fn: "replace",
      args: [
        { kind: "literal", value: "aaa" },
        { kind: "literal", value: "a" },
        { kind: "literal", value: "$`$'" },
      ],
    },
  );
  const result = "aaa".replaceAll("a", "$`$'");
  const bytes = c.string + 2 * result.length + c.array + 3 * c.arraySlot;
  const scope = new MemoryScope(new MemoryBudget(bytes));
  expect(values.valueAt(0, scope)).toBe(result);
  expect(scope.budget.usedBytes).toBe(bytes);
  scope.dispose();
  const rejected = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => values.valueAt(0, rejected)).toThrow(/budget/i);
  expect(rejected.budget.usedBytes).toBe(0);
  rejected.dispose();
});
