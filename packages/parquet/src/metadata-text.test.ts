import { MATERIALIZATION_CHARGES, MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { decodeMetadataText } from "./metadata-text.js";

it.each(["", "ASCII", "é😀", "\ufffd"])("admits metadata text before decoding %j", (value) => {
  const input = new TextEncoder().encode(value);
  const bytes =
    MATERIALIZATION_CHARGES.string + input.length * MATERIALIZATION_CHARGES.stringCodeUnit;
  const budget = new MemoryBudget(bytes);
  const scope = new MemoryScope(budget);
  expect(decodeMetadataText(input, scope)).toBe(value);
  expect(budget.usedBytes).toBe(bytes);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  const spy = vi.spyOn(TextDecoder.prototype, "decode");
  try {
    expect(() => decodeMetadataText(input, tight)).toThrow(/budget/);
    expect(spy).not.toHaveBeenCalled();
    expect(tight.budget.usedBytes).toBe(0);
  } finally {
    spy.mockRestore();
    tight.dispose();
  }
});

it("keeps decoded text admission with a retained metadata owner", () => {
  const scope = new MemoryScope(new MemoryBudget(1024));
  expect(decodeMetadataText(undefined, scope)).toBeUndefined();
  expect(scope.budget.usedBytes).toBe(0);
  const value = decodeMetadataText(new Uint8Array([0xff, 0xff]), scope);
  const owner = scope.retainAllocations(value);
  scope.dispose();
  expect(owner.value).toBe("\ufffd\ufffd");
  expect(scope.budget.usedBytes).toBeGreaterThan(0);
  owner.release();
  expect(scope.budget.usedBytes).toBe(0);
});

it("preserves an explicitly supplied decoder's BOM policy", () => {
  const input = new TextEncoder().encode("\ufeffbounds");
  const scope = new MemoryScope(new MemoryBudget(1024));
  try {
    expect(decodeMetadataText(input, scope)).toBe("bounds");
    expect(decodeMetadataText(input, scope, new TextDecoder("utf-8", { ignoreBOM: true }))).toBe(
      "\ufeffbounds",
    );
  } finally {
    scope.dispose();
  }
});
