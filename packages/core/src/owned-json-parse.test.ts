import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { parseOwnedJson } from "./owned-json-parse.js";

it("admits JSON text and parsed tree overlap with escaped strings and nested values", () => {
  const bytes = new TextEncoder().encode('{"雪":[1,true,null,{"x":"\\"{}[]\\\\"}],"__proto__":2}');
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const result = parseOwnedJson(bytes, scope);
  expect(result.value).toEqual(JSON.parse(new TextDecoder().decode(bytes)));
  const peak = pool.peakBytes;
  scope.dispose();
  expect(pool.usedBytes).toBe(result.bytes);
  result.release();
  expect(pool.usedBytes).toBe(0);
  for (const cap of [peak - 1, peak]) {
    const target = new MemoryScope(new MemoryBudget(cap));
    if (cap < peak) expect(() => parseOwnedJson(bytes, target)).toThrow(/budget/i);
    else parseOwnedJson(bytes, target).release();
    target.dispose();
    expect(target.budget.usedBytes).toBe(0);
  }
});

it("releases reservations when JSON parsing rejects malformed input", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(() => parseOwnedJson(new TextEncoder().encode('{"a":[1,'), scope)).toThrow(SyntaxError);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("parses borrowed UTF16 text without allocating an encoded or decoded copy", () => {
  const text = '{"雪":["😀","\\ud800",1,null]}';
  const scope = new MemoryScope(new MemoryBudget(65536));
  const owner = parseOwnedJson(text, scope);
  expect(owner.value).toEqual(JSON.parse(text));
  const peak = scope.budget.peakBytes;
  expect(peak).toBe(owner.bytes);
  owner.release();
  scope.dispose();
  for (const capacity of [peak - 1, peak]) {
    const target = new MemoryScope(new MemoryBudget(capacity));
    if (capacity < peak) expect(() => parseOwnedJson(text, target)).toThrow(/budget/i);
    else parseOwnedJson(text, target).release();
    target.dispose();
    expect(target.budget.usedBytes).toBe(0);
  }
});
