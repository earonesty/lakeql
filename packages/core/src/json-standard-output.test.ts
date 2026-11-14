import { expect, it } from "vitest";
import { encodeOwnedJsonOutput } from "./json-output-encoding.js";
import { jsonOutputUtf8Bytes } from "./json-output-size.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  { date: new Date("2026-11-14T00:00:00Z"), invalid: new Date(Number.NaN) },
  { wide: new Date(8640000000000000), early: new Date(-8640000000000000) },
  { bytes: new Uint8Array([0, 127, 255]), numbers: new Float64Array([1.5, Number.NaN]) },
  { scalar: Object(3.5), text: Object("雪😀"), flag: Object(false) },
  { values: [undefined, Number.NaN, Infinity], omitted: undefined, empty: {} },
])("matches native JSON data semantics for %j", (value) => {
  const expected = new TextEncoder().encode(JSON.stringify(value));
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(jsonOutputUtf8Bytes(value, scope, "standard")).toBe(expected.length);
  const output = encodeOwnedJsonOutput(value, scope, {}, "standard");
  expect(output.value).toEqual(expected);
  const peak = scope.budget.peakBytes;
  output.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  for (const capacity of [peak - 1, peak]) {
    const exact = new MemoryScope(new MemoryBudget(capacity));
    if (capacity < peak)
      expect(() => encodeOwnedJsonOutput(value, exact, {}, "standard")).toThrow(/budget/i);
    else encodeOwnedJsonOutput(value, exact, {}, "standard").release();
    exact.dispose();
    expect(exact.budget.usedBytes).toBe(0);
  }
});

it("rejects nonforecastable conversions without invoking callbacks", () => {
  let calls = 0;
  const callback = () => {
    calls++;
    return "custom";
  };
  const date = new Date();
  date.toISOString = callback;
  const boxed = Object(1);
  boxed.valueOf = callback;
  for (const value of [1n, Object(1n), { value: 1n }, { toJSON: callback }, date, boxed]) {
    const scope = new MemoryScope(new MemoryBudget(65536));
    expect(() => encodeOwnedJsonOutput(value, scope, {}, "standard")).toThrow();
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
  expect(calls).toBe(0);
});
