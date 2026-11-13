import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { jsonSafeValue } from "./evaluator.js";
import { intervalValue } from "./interval.js";
import { encodeOwnedJsonOutput } from "./json-output-encoding.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { timestampFromEpoch } from "./timestamp.js";

const inputs = [
  "plain ascii text",
  "😀",
  "\u007f",
  "\ud800",
  "\udfff",
  null,
  true,
  false,
  0,
  -0,
  NaN,
  Infinity,
  -Infinity,
  1e100,
  9007199254740993n,
  -9007199254740993n,
  123n,
  '雪😀\ud800\udc00\ud800\n"\\\u0000',
  new Uint8Array([0, 255]),
  { a: undefined, b: NaN, c: [undefined, Infinity, Symbol("x"), () => 1] },
  JSON.parse('{"__proto__":{"value":1},"constructor":null}'),
  new Array(3),
  new Date(0),
  timestampFromEpoch(-123456789n, "nanos"),
  intervalValue("1 month 2 days 3 seconds"),
];
it.each(
  inputs.map((value, index) => ({ value, index })),
)("encodes case $index at exact output capacity", ({ value }) => {
  const expected = new TextEncoder().encode(`,${JSON.stringify(jsonSafeValue(value))}\n`);
  const bytes = expected.length + c.view;
  const pool = new MemoryBudget(bytes);
  const scope = new MemoryScope(pool);
  const output = encodeOwnedJsonOutput(value, scope, { prefix: ",", suffix: "\n" });
  scope.dispose();
  expect(output.value).toEqual(expected);
  expect(pool.usedBytes).toBe(bytes);
  output.release();
  expect(pool.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => encodeOwnedJsonOutput(value, tight, { prefix: ",", suffix: "\n" })).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("matches JSON escaping for every UTF16 code unit and surrogate boundaries", () => {
  let text = "";
  for (let code = 0; code <= 0xffff; code++) text += String.fromCharCode(code);
  text += "\ud800\udc00\udbff\udfff\ud800x\udc00";
  const expected = new TextEncoder().encode(JSON.stringify(text));
  const scope = new MemoryScope(new MemoryBudget(expected.length + c.view));
  const output = encodeOwnedJsonOutput(text, scope);
  try {
    expect(output.value).toEqual(expected);
  } finally {
    output.release();
    scope.dispose();
  }
});

it("rolls back changed extents and rejects unforecastable roots", () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  let reads = 0;
  const changed = Object.defineProperty({}, "value", {
    enumerable: true,
    get() {
      return reads++ === 0 ? "short" : "longer";
    },
  });
  expect(() => encodeOwnedJsonOutput(changed, scope)).toThrow(/changed/);
  expect(scope.budget.usedBytes).toBe(0);
  for (const value of [undefined, Symbol("x"), () => 1])
    expect(() => encodeOwnedJsonOutput(value, scope)).toThrow(/Missing JSON/);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
