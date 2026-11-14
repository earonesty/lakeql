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
)("encodes case $index at exact measured output/workspace capacity", ({ value }) => {
  const expected = new TextEncoder().encode(`,${JSON.stringify(jsonSafeValue(value))}\n`);
  const bytes = expected.length + c.view;
  const pool = new MemoryBudget(1024 * 1024);
  const scope = new MemoryScope(pool);
  const output = encodeOwnedJsonOutput(value, scope, { prefix: ",", suffix: "\n" });
  const peak = pool.peakBytes;
  scope.dispose();
  expect(output.value).toEqual(expected);
  expect(pool.usedBytes).toBe(bytes);
  output.release();
  expect(pool.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  const repeated = encodeOwnedJsonOutput(value, exact, { prefix: ",", suffix: "\n" });
  expect(repeated.value).toEqual(expected);
  repeated.release();
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
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

it("bounds nested scalar workspace independently of array length", () => {
  const overheads: number[] = [];
  for (const count of [1000, 10000]) {
    const value = { values: Array.from({ length: count }, (_, i) => i + 0.25) };
    const expected = new TextEncoder().encode(JSON.stringify(value));
    const pool = new MemoryBudget(expected.length + 4096);
    const scope = new MemoryScope(pool);
    const output = encodeOwnedJsonOutput(value, scope);
    expect(output.value).toEqual(expected);
    expect(pool.peakBytes - output.bytes).toBeGreaterThan(0);
    expect(pool.peakBytes - output.bytes).toBeLessThan(4096);
    overheads.push(pool.peakBytes - output.bytes);
    scope.dispose();
    output.release();
    expect(pool.usedBytes).toBe(0);
  }
  expect(overheads[0]).toBe(overheads[1]);
});
it("rejects nested timestamp formatting before its formatter runs", () => {
  const timestamp = timestampFromEpoch(0n, "nanos");
  let calls = 0;
  timestamp.toJSON = () => {
    calls++;
    return "value";
  };
  const scope = new MemoryScope(new MemoryBudget(1));
  expect(() => encodeOwnedJsonOutput({ nested: [timestamp] }, scope)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
  expect(calls).toBe(0);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("writes safe integer boundaries without scalar formatting capacity", () => {
  const values = [0, -0, Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER];
  for (let exponent = 0; exponent <= 15; exponent++)
    for (const delta of [-1, 0, 1]) {
      const value = 10 ** exponent + delta;
      values.push(value, -value);
    }
  let seed = 17;
  for (let i = 0; i < 1000; i++) {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    values.push(
      seed * 1048576 + (seed >>> 16),
      -seed * 1048576 - (seed >>> 16),
      Number.MAX_SAFE_INTEGER - seed,
      Number.MIN_SAFE_INTEGER + seed,
    );
  }
  for (const value of values) {
    const expected = new TextEncoder().encode(JSON.stringify(value));
    const scope = new MemoryScope(new MemoryBudget(expected.length + c.view));
    const output = encodeOwnedJsonOutput(value, scope);
    expect(output.value).toEqual(expected);
    expect(scope.budget.peakBytes).toBe(output.bytes);
    output.release();
    scope.dispose();
  }
});

it("releases output and temporary formatting when the write-pass formatter fails", () => {
  const timestamp = timestampFromEpoch(0n, "nanos");
  const failure = new Error("formatter failed during writing");
  let calls = 0;
  timestamp.toJSON = () => {
    if (++calls === 2) throw failure;
    return "stable";
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(() => encodeOwnedJsonOutput({ nested: [timestamp] }, scope)).toThrow(failure);
  expect(calls).toBe(2);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("encodes 20000 nested containers with exact admitted capacity", () => {
  let value: unknown = "雪😀";
  for (let i = 0; i < 20000; i++) value = i % 2 ? { value } : [value];
  const expected = `${'{"value":['.repeat(10000)}"雪😀"${"]}".repeat(10000)}`;
  const scope = new MemoryScope(new MemoryBudget(64 * 1024 * 1024));
  const result = encodeOwnedJsonOutput(value, scope);
  expect(new TextDecoder().decode(result.value)).toBe(expected);
  const peak = scope.budget.peakBytes;
  result.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  for (const capacity of [peak - 1, peak]) {
    const exact = new MemoryScope(new MemoryBudget(capacity));
    if (capacity < peak) expect(() => encodeOwnedJsonOutput(value, exact)).toThrow(/budget/i);
    else {
      const encoded = encodeOwnedJsonOutput(value, exact);
      expect(new TextDecoder().decode(encoded.value)).toBe(expected);
      encoded.release();
    }
    exact.dispose();
    expect(exact.budget.usedBytes).toBe(0);
  }
});

it("admits traversal while the output backing is live and unwinds writer failures", () => {
  const value = ["x".repeat(4096)];
  const measured = new MemoryScope(new MemoryBudget(16384));
  const output = encodeOwnedJsonOutput(value, measured);
  const peak = measured.budget.peakBytes;
  expect(peak).toBeGreaterThan(output.bytes);
  output.release();
  measured.dispose();
  for (const capacity of [peak - 1, peak]) {
    const scope = new MemoryScope(new MemoryBudget(capacity));
    if (capacity < peak) expect(() => encodeOwnedJsonOutput(value, scope)).toThrow(/budget/i);
    else encodeOwnedJsonOutput(value, scope).release();
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});
