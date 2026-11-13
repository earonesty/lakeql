import { expect, it } from "vitest";
import { jsonSafeValue } from "./evaluator.js";
import { intervalValue } from "./interval.js";
import { jsonOutputUtf8Bytes } from "./json-output-size.js";
import { timestampFromEpoch } from "./timestamp.js";

const values = [
  null,
  undefined,
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
  values.map((value, index) => ({ value, index })),
)("matches actual JSON output for case $index", ({ value }) => {
  const json = JSON.stringify(jsonSafeValue(value));
  expect(jsonOutputUtf8Bytes(value)).toBe(
    json === undefined ? undefined : new TextEncoder().encode(json).length,
  );
});
it("allows shared references while rejecting cycles and custom serializer hooks", () => {
  const shared = { a: 1 };
  const value = { left: shared, right: shared };
  expect(jsonOutputUtf8Bytes(value)).toBe(new TextEncoder().encode(JSON.stringify(value)).length);
  const cyclic: Record<string, unknown> = {};
  cyclic.self = cyclic;
  expect(() => jsonOutputUtf8Bytes(cyclic)).toThrow(/Cyclic/);
  expect(() =>
    jsonOutputUtf8Bytes({
      toJSON() {
        return "unpredictable";
      },
    }),
  ).toThrow(/cannot be forecast/);
});

it("rejects function-valued custom serialization hooks", () => {
  const fn = Object.assign(() => undefined, { toJSON: () => "custom" });
  expect(() => jsonOutputUtf8Bytes(fn)).toThrow(/cannot be forecast/);
  expect(() => jsonOutputUtf8Bytes({ fn })).toThrow(/cannot be forecast/);
});
