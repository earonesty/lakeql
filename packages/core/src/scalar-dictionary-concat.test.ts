import { expect, it } from "vitest";
import { concatScalarDictionaries } from "./packed-gather.js";

it("preserves timestamp units and rejects incompatible timestamp metadata", () => {
  const dictionary = {
    type: "timestamp" as const,
    values: BigInt64Array.of(9007199254740993n),
    unit: "ns" as const,
    isAdjustedToUTC: true,
  };
  const left = { type: "dict" as const, indices: Uint32Array.of(0), dictionary };
  const right = {
    ...left,
    dictionary: { ...dictionary, values: BigInt64Array.of(-9007199254740993n) },
  };
  const output = concatScalarDictionaries([left, right], undefined);
  expect(output).toEqual({
    ...dictionary,
    values: BigInt64Array.of(9007199254740993n, -9007199254740993n),
  });
  expect(
    concatScalarDictionaries(
      [left, { ...right, dictionary: { ...right.dictionary, unit: "us" } }],
      undefined,
    ),
  ).toBeUndefined();
  expect(
    concatScalarDictionaries(
      [left, { ...right, dictionary: { ...right.dictionary, isAdjustedToUTC: false } }],
      undefined,
    ),
  ).toBeUndefined();
});

it("declines unsupported or mixed leaves without materializing values", () => {
  const indices = Uint32Array.of(0);
  const left = {
    type: "dict" as const,
    indices,
    dictionary: { type: "i32" as const, values: Int32Array.of(1) },
  };
  expect(
    concatScalarDictionaries(
      [left, { type: "dict", indices, dictionary: { type: "f64", values: Float64Array.of(1.5) } }],
      undefined,
    ),
  ).toBeUndefined();
  expect(
    concatScalarDictionaries(
      [{ type: "dict", indices, dictionary: { type: "utf8", values: ["a"] } }],
      undefined,
    ),
  ).toBeUndefined();
  expect(concatScalarDictionaries([], undefined)).toBeUndefined();
});
