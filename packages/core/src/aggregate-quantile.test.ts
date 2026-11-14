import { expect, it } from "vitest";
import { continuousQuantile, continuousQuantileInPlace } from "./aggregate-quantile.js";
import { col, lit } from "./expr.js";
import { aggregateWindowValue } from "./window-aggregate.js";

it.each([
  0, 0.25, 0.5, 0.75, 1,
])("interpolates opposite finite extremes at %s without overflow", (q) => {
  const input = [Number.MAX_VALUE, -Number.MAX_VALUE];
  const check = (actual: number | null) => {
    expect(actual).not.toBeNull();
    expect(Number.isFinite(actual)).toBe(true);
    expect(Math.abs((actual ?? NaN) / Number.MAX_VALUE - (2 * q - 1))).toBeLessThanOrEqual(
      Number.EPSILON,
    );
  };
  check(continuousQuantile(input, q));
  expect(input).toEqual([Number.MAX_VALUE, -Number.MAX_VALUE]);
  check(continuousQuantileInPlace(input, q));
  expect(input).toEqual([-Number.MAX_VALUE, Number.MAX_VALUE]);
  const windowValue = aggregateWindowValue(
    input.map((value) => ({ value })),
    {
      fn: { aggregate: "quantile" },
      args: [col("value"), lit(q)],
      over: { partitionBy: [], orderBy: [] },
    },
  );
  expect(typeof windowValue).toBe("number");
  check(typeof windowValue === "number" ? windowValue : null);
});

it("preserves empty, singleton, duplicate and same-sign quantiles across ownership modes", () => {
  for (const values of [
    [],
    [7],
    [2, 2, 2],
    [-8, -4, -2],
    [8, 4, 2],
    [Number.MAX_VALUE, Number.MAX_VALUE],
  ]) {
    const original = [...values];
    for (const q of [0, 0.1, 0.5, 0.9, 1]) {
      expect(continuousQuantileInPlace([...values], q)).toBe(continuousQuantile(values, q));
      expect(values).toEqual(original);
    }
  }
});
