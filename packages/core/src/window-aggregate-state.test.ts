import { expect, it } from "vitest";
import { col, lit } from "./expr.js";
import type { AggregateOp } from "./query.js";
import type { WindowExpr } from "./window.js";
import {
  createWindowAggregateAccumulator,
  createWindowAggregateEvaluator,
} from "./window-aggregate.js";

it.each([
  ["sum", 8, null],
  ["avg", 2, null],
  ["count", 4, 0],
  ["count_distinct", 3, 0],
  ["min", 1, null],
  ["max", 3, null],
  ["first", 3, null],
  ["last", 2, null],
  ["any", 3, null],
  ["mode", 2, null],
  ["median", 2, null],
  ["quantile", 2, null],
] as const)("clears reusable %s state between populated and empty frames", (aggregate, expected, empty) => {
  const expr: WindowExpr = {
    fn: { aggregate },
    args: [col("value"), lit(0.5)],
    over: { partitionBy: [], orderBy: [] },
  };
  const evaluate = createWindowAggregateEvaluator(expr);
  for (let round = 0; round < 5; round++) {
    expect(evaluate([3, 1, 2, 2, null].map((value) => ({ value })))).toBe(expected);
    expect(evaluate([])).toBe(empty);
    expect(evaluate([{ value: 7 }])).toBe(
      aggregate === "count" || aggregate === "count_distinct" ? 1 : 7,
    );
  }
});

it("clears distinct and variance state and recovers after consumer failure", () => {
  for (const aggregate of ["sum", "var_pop", "var_samp"] as AggregateOp[]) {
    const evaluate = createWindowAggregateEvaluator({
      fn: { aggregate },
      args: [col("value")],
      distinct: true,
      over: { partitionBy: [], orderBy: [] },
    });
    const expected = aggregate === "sum" ? 6 : aggregate === "var_pop" ? 2 / 3 : 1;
    for (let round = 0; round < 3; round++) {
      expect(evaluate([1, 2, 2, 3].map((value) => ({ value })))).toBe(expected);
      expect(() =>
        evaluate(
          (function* () {
            yield { value: 100 };
            throw new Error("frame input failed");
          })(),
        ),
      ).toThrow("frame input failed");
    }
    expect(evaluate([1, 2, 3].map((value) => ({ value })))).toBe(expected);
  }
});

it("rejects reentrant evaluation without corrupting subsequent frames", () => {
  const evaluate = createWindowAggregateEvaluator({
    fn: { aggregate: "sum" },
    args: [col("value")],
    over: { partitionBy: [], orderBy: [] },
  });
  expect(() =>
    evaluate([
      {
        get value() {
          return evaluate([{ value: 1 }]);
        },
      },
    ]),
  ).toThrow(/reenter/);
  expect(evaluate([{ value: 2 }])).toBe(2);
});

it("avoids collection construction for fixed-state aggregate frames", () => {
  const OriginalMap = globalThis.Map;
  const OriginalSet = globalThis.Set;
  let maps = 0;
  let sets = 0;
  const results: unknown[] = [];
  try {
    globalThis.Map = class<K, V> extends OriginalMap<K, V> {
      constructor(entries?: Iterable<readonly [K, V]> | null) {
        super(entries);
        maps++;
      }
    };
    globalThis.Set = class<T> extends OriginalSet<T> {
      constructor(values?: Iterable<T> | null) {
        super(values);
        sets++;
      }
    };
    const evaluate = createWindowAggregateEvaluator({
      fn: { aggregate: "sum" },
      args: [col("value")],
      over: { partitionBy: [], orderBy: [] },
    });
    for (let round = 0; round < 100; round++) results.push(evaluate([{ value: 1 }, { value: 2 }]));
  } finally {
    globalThis.Map = OriginalMap;
    globalThis.Set = OriginalSet;
  }
  expect(results).toEqual(Array(100).fill(3));
  expect(maps).toBe(0);
  expect(sets).toBe(0);
});

it("appends state across chunks and clears partial progress after failure", () => {
  const accumulator = createWindowAggregateAccumulator({
    fn: { aggregate: "sum" },
    args: [col("value")],
    over: { partitionBy: [], orderBy: [] },
  });
  expect(accumulator.append([{ value: 1 }, { value: 2 }])).toBe(3);
  expect(accumulator.append([])).toBe(3);
  expect(accumulator.append([{ value: 3 }])).toBe(6);
  expect(() =>
    accumulator.append(
      (function* () {
        yield { value: 100 };
        throw new Error("failed append");
      })(),
    ),
  ).toThrow("failed append");
  expect(accumulator.append([{ value: 7 }])).toBe(7);
  accumulator.reset();
  expect(accumulator.append([])).toBeNull();
  expect(() =>
    accumulator.append([
      {
        get value() {
          accumulator.reset();
          return 1;
        },
      },
    ]),
  ).toThrow(/reenter/);
  expect(accumulator.append([{ value: 9 }])).toBe(9);
  accumulator.reset();
});
