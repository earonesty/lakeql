import { expect, it } from "vitest";
import { col, lit } from "./expr.js";
import type { WindowExpr } from "./window.js";
import { evaluateWindowRows } from "./window-backend.js";
import { partitionRows, type WindowRow } from "./window-utils.js";

it.each([
  "lag",
  "lead",
] as const)("matches independent %s offset selection with linear value reads", (fn) => {
  for (const length of [0, 1, 128])
    for (const pattern of ["all null", "sparse", "dense"]) {
      const values = Array.from({ length }, (_, index) =>
        pattern === "all null" || (pattern === "sparse" && index % 17 !== 0) ? null : index,
      );
      for (const offset of [0, 1, 2, 5, 31, 200, Number.MAX_SAFE_INTEGER])
        for (const ignoreNulls of [false, true]) {
          let reads = 0;
          const rows = values.map((value, index) => ({
            get value() {
              reads++;
              return value;
            },
            fallback: -index - 1,
          }));
          const expr: WindowExpr = {
            fn,
            args: [col("value"), lit(offset), col("fallback")],
            ignoreNulls,
            over: { partitionBy: [], orderBy: [] },
          };
          const partition = partitionRows(rows, expr);
          const expected = values.map((value, index) => {
            if (offset === 0) return value;
            const candidates =
              fn === "lead" ? values.slice(index + 1) : values.slice(0, index).reverse();
            const eligible = ignoreNulls
              ? candidates.filter((value) => value !== null)
              : candidates;
            return offset <= eligible.length ? eligible[offset - 1] : -index - 1;
          });
          expect(
            evaluateWindowRows(length, partition, [{ start: 0, end: length }], expr, {}),
          ).toEqual(expected);
          expect(reads).toBeLessThanOrEqual(length * 2);
        }
    }
});

it("computes positional and ranking values without projecting input rows", () => {
  const length = 8;
  const partition: WindowRow[] = Array.from({ length }, (_, index) => ({
    get row(): never {
      throw new Error("Ranking does not need the source row");
    },
    originalIndex: index,
    partitionKey: [],
    orderKey: [Math.floor(index / 2)],
  }));
  for (const fn of [
    "row_number",
    "rank",
    "dense_rank",
    "percent_rank",
    "cume_dist",
    "ntile",
  ] as const) {
    const expr: WindowExpr = {
      fn,
      args: fn === "ntile" ? [lit(3)] : [],
      over: { partitionBy: [], orderBy: [] },
    };
    const expected = Array.from({ length }, (_, index) => {
      const group = Math.floor(index / 2);
      if (fn === "row_number") return index + 1;
      if (fn === "rank") return group * 2 + 1;
      if (fn === "dense_rank") return group + 1;
      if (fn === "percent_rank") return (group * 2) / (length - 1);
      if (fn === "cume_dist") return ((group + 1) * 2) / length;
      return index < 3 ? 1 : index < 6 ? 2 : 3;
    });
    expect(evaluateWindowRows(length, partition, [{ start: 0, end: length }], expr, {})).toEqual(
      expected,
    );
  }
});
