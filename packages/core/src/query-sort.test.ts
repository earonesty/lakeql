import { expect, it } from "vitest";
import type { OrderByTerm } from "./query.js";
import { compareRows, mergeSortRuns } from "./query-sort.js";

it("preserves encounter order for equal keys across three runs", () => {
  const runs = [
    [0, 1],
    [2, 3],
    [4, 5],
  ].map((ids) => ids.map((id) => ({ id, key: 1 })));
  expect(mergeSortRuns(runs, [{ column: "key" }]).map((row) => row.id)).toEqual([0, 1, 2, 3, 4, 5]);
});

it("matches stable sorting across run sizes, nulls, directions and multiple keys without mutating runs", () => {
  const input = Array.from({ length: 257 }, (_, id) => ({
    id,
    a: id % 7 === 0 ? null : id % 5,
    b: id % 3,
  }));
  for (const direction of ["asc", "desc"] as const) {
    for (const nulls of ["first", "last"] as const) {
      const order: OrderByTerm[] = [
        { column: "a", direction, nulls },
        { column: "b", direction: "desc" },
      ];
      const compare = (a: (typeof input)[number], b: (typeof input)[number]) =>
        compareRows(a, b, order);
      const expected = [...input].sort(compare);
      for (const width of [1, 2, 3, 17, 256, 300]) {
        const runs = [
          [],
          ...Array.from({ length: Math.ceil(input.length / width) }, (_, i) =>
            input.slice(i * width, (i + 1) * width).sort(compare),
          ),
          [],
        ];
        const before = runs.map((run) => [...run]);
        expect(mergeSortRuns(runs, order)).toEqual(expected);
        expect(runs).toEqual(before);
      }
    }
  }
  expect(mergeSortRuns([], [{ column: "a" }])).toEqual([]);
});
