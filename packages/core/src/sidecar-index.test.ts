import { describe, expect, it, vi } from "vitest";
import type { Expr } from "./expr.js";
import { and, between, col, eq, fn, gt, gte, isIn, lit, lt, lte, ne, not, or } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import {
  bboxMayIntersect,
  buildBBoxIndex,
  buildMinMaxIndex,
  pruneFilesWithIndex,
  type SidecarFileIndex,
} from "./sidecar-index.js";

const files: SidecarFileIndex[] = [
  {
    path: "lake/b.parquet",
    columns: { amount: { min: 100, max: 199 }, region: { min: "east", max: "west" } },
  },
  {
    path: "lake/a.parquet",
    columns: { amount: { min: 0, max: 99 }, region: { min: "north", max: "south" } },
  },
  {
    path: "lake/c.parquet",
    columns: { amount: { min: 200, max: 299 }, region: { min: "west", max: "west" } },
  },
];

describe("sidecar min/max indexes", () => {
  it("builds deterministic min/max entries from rows", () => {
    expect(
      buildMinMaxIndex(
        [
          { amount: 3, region: "west" },
          { amount: 1, region: "east" },
          { amount: null, region: "north" },
        ],
        ["amount", "region"],
      ),
    ).toEqual({
      amount: { min: 1, max: 3, nullCount: 1 },
      region: { min: "east", max: "west" },
    });
    expect(
      buildMinMaxIndex(
        [
          { id: 2n, flag: true },
          { id: 1n, flag: false },
        ],
        ["id", "flag"],
      ),
    ).toEqual({
      id: { min: 1n, max: 2n },
      flag: { min: false, max: true },
    });
  });

  it("prunes files for comparison, between, and in predicates", () => {
    expect(pruneFilesWithIndex(files, gt("amount", 150)).planned.map((file) => file.path)).toEqual([
      "lake/b.parquet",
      "lake/c.parquet",
    ]);
    expect(pruneFilesWithIndex(files, lt("amount", 50)).planned.map((file) => file.path)).toEqual([
      "lake/a.parquet",
    ]);
    expect(
      pruneFilesWithIndex(files, between("amount", 90, 110)).planned.map((file) => file.path),
    ).toEqual(["lake/a.parquet", "lake/b.parquet"]);
    expect(
      pruneFilesWithIndex(files, isIn("amount", [25, 225])).planned.map((file) => file.path),
    ).toEqual(["lake/a.parquet", "lake/c.parquet"]);
    expect(
      pruneFilesWithIndex(files, eq("region", "west")).planned.map((file) => file.path),
    ).toEqual(["lake/b.parquet", "lake/c.parquet"]);
    expect(pruneFilesWithIndex(files, lte("amount", 99)).planned.map((file) => file.path)).toEqual([
      "lake/a.parquet",
    ]);
    expect(pruneFilesWithIndex(files, gte("amount", 200)).planned.map((file) => file.path)).toEqual(
      ["lake/c.parquet"],
    );
    expect(
      pruneFilesWithIndex(files, ne("region", "west")).planned.map((file) => file.path),
    ).toEqual(["lake/a.parquet", "lake/b.parquet"]);
    expect(
      pruneFilesWithIndex(files, {
        kind: "compare",
        op: "eq",
        left: lit(250),
        right: col("amount"),
      }).planned.map((file) => file.path),
    ).toEqual(["lake/c.parquet"]);
    expect(
      pruneFilesWithIndex(
        [{ path: "big.parquet", columns: { id: { min: 1n, max: 3n } } }],
        eq("id", 2),
      ).planned,
    ).toHaveLength(1);
  });

  it("keeps planning conservative for unsupported predicates", () => {
    expect(pruneFilesWithIndex(files, not(eq("amount", 1))).planned).toHaveLength(3);
    expect(
      pruneFilesWithIndex(files, and(gt("amount", 150), lt("amount", 250))).planned.map(
        (file) => file.path,
      ),
    ).toEqual(["lake/b.parquet", "lake/c.parquet"]);
    expect(
      pruneFilesWithIndex(files, or(eq("amount", 1), eq("amount", 250))).planned.map(
        (file) => file.path,
      ),
    ).toEqual(["lake/a.parquet", "lake/c.parquet"]);
    expect(pruneFilesWithIndex(files, fn("lower", col("region"))).planned).toHaveLength(3);
    expect(
      pruneFilesWithIndex(files, { kind: "compare", op: "eq", left: col("missing"), right: lit(1) })
        .planned,
    ).toHaveLength(3);
    expect(
      pruneFilesWithIndex(files, {
        kind: "between",
        target: fn("lower", col("region")),
        low: lit("a"),
        high: lit("z"),
      }).planned,
    ).toHaveLength(3);
    expect(
      pruneFilesWithIndex(files, isIn("amount", [fn("abs", col("amount"))])).planned,
    ).toHaveLength(3);
    expect(
      pruneFilesWithIndex(
        [{ path: "typed.parquet", columns: { amount: { min: "a", max: "z" } } }],
        eq("amount", 1),
      ).planned,
    ).toHaveLength(1);
    expect(
      pruneFilesWithIndex(
        [{ path: "typed.parquet", columns: { amount: { min: "a", max: "z" } } }],
        between("amount", 1, 2),
      ).planned,
    ).toHaveLength(1);
    expect(
      pruneFilesWithIndex(
        [{ path: "typed.parquet", columns: { amount: { min: "a", max: "z" } } }],
        isIn("amount", [1]),
      ).planned,
    ).toHaveLength(1);
    expect(
      pruneFilesWithIndex(
        [{ path: "nan.parquet", columns: { amount: { min: Number.NaN, max: Number.NaN } } }],
        eq("amount", 1),
      ).planned,
    ).toHaveLength(1);
    expect(
      pruneFilesWithIndex(
        [
          {
            path: "big.parquet",
            columns: { id: { min: 9_007_199_254_740_993n, max: 9_007_199_254_740_995n } },
          },
        ],
        eq("id", 9_007_199_254_740_994),
      ).planned,
    ).toHaveLength(1);
  });

  it("throws typed errors for invalid index inputs", () => {
    expect(() => buildMinMaxIndex([{ value: { nested: true } }], ["value"])).toThrow(/scalar/u);
    expect(() => buildMinMaxIndex([{ value: Number.NaN }], ["value"])).toThrow(/scalar/u);
    expect(() =>
      buildBBoxIndex([], { minx: "minx", miny: "miny", maxx: "maxx", maxy: "maxy" }),
    ).toThrow(/empty/u);
    expect(() =>
      bboxMayIntersect(
        { minx: 2, miny: 0, maxx: 1, maxy: 1 },
        { minx: 0, miny: 0, maxx: 1, maxy: 1 },
      ),
    ).toThrow(/ordered/u);
    expect(() =>
      buildBBoxIndex([{ minx: "bad", miny: 0, maxx: 1, maxy: 1 }], {
        minx: "minx",
        miny: "miny",
        maxx: "maxx",
        maxy: "maxy",
      }),
    ).toThrow(/finite/u);
  });
});

describe("sidecar bbox and h3 indexes", () => {
  it("builds bbox sidecars and prunes st_intersects calls", () => {
    const bbox = buildBBoxIndex(
      [
        { minx: -119, miny: 33, maxx: -118, maxy: 34 },
        { minx: -118.5, miny: 34, maxx: -117, maxy: 35 },
      ],
      { minx: "minx", miny: "miny", maxx: "maxx", maxy: "maxy" },
    );
    expect(bbox).toEqual({ minx: -119, miny: 33, maxx: -117, maxy: 35 });
    expect(bboxMayIntersect(bbox, { minx: -118.2, miny: 34.1, maxx: -118, maxy: 34.3 })).toBe(true);

    const result = pruneFilesWithIndex(
      [
        { path: "la.parquet", bbox: { geom: bbox } },
        { path: "ny.parquet", bbox: { geom: { minx: -74.1, miny: 40, maxx: -73.8, maxy: 41 } } },
      ],
      {
        kind: "call",
        fn: "st_intersects",
        args: [
          { kind: "column", name: "geom" },
          {
            kind: "literal",
            value: '{"type":"BBox","minx":-118.2,"miny":34.1,"maxx":-118,"maxy":34.3}',
          },
        ],
      },
    );
    expect(result.planned.map((file) => file.path)).toEqual(["la.parquet"]);
    expect(
      pruneFilesWithIndex([{ path: "bad.parquet", bbox: { geom: bbox } }], {
        kind: "call",
        fn: "st_intersects",
        args: [
          { kind: "column", name: "geom" },
          { kind: "literal", value: "{}" },
        ],
      }).planned,
    ).toHaveLength(1);
    expect(
      pruneFilesWithIndex([{ path: "bad.parquet", bbox: { geom: bbox } }], {
        kind: "call",
        fn: "st_intersects",
        args: [
          { kind: "literal", value: "geom" },
          { kind: "literal", value: "{}" },
        ],
      }).planned,
    ).toHaveLength(1);
  });

  it("prunes h3_in calls against h3 sidecars", () => {
    const result = pruneFilesWithIndex(
      [
        { path: "a.parquet", h3: { h3_8: ["8829a1d757fffff"] } },
        { path: "b.parquet", h3: { h3_8: ["8829a1d75bfffff"] } },
      ],
      {
        kind: "call",
        fn: "h3_in",
        args: [
          { kind: "column", name: "h3_8" },
          { kind: "literal", value: '["8829a1d757fffff","8829a1d758fffff"]' },
        ],
      },
    );
    expect(result).toMatchObject({
      planned: [{ path: "a.parquet" }],
      skipped: [{ path: "b.parquet" }],
    });
    expect(
      pruneFilesWithIndex([{ path: "bad.parquet", h3: { h3_8: ["a"] } }], {
        kind: "call",
        fn: "h3_in",
        args: [
          { kind: "column", name: "h3_8" },
          { kind: "literal", value: "[1]" },
        ],
      }).planned,
    ).toHaveLength(1);
    expect(
      pruneFilesWithIndex([{ path: "missing-index.parquet" }], {
        kind: "call",
        fn: "h3_in",
        args: [
          { kind: "column", name: "h3_8" },
          { kind: "literal", value: '["a"]' },
        ],
      }).planned,
    ).toHaveLength(1);
    expect(
      pruneFilesWithIndex([{ path: "bad-args.parquet", h3: { h3_8: ["a"] } }], {
        kind: "call",
        fn: "h3_in",
        args: [
          { kind: "literal", value: "h3_8" },
          { kind: "literal", value: '["a"]' },
        ],
      }).planned,
    ).toHaveLength(1);
  });

  it("prunes h3_within exact-radius calls conservatively", () => {
    expect(
      pruneFilesWithIndex(
        [
          { path: "a.parquet", h3: { h3_8: ["8829a1d757fffff"] } },
          { path: "b.parquet", h3: { h3_8: ["8829a1d75bfffff"] } },
        ],
        fn("h3_within", col("h3_8"), "8829a1d757fffff", 0),
      ),
    ).toMatchObject({
      planned: [{ path: "a.parquet" }],
      skipped: [{ path: "b.parquet" }],
    });
    expect(
      pruneFilesWithIndex(
        [{ path: "positive-radius.parquet", h3: { h3_8: ["8829a1d75bfffff"] } }],
        fn("h3_within", col("h3_8"), "8829a1d757fffff", 1),
      ).planned,
    ).toHaveLength(1);
    expect(
      pruneFilesWithIndex(
        [{ path: "bad-radius.parquet", h3: { h3_8: ["8829a1d75bfffff"] } }],
        fn("h3_within", col("h3_8"), "8829a1d757fffff", -1),
      ).planned,
    ).toHaveLength(1);
  });
});

it("resolves sidecars separately while preserving file identity and conservative missing indexes", () => {
  const a = { path: "a", size: 3, rangeReadMode: "emulated" };
  const b = { path: "b", size: 4, rangeReadMode: "native" };
  const input = [b, a];
  const result = pruneFilesWithIndex(input, gt("amount", 10), {
    indexForFile: (file) =>
      file.path === "a" ? { path: "a", columns: { amount: { min: 0, max: 5 } } } : undefined,
  });
  expect(result.planned).toEqual([b]);
  expect(result.planned[0]).toBe(b);
  expect(result.skipped[0]).toBe(a);
  expect(input).toEqual([b, a]);
});

it("admits result lists and only sorts unsorted inputs, with exact-capacity rollback", () => {
  const run = (capacity: number, sorted: boolean) => {
    const budget = new MemoryBudget(capacity);
    const scope = new MemoryScope(budget);
    scope.allocate(8, "sibling", () => undefined);
    const input = sorted ? [files[1], files[0], files[2]] : [...files];
    const valid = input.filter((file): file is SidecarFileIndex => file !== undefined);
    try {
      const result = pruneFilesWithIndex(valid, gt("amount", 150), { scope });
      expect(result.planned.map((file) => file.path)).toEqual(["lake/b.parquet", "lake/c.parquet"]);
      return budget.peakBytes;
    } catch (error) {
      expect(budget.usedBytes).toBe(8);
      throw error;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  };
  const sortedPeak = run(65536, true),
    unsortedPeak = run(65536, false);
  expect(unsortedPeak).toBeGreaterThan(sortedPeak);
  for (const sorted of [false, true]) {
    const peak = sorted ? sortedPeak : unsortedPeak;
    expect(run(peak, sorted)).toBe(peak);
    expect(() => run(peak - 1, sorted)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
    );
  }
});

it("rolls back partial pruning when index lookup fails", () => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  scope.allocate(8, "sibling", () => undefined);
  let calls = 0;
  expect(() =>
    pruneFilesWithIndex(files, undefined, {
      scope,
      indexForFile: (file) => {
        if (++calls === 2) throw Error("lookup failed");
        return file;
      },
    }),
  ).toThrow("lookup failed");
  expect(pool.usedBytes).toBe(8);
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("parses spatial literals once per pruning operation and admits their lifetime", () => {
  const input = Array.from({ length: 100 }, (_, i) => ({
    path: String(i).padStart(3, "0"),
    h3: { cell: ["a"] },
  }));
  const predicate = fn("h3_in", col("cell"), '["a","雪"]');
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const parse = vi.spyOn(JSON, "parse");
  try {
    expect(pruneFilesWithIndex(input, predicate, { scope }).planned).toHaveLength(100);
    expect(parse).toHaveBeenCalledTimes(1);
  } finally {
    parse.mockRestore();
  }
  const peak = pool.peakBytes;
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
  for (const cap of [peak - 1, peak]) {
    const target = new MemoryScope(new MemoryBudget(cap));
    try {
      if (cap < peak)
        expect(() => pruneFilesWithIndex(input, predicate, { scope: target })).toThrow(/budget/i);
      else
        expect(pruneFilesWithIndex(input, predicate, { scope: target }).planned).toHaveLength(100);
    } finally {
      target.dispose();
    }
    expect(target.budget.usedBytes).toBe(0);
  }
});
it("keeps malformed spatial literals lazy and rolls back on reached parse failures", () => {
  const predicate = fn("h3_in", col("cell"), "[");
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(pruneFilesWithIndex([{ path: "a" }], predicate, { scope }).planned).toHaveLength(1);
  const baseline = scope.budget.usedBytes;
  expect(() =>
    pruneFilesWithIndex([{ path: "b", h3: { cell: ["a"] } }], predicate, { scope }),
  ).toThrow(SyntaxError);
  expect(scope.budget.usedBytes).toBe(baseline);
  scope.dispose();
});

it("reverses ordered comparisons when the literal is on the left", () => {
  const input = [{ path: "a", columns: { x: { min: 10, max: 20 } } }];
  for (const [op, expected] of [
    ["lt", true],
    ["lte", true],
    ["gt", false],
    ["gte", false],
    ["eq", false],
    ["ne", true],
  ] as const) {
    const result = pruneFilesWithIndex(input, {
      kind: "compare",
      op,
      left: lit(5),
      right: col("x"),
    });
    expect(result.planned.length, op).toBe(expected ? 1 : 0);
  }
  for (const [op, expected] of [
    ["lt", false],
    ["lte", true],
    ["gt", false],
    ["gte", true],
  ] as const) {
    const result = pruneFilesWithIndex([{ path: "a", columns: { x: { min: 10, max: 10 } } }], {
      kind: "compare",
      op,
      left: lit(10),
      right: col("x"),
    });
    expect(result.planned.length, op).toBe(expected ? 1 : 0);
  }
});
it("treats inherited index names as absent and preserves explicit prototype-named columns", () => {
  for (const name of ["constructor", "toString", "__proto__"]) {
    const file = { path: "a", columns: {}, bbox: {}, h3: {} };
    for (const predicate of [
      eq(col(name), 1),
      fn("h3_in", col(name), '["a"]'),
      fn("h3_within", col(name), "a", 0),
      fn("st_intersects", col(name), '{"minx":0,"miny":0,"maxx":1,"maxy":1}'),
    ]) {
      expect(pruneFilesWithIndex([file], predicate).planned).toEqual([file]);
    }
    const row = { [name]: 10 };
    const columns = buildMinMaxIndex([row], [name]);
    expect(Object.hasOwn(columns, name)).toBe(true);
    expect(pruneFilesWithIndex([{ path: "a", columns }], eq(col(name), 1)).skipped).toHaveLength(1);
  }
});

it("uses admitted reusable frames for deeply nested logical pruning", () => {
  let predicate: Expr = eq(col("x"), 1);
  for (let i = 0; i < 20000; i++)
    predicate = { kind: "logical", op: i % 2 ? "and" : "or", operands: [predicate] };
  const input = [
    { path: "a", columns: { x: { min: 1, max: 1 } } },
    { path: "b", columns: { x: { min: 2, max: 2 } } },
  ];
  const pool = new MemoryBudget(10_000_000);
  const scope = new MemoryScope(pool);
  expect(pruneFilesWithIndex(input, predicate, { scope }).planned).toEqual([input[0]]);
  const peak = pool.peakBytes;
  // Two files share the same 20,000 frames, rather than allocating two stacks.
  expect(peak).toBeLessThan(5_000_000);
  scope.dispose();
  for (const capacity of [peak - 1, peak]) {
    const target = new MemoryScope(new MemoryBudget(capacity));
    try {
      if (capacity < peak)
        expect(() => pruneFilesWithIndex(input, predicate, { scope: target })).toThrow(/budget/i);
      else
        expect(pruneFilesWithIndex(input, predicate, { scope: target }).skipped).toEqual([
          input[1],
        ]);
    } finally {
      target.dispose();
    }
    expect(target.budget.usedBytes).toBe(0);
  }
});
it("preserves empty logic and nested short-circuit behavior", () => {
  const input = [{ path: "a", columns: { x: { min: 1, max: 1 } }, h3: { cell: ["a"] } }];
  const bad = fn("h3_in", col("cell"), "[");
  const yes: Expr = { kind: "logical", op: "and", operands: [] };
  const no: Expr = { kind: "logical", op: "or", operands: [] };
  expect(pruneFilesWithIndex(input, yes).planned).toHaveLength(1);
  expect(pruneFilesWithIndex(input, no).skipped).toHaveLength(1);
  expect(pruneFilesWithIndex(input, or(and(no, bad), yes, bad)).planned).toHaveLength(1);
  expect(pruneFilesWithIndex(input, and(or(yes, bad), no, bad)).skipped).toHaveLength(1);
});
