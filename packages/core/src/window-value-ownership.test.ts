import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { col, type Expr, lit } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import type { WindowExpr } from "./window.js";
import { applyWindowsToRows, applyWindowsToRuns } from "./window-backend.js";
import { withWindowValue } from "./window-value-ownership.js";
import { applyWindowsToVectorBatches } from "./window-vector.js";

function replacement(size: number): Expr {
  return { kind: "call", fn: "replace", args: [col("text"), lit("x"), lit("a".repeat(size))] };
}
function expression(
  fn: "lag" | "lead" | "first_value" | "last_value" | "nth_value",
  arg: Expr,
  ignoreNulls = false,
): WindowExpr {
  return {
    fn,
    args: fn === "nth_value" ? [arg, lit(2)] : [arg],
    ignoreNulls,
    over: {
      partitionBy: [],
      orderBy: [{ expr: col("id") }],
      frame: {
        mode: "rows",
        start: { kind: "unbounded-preceding" },
        end: { kind: "unbounded-following" },
      },
    },
  };
}

for (const fn of ["lag", "lead", "first_value", "last_value", "nth_value"] as const) {
  for (const ignoreNulls of [false, true]) {
    it.each([
      "rows",
      "runs",
      "vectors",
    ])(`admits ${fn} value expansion (ignoreNulls=${ignoreNulls}, %s)`, async (route) => {
      for (const expand of [false, true]) {
        const pool = new MemoryBudget(65536);
        const sibling = pool.reserve(1024, "sibling");
        const window = expression(fn, replacement(expand ? 100000 : 2), ignoreNulls);
        const text = ["x", "x", null, "x"];
        const rows = text.map((text, id) => ({ id, text }));
        const run = async () =>
          route === "rows"
            ? applyWindowsToRows(rows, { value: window }, { memoryBudget: pool })
            : route === "runs"
              ? applyWindowsToRuns(
                  [rows.slice(0, 2), rows.slice(2)],
                  { value: window },
                  { memoryBudget: pool },
                ).flat()
              : applyWindowsToVectorBatches(
                  [{ batch: batchFromColumns({ id: [0, 1, 2, 3], text }) }],
                  { value: window },
                  { memoryBudget: pool },
                );
        if (expand) await expect(run()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
        else {
          const expected =
            fn === "lag"
              ? [null, "aa", "aa", ignoreNulls ? "aa" : null]
              : fn === "lead"
                ? ["aa", ignoreNulls ? "aa" : null, "aa", null]
                : ["aa", "aa", "aa", "aa"];
          expect((await run()).map((row) => row.value)).toEqual(expected);
        }
        expect(pool.usedBytes).toBe(sibling.bytes);
        sibling.release();
      }
    });
  }
}

it("retains one shared immutable result and releases per-probe scratch", () => {
  const pool = new MemoryBudget(256 * 1024);
  const resultScope = new MemoryScope(pool);
  const rows = Array.from({ length: 100 }, (_, id) => ({ id, text: "x" }));
  const output = applyWindowsToRows(
    rows,
    { value: expression("first_value", replacement(10000)) },
    { memoryBudget: pool, resultScope },
  );
  expect(output.every((row) => row.value === "a".repeat(10000))).toBe(true);
  expect(pool.usedBytes).toBeGreaterThanOrEqual(20000);
  expect(pool.usedBytes).toBeLessThan(55000);
  const live = pool.usedBytes;
  for (let index = 0; index < 100; index++)
    expect(
      withWindowValue(replacement(10000), { text: "x" }, resultScope, (value) => value !== null),
    ).toBe(true);
  expect(pool.usedBytes).toBe(live);
  resultScope.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("rolls failed result construction back without releasing prior caller allocations", () => {
  const pool = new MemoryBudget(65536);
  const resultScope = new MemoryScope(pool);
  resultScope.allocate(1024, "prior result", () => "prior");
  const window = { ...expression("lag", replacement(100000)), args: [replacement(100000), lit(0)] };
  expect(() =>
    applyWindowsToRows(
      [
        { id: 0, text: "y" },
        { id: 1, text: "x" },
      ],
      { value: window },
      { memoryBudget: pool, resultScope },
    ),
  ).toThrowError(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
  expect(pool.usedBytes).toBe(1024);
  resultScope.dispose();
});

it("keeps computed window values admitted while output is suspended", async () => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        yield [
          { id: 0, text: "x" },
          { id: 1, text: "x" },
        ];
      },
    },
  });
  const stream = lake
    .path("table")
    .window("value", expression("first_value", replacement(10000)))
    .batchSize(1)
    .run()
    .batches()
    [Symbol.asyncIterator]();
  expect((await stream.next()).value?.[0]?.value).toBe("a".repeat(10000));
  expect(pool.usedBytes).toBeGreaterThanOrEqual(20000);
  await stream.return?.();
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "lag",
  "lead",
] as const)("admits %s default expressions with and without IGNORE NULLS", (fn) => {
  for (const ignoreNulls of [false, true]) {
    const pool = new MemoryBudget(65536);
    const window = {
      ...expression(fn, col("text"), ignoreNulls),
      args: [col("text"), lit(4), replacement(100000)],
    };
    expect(() =>
      applyWindowsToRows([{ id: 0, text: "x" }], { value: window }, { memoryBudget: pool }),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
    expect(pool.usedBytes).toBe(0);
  }
});

it("keeps computed values admitted until checkpoint writes settle", async () => {
  const { memorySpillAdapter } = await import("./runtime.js");
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        yield [{ id: 0, text: "x" }];
      },
    },
  });
  let started!: () => void;
  let finish!: () => void;
  const ready = new Promise<void>((resolve) => {
    started = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const backing = memorySpillAdapter();
  const result = lake
    .path("table")
    .window("value", expression("first_value", replacement(10000)))
    .windowWithState({
      spill: {
        ...backing,
        async write(id, bytes) {
          started();
          await pending;
          return backing.write(id, bytes);
        },
      },
    });
  await ready;
  expect(pool.usedBytes).toBeGreaterThanOrEqual(20000);
  finish();
  expect((await result).rows[0]?.value).toBe("a".repeat(10000));
  expect(pool.usedBytes).toBe(0);
});

it.each(["rows", "runs"])("admits result records at the exact boundary (%s)", (route) => {
  // One array, two slots, two records with one property; runs add one outer array/slot.
  const bytes = 64 + 2 * 16 + 2 * (64 + 64) + (route === "runs" ? 80 : 0);
  for (const limit of [bytes, bytes - 1]) {
    const budget = new MemoryBudget(limit);
    const resultScope = new MemoryScope(budget);
    const rows = [{ id: 1 }, { id: 2 }];
    const run = () =>
      route === "rows"
        ? applyWindowsToRows(rows, {}, { memoryBudget: budget, resultScope })
        : applyWindowsToRuns([rows], {}, { memoryBudget: budget, resultScope });
    if (limit === bytes) {
      expect(run()).toEqual(route === "rows" ? rows : [rows]);
      expect(budget.usedBytes).toBe(bytes);
    } else {
      expect(run).toThrow(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
      expect(budget.usedBytes).toBe(0);
    }
    resultScope.dispose();
    expect(budget.usedBytes).toBe(0);
  }
});

it("stores special window aliases as own data without changing the result prototype", () => {
  const windows = Object.create(null) as Record<string, WindowExpr>;
  Object.defineProperty(windows, "__proto__", {
    value: expression("first_value", col("id")),
    enumerable: true,
  });
  const rows = applyWindowsToRows([{ id: 7 }], windows);
  expect(Object.getPrototypeOf(rows[0])).toBe(Object.prototype);
  expect(Object.hasOwn(rows[0] ?? {}, "__proto__")).toBe(true);
  expect(Object.getOwnPropertyDescriptor(rows[0], "__proto__")?.value).toBe(7);
});

it("admits selected vector materialization at its exact peak", async () => {
  const inputs = [
    {
      batch: batchFromColumns({ id: [1, 2], nested: [["猫", "x"], ["ignored"]] }),
      selection: Uint8Array.of(1, 0),
    },
  ];
  const measured = new MemoryBudget(65536);
  const scope = new MemoryScope(measured);
  expect(
    await applyWindowsToVectorBatches(inputs, {}, { memoryBudget: measured, resultScope: scope }),
  ).toEqual([{ id: 1, nested: ["猫", "x"] }]);
  expect(measured.usedBytes).toBeGreaterThan(0);
  const peak = measured.peakBytes;
  scope.dispose();
  expect(measured.usedBytes).toBe(0);
  for (const limit of [peak, peak - 1]) {
    const budget = new MemoryBudget(limit);
    const run = () => applyWindowsToVectorBatches(inputs, {}, { memoryBudget: budget });
    if (limit === peak) expect(await run()).toEqual([{ id: 1, nested: ["猫", "x"] }]);
    else await expect(run()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    expect(budget.usedBytes).toBe(0);
  }
});
