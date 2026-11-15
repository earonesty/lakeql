import { type Expr, SharedMemoryCache, TimestampValue } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { parseSql, type SqlQueryAst } from "./index.js";
import {
  resolveSqlCorrelatedLookups,
  resolveSqlScalarSubqueries,
  sqlCorrelationKey,
  sqlMemoKey,
} from "./subquery-memo.js";

it("deduplicates equivalent scalar subqueries despite generated IDs and parent registries", async () => {
  const ast = parseSql(
    "SELECT id, (SELECT MAX(amount) FROM purchases) AS a, (SELECT MAX(amount) FROM purchases) AS b FROM accounts",
  );
  const execute = vi.fn(async () => [{ max: 20 }]);
  const out = await resolveSqlScalarSubqueries(ast, execute, new SharedMemoryCache());
  expect(execute).toHaveBeenCalledTimes(1);
  expect(out.projections).toMatchObject({
    a: { kind: "literal", value: 20 },
    b: { kind: "literal", value: 20 },
  });
  expect(out.scalarSubqueries).toBeUndefined();
});

it("deduplicates correlated preparation while keeping all correlation keys distinct", async () => {
  const inner = "(SELECT MAX(p.amount) FROM purchases p WHERE p.group_id = a.group_id)";
  const ast = parseSql(`SELECT a.id, ${inner} AS x, ${inner} AS y FROM accounts a`);
  const execute = vi.fn(async () => [
    { group_id: 1, max: 10 },
    { group_id: 2, max: 99 },
  ]);
  const lookups = await resolveSqlCorrelatedLookups(ast, execute, new SharedMemoryCache());
  expect(execute).toHaveBeenCalledTimes(1);
  const values = [...lookups.values()];
  expect(values).toHaveLength(2);
  expect(values[0]?.rows).toBe(values[1]?.rows);
  expect(values[0]?.rows.get(sqlCorrelationKey({ key: 1 }, ["key"]) ?? "")?.max).toBe(10);
  expect(values[0]?.rows.get(sqlCorrelationKey({ key: 2 }, ["key"]) ?? "")?.max).toBe(99);
});

it("supports bound file paths and preserves typed parameter identity", () => {
  const plan = (value: unknown): SqlQueryAst =>
    ({
      source: "/tmp/file.parquet",
      where: {
        kind: "compare",
        op: "eq",
        left: { kind: "column", name: "id" },
        right: { kind: "literal", value },
      },
    }) as SqlQueryAst;
  const keys = [
    1,
    1n,
    "1",
    -0,
    0,
    new TimestampValue(1n, "nanos"),
    new TimestampValue(1n, "micros"),
  ].map((value) => sqlMemoKey(plan(value)));
  expect(new Set(keys).size).toBe(keys.length);
  expect(sqlMemoKey(plan(1))).toBe(sqlMemoKey(plan(1)));
});

it("includes actual nested dependencies in the key and rejects dependency cycles", () => {
  const query: SqlQueryAst = {
    source: "outer",
    projections: {
      value: {
        kind: "call",
        fn: "__lakeql_scalar_subquery",
        args: [{ kind: "literal", value: "s" }],
      },
    },
    scalarSubqueries: { s: { query: { source: "inner" }, column: "value" } },
  };
  const before = sqlMemoKey(query);
  const dependency = query.scalarSubqueries?.s;
  if (dependency === undefined) throw Error("missing fixture");
  dependency.query.source = "another";
  expect(sqlMemoKey(query)).not.toBe(before);
  dependency.query = query;
  expect(() => sqlMemoKey(query)).toThrow(/Cyclic/);
});

it("resolves only actual nested dependencies through shared parser registries", async () => {
  const inner =
    "(SELECT MAX(amount) FROM purchases WHERE amount > (SELECT AVG(amount) FROM purchases))";
  const ast = parseSql(`SELECT ${inner} AS a, ${inner} AS b FROM accounts`);
  const execute = vi.fn(async (query: SqlQueryAst) => {
    expect(query.scalarSubqueries).toBeUndefined();
    if (query.where === undefined) return [{ avg: 20 }];
    expect(query.where).toMatchObject({ right: { kind: "literal", value: 20 } });
    return [{ max: 30 }];
  });
  const out = await resolveSqlScalarSubqueries(ast, execute, new SharedMemoryCache());
  expect(execute).toHaveBeenCalledTimes(2);
  expect(out.projections).toMatchObject({
    a: { kind: "literal", value: 30 },
    b: { kind: "literal", value: 30 },
  });
});

it("rewrites scalar subqueries in aggregate arguments", async () => {
  const ast = parseSql(
    "SELECT SUM(amount + (SELECT MAX(bonus) FROM bonuses)) AS total FROM purchases",
  );
  const out = await resolveSqlScalarSubqueries(
    ast,
    async () => [{ max: 5 }],
    new SharedMemoryCache(),
  );
  expect(out.aggregates?.total?.expr).toMatchObject({ right: { kind: "literal", value: 5 } });
  expect(out.scalarSubqueries).toBeUndefined();
});

it("uses typed correlation keys and excludes null matches", async () => {
  const key = (value: unknown) => sqlCorrelationKey({ key: value }, ["key"]);
  expect(
    new Set([1n, 1, "1", "bigint:1", 9007199254740993n, "9007199254740993"].map(key)).size,
  ).toBe(6);
  expect(key(null)).toBeUndefined();
  expect(key(undefined)).toBeUndefined();
  const ast = parseSql(
    "SELECT (SELECT MAX(p.amount) FROM purchases p WHERE p.id = a.id) AS x FROM accounts a",
  );
  const lookups = await resolveSqlCorrelatedLookups(
    ast,
    async () => [
      { id: 1n, max: 10 },
      { id: 1, max: 20 },
      { id: "1", max: 30 },
      { id: null, max: 40 },
    ],
    new SharedMemoryCache(),
  );
  const rows = [...lookups.values()][0]?.rows;
  expect(rows?.size).toBe(3);
  expect(rows?.get(key(1n) ?? "")?.max).toBe(10);
  expect(rows?.get(key(1) ?? "")?.max).toBe(20);
  expect(rows?.get(key("1") ?? "")?.max).toBe(30);
});

it("resolves correlated residual scalar dependencies with or without prior root resolution", async () => {
  const correlated =
    "(SELECT MAX(p.amount) FROM purchases p WHERE p.id = a.id AND p.amount > (SELECT AVG(amount) FROM purchases))";
  for (const resolveRoot of [false, true]) {
    const ast = parseSql(`SELECT a.id, ${correlated} AS x, ${correlated} AS y FROM accounts a`);
    const execute = vi.fn(async (query: SqlQueryAst) => {
      expect(query.scalarSubqueries).toBeUndefined();
      if (query.where === undefined) return [{ avg: 20 }];
      expect(query.where).toMatchObject({ right: { kind: "literal", value: 20 } });
      return [{ id: 1n, max: 30 }];
    });
    const memo = new SharedMemoryCache();
    const root = resolveRoot ? await resolveSqlScalarSubqueries(ast, execute, memo) : ast;
    const lookups = await resolveSqlCorrelatedLookups(root, execute, memo);
    expect(execute).toHaveBeenCalledTimes(2);
    expect(lookups.size).toBe(2);
    const values = [...lookups.values()];
    expect(values[0]?.rows).toBe(values[1]?.rows);
    expect(values[0]?.rows.get(sqlCorrelationKey({ id: 1n }, ["id"]) ?? "")?.max).toBe(30);
  }
});

it("resolves scalar dependencies across QUALIFY and every window expression slot", async () => {
  const placeholder = (): Expr => ({
    kind: "call",
    fn: "__lakeql_scalar_subquery",
    args: [{ kind: "literal", value: "s" }],
  });
  const ast: SqlQueryAst = {
    source: "purchases",
    qualify: placeholder(),
    windows: {
      total: {
        fn: { aggregate: "sum" },
        args: [placeholder()],
        filter: placeholder(),
        ignoreNulls: true,
        over: {
          partitionBy: [placeholder()],
          orderBy: [{ expr: placeholder(), direction: "desc", nulls: "first" }],
          frame: {
            mode: "rows",
            start: { kind: "preceding", offset: placeholder() },
            end: { kind: "following", offset: placeholder() },
            exclude: "ties",
          },
        },
      },
    },
    scalarSubqueries: { s: { query: { source: "bonuses" }, column: "value" } },
  };
  const before = structuredClone(ast);
  const execute = vi.fn(async () => [{ value: 5 }]);
  const out = await resolveSqlScalarSubqueries(ast, execute, new SharedMemoryCache());
  expect(execute).toHaveBeenCalledTimes(1);
  expect(out.scalarSubqueries).toBeUndefined();
  expect(JSON.stringify(out)).not.toContain("__lakeql_scalar_subquery");
  expect(out.windows?.total).toMatchObject({
    args: [{ kind: "literal", value: 5 }],
    filter: { kind: "literal", value: 5 },
    ignoreNulls: true,
    over: {
      partitionBy: [{ kind: "literal", value: 5 }],
      orderBy: [{ expr: { kind: "literal", value: 5 }, direction: "desc", nulls: "first" }],
      frame: {
        mode: "rows",
        start: { offset: { value: 5 } },
        end: { offset: { value: 5 } },
        exclude: "ties",
      },
    },
  });
  expect(out.qualify).toEqual({ kind: "literal", value: 5 });
  expect(ast).toEqual(before);
});

it("resolves aggregate and HAVING dependencies in grouped predicate subqueries", async () => {
  const ast = parseSql(
    "SELECT amount FROM purchases WHERE group_id IN (SELECT group_id FROM purchases GROUP BY group_id HAVING MAX(amount + (SELECT MAX(bonus) FROM purchases)) > (SELECT MAX(amount) FROM purchases))",
  );
  const execute = vi.fn(async (query: SqlQueryAst) => [
    { max: query.aggregates?.max?.column === "bonus" ? 5 : 30 },
  ]);
  const out = await resolveSqlScalarSubqueries(ast, execute, new SharedMemoryCache());
  expect(execute).toHaveBeenCalledTimes(2);
  expect(JSON.stringify(out)).not.toContain("__lakeql_scalar_subquery");
  expect(out.subqueryJoin?.having).toMatchObject({ right: { kind: "literal", value: 30 } });
});
