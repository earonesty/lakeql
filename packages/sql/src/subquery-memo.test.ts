import { SharedMemoryCache, TimestampValue } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { parseSql, type SqlQueryAst } from "./index.js";
import {
  resolveSqlCorrelatedLookups,
  resolveSqlScalarSubqueries,
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
  expect(values[0]?.rows.get("[1]")?.max).toBe(10);
  expect(values[0]?.rows.get("[2]")?.max).toBe(99);
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
