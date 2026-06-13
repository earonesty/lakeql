import { describe, expect, it } from "vitest";
import { formatSql, parseSql } from "./index.js";

describe("parseSql", () => {
  it("compiles the core VISION read-query shape", () => {
    expect(
      parseSql(`
        from sales
        select store_id, date, amount
        where region = 'west'
          and date between '2026-01-01' and '2026-06-01'
          and amount > 100
        order by amount desc
        limit 500
      `),
    ).toMatchObject({
      source: "sales",
      select: ["store_id", "date", "amount"],
      where: {
        kind: "logical",
        op: "and",
        operands: [
          { kind: "compare", op: "eq" },
          { kind: "between" },
          { kind: "compare", op: "gt" },
        ],
      },
      orderBy: [{ column: "amount", direction: "desc" }],
      limit: 500,
    });
  });

  it("compiles function predicates, literals, order nulls, and offset", () => {
    expect(
      parseSql(`
        from places
        select id, name, lat, lon
        where country = 'US'
          and state = 'CA'
          and h3_within(h3_8, '8829a1d757fffff', 2)
          and st_intersects(geom, st_bbox(-118.35, 33.95, -118.15, 34.15))
        order by name asc nulls first
        limit 100
        offset 10
      `),
    ).toMatchObject({
      source: "places",
      select: ["id", "name", "lat", "lon"],
      where: {
        kind: "logical",
        op: "and",
      },
      orderBy: [{ column: "name", direction: "asc", nulls: "first" }],
      limit: 100,
      offset: 10,
    });
  });

  it("compiles aggregate query clauses", () => {
    expect(
      parseSql(`
        from events
        select event, count() as n, count_distinct(user_id) as users,
          approx_count_distinct(session_id) as sessions
        where date = '2026-06-10'
        group by event
        having n > 100
        order by n desc
      `),
    ).toMatchObject({
      source: "events",
      select: ["event"],
      aggregates: {
        n: { op: "count" },
        users: { op: "count_distinct", column: "user_id" },
        sessions: { op: "approx_count_distinct", column: "session_id" },
      },
      groupBy: ["event"],
      having: { kind: "compare", op: "gt" },
      orderBy: [{ column: "n", direction: "desc" }],
    });
  });

  it("round-trips parsed ASTs through SQL text", () => {
    const queries = [
      `
        from sales
        select store_id, date, amount
        where region = 'west'
          and date between '2026-01-01' and '2026-06-01'
          and amount > 100
        order by amount desc
        limit 500
      `,
      `
        from places
        select id, name, lat, lon
        where country = 'US'
          and state = 'CA'
          and h3_within(h3_8, '8829a1d757fffff', 2)
          and st_intersects(geom, st_bbox(-118.35, 33.95, -118.15, 34.15))
        order by name asc nulls first
        limit 100
        offset 10
      `,
      `
        from events
        select event, count() as n, count_distinct(user_id) as users
        where date = '2026-06-10'
        group by event
        having n > 100
        order by n desc
      `,
    ];

    for (const query of queries) {
      const ast = parseSql(query);
      expect(parseSql(formatSql(ast))).toEqual(ast);
    }
  });

  it("formats expression variants and comparison operators", () => {
    const ast = parseSql(`
      from t
      select id
      where a != 1
        and b < 2
        and c <= 3
        and d >= 4
        and region not in ('US', 'CA')
        and deleted is not null
        and not (name ilike 'test%')
        and note like 'ok%'
    `);

    const formatted = formatSql(ast);
    expect(formatted).toContain("a != 1");
    expect(formatted).toContain("b < 2");
    expect(formatted).toContain("c <= 3");
    expect(formatted).toContain("d >= 4");
    expect(formatted).toContain("region not in ('US', 'CA')");
    expect(formatted).toContain("deleted is not null");
    expect(formatted).toContain("not (name ilike 'test%')");
    expect(formatted).toContain("note like 'ok%'");
    expect(parseSql(formatted)).toEqual(ast);
  });

  it("supports boolean, null, in, like, not, and parenthesized expressions", () => {
    const ast = parseSql(`
      from t
      select id
      where active = true
        and deleted is not null
        and region in ('US', 'CA')
        and not (name ilike 'test%')
    `);

    expect(ast.where).toMatchObject({ kind: "logical", op: "and" });
    expect(
      parseSql(`
        from t
        select id
        where country not in ('US', 'CA')
          or name not like 'test%'
          or deleted is null
          or flag
      `).where,
    ).toMatchObject({ kind: "logical", op: "or" });
    expect(parseSql("from t select id where noop()").where).toEqual({
      kind: "call",
      fn: "noop",
      args: [],
    });
  });

  it("compiles alternate comparison and aggregate operators", () => {
    const comparisons = parseSql(`
      from t
      select id
      where a != 1
        and b <> 2
        and c < 3
        and d <= 4
        and e >= 5
    `);
    expect(comparisons.where).toMatchObject({
      kind: "logical",
      operands: [{ op: "ne" }, { op: "ne" }, { op: "lt" }, { op: "lte" }, { op: "gte" }],
    });

    expect(
      parseSql(`
        from t
        select min(a) as min_a, max(a) as max_a, avg(a) as avg_a, sum(a) as sum_a,
          approx_count_distinct(a) as approx_a,
          first(a) as first_a, last(a) as last_a, any(a) as any_a
      `).aggregates,
    ).toMatchObject({
      min_a: { op: "min" },
      max_a: { op: "max" },
      avg_a: { op: "avg" },
      sum_a: { op: "sum" },
      approx_a: { op: "approx_count_distinct" },
      first_a: { op: "first" },
      last_a: { op: "last" },
      any_a: { op: "any" },
    });
  });

  it("throws typed parse errors", () => {
    expect(() => parseSql("select id from t")).toThrow(/Expected FROM/u);
    expect(() => parseSql("from t select id where a between 1")).toThrow(/AND/u);
    expect(() => parseSql("from t select nope() as x")).toThrow(/Unsupported aggregate/u);
    expect(() => parseSql("from t limit -1")).toThrow(/non-negative integer/u);
    expect(() => parseSql("from t where name like 1")).toThrow(/LIKE pattern/u);
    expect(() => parseSql("from t where name not between 1 and 2")).toThrow(/Expected IN/u);
    expect(() => parseSql("from t where = 1")).toThrow(/Expected expression/u);
    expect(() => parseSql("from t select id nope")).toThrow(/Unexpected token/u);
    expect(() => parseSql("from t group by ,")).toThrow(/Expected column/u);
    expect(() => parseSql("from t order by a nulls middle")).toThrow(/Expected FIRST/u);
    expect(() => parseSql("from t where name = 'unterminated")).toThrow(/Unterminated/u);
    expect(() => parseSql("from t where name = @bad")).toThrow(/Unexpected character/u);
    expect(() => formatSql({ source: "bad source" })).toThrow(/cannot be represented/u);
  });
});
