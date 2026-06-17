import { LakeqlError } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { formatSql, parseSql, parseSqlStatement } from "./index.js";

describe("parseSql", () => {
  it("parses DESCRIBE as a metadata statement without changing SELECT parsing", () => {
    expect(parseSqlStatement("describe input")).toEqual({ type: "describe", source: "input" });
    expect(parseSqlStatement('describe "sales table";')).toEqual({
      type: "describe",
      source: "sales table",
    });
    expect(parseSqlStatement("select * from input")).toMatchObject({
      source: "input",
      select: ["*"],
    });
    expect(() => parseSql("describe input")).toThrow(LakeqlError);
  });

  it("accepts select-first SQL with a later FROM clause", () => {
    expect(
      parseSql(`
        select store_id, amount
        from sales
        where amount > 100
        order by amount desc
        limit 10
      `),
    ).toMatchObject({
      source: "sales",
      select: ["store_id", "amount"],
      where: { kind: "compare", op: "gt" },
      orderBy: [{ column: "amount", direction: "desc" }],
      limit: 10,
    });
  });

  it("compiles the core VISION read-query shape", () => {
    expect(
      parseSql(`
        select store_id, date, amount
        from sales
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
        select id, name, lat, lon
        from places
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

  it("compiles SELECT DISTINCT", () => {
    expect(parseSql("select distinct region from sales order by region")).toMatchObject({
      source: "sales",
      select: ["region"],
      distinct: true,
      orderBy: [{ column: "region" }],
    });
    expect(formatSql(parseSql("select distinct region from sales"))).toContain(
      "select distinct region",
    );
  });

  it("binds positional SQL parameters as scalar literals", () => {
    const ast = parseSql(
      `
        select store_id, amount
        from sales
        where region = $1
          and amount between $2 and $3
          and active = $4
        order by amount desc
        limit $5
        offset $6
      `,
      { parameters: ["west", 100, 500, true, 10, 2] },
    );

    expect(ast).toMatchObject({
      where: {
        kind: "logical",
        operands: [
          { kind: "compare", right: { kind: "literal", value: "west" } },
          {
            kind: "between",
            low: { kind: "literal", value: 100 },
            high: { kind: "literal", value: 500 },
          },
          { kind: "compare", right: { kind: "literal", value: true } },
        ],
      },
      limit: 10,
      offset: 2,
    });
    expect(formatSql(ast)).toContain("region = 'west'");
  });

  it("binds parameters inside CTEs and scalar subqueries", () => {
    expect(
      parseSql(
        `
          with recent as (
            select store_id, amount
            from sales
            where amount > $1
          )
          select store_id
          from recent
          where amount < (select max(amount) as max_amount from sales where region = $2)
        `,
        { parameters: [100, "west"] },
      ),
    ).toMatchObject({
      cte: {
        query: {
          where: { kind: "compare", right: { kind: "literal", value: 100 } },
        },
      },
      scalarSubqueries: {
        scalar_0: {
          query: {
            where: { kind: "compare", right: { kind: "literal", value: "west" } },
          },
        },
      },
    });
  });

  it("rejects unbound or non-scalar SQL parameters", () => {
    expect(() => parseSql("select * from sales where amount > $1")).toThrow(LakeqlError);
    expect(() => parseSql("select * from sales where amount > $1")).toThrow(
      "SQL parameter $1 has no bound value",
    );
    expect(() =>
      parseSql("select * from sales where amount > $1", {
        parameters: [{ amount: 100 } as never],
      }),
    ).toThrow("SQL parameter $1 must be a scalar value");
    expect(() =>
      parseSql("select * from sales limit $1", {
        parameters: ["ten"],
      }),
    ).toThrow("LIMIT must be a non-negative integer");
  });

  it("compiles computed projections and CASE expressions", () => {
    const ast = parseSql(`
      select amount * 2 as doubled,
        case when amount > 100 then 'large' else 'small' end as bucket
      from sales
      where amount + 1 > 10
    `);

    expect(ast.projections).toMatchObject({
      doubled: { kind: "arithmetic", op: "mul" },
      bucket: { kind: "case" },
    });
    expect(ast.where).toMatchObject({
      kind: "compare",
      left: { kind: "arithmetic", op: "add" },
    });
    expect(parseSql(formatSql(ast))).toEqual(ast);

    expect(
      parseSql(`
        with totals as (
          select region, count(*) as rows, max(amount) as max_amount
          from sales
          group by region
        )
        select region, rows
        from totals
        where max_amount > 900
      `),
    ).toMatchObject({
      source: "totals",
      cte: {
        name: "totals",
        query: {
          source: "sales",
          select: ["region"],
          aggregates: {
            rows: { op: "count" },
            max_amount: { op: "max", column: "amount" },
          },
          groupBy: ["region"],
        },
      },
      where: { kind: "compare", left: { kind: "column", name: "max_amount" } },
    });

    expect(
      parseSql(`
        with enriched as (
          select distinct store_id, amount * 2 as doubled
          from sales
        )
        select store_id, doubled
        from enriched
        order by doubled
        limit 1
      `),
    ).toMatchObject({
      source: "enriched",
      cte: {
        name: "enriched",
        query: {
          distinct: true,
          select: ["store_id"],
          projections: { doubled: { kind: "arithmetic", op: "mul" } },
        },
      },
      orderBy: [{ column: "doubled" }],
      limit: 1,
    });
  });

  it("compiles aggregate query clauses", () => {
    expect(
      parseSql(`
        select event, count(*) as n, count_distinct(user_id) as users,
          approx_count_distinct(session_id) as sessions,
          quantile_cont(duration, 0.75) as duration_p75
        from events
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
        duration_p75: { op: "quantile", column: "duration", quantile: 0.75 },
      },
      groupBy: ["event"],
      having: { kind: "compare", op: "gt" },
      orderBy: [{ column: "n", direction: "desc" }],
    });

    const expressionAggregates = parseSql(`
      select region, sum(amount * 2) as doubled_total,
        count(distinct user_id) as users,
        avg(case when amount > 10 then amount else 0 end) as avg_amount
      from events
      group by region
    `);
    expect(expressionAggregates).toMatchObject({
      select: ["region"],
      aggregates: {
        doubled_total: { op: "sum", expr: { kind: "arithmetic", op: "mul" } },
        users: { op: "count_distinct", column: "user_id" },
        avg_amount: { op: "avg", expr: { kind: "case" } },
      },
      groupBy: ["region"],
    });
    expect(parseSql(formatSql(expressionAggregates))).toEqual(expressionAggregates);

    const parameterizedQuantile = parseSql(
      "select quantile_cont(amount * 2, $1) as amount_p90 from events",
      { parameters: [0.9] },
    );
    expect(parameterizedQuantile.aggregates).toMatchObject({
      amount_p90: { op: "quantile", expr: { kind: "arithmetic", op: "mul" }, quantile: 0.9 },
    });
    expect(parseSql(formatSql(parameterizedQuantile))).toEqual(parameterizedQuantile);
  });

  it("compiles bounded equi-join clauses", () => {
    const ast = parseSql(`
      select s.store_id, d.segment
      from sales s
      join stores d on s.store_id = d.store_id
      where d.segment = 'enterprise'
      order by s.store_id
      limit 3
    `);

    expect(ast).toMatchObject({
      source: "sales",
      select: ["s.store_id", "d.segment"],
      join: {
        source: "stores",
        alias: "d",
        type: "inner",
        leftKey: ["s.store_id"],
        rightKey: ["d.store_id"],
      },
      where: { kind: "compare", left: { kind: "column", name: "d.segment" } },
      orderBy: [{ column: "s.store_id" }],
      limit: 3,
    });
    expect(parseSql(formatSql(ast))).toEqual(ast);

    expect(parseSql("select * from sales left join stores using (store_id)")).toMatchObject({
      join: {
        source: "stores",
        alias: "stores",
        type: "left",
        leftKey: ["sales.store_id"],
        rightKey: ["stores.store_id"],
      },
    });

    expect(parseSql("select * from sales s join stores d using (store_id, region)")).toMatchObject({
      join: {
        source: "stores",
        alias: "d",
        type: "inner",
        leftKey: ["s.store_id", "s.region"],
        rightKey: ["d.store_id", "d.region"],
      },
    });

    expect(
      parseSql(`
        select *
        from sales s
        join stores d on s.store_id = d.store_id and s.region = d.region
      `),
    ).toMatchObject({
      join: {
        leftKey: ["s.store_id", "s.region"],
        rightKey: ["d.store_id", "d.region"],
      },
    });

    expect(
      parseSql("select * from sales s join stores d on d.store_id = s.store_id"),
    ).toMatchObject({
      join: {
        leftKey: ["s.store_id"],
        rightKey: ["d.store_id"],
      },
    });
  });

  it("compiles IN subqueries as semi and anti joins", () => {
    const ast = parseSql(`
      select store_id
      from sales
      where store_id in (select store_id from stores where segment = 'enterprise')
      order by store_id
    `);

    expect(ast).toMatchObject({
      source: "sales",
      select: ["store_id"],
      subqueryJoin: {
        source: "stores",
        type: "semi",
        leftKey: ["store_id"],
        rightKey: ["store_id"],
        where: { kind: "compare", left: { kind: "column", name: "segment" } },
      },
      orderBy: [{ column: "store_id" }],
    });
    expect(parseSql(formatSql(ast))).toEqual(ast);

    expect(
      parseSql(`
        select store_id
        from sales
        where (store_id, region) not in (select store_id, region from stores)
      `),
    ).toMatchObject({
      subqueryJoin: {
        type: "anti",
        leftKey: ["store_id", "region"],
        rightKey: ["store_id", "region"],
      },
    });
  });

  it("compiles simple non-recursive CTEs", () => {
    const ast = parseSql(`
      with recent as (
        select store_id, amount
        from sales
        where amount > 900
      )
      select store_id
      from recent
      order by amount desc
      limit 2
    `);

    expect(ast).toMatchObject({
      source: "recent",
      select: ["store_id"],
      cte: {
        name: "recent",
        query: {
          source: "sales",
          select: ["store_id", "amount"],
          where: { kind: "compare", op: "gt" },
        },
      },
      orderBy: [{ column: "amount", direction: "desc" }],
      limit: 2,
    });
    expect(parseSql(formatSql(ast))).toEqual(ast);
  });

  it("compiles scalar subqueries in WHERE expressions", () => {
    const aggregateScalar = parseSql(`
      select store_id
      from sales
      where amount = (select max(amount) as max_amount from sales)
    `);

    expect(aggregateScalar).toMatchObject({
      where: {
        kind: "compare",
        right: { kind: "call", fn: "__lakeql_scalar_subquery" },
      },
      scalarSubqueries: {
        scalar_0: {
          column: "max_amount",
          query: {
            source: "sales",
            aggregates: { max_amount: { op: "max", column: "amount" } },
          },
        },
      },
    });
    expect(parseSql(formatSql(aggregateScalar))).toEqual(aggregateScalar);

    expect(
      parseSql(`
        select store_id
        from sales
        where amount > (select amount from sales order by amount desc limit 1)
      `),
    ).toMatchObject({
      scalarSubqueries: {
        scalar_0: {
          column: "amount",
          query: { source: "sales", select: ["amount"], limit: 1 },
        },
      },
    });

    const projectionScalar = parseSql(`
      select store_id, (select max(amount) as max_amount from sales) as max_amount
      from sales
      limit 1
    `);
    expect(projectionScalar).toMatchObject({
      select: ["store_id"],
      projections: {
        max_amount: { kind: "call", fn: "__lakeql_scalar_subquery" },
      },
      scalarSubqueries: {
        scalar_0: {
          column: "max_amount",
          query: {
            source: "sales",
            aggregates: { max_amount: { op: "max", column: "amount" } },
          },
        },
      },
      limit: 1,
    });
    expect(parseSql(formatSql(projectionScalar))).toEqual(projectionScalar);
  });

  it("compiles additional VISION geospatial and H3 SQL examples", () => {
    expect(
      parseSql(`
        select parcel_id, owner
        from parcels
        where st_intersects(geom, st_bbox(-118.9, 34.1, -118.6, 34.3))
      `),
    ).toMatchObject({
      source: "parcels",
      select: ["parcel_id", "owner"],
      where: {
        kind: "call",
        fn: "st_intersects",
        args: [
          { kind: "column", name: "geom" },
          { kind: "call", fn: "st_bbox" },
        ],
      },
    });

    expect(
      parseSql(`
        select id, name
        from places
        where h3_within(h3_8, h3_cell(34.0522, -118.2437, 8), 2)
      `),
    ).toMatchObject({
      source: "places",
      select: ["id", "name"],
      where: {
        kind: "call",
        fn: "h3_within",
        args: [
          { kind: "column", name: "h3_8" },
          { kind: "call", fn: "h3_cell" },
          { kind: "literal", value: 2 },
        ],
      },
    });
  });

  it("round-trips parsed ASTs through SQL text", () => {
    const queries = [
      `
        select store_id, amount
        from sales
        where amount > 100
        order by amount desc
        limit 10
      `,
      `
        select store_id, date, amount
        from sales
        where region = 'west'
          and date between '2026-01-01' and '2026-06-01'
          and amount > 100
        order by amount desc
        limit 500
      `,
      `
        select id, name, lat, lon
        from places
        where country = 'US'
          and state = 'CA'
          and h3_within(h3_8, '8829a1d757fffff', 2)
          and st_intersects(geom, st_bbox(-118.35, 33.95, -118.15, 34.15))
        order by name asc nulls first
        limit 100
        offset 10
      `,
      `
        select event, count(*) as n, count_distinct(user_id) as users
        from events
        where date = '2026-06-10'
        group by event
        having n > 100
        order by n desc
      `,
      `
        select parcel_id, owner
        from parcels
        where st_intersects(geom, st_bbox(-118.9, 34.1, -118.6, 34.3))
      `,
      `
        select id, name
        from places
        where h3_within(h3_8, h3_cell(34.0522, -118.2437, 8), 2)
      `,
    ];

    for (const query of queries) {
      const ast = parseSql(query);
      expect(parseSql(formatSql(ast))).toEqual(ast);
    }
  });

  it("formats expression variants and comparison operators", () => {
    const ast = parseSql(`
      select id
      from t
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
      select id
      from t
      where active = true
        and deleted is not null
        and region in ('US', 'CA')
        and not (name ilike 'test%')
    `);

    expect(ast.where).toMatchObject({ kind: "logical", op: "and" });
    expect(
      parseSql(`
        select id
        from t
        where country not in ('US', 'CA')
          or name not like 'test%'
          or deleted is null
          or flag
      `).where,
    ).toMatchObject({ kind: "logical", op: "or" });
    expect(parseSql("select id from t where noop()").where).toEqual({
      kind: "call",
      fn: "noop",
      args: [],
    });
    expect(parseSql("select id from t where regexp_matches(name, '^a', 'i')").where).toEqual({
      kind: "call",
      fn: "regexp_matches",
      args: [
        { kind: "column", name: "name" },
        { kind: "literal", value: "^a" },
        { kind: "literal", value: "i" },
      ],
    });
  });

  it("compiles unary, null, between, and function argument variants", () => {
    const ast = parseSql(`
      select id,
        coalesce(name, 'unknown') as display_name,
        -amount as negative_amount
      from t
      where score not between 1 and 10
        and deleted is null
        and not active
        and note not ilike 'test%'
        and amount % 2 = 0
    `);

    expect(ast.projections).toMatchObject({
      display_name: {
        kind: "call",
        fn: "coalesce",
        args: [
          { kind: "column", name: "name" },
          { kind: "literal", value: "unknown" },
        ],
      },
      negative_amount: { kind: "arithmetic", op: "mul" },
    });
    expect(ast.where).toMatchObject({ kind: "logical", op: "and" });
    const formatted = formatSql(ast);
    expect(formatted).toContain("not (score between 1 and 10)");
    expect(formatted).toContain("deleted is null");
    expect(formatted).toContain("not (active)");
    expect(formatted).toContain("not (note ilike 'test%')");
    expect(formatted).toContain("amount % 2 = 0");
    expect(parseSql(formatted)).toEqual(ast);

    const regex = parseSql(`
      select regexp_replace(name, '(a)(b)', '\\2\\1') as swapped
      from t
      where regexp_matches(name, '^a', 'i')
    `);
    expect(formatSql(regex)).toContain("regexp_replace(name, '(a)(b)', '\\2\\1') as swapped");
    expect(parseSql(formatSql(regex))).toEqual(regex);
  });

  it("compiles alternate comparison and aggregate operators", () => {
    const comparisons = parseSql(`
      select id
      from t
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
        select min(a) as min_a, max(a) as max_a, avg(a) as avg_a, sum(a) as sum_a,
          approx_count_distinct(a) as approx_a,
          var(a) as var_a, variance(a) as variance_a, var_pop(a) as var_pop_a,
          stddev(a) as stddev_a, stddev_pop(a) as stddev_pop_a,
          median(a) as median_a,
          quantile_cont(a, 0.25) as p25_a,
          mode(a) as mode_a,
          first(a) as first_a, last(a) as last_a, any(a) as any_a
        from t
      `).aggregates,
    ).toMatchObject({
      min_a: { op: "min" },
      max_a: { op: "max" },
      avg_a: { op: "avg" },
      sum_a: { op: "sum" },
      approx_a: { op: "approx_count_distinct" },
      var_a: { op: "var_samp" },
      variance_a: { op: "var_samp" },
      var_pop_a: { op: "var_pop" },
      stddev_a: { op: "stddev_samp" },
      stddev_pop_a: { op: "stddev_pop" },
      median_a: { op: "median" },
      p25_a: { op: "quantile", quantile: 0.25 },
      mode_a: { op: "mode" },
      first_a: { op: "first" },
      last_a: { op: "last" },
      any_a: { op: "any" },
    });

    expect(parseSql("select count(*) from t").aggregates).toEqual({
      count: { op: "count" },
    });
    expect(parseSql("select id as ident from t").projections).toEqual({
      ident: { kind: "column", name: "id" },
    });
    expect(() => parseSql("select quantile_cont(a, 1.5) as p from t")).toThrow(
      "quantile_cont percentile must be a numeric literal between 0 and 1",
    );
  });

  it("formats less common expression nodes and literals", () => {
    expect(formatSql(parseSql("select amount - 1 as x from t"))).toContain("amount - 1 as x");
    expect(formatSql(parseSql("select amount / 2 as x from t"))).toContain("amount / 2 as x");
    expect(
      formatSql({
        source: "t",
        projections: {
          nothing: { kind: "literal", value: null },
          huge: { kind: "literal", value: 9007199254740993n },
        },
      }),
    ).toContain("select null as nothing, 9007199254740993 as huge");
    expect(() =>
      formatSql({
        source: "t",
        projections: { bad: { kind: "call", fn: "__lakeql_scalar_subquery", args: [] } },
      }),
    ).toThrow(/Invalid scalar subquery/u);
    expect(() =>
      formatSql({
        source: "t",
        projections: {
          missing: {
            kind: "call",
            fn: "__lakeql_scalar_subquery",
            args: [{ kind: "literal", value: "scalar_0" }],
          },
        },
      }),
    ).toThrow(/Missing scalar subquery/u);
    expect(() => parseSql("select 1 # 2 as x from t")).toThrow(/Unsupported comparison operator/u);
  });

  it("throws typed parse errors", () => {
    expect(() => parseSql("select id from t; select id from t")).toThrow(/one SELECT/u);
    expect(() => parseSql("delete from t")).toThrowError(LakeqlError);
    expect(() => parseSql("select from t")).toThrowError(LakeqlError);
    expect(() => parseSql("select id")).toThrow(/exactly one FROM/u);
    expect(() => parseSql("from a select id from b")).toThrowError(LakeqlError);
    expect(() => parseSql("select * as all_rows from t")).toThrow(/Aliases on SELECT \*/u);
    expect(() => parseSql("select id from t where id in 1")).toThrow(/IN subqueries/u);
    expect(() => parseSql("select id from t where a between 1")).toThrowError(LakeqlError);
    expect(() => parseSql("select sum() as x from t")).toThrow(/requires exactly one/u);
    expect(() => parseSql("select id from t limit -1")).toThrow(/non-negative integer/u);
    expect(() => parseSql("select id from t where name like 1")).toThrow(/LIKE pattern/u);
    expect(() => parseSql("select id from t where = 1")).toThrowError(LakeqlError);
    expect(() => parseSql("select id + 1 from t")).toThrow(/explicit alias/u);
    expect(() => parseSql("select count(distinct *) as x from t")).toThrow(/COUNT\(DISTINCT \*\)/u);
    expect(() => parseSql("select id from t group by ,")).toThrowError(LakeqlError);
    expect(() => parseSql("select id from t order by a nulls middle")).toThrowError(LakeqlError);
    expect(() => parseSql("select id from t where name = 'unterminated")).toThrowError(LakeqlError);
    expect(() => parseSql("select id from t where name = @bad")).toThrowError(LakeqlError);
    expect(() => parseSql("select id from t where id = (select id from t)")).toThrow(
      /Scalar subqueries/u,
    );
    expect(() => parseSql("select sum(distinct amount) as total from t")).toThrow(
      /Only COUNT\(DISTINCT x\)/u,
    );
    expect(() => parseSql("select id from t where a ^ 1")).toThrowError(LakeqlError);
    expect(() => parseSql("select id from t where +a")).toThrowError(LakeqlError);
    expect(() => formatSql({ source: "bad source" })).toThrow(/cannot be represented/u);
  });

  it("rejects SQL outside the documented subset with typed parse errors", () => {
    const unsupported = [
      "with a as (select id from t), b as (select id from t) select id from a",
      "with recent as (select * from orders join customers on orders.customer_id = customers.id) select * from recent",
      "select * from orders full join customers on orders.customer_id = customers.id",
      "select * from orders cross join customers",
      "select * from orders join customers on orders.customer_id > customers.id",
      "select id from (select id from orders) orders",
      "select id from orders where id in (select order_id from refunds order by order_id)",
      "select id from orders o where id in (select order_id from refunds r where r.customer_id = o.customer_id)",
      "select case status when 'paid' then 1 else 0 end as paid from orders",
      "select row_number() over (partition by customer_id order by id) as rn from orders",
      "select sum(total) over (partition by customer_id order by id) as running_total from orders",
    ];

    for (const sql of unsupported) {
      expect(() => parseSql(sql)).toThrowError(LakeqlError);
      expect(() => parseSql(sql)).toThrow(
        expect.objectContaining({
          code: expect.stringMatching(/^LAKEQL_(PARSE_ERROR|SQL_UNSUPPORTED)$/u),
        }),
      );
    }
  });

  it("rejects excessive expression nesting and token counts with parse errors", () => {
    expect(() => parseSql(`select id from t where ${"not ".repeat(129)}a`)).toThrow(
      /nesting exceeds/u,
    );
    expect(() => parseSql(`select id from t where a = 1 ${" ".repeat(128_000)}`)).toThrow(
      /input length exceeds/u,
    );
  });
});
