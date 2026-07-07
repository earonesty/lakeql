import { intervalValue, LakeqlError } from "lakeql-core";
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

  it("maps read_parquet FROM calls to path sources", () => {
    expect(parseSql("select * from read_parquet('events/year=*/*.parquet') p")).toMatchObject({
      source: "events/year=*/*.parquet",
      select: ["*"],
    });
    expect(() => parseSql("select * from read_parquet(1)")).toThrow(/string literal/u);
    expect(() => parseSql("select * from read_parquet('a.parquet', 'b.parquet')")).toThrow(
      /exactly one/u,
    );
    expect(() => parseSql("select * from generate_series(1)")).toThrow(
      /Only read_parquet FROM functions/u,
    );
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

    const simpleCase = parseSql(`
      select case status
        when 'paid' then 1
        when 'open' then 2
        else 0
      end as status_rank
      from orders
    `);
    expect(simpleCase.projections).toMatchObject({
      status_rank: {
        kind: "case",
        whens: [
          {
            when: {
              kind: "compare",
              op: "eq",
              left: { kind: "column", name: "status" },
              right: { kind: "literal", value: "paid" },
            },
            value: { kind: "literal", value: 1 },
          },
          {
            when: {
              kind: "compare",
              op: "eq",
              left: { kind: "column", name: "status" },
              right: { kind: "literal", value: "open" },
            },
            value: { kind: "literal", value: 2 },
          },
        ],
        else: { kind: "literal", value: 0 },
      },
    });
    expect(parseSql(formatSql(simpleCase))).toEqual(simpleCase);

    const nestedCase = parseSql(`
      select case
        when amount > 100 then case status when 'paid' then 'large-paid' else 'large-open' end
        else 'small'
      end as bucket
      from orders
    `);
    expect(nestedCase.projections).toMatchObject({
      bucket: {
        kind: "case",
        whens: [
          {
            value: {
              kind: "case",
              whens: [
                {
                  when: {
                    kind: "compare",
                    op: "eq",
                    left: { kind: "column", name: "status" },
                    right: { kind: "literal", value: "paid" },
                  },
                  value: { kind: "literal", value: "large-paid" },
                },
              ],
              else: { kind: "literal", value: "large-open" },
            },
          },
        ],
        else: { kind: "literal", value: "small" },
      },
    });
    expect(parseSql(formatSql(nestedCase))).toEqual(nestedCase);

    expect(() =>
      parseSql("select case when amount > 100 then 1 else 'small' end as bucket from orders"),
    ).toThrow("CASE result branches must have compatible literal types");

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

    const directHaving = parseSql(`
      select region
      from events
      group by region
      having count(*) > 2 and max(amount) > 10
      order by region
    `);
    expect(directHaving).toMatchObject({
      source: "events",
      select: ["region"],
      groupBy: ["region"],
      hiddenAggregates: ["__lakeql_having_0", "__lakeql_having_1"],
      aggregates: {
        __lakeql_having_0: { op: "count" },
        __lakeql_having_1: { op: "max", column: "amount" },
      },
      having: {
        kind: "logical",
        op: "and",
        operands: [
          { kind: "compare", left: { kind: "column", name: "__lakeql_having_0" } },
          { kind: "compare", left: { kind: "column", name: "__lakeql_having_1" } },
        ],
      },
    });
    expect(parseSql(formatSql(directHaving))).toEqual(directHaving);

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

  it("round-trips rich computed aggregate queries", () => {
    const ast = parseSql(
      `
      select region,
        date_trunc('month', loaded_at) as loaded_month,
        case
          when amount >= 1000 then 'large'
          when amount >= 100 then 'medium'
          else 'small'
        end as amount_bucket,
        sum(round(amount, 2)) as rounded_total,
        avg(nullif(discount, 0)) as avg_discount,
        max(coalesce(updated_at, loaded_at)) as last_seen
      from events
      where regexp_matches(source, '^web|app$', 'i')
        and not (status in ('cancelled', 'void'))
        and amount between 0 and $1
      group by region, loaded_month, amount_bucket
      having rounded_total > 10
      order by loaded_month desc nulls last, region asc
      limit $2
      offset $3
    `,
      {
        parameters: [5000, 25, 5],
      },
    );

    expect(ast).toMatchObject({
      source: "events",
      select: ["region"],
      projections: {
        loaded_month: { kind: "call", fn: "date_trunc" },
        amount_bucket: {
          kind: "case",
          whens: [
            { when: { kind: "compare", op: "gte" } },
            { when: { kind: "compare", op: "gte" } },
          ],
          else: { kind: "literal", value: "small" },
        },
      },
      aggregates: {
        rounded_total: { op: "sum", expr: { kind: "call", fn: "round" } },
        avg_discount: { op: "avg", expr: { kind: "call", fn: "nullif" } },
        last_seen: { op: "max", expr: { kind: "call", fn: "coalesce" } },
      },
      groupBy: ["region", "loaded_month", "amount_bucket"],
      having: { kind: "compare", left: { kind: "column", name: "rounded_total" } },
      orderBy: [
        { column: "loaded_month", direction: "desc", nulls: "last" },
        { column: "region", direction: "asc" },
      ],
      limit: 25,
      offset: 5,
    });
    expect(parseSql(formatSql(ast))).toEqual(ast);

    const nested = parseSql(`
      select id
      from (
        select id, amount
        from (
          select id, amount
          from orders
          where amount > 10
        ) inner_filtered
        where amount < 100
      ) outer_filtered
      order by amount asc
    `);

    expect(nested).toMatchObject({
      source: "outer_filtered",
      cte: {
        name: "outer_filtered",
        query: {
          source: "inner_filtered",
          cte: {
            name: "inner_filtered",
            query: {
              source: "orders",
              where: { kind: "compare", op: "gt" },
            },
          },
          where: { kind: "compare", op: "lt" },
        },
      },
      orderBy: [{ column: "amount", direction: "asc" }],
    });
    expect(parseSql(formatSql(nested))).toEqual(nested);
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

    expect(parseSql("select * from sales right join stores using (store_id)")).toMatchObject({
      join: {
        source: "stores",
        alias: "stores",
        type: "right",
        leftKey: ["sales.store_id"],
        rightKey: ["stores.store_id"],
      },
    });

    expect(parseSql("select * from sales full join stores using (store_id)")).toMatchObject({
      join: {
        source: "stores",
        alias: "stores",
        type: "full",
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

    const nonEqui = parseSql(`
      select s.store_id, b.bucket
      from sales s
      join buckets b on s.amount >= b.min_amount and s.amount < b.max_amount
      order by s.store_id
    `);
    expect(nonEqui).toMatchObject({
      joins: [
        {
          source: "buckets",
          alias: "b",
          type: "inner",
          leftKey: [],
          rightKey: [],
          predicate: {
            kind: "logical",
            op: "and",
            operands: [
              { kind: "compare", op: "gte" },
              { kind: "compare", op: "lt" },
            ],
          },
        },
      ],
    });
    expect(formatSql(nonEqui)).toContain(
      "join buckets b on (s.amount >= b.min_amount) and (s.amount < b.max_amount)",
    );
    expect(parseSql(formatSql(nonEqui))).toEqual(nonEqui);

    expect(
      parseSql("select * from sales s left join buckets b on s.amount < b.max_amount"),
    ).toMatchObject({
      join: {
        type: "left",
        predicate: { kind: "compare", op: "lt" },
      },
    });
    expect(
      parseSql("select * from sales s right join buckets b on s.amount < b.max_amount"),
    ).toMatchObject({
      join: {
        type: "right",
        predicate: { kind: "compare", op: "lt" },
      },
    });
    expect(
      parseSql("select * from sales s full join buckets b on s.amount < b.max_amount"),
    ).toMatchObject({
      join: {
        type: "full",
        predicate: { kind: "compare", op: "lt" },
      },
    });

    const chain = parseSql(`
      select s.store_id, d.segment, r.manager
      from sales s
      join stores d on s.store_id = d.store_id
      left join regions r on d.region = r.region
      where r.manager is not null
      order by s.store_id
    `);
    expect(chain).toMatchObject({
      joins: [
        {
          source: "stores",
          alias: "d",
          type: "inner",
          leftKey: ["s.store_id"],
          rightKey: ["d.store_id"],
        },
        {
          source: "regions",
          alias: "r",
          type: "left",
          leftKey: ["d.region"],
          rightKey: ["r.region"],
        },
      ],
      select: ["s.store_id", "d.segment", "r.manager"],
      where: { kind: "null-check", negated: true, target: { kind: "column", name: "r.manager" } },
    });
    expect(chain.join).toEqual(chain.joins?.[0]);
    expect(parseSql(formatSql(chain))).toEqual(chain);

    const cross = parseSql("select * from stores s cross join regions r");
    expect(cross).toMatchObject({
      joins: [
        {
          source: "regions",
          alias: "r",
          type: "cross",
          leftKey: [],
          rightKey: [],
        },
      ],
    });
    expect(formatSql(cross)).toContain("cross join regions r");
    expect(parseSql("select * from stores s, regions r")).toMatchObject({
      joins: [{ type: "cross", source: "regions", alias: "r" }],
    });
    expect(
      formatSql(parseSql("select * from sales s right join stores d using (store_id)")),
    ).toContain("right join stores d");
    expect(
      formatSql(parseSql("select * from sales s full join stores d using (store_id)")),
    ).toContain("full join stores d");
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

    const correlated = parseSql(`
      select id
      from orders o
      where id in (
        select order_id
        from refunds r
        where r.customer_id = o.customer_id
          and r.reason = 'duplicate'
      )
    `);
    expect(correlated).toMatchObject({
      subqueryJoin: {
        source: "refunds",
        type: "semi",
        leftKey: ["id", "customer_id"],
        rightKey: ["order_id", "customer_id"],
        where: { kind: "compare", left: { kind: "column", name: "reason" } },
      },
    });
    expect(parseSql(formatSql(correlated))).toEqual(correlated);

    const predicateCorrelated = parseSql(`
      select id
      from orders o
      where id in (
        select order_id
        from refunds r
        where r.amount > o.amount
          and r.reason = 'duplicate'
      )
    `);
    expect(predicateCorrelated).toMatchObject({
      subqueryJoin: {
        source: "refunds",
        type: "semi",
        leftKey: ["id"],
        rightKey: ["order_id"],
        leftAlias: "o",
        alias: "r",
        predicate: {
          kind: "compare",
          op: "gt",
          left: { kind: "column", name: "r.amount" },
          right: { kind: "column", name: "o.amount" },
        },
        where: { kind: "compare", left: { kind: "column", name: "reason" } },
      },
    });
    expect(parseSql(formatSql(predicateCorrelated))).toEqual(predicateCorrelated);

    const orderedLimited = parseSql(`
      select store_id
      from sales
      where store_id in (
        select store_id
        from stores
        order by store_id asc
        limit 2
        offset 1
      )
    `);
    expect(orderedLimited).toMatchObject({
      subqueryJoin: {
        source: "stores",
        type: "semi",
        leftKey: ["store_id"],
        rightKey: ["store_id"],
        orderBy: [{ column: "store_id", direction: "asc" }],
        limit: 2,
        offset: 1,
      },
    });
    expect(parseSql(formatSql(orderedLimited))).toEqual(orderedLimited);
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

  it("rejects recursive CTEs with an explicit SQL error", () => {
    expect(() =>
      parseSql("with recursive recent as (select store_id from sales) select store_id from recent"),
    ).toThrow("Recursive CTEs are not supported");
    expect(() =>
      parseSql("with recursive recent as (select store_id from sales) select store_id from recent"),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_SQL_UNSUPPORTED" }));
  });

  it("compiles single-source derived tables through CTE materialization", () => {
    const ast = parseSql(`
      select id
      from (
        select id, amount
        from orders
        where amount > 10
      ) filtered
      where id > 1
      order by amount desc
      limit 5
    `);

    expect(ast).toMatchObject({
      source: "filtered",
      cte: {
        name: "filtered",
        query: {
          source: "orders",
          select: ["id", "amount"],
          where: { kind: "compare", op: "gt" },
        },
      },
      select: ["id"],
      where: { kind: "compare", left: { kind: "column", name: "id" } },
      orderBy: [{ column: "amount", direction: "desc" }],
      limit: 5,
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

  it("compiles uncorrelated EXISTS subqueries through scalar counts", () => {
    const exists = parseSql(`
      select store_id
      from sales
      where exists (
        select 1
        from stores
        where segment = 'enterprise'
      )
    `);

    expect(exists).toMatchObject({
      where: {
        kind: "compare",
        op: "gt",
        left: { kind: "call", fn: "__lakeql_scalar_subquery" },
        right: { kind: "literal", value: 0 },
      },
      scalarSubqueries: {
        scalar_0: {
          column: "__lakeql_exists_count",
          query: {
            source: "stores",
            aggregates: { __lakeql_exists_count: { op: "count" } },
            where: { kind: "compare", left: { kind: "column", name: "segment" } },
          },
        },
      },
    });
    expect(parseSql(formatSql(exists))).toEqual(exists);

    const notExists = parseSql(`
      select store_id
      from sales
      where not exists (select 1 from stores)
    `);
    expect(notExists.where).toMatchObject({
      kind: "not",
      operand: {
        kind: "compare",
        op: "gt",
        left: { kind: "call", fn: "__lakeql_scalar_subquery" },
        right: { kind: "literal", value: 0 },
      },
    });
    expect(parseSql(formatSql(notExists))).toEqual(notExists);
  });

  it("compiles correlated EXISTS subqueries as semi and anti joins", () => {
    const exists = parseSql(`
      select id
      from orders o
      where exists (
        select 1
        from refunds r
        where r.order_id = o.id
          and r.reason = 'duplicate'
      )
    `);

    expect(exists).toMatchObject({
      subqueryJoin: {
        source: "refunds",
        type: "semi",
        leftKey: ["id"],
        rightKey: ["order_id"],
        where: { kind: "compare", left: { kind: "column", name: "reason" } },
      },
    });
    expect(parseSql(formatSql(exists))).toEqual(exists);

    const notExists = parseSql(`
      select id
      from orders o
      where not exists (
        select 1
        from refunds r
        where r.order_id = o.id
      )
    `);

    expect(notExists).toMatchObject({
      subqueryJoin: {
        source: "refunds",
        type: "anti",
        leftKey: ["id"],
        rightKey: ["order_id"],
      },
    });
    expect(parseSql(formatSql(notExists))).toEqual(notExists);

    const predicateExists = parseSql(`
      select id
      from orders o
      where exists (
        select 1
        from refunds r
        where r.amount > o.amount
          and r.reason = 'duplicate'
      )
    `);

    expect(predicateExists).toMatchObject({
      subqueryJoin: {
        source: "refunds",
        type: "semi",
        leftAlias: "o",
        alias: "r",
        leftKey: [],
        rightKey: [],
        predicate: {
          kind: "compare",
          op: "gt",
          left: { kind: "column", name: "r.amount" },
          right: { kind: "column", name: "o.amount" },
        },
        where: { kind: "compare", left: { kind: "column", name: "reason" } },
      },
    });
    expect(parseSql(formatSql(predicateExists))).toEqual(predicateExists);
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
    expect(() => parseSql("select id")).toThrow(/FROM table/u);
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
    const unsupported = ["with a as (select id from t), b as (select id from t) select id from a"];

    for (const sql of unsupported) {
      expect(() => parseSql(sql)).toThrowError(LakeqlError);
      expect(() => parseSql(sql)).toThrow(
        expect.objectContaining({
          code: expect.stringMatching(/^LAKEQL_(PARSE_ERROR|SQL_UNSUPPORTED)$/u),
        }),
      );
    }
  });

  it("compiles basic window functions into core window specs", () => {
    expect(
      parseSql(
        "select row_number() over (partition by customer_id order by id) as rn, sum(total) over (partition by customer_id order by id) as running_total from orders",
      ),
    ).toMatchObject({
      source: "orders",
      select: ["rn", "running_total"],
      windows: {
        rn: {
          fn: "row_number",
          args: [],
          over: {
            partitionBy: [{ kind: "column", name: "customer_id" }],
            orderBy: [{ expr: { kind: "column", name: "id" } }],
          },
        },
        running_total: {
          fn: { aggregate: "sum" },
          args: [{ kind: "column", name: "total" }],
          over: {
            partitionBy: [{ kind: "column", name: "customer_id" }],
            orderBy: [{ expr: { kind: "column", name: "id" } }],
          },
        },
      },
    });
  });

  it("extracts QUALIFY predicates with window expressions", () => {
    expect(
      parseSql(
        "select customer_id, id from orders qualify row_number() over (partition by customer_id order by id) = 1 order by id",
      ),
    ).toMatchObject({
      source: "orders",
      select: ["customer_id", "id"],
      qualify: {
        kind: "compare",
        op: "eq",
        left: { kind: "column", name: "__lakeql_window_0" },
        right: { kind: "literal", value: 1 },
      },
      windows: {
        __lakeql_window_0: {
          fn: "row_number",
          over: {
            partitionBy: [{ kind: "column", name: "customer_id" }],
            orderBy: [{ expr: { kind: "column", name: "id" } }],
          },
        },
      },
      orderBy: [{ column: "id" }],
    });
  });

  it("uses unique synthetic aliases for unaliased window projections", () => {
    const ast = parseSql(
      "select row_number() over (order by a), row_number() over (order by b desc) from t",
    );
    expect(ast.select).toEqual(["__lakeql_window_0", "__lakeql_window_1"]);
    expect(Object.keys(ast.windows ?? {})).toEqual(["__lakeql_window_0", "__lakeql_window_1"]);
    expect(ast.windows?.__lakeql_window_0?.over.orderBy).toEqual([
      { expr: { kind: "column", name: "a" } },
    ]);
    expect(ast.windows?.__lakeql_window_1?.over.orderBy).toEqual([
      { expr: { kind: "column", name: "b" }, direction: "desc" },
    ]);
  });

  it("rejects duplicate explicit output aliases", () => {
    expect(() => parseSql("select id, row_number() over (order by id) as id from orders")).toThrow(
      "Duplicate SELECT output alias id",
    );
  });

  it("rejects output aliases that would mutate plain object prototypes", () => {
    for (const alias of ['"__proto__"', '"constructor"', '"prototype"']) {
      expect(() => parseSql(`select id as ${alias} from orders`)).toThrow(
        "Unsafe SELECT output alias",
      );
      expect(() =>
        parseSql(`select row_number() over (order by id) as ${alias} from orders`),
      ).toThrow("Unsafe SELECT output alias");
    }
  });

  it("runs the window prepass on QUALIFY predicates", () => {
    const framed = parseSql(`
      select id
      from orders
      qualify sum(total) over (order by id rows between 1 preceding and current row) > 10
    `);
    expect(framed.windows?.__lakeql_window_0?.over.frame).toMatchObject({
      mode: "rows",
      start: { kind: "preceding" },
      end: { kind: "current-row" },
    });

    const named = parseSql(`
      select id
      from orders
      qualify row_number() over ranked = 1
      window ranked as (partition by customer_id order by id)
    `);
    expect(named.windows?.__lakeql_window_0?.over).toMatchObject({
      partitionBy: [{ kind: "column", name: "customer_id" }],
      orderBy: [{ expr: { kind: "column", name: "id" } }],
    });
  });

  it("keeps QUALIFY and synthetic window aliases scoped to the owning SELECT", () => {
    const ast = parseSql(`
        with recent as (
          select customer_id, id
          from orders
          where id > 10
        )
        select customer_id, id
        from recent
        qualify row_number() over (partition by customer_id order by id) = 1
      `);
    expect(ast).toMatchObject({
      cte: {
        query: {
          source: "orders",
          select: ["customer_id", "id"],
          where: { kind: "compare", op: "gt" },
        },
      },
      windows: {
        __lakeql_window_0: {
          fn: "row_number",
          over: {
            partitionBy: [{ kind: "column", name: "customer_id" }],
            orderBy: [{ expr: { kind: "column", name: "id" } }],
          },
        },
      },
      qualify: {
        kind: "compare",
        left: { kind: "column", name: "__lakeql_window_0" },
        right: { kind: "literal", value: 1 },
      },
    });
    expect(ast.cte?.query.windows).toBeUndefined();
    expect(ast.cte?.query.qualify).toBeUndefined();
  });

  it("compiles aggregate window FILTER predicates and count distinct windows", () => {
    expect(
      parseSql(`
        select
          sum(total) filter (where status = 'paid') over (partition by customer_id order by id) as paid_total,
          count(distinct user_id) over (partition by customer_id) as distinct_users
        from orders
      `),
    ).toMatchObject({
      windows: {
        paid_total: {
          fn: { aggregate: "sum" },
          filter: {
            kind: "compare",
            op: "eq",
            left: { kind: "column", name: "status" },
            right: { kind: "literal", value: "paid" },
          },
        },
        distinct_users: {
          fn: { aggregate: "count_distinct" },
          args: [{ kind: "column", name: "user_id" }],
        },
      },
    });
  });

  it("compiles window null-treatment modifiers into value window specs", () => {
    expect(
      parseSql(`
        select
          first_value(total ignore nulls) over (partition by customer_id order by id) as first_paid,
          lag(total) respect nulls over (partition by customer_id order by id) as previous_total
        from orders
      `),
    ).toMatchObject({
      windows: {
        first_paid: {
          fn: "first_value",
          ignoreNulls: true,
        },
        previous_total: {
          fn: "lag",
          ignoreNulls: false,
        },
      },
    });
  });

  it("compiles named window clauses and inherited named windows", () => {
    expect(
      parseSql(`
        select
          row_number() over ranked as rn,
          sum(total) over (running rows between 1 preceding and current row) as rolling_total
        from orders
        window ranked as (partition by customer_id order by id),
          running as (ranked)
      `),
    ).toMatchObject({
      windows: {
        rn: {
          fn: "row_number",
          over: {
            partitionBy: [{ kind: "column", name: "customer_id" }],
            orderBy: [{ expr: { kind: "column", name: "id" } }],
          },
        },
        rolling_total: {
          fn: { aggregate: "sum" },
          over: {
            partitionBy: [{ kind: "column", name: "customer_id" }],
            orderBy: [{ expr: { kind: "column", name: "id" } }],
            frame: {
              mode: "rows",
              start: { kind: "preceding", offset: { kind: "literal", value: 1 } },
              end: { kind: "current-row" },
              exclude: "no-others",
            },
          },
        },
      },
    });
  });

  it("compiles explicit window frame clauses", () => {
    expect(
      parseSql(
        "select sum(total) over (partition by customer_id order by id rows between 1 preceding and current row exclude current row) as prior_total from orders",
      ),
    ).toMatchObject({
      windows: {
        prior_total: {
          fn: { aggregate: "sum" },
          over: {
            frame: {
              mode: "rows",
              start: { kind: "preceding", offset: { kind: "literal", value: 1 } },
              end: { kind: "current-row" },
              exclude: "current-row",
            },
          },
        },
      },
    });

    expect(
      parseSql(
        "select last_value(total) over (partition by customer_id order by id groups between current row and 1 following exclude ties) as grouped_value from orders",
      ),
    ).toMatchObject({
      windows: {
        grouped_value: {
          fn: "last_value",
          over: {
            frame: {
              mode: "groups",
              start: { kind: "current-row" },
              end: { kind: "following", offset: { kind: "literal", value: 1 } },
              exclude: "ties",
            },
          },
        },
      },
    });

    expect(
      parseSql(
        "select sum(total) over (partition by customer_id order by created_at range between interval '1 day' preceding and current row) as day_total from orders",
      ),
    ).toMatchObject({
      windows: {
        day_total: {
          fn: { aggregate: "sum" },
          over: {
            frame: {
              mode: "range",
              start: {
                kind: "preceding",
                offset: { kind: "literal", value: intervalValue("1 day") },
              },
              end: { kind: "current-row" },
              exclude: "no-others",
            },
          },
        },
      },
    });
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
