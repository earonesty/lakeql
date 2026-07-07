import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DuckDBConnection } from "@duckdb/node-api";
import { memoryStore, type Row, stableStringify } from "lakeql-core";
import { fixturePath, SALES } from "lakeql-fixtures";
import { writeParquet } from "lakeql-parquet";
import { describe, expect, it } from "vitest";
import { runCli } from "./index.js";

const describeReference = process.env.LAKEQL_REFERENCE === "1" ? describe : describe.skip;

describeReference("SQL CLI DuckDB reference comparisons", () => {
  it.each([
    {
      name: "filtered ordered scan",
      lakeql:
        "select store_id, region, amount from input where region = 'west' order by amount asc limit 5",
      duckdb: `select store_id, region, amount from read_parquet('${sqlString(fixturePath(SALES.file))}') where region = 'west' order by amount asc limit 5`,
    },
    {
      name: "distinct projection",
      lakeql: "select distinct region from input order by region asc",
      duckdb: `select distinct region from read_parquet('${sqlString(fixturePath(SALES.file))}') order by region asc`,
    },
    {
      name: "computed projection",
      lakeql:
        "select store_id, amount * 2 as doubled, case when amount > 10 then 'large' else 'small' end as bucket from input where amount < 20 order by amount asc limit 2",
      duckdb: `select store_id, amount * 2 as doubled, case when amount > 10 then 'large' else 'small' end as bucket from read_parquet('${sqlString(fixturePath(SALES.file))}') where amount < 20 order by amount asc limit 2`,
    },
    {
      name: "grouped aggregate",
      lakeql:
        "select region, count(*) as rows, max(amount) as max_amount from input group by region order by region asc",
      duckdb: `select region, count(*) as rows, max(amount) as max_amount from read_parquet('${sqlString(fixturePath(SALES.file))}') group by region order by region asc`,
    },
    {
      name: "grouped aggregate over computed alias",
      lakeql:
        "select case when amount < 500 then 'small' else 'large' end as bucket, count(*) as rows from input group by bucket order by bucket asc",
      duckdb: `select case when amount < 500 then 'small' else 'large' end as bucket, count(*) as rows from read_parquet('${sqlString(fixturePath(SALES.file))}') group by bucket order by bucket asc`,
    },
    {
      name: "grouped aggregate expression and count distinct",
      lakeql:
        "select region, max(amount * 2) as max_doubled, count(distinct store_id) as stores from input group by region order by region asc",
      duckdb: `select region, max(amount * 2) as max_doubled, count(distinct store_id) as stores from read_parquet('${sqlString(fixturePath(SALES.file))}') group by region order by region asc`,
    },
    {
      name: "global aggregate",
      lakeql: "select count(*) as rows, max(amount) as max_amount from input",
      duckdb: `select count(*) as rows, max(amount) as max_amount from read_parquet('${sqlString(fixturePath(SALES.file))}')`,
    },
    {
      name: "multiple group keys",
      lakeql:
        "select region, store_id, count(*) as rows from input group by region, store_id order by region asc, store_id asc limit 5",
      duckdb: `select region, store_id, count(*) as rows from read_parquet('${sqlString(fixturePath(SALES.file))}') group by region, store_id order by region asc, store_id asc limit 5`,
    },
    {
      name: "group-by-only projection",
      lakeql: "select region from input group by region order by region asc",
      duckdb: `select region from read_parquet('${sqlString(fixturePath(SALES.file))}') group by region order by region asc`,
    },
    {
      name: "grouped count without group projection",
      lakeql: "select count(*) as rows from input group by region order by region asc",
      duckdb: `select count(*) as rows from read_parquet('${sqlString(fixturePath(SALES.file))}') group by region order by region asc`,
    },
    {
      name: "distinct grouped count projection",
      lakeql:
        "select distinct count(*) as rows from input group by region order by region asc limit 2",
      duckdb: `select distinct count(*) as rows from read_parquet('${sqlString(fixturePath(SALES.file))}') group by region order by region asc limit 2`,
    },
    {
      name: "grouped aggregate having",
      lakeql:
        "select region, count(*) as rows, max(amount) as max_amount from input group by region having max_amount > 980 order by region asc limit 2",
      duckdb: `select region, count(*) as rows, max(amount) as max_amount from read_parquet('${sqlString(fixturePath(SALES.file))}') group by region having max_amount > 980 order by region asc limit 2`,
    },
    {
      name: "simple filtered CTE",
      lakeql:
        "with recent as (select store_id, amount from input where amount > 900) select store_id, amount from recent order by amount desc limit 2",
      duckdb: `with recent as (select store_id, amount from read_parquet('${sqlString(fixturePath(SALES.file))}') where amount > 900) select store_id, amount from recent order by amount desc limit 2`,
    },
    {
      name: "aggregate CTE",
      lakeql:
        "with totals as (select region, count(*) as rows, max(amount) as max_amount from input group by region) select region, rows from totals where max_amount > 990 order by region asc",
      duckdb: `with totals as (select region, count(*) as rows, max(amount) as max_amount from read_parquet('${sqlString(fixturePath(SALES.file))}') group by region) select region, rows from totals where max_amount > 990 order by region asc`,
    },
    {
      name: "aggregate scalar subquery",
      lakeql:
        "select store_id, amount from input where amount = (select max(amount) as max_amount from input)",
      duckdb: `select store_id, amount from read_parquet('${sqlString(fixturePath(SALES.file))}') where amount = (select max(amount) as max_amount from read_parquet('${sqlString(fixturePath(SALES.file))}'))`,
    },
    {
      name: "limit scalar subquery",
      lakeql:
        "select store_id, amount from input where amount >= (select amount from input order by amount desc limit 1)",
      duckdb: `select store_id, amount from read_parquet('${sqlString(fixturePath(SALES.file))}') where amount >= (select amount from read_parquet('${sqlString(fixturePath(SALES.file))}') order by amount desc limit 1)`,
    },
    {
      name: "projection scalar subquery",
      lakeql:
        "select store_id, (select max(amount) as max_amount from input) as max_amount from input order by amount asc limit 1",
      duckdb: `select store_id, (select max(amount) as max_amount from read_parquet('${sqlString(fixturePath(SALES.file))}')) as max_amount from read_parquet('${sqlString(fixturePath(SALES.file))}') order by amount asc limit 1`,
    },
    {
      name: "running aggregate window",
      lakeql:
        "select region, store_id, amount, sum(amount) over (partition by region order by amount asc, store_id asc) as running_amount from input order by region asc, amount asc, store_id asc limit 12",
      duckdb: `select region, store_id, amount, sum(amount) over (partition by region order by amount asc, store_id asc) as running_amount from read_parquet('${sqlString(fixturePath(SALES.file))}') order by region asc, amount asc, store_id asc limit 12`,
    },
    {
      name: "qualify top rows per partition",
      lakeql:
        "select region, store_id, amount from input qualify row_number() over (partition by region order by amount desc, store_id asc) <= 2 order by region asc, amount desc, store_id asc",
      duckdb: `select region, store_id, amount from read_parquet('${sqlString(fixturePath(SALES.file))}') qualify row_number() over (partition by region order by amount desc, store_id asc) <= 2 order by region asc, amount desc, store_id asc`,
    },
    {
      name: "rows frame moving aggregate window",
      lakeql:
        "select region, store_id, amount, sum(amount) over (partition by region order by amount asc, store_id asc rows between 1 preceding and current row) as moving_amount from input order by region asc, amount asc, store_id asc limit 12",
      duckdb: `select region, store_id, amount, sum(amount) over (partition by region order by amount asc, store_id asc rows between 1 preceding and current row) as moving_amount from read_parquet('${sqlString(fixturePath(SALES.file))}') order by region asc, amount asc, store_id asc limit 12`,
    },
    {
      name: "groups frame peer aggregate window",
      lakeql:
        "select region, store_id, amount, count(*) over (partition by region order by floor(amount / 100) groups between current row and 1 following) as peer_rows from input order by region asc, amount asc, store_id asc limit 12",
      duckdb: `select region, store_id, amount, count(*) over (partition by region order by floor(amount / 100) groups between current row and 1 following) as peer_rows from read_parquet('${sqlString(fixturePath(SALES.file))}') order by region asc, amount asc, store_id asc limit 12`,
    },
    {
      name: "range frame numeric offset window",
      lakeql:
        "select region, store_id, amount, count(*) over (partition by region order by amount range between 25 preceding and current row) as nearby_rows from input order by region asc, amount asc, store_id asc limit 12",
      duckdb: `select region, store_id, amount, count(*) over (partition by region order by amount range between 25 preceding and current row) as nearby_rows from read_parquet('${sqlString(fixturePath(SALES.file))}') order by region asc, amount asc, store_id asc limit 12`,
    },
    {
      name: "filtered aggregate window",
      lakeql:
        "select region, store_id, amount, sum(amount) filter (where amount >= 500) over (partition by region order by amount asc, store_id asc) as running_large_amount from input order by region asc, amount asc, store_id asc limit 12",
      duckdb: `select region, store_id, amount, sum(amount) filter (where amount >= 500) over (partition by region order by amount asc, store_id asc) as running_large_amount from read_parquet('${sqlString(fixturePath(SALES.file))}') order by region asc, amount asc, store_id asc limit 12`,
    },
    {
      name: "named window clause",
      lakeql:
        "select region, store_id, amount, row_number() over ranked as rn from input window ranked as (partition by region order by amount desc, store_id asc) order by region asc, rn asc limit 12",
      duckdb: `select region, store_id, amount, row_number() over ranked as rn from read_parquet('${sqlString(fixturePath(SALES.file))}') window ranked as (partition by region order by amount desc, store_id asc) order by region asc, rn asc limit 12`,
    },
  ])("matches DuckDB for $name", async ({ lakeql, duckdb }) => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      lakeql,
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for deterministic randomized window differential seeds", async () => {
    const fixture = fixturePath(SALES.file);
    for (const testCase of randomizedWindowCases(0x5eed_c0de, 18)) {
      const lakeql = testCase.sql("input");
      const duckdb = testCase.sql(`read_parquet('${sqlString(fixture)}')`);
      const result = await runCli([
        "query",
        "--path",
        fixture,
        "--sql",
        lakeql,
        "--format",
        "json",
      ]);

      try {
        expect(result).toMatchObject({ exitCode: 0, stderr: "" });
        const lakeqlRows = JSON.parse(result.stdout) as Row[];
        const referenceRows = await duckDbRows(duckdb);
        expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
      } catch (cause) {
        throw new Error(`randomized window differential failed seed=${testCase.seed}`, { cause });
      }
    }
  });

  it("matches DuckDB for timestamp RANGE interval window frames", async () => {
    const path = await timestampWindowFixturePath();
    const lakeql =
      "select account, id, amount, sum(amount) over (partition by account order by event_ts range between interval '1 day' preceding and current row) as day_amount from input order by account asc, id asc";
    const duckdb = `select account, id, amount, sum(amount) over (partition by account order by event_ts range between interval '1 day' preceding and current row) as day_amount from read_parquet('${sqlString(path)}') order by account asc, id asc`;

    const result = await runCli(["query", "--path", path, "--sql", lakeql, "--format", "json"]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it.each([
    {
      name: "ranking and distribution windows",
      sql: (table: string) =>
        `select account, id, row_number() over (partition by account order by amount asc, id asc) as rn, rank() over (partition by account order by bucket asc) as rnk, dense_rank() over (partition by account order by bucket asc) as dense, percent_rank() over (partition by account order by amount asc, id asc) as pct, cume_dist() over (partition by account order by amount asc, id asc) as cume, ntile(3) over (partition by account order by amount asc, id asc) as tile from ${table} order by account asc, id asc`,
    },
    {
      name: "value windows with null defaults",
      sql: (table: string) =>
        `select account, id, lag(amount, 1, -1) over (partition by account order by id asc) as prev_amount, lead(amount, 2, -1) over (partition by account order by id asc) as next2_amount, first_value(label) over (partition by account order by id asc rows between 1 preceding and current row) as first_label, last_value(label) over (partition by account order by id asc rows between current row and 1 following) as last_label, nth_value(label, 2) over (partition by account order by id asc rows between current row and 2 following) as second_label from ${table} order by account asc, id asc`,
    },
    {
      name: "holistic aggregate windows",
      sql: (table: string) =>
        `select account, id, median(amount) over (partition by account order by id asc rows between unbounded preceding and current row) as median_amount, quantile_cont(amount, 0.75) over (partition by account order by id asc rows between unbounded preceding and current row) as p75_amount, mode(label) over (partition by account order by id asc rows between unbounded preceding and current row) as mode_label, count(distinct label) over (partition by account order by id asc rows between unbounded preceding and current row) as distinct_labels from ${table} order by account asc, id asc`,
    },
    {
      name: "statistical and filtered aggregate windows",
      sql: (table: string) =>
        `select account, id, min(amount) over (partition by account order by id asc rows between unbounded preceding and current row) as min_amount, max(amount) over (partition by account order by id asc rows between unbounded preceding and current row) as max_amount, round(var_pop(amount) over (partition by account order by id asc rows between unbounded preceding and current row), 12) as varp_amount, round(stddev_samp(amount) over (partition by account order by id asc rows between unbounded preceding and current row), 12) as stds_amount, sum(amount) filter (where label = 'hot') over (partition by account order by id asc rows between unbounded preceding and current row) as hot_amount from ${table} order by account asc, id asc`,
    },
    {
      name: "exclude frame modes",
      sql: (table: string) =>
        `select account, id, sum(amount) over (partition by account order by bucket asc groups between unbounded preceding and current row exclude current row) as without_current, sum(amount) over (partition by account order by bucket asc groups between unbounded preceding and current row exclude group) as without_group, sum(amount) over (partition by account order by bucket asc groups between unbounded preceding and current row exclude ties) as without_ties from ${table} order by account asc, id asc`,
    },
    {
      name: "bigint and dictionary-like string ordering",
      sql: (table: string) =>
        `select account, id, big_score, label, dense_rank() over (partition by label order by big_score asc) as big_rank, min(big_score) over (partition by label order by big_score asc rows between unbounded preceding and current row) as min_big, first_value(account) over (partition by label order by big_score asc, id asc) as first_account from ${table} order by label asc, big_score asc, id asc`,
    },
    {
      name: "timestamp interval range and qualify",
      sql: (table: string) =>
        `select account, id, amount, sum(amount) over (partition by account order by event_ts range between interval '2 days' preceding and current row) as amount_2d from ${table} qualify row_number() over (partition by account order by event_ts desc, id asc) <= 3 order by account asc, id asc`,
    },
  ])("matches DuckDB for expanded window matrix: $name", async ({ sql }) => {
    const path = await windowMatrixFixturePath();
    const lakeql = sql("input");
    const duckdb = sql(`read_parquet('${sqlString(path)}')`);

    const result = await runCli(["query", "--path", path, "--sql", lakeql, "--format", "json"]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for bounded inner join", async () => {
    const storesPath = await storesFixturePath();
    const lakeql =
      "select s.store_id as store_id, s.amount as amount, d.segment as segment from sales s join stores d on s.store_id = d.store_id where d.segment = 'enterprise' order by s.amount asc limit 2";
    const duckdb = `select s.store_id as store_id, s.amount as amount, d.segment as segment from read_parquet('${sqlString(
      fixturePath(SALES.file),
    )}') s join read_parquet('${sqlString(
      storesPath,
    )}') d on s.store_id = d.store_id where d.segment = 'enterprise' order by s.amount asc limit 2`;

    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      lakeql,
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for bounded inner join with side filters", async () => {
    const storesPath = await storesFixturePath();
    const lakeql =
      "select s.store_id as store_id, s.amount as amount, d.segment as segment from sales s join stores d on s.store_id = d.store_id where s.amount < 40 and d.segment = 'enterprise' order by s.amount asc limit 2";
    const duckdb = `select s.store_id as store_id, s.amount as amount, d.segment as segment from read_parquet('${sqlString(
      fixturePath(SALES.file),
    )}') s join read_parquet('${sqlString(
      storesPath,
    )}') d on s.store_id = d.store_id where s.amount < 40 and d.segment = 'enterprise' order by s.amount asc limit 2`;

    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      lakeql,
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for bounded multi-key inner join", async () => {
    const storesPath = await storesFixturePath();
    const lakeql =
      "select s.store_id as store_id, s.region as region, d.segment as segment from sales s join stores d on s.store_id = d.store_id and s.region = d.region order by s.amount asc limit 2";
    const duckdb = `select s.store_id as store_id, s.region as region, d.segment as segment from read_parquet('${sqlString(
      fixturePath(SALES.file),
    )}') s join read_parquet('${sqlString(
      storesPath,
    )}') d on s.store_id = d.store_id and s.region = d.region order by s.amount asc limit 2`;

    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      lakeql,
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for bounded multi-key USING join", async () => {
    const storesPath = await storesFixturePath();
    const lakeql =
      "select s.store_id as store_id, s.region as region, d.segment as segment from sales s join stores d using (store_id, region) order by s.amount asc limit 2";
    const duckdb = `select s.store_id as store_id, s.region as region, d.segment as segment from read_parquet('${sqlString(
      fixturePath(SALES.file),
    )}') s join read_parquet('${sqlString(
      storesPath,
    )}') d using (store_id, region) order by s.amount asc limit 2`;

    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      lakeql,
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for bounded left join nulls", async () => {
    const storesPath = await storesFixturePath();
    const lakeql =
      "select s.store_id as store_id, d.segment as segment from sales s left join stores d on s.store_id = d.store_id where s.store_id = 'store-005' order by s.amount asc limit 1";
    const duckdb = `select s.store_id as store_id, d.segment as segment from read_parquet('${sqlString(
      fixturePath(SALES.file),
    )}') s left join read_parquet('${sqlString(
      storesPath,
    )}') d on s.store_id = d.store_id where s.store_id = 'store-005' order by s.amount asc limit 1`;

    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      lakeql,
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for bounded right join nulls", async () => {
    const storesPath = await rightStoresFixturePath();
    const lakeql =
      "select d.store_id as store_id, s.amount as amount, d.segment as segment from sales s right join stores d on s.store_id = d.store_id order by d.store_id desc, s.amount asc nulls last limit 2";
    const duckdb = `select d.store_id as store_id, s.amount as amount, d.segment as segment from read_parquet('${sqlString(
      fixturePath(SALES.file),
    )}') s right join read_parquet('${sqlString(
      storesPath,
    )}') d on s.store_id = d.store_id order by d.store_id desc, s.amount asc nulls last limit 2`;

    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      lakeql,
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for bounded full join nulls", async () => {
    const storesPath = await rightStoresFixturePath();
    const lakeql =
      "select distinct s.store_id as sales_store, d.store_id as dim_store, d.segment as segment from sales s full join stores d on s.store_id = d.store_id where s.store_id = 'store-005' or d.store_id = 'store-999' order by s.store_id asc nulls last, d.store_id asc nulls last";
    const duckdb = `select distinct s.store_id as sales_store, d.store_id as dim_store, d.segment as segment from read_parquet('${sqlString(
      fixturePath(SALES.file),
    )}') s full join read_parquet('${sqlString(
      storesPath,
    )}') d on s.store_id = d.store_id where s.store_id = 'store-005' or d.store_id = 'store-999' order by s.store_id asc nulls last, d.store_id asc nulls last`;

    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      lakeql,
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for bounded left join right-side WHERE filters", async () => {
    const storesPath = await storesFixturePath();
    const lakeql =
      "select s.store_id as store_id, d.segment as segment from sales s left join stores d on s.store_id = d.store_id where d.segment = 'enterprise' order by s.amount asc limit 2";
    const duckdb = `select s.store_id as store_id, d.segment as segment from read_parquet('${sqlString(
      fixturePath(SALES.file),
    )}') s left join read_parquet('${sqlString(
      storesPath,
    )}') d on s.store_id = d.store_id where d.segment = 'enterprise' order by s.amount asc limit 2`;

    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      lakeql,
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for IN subquery semi join", async () => {
    const storesPath = await storesFixturePath();
    const lakeql =
      "select store_id, amount from sales where store_id in (select store_id from stores where segment = 'enterprise') order by amount asc limit 2";
    const duckdb = `select store_id, amount from read_parquet('${sqlString(
      fixturePath(SALES.file),
    )}') where store_id in (select store_id from read_parquet('${sqlString(
      storesPath,
    )}') where segment = 'enterprise') order by amount asc limit 2`;

    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      lakeql,
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for ordered limited IN subquery semi join", async () => {
    const storesPath = await rightStoresFixturePath();
    const lakeql =
      "select distinct store_id from sales where store_id in (select store_id from stores order by store_id asc limit 1) order by store_id";
    const duckdb = `select distinct store_id from read_parquet('${sqlString(
      fixturePath(SALES.file),
    )}') where store_id in (select store_id from read_parquet('${sqlString(
      storesPath,
    )}') order by store_id asc limit 1) order by store_id`;

    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      lakeql,
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });

  it("matches DuckDB for NOT IN subquery anti join", async () => {
    const storesPath = await storesFixturePath();
    const lakeql =
      "select store_id, amount from sales where store_id not in (select store_id from stores) order by amount asc limit 2";
    const duckdb = `select store_id, amount from read_parquet('${sqlString(
      fixturePath(SALES.file),
    )}') where store_id not in (select store_id from read_parquet('${sqlString(
      storesPath,
    )}')) order by amount asc limit 2`;

    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      lakeql,
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const lakeqlRows = JSON.parse(result.stdout) as Row[];
    const referenceRows = await duckDbRows(duckdb);
    expect(canonicalRows(lakeqlRows)).toEqual(canonicalRows(referenceRows));
  });
});

async function duckDbRows(sql: string): Promise<Row[]> {
  const connection = await DuckDBConnection.create();
  const result = await connection.runAndReadAll(sql);
  return result.getRowObjectsJson() as Row[];
}

function canonicalRows(rows: Row[]): string[] {
  return rows.map((row) => stableStringify(normalizeRow(row)));
}

function normalizeRow(row: Row): Row {
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [
      key,
      typeof value === "string" && /^-?\d+$/u.test(value) ? Number(value) : value,
    ]),
  );
}

function sqlString(value: string): string {
  return value.replaceAll("'", "''");
}

interface RandomizedWindowCase {
  seed: number;
  sql(table: string): string;
}

function randomizedWindowCases(seed: number, count: number): RandomizedWindowCase[] {
  const random = seededRandom(seed);
  const cases: RandomizedWindowCase[] = [];
  const specs = [
    "partition by region order by amount asc, store_id asc",
    "partition by region order by amount desc, store_id asc",
    "order by amount asc, store_id asc",
    "partition by region order by floor(amount / 100) asc, amount asc",
    "partition by region order by amount",
  ];
  const frames = [
    "",
    " rows between 2 preceding and current row",
    " rows between current row and 2 following",
    " groups between current row and 1 following",
    " range between 25 preceding and current row",
  ];
  const expressions = [
    (over: string) => `sum(amount) over (${over}) as w`,
    (over: string) => `avg(amount) over (${over}) as w`,
    (over: string) => `count(*) over (${over}) as w`,
    (over: string) => `count(distinct store_id) over (${over}) as w`,
    (over: string) => `sum(amount) filter (where amount >= 500) over (${over}) as w`,
    (over: string) => `row_number() over (${over}) as w`,
    (over: string) => `rank() over (${over}) as w`,
    (over: string) => `dense_rank() over (${over}) as w`,
    (over: string) => `lag(amount, 1, -1) over (${over}) as w`,
    (over: string) => `first_value(amount) over (${over}) as w`,
    (over: string) => `last_value(amount) over (${over}) as w`,
    (over: string) => `nth_value(amount, 2) over (${over}) as w`,
  ];

  for (let index = 0; index < count; index += 1) {
    const caseSeed = random.nextSeed();
    const spec = random.pick(specs);
    const frame = compatibleFrame(spec, random.pick(frames));
    const expression = random.pick(expressions)(`${spec}${frame}`);
    cases.push({
      seed: caseSeed,
      sql: (table) =>
        `select region, store_id, amount, ${expression} from ${table} order by region asc, amount asc, store_id asc limit 20`,
    });
  }

  cases.push({
    seed: random.nextSeed(),
    sql: (table) =>
      `select region, store_id, amount from ${table} qualify row_number() over (partition by region order by amount desc, store_id asc) <= 3 order by region asc, amount desc, store_id asc`,
  });
  cases.push({
    seed: random.nextSeed(),
    sql: (table) =>
      `select region, store_id, amount, row_number() over ranked as w from ${table} window ranked as (partition by region order by amount desc, store_id asc) order by region asc, w asc limit 20`,
  });

  return cases;
}

function compatibleFrame(spec: string, frame: string): string {
  if (frame.startsWith(" range") && spec !== "partition by region order by amount") return "";
  if (frame.startsWith(" groups") && !spec.includes("floor(amount / 100)")) return "";
  return frame;
}

function seededRandom(seed: number) {
  let state = seed >>> 0;
  const nextSeed = () => {
    state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
    return state;
  };
  return {
    nextSeed,
    pick<T>(values: readonly T[]): T {
      return values[nextSeed() % values.length] as T;
    },
  };
}

async function storesFixturePath(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "lakeql-reference-join-"));
  const path = join(dir, "stores.parquet");
  const key = "tmp/stores.parquet";
  const store = memoryStore();
  await writeParquet(store, key, {
    columnData: [
      { name: "store_id", data: ["store-000", "store-000", "store-001"], type: "STRING" },
      { name: "region", data: ["west", "east", "east"], type: "STRING" },
      { name: "segment", data: ["enterprise", "wrong-region", "retail"], type: "STRING" },
    ],
  });
  const bytes = await store.get(key);
  if (bytes === null) throw new Error("failed to write stores fixture");
  await writeFile(path, bytes);
  return path;
}

async function rightStoresFixturePath(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "lakeql-reference-right-join-"));
  const path = join(dir, "stores.parquet");
  const key = "tmp/right-stores.parquet";
  const store = memoryStore();
  await writeParquet(store, key, {
    columnData: [
      { name: "store_id", data: ["store-000", "store-999"], type: "STRING" },
      { name: "segment", data: ["enterprise", "unmatched"], type: "STRING" },
    ],
  });
  const bytes = await store.get(key);
  if (bytes === null) throw new Error("failed to write right stores fixture");
  await writeFile(path, bytes);
  return path;
}

async function timestampWindowFixturePath(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "lakeql-reference-window-"));
  const path = join(dir, "events.parquet");
  const key = "tmp/events.parquet";
  const store = memoryStore();
  await writeParquet(store, key, {
    schema: [
      { name: "root", num_children: 4 },
      { name: "id", type: "INT32", repetition_type: "OPTIONAL" },
      { name: "account", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
      {
        name: "event_ts",
        type: "INT64",
        converted_type: "TIMESTAMP_MILLIS",
        logical_type: { type: "TIMESTAMP", isAdjustedToUTC: true, unit: "MILLIS" },
        repetition_type: "OPTIONAL",
      },
      { name: "amount", type: "DOUBLE", repetition_type: "OPTIONAL" },
    ],
    columnData: [
      { name: "id", data: [1, 2, 3, 4, 5] },
      { name: "account", data: ["a", "a", "a", "a", "b"] },
      {
        name: "event_ts",
        data: [
          new Date("2026-01-01T00:00:00.000Z"),
          new Date("2026-01-02T00:00:00.000Z"),
          new Date("2026-01-04T00:00:00.000Z"),
          new Date("2026-01-05T00:00:00.000Z"),
          new Date("2026-01-02T12:00:00.000Z"),
        ],
      },
      { name: "amount", data: [5, 7, 11, 13, 17] },
    ],
  });
  const bytes = await store.get(key);
  if (bytes === null) throw new Error("failed to write timestamp window fixture");
  await writeFile(path, bytes);
  return path;
}

async function windowMatrixFixturePath(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "lakeql-reference-window-matrix-"));
  const path = join(dir, "window-matrix.parquet");
  const key = "tmp/window-matrix.parquet";
  const store = memoryStore();
  await writeParquet(store, key, {
    rowGroupSize: [4],
    schema: [
      { name: "root", num_children: 7 },
      { name: "id", type: "INT32", repetition_type: "OPTIONAL" },
      { name: "account", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
      { name: "bucket", type: "INT32", repetition_type: "OPTIONAL" },
      { name: "amount", type: "DOUBLE", repetition_type: "OPTIONAL" },
      { name: "label", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
      { name: "big_score", type: "INT64", repetition_type: "OPTIONAL" },
      {
        name: "event_ts",
        type: "INT64",
        converted_type: "TIMESTAMP_MILLIS",
        logical_type: { type: "TIMESTAMP", isAdjustedToUTC: true, unit: "MILLIS" },
        repetition_type: "OPTIONAL",
      },
    ],
    columnData: [
      { name: "id", data: [1, 2, 3, 4, 5, 6, 7, 8] },
      { name: "account", data: ["a", "a", "a", "a", "b", "b", "b", "c"] },
      { name: "bucket", data: [1, 1, 2, 3, 1, 2, 2, 1] },
      { name: "amount", data: [10, 20, null, 40, 5, 5, 15, 30] },
      { name: "label", data: ["hot", "hot", "cold", "warm", "cold", "cold", "hot", "warm"] },
      {
        name: "big_score",
        data: [10n, 20n, 20n, 40n, 5n, 15n, 15n, 30n],
      },
      {
        name: "event_ts",
        data: [
          new Date("2026-02-01T00:00:00.000Z"),
          new Date("2026-02-02T00:00:00.000Z"),
          new Date("2026-02-03T12:00:00.000Z"),
          new Date("2026-02-07T00:00:00.000Z"),
          new Date("2026-02-01T06:00:00.000Z"),
          new Date("2026-02-02T06:00:00.000Z"),
          new Date("2026-02-04T06:00:00.000Z"),
          new Date("2026-02-03T00:00:00.000Z"),
        ],
      },
    ],
  });
  const bytes = await store.get(key);
  if (bytes === null) throw new Error("failed to write window matrix fixture");
  await writeFile(path, bytes);
  return path;
}
