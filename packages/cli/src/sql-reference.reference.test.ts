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
