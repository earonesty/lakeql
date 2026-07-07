import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { memoryStore, readOutputManifest } from "lakeql-core";
import { fixturePath, SALES } from "lakeql-fixtures";
import { createParquetLake, writeParquet } from "lakeql-parquet";
import { describe, expect, it } from "vitest";
import { COMMANDS, runCli, usage } from "./index.js";

describe("usage", () => {
  it("lists every command", () => {
    const text = usage();
    for (const cmd of COMMANDS) {
      expect(text).toContain(cmd);
    }
  });

  it("matches the CLI help snapshot", () => {
    expect(usage()).toMatchInlineSnapshot(`
      "usage: lakeql <command> [options]

      commands:
        compact --path <file.parquet> --output <prefix> [--max-rows-per-file n]
        query   --path <file.parquet> --sql <query> [--format csv|json|ndjson]
        query   --table name=file.parquet [--table name=file.parquet ...] --sql <join-query> [--join-max-right-rows n] [--join-max-output-rows n]
        explain --path <file.parquet> --sql <query>
        inspect --path <file.parquet>
        write   --path <file.parquet> --sql <query> --output <prefix> [--partition-by a,b] [--max-rows-per-file n] [--manifest <path>] [--job-id id]
        schema  --path <file.parquet>"
    `);
  });
});

describe("runCli", () => {
  it("queries a local Parquet path as NDJSON", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, amount where region = 'west' order by amount asc limit 2",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(result.stdout).toBe(
      '{"store_id":"store-000","amount":0}\n{"store_id":"store-000","amount":36.28}\n',
    );
  });

  it("queries a local Parquet path as JSON", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select region from input where amount > 900 limit 1",
      "--format",
      "json",
    ]);

    expect(result.exitCode).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual([{ region: "east" }]);
  });

  it("executes bounded SQL joins over named CLI tables", async () => {
    const storesPath = await storesFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select s.store_id, s.amount, d.segment from sales s join stores d on s.store_id = d.store_id where d.segment = 'enterprise' order by s.amount asc limit 2",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { "s.store_id": "store-000", "s.amount": 0, "d.segment": "enterprise" },
      { "s.store_id": "store-000", "s.amount": 36.28, "d.segment": "enterprise" },
    ]);
  });

  it("executes bounded SQL join chains over named CLI tables", async () => {
    const storesPath = await storesFixturePath();
    const regionsPath = await regionsFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--table",
      `regions=${regionsPath}`,
      "--sql",
      [
        "select s.store_id as store_id, d.segment as segment, r.manager as manager",
        "from sales s",
        "join stores d on s.store_id = d.store_id",
        "join regions r on d.region = r.region",
        "where s.amount < 40 and d.segment = 'enterprise'",
        "order by s.amount asc",
        "limit 2",
      ].join(" "),
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { store_id: "store-000", segment: "enterprise", manager: "Ada" },
      { store_id: "store-000", segment: "enterprise", manager: "Ada" },
    ]);
  });

  it("executes bounded SQL right joins over named CLI tables", async () => {
    const rightStoresPath = await rightStoresFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${rightStoresPath}`,
      "--sql",
      [
        "select d.store_id as store_id, s.amount as amount, d.segment as segment",
        "from sales s right join stores d on s.store_id = d.store_id",
        "order by d.store_id desc, s.amount asc nulls last",
        "limit 2",
      ].join(" "),
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { store_id: "store-999", amount: null, segment: "unmatched" },
      { store_id: "store-000", amount: 0, segment: "enterprise" },
    ]);
  });

  it("executes bounded SQL full joins over named CLI tables", async () => {
    const rightStoresPath = await rightStoresFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${rightStoresPath}`,
      "--sql",
      [
        "select distinct s.store_id as sales_store, d.store_id as dim_store, d.segment as segment",
        "from sales s full join stores d on s.store_id = d.store_id",
        "where s.store_id = 'store-005' or d.store_id = 'store-999'",
        "order by s.store_id asc nulls last, d.store_id asc nulls last",
      ].join(" "),
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { sales_store: "store-005", dim_store: null, segment: null },
      { sales_store: null, dim_store: "store-999", segment: "unmatched" },
    ]);
  });

  it("executes bounded SQL cross joins over named CLI tables", async () => {
    const storesPath = await storesFixturePath();
    const regionsPath = await regionsFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `stores=${storesPath}`,
      "--table",
      `regions=${regionsPath}`,
      "--sql",
      [
        "select d.store_id as store_id, r.manager as manager",
        "from stores d cross join regions r",
        "order by d.store_id asc, r.manager asc",
        "limit 3",
      ].join(" "),
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { store_id: "store-000", manager: "Ada" },
      { store_id: "store-000", manager: "Ada" },
      { store_id: "store-000", manager: "Grace" },
    ]);

    await expect(
      runCli([
        "query",
        "--table",
        `stores=${storesPath}`,
        "--table",
        `regions=${regionsPath}`,
        "--sql",
        "select * from stores cross join regions",
        "--join-max-output-rows",
        "5",
      ]),
    ).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("maxOutputRows"),
    });
  });

  it("executes bounded SQL joins with side-qualified filters", async () => {
    const storesPath = await storesFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select s.store_id, s.amount, d.segment from sales s join stores d on s.store_id = d.store_id where s.amount < 40 and d.segment = 'enterprise' order by s.amount asc limit 2",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { "s.store_id": "store-000", "s.amount": 0, "d.segment": "enterprise" },
      { "s.store_id": "store-000", "s.amount": 36.28, "d.segment": "enterprise" },
    ]);
  });

  it("pushes rich side-qualified join filter expressions safely", async () => {
    const storesPath = await storesFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      [
        "select s.store_id as store_id, d.segment as segment",
        "from sales s join stores d on s.store_id = d.store_id",
        "where s.amount between 0 and 40",
        "and s.region = 'west'",
        "and s.region is not null",
        "and s.region like 'w%'",
        "and lower(s.region) = 'west'",
        "and s.amount + 1 >= 1",
        "and case when s.amount >= 0 then s.region else 'x' end = 'west'",
        "and not (s.amount < 0)",
        "and d.segment = 'enterprise'",
        "order by s.amount asc",
        "limit 2",
      ].join(" "),
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { store_id: "store-000", segment: "enterprise" },
      { store_id: "store-000", segment: "enterprise" },
    ]);
  });

  it("enforces bounded SQL join right-side rows", async () => {
    const storesPath = await storesFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--join-max-right-rows",
      "1",
      "--sql",
      "select s.store_id, d.segment from sales s join stores d on s.store_id = d.store_id",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 1 });
    expect(result.stderr).toContain("LAKEQL_BUDGET_EXCEEDED");
  });

  it("executes bounded SQL joins with multiple equality keys", async () => {
    const storesPath = await storesFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select s.store_id, s.region, d.segment from sales s join stores d on s.store_id = d.store_id and s.region = d.region order by s.amount asc limit 2",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { "s.store_id": "store-000", "s.region": "west", "d.segment": "enterprise" },
      { "s.store_id": "store-000", "s.region": "west", "d.segment": "enterprise" },
    ]);
  });

  it("executes bounded SQL joins with multiple USING keys", async () => {
    const storesPath = await storesFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select s.store_id, s.region, d.segment from sales s join stores d using (store_id, region) order by s.amount asc limit 2",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { "s.store_id": "store-000", "s.region": "west", "d.segment": "enterprise" },
      { "s.store_id": "store-000", "s.region": "west", "d.segment": "enterprise" },
    ]);
  });

  it("executes LEFT JOIN with null right-side projections", async () => {
    const storesPath = await storesFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select s.store_id as store_id, d.segment as segment from sales s left join stores d on s.store_id = d.store_id where s.store_id = 'store-005' order by s.amount asc limit 1",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([{ store_id: "store-005", segment: null }]);
  });

  it("executes LEFT JOIN wildcard output with null right-side columns", async () => {
    const storesPath = await storesFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select * from sales s left join stores d on s.store_id = d.store_id where s.store_id = 'store-005' order by s.amount asc limit 1",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      expect.objectContaining({
        "s.store_id": "store-005",
        "d.store_id": null,
        "d.region": null,
        "d.segment": null,
      }),
    ]);
  });

  it("applies right-side WHERE filters after LEFT JOIN", async () => {
    const storesPath = await storesFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select s.store_id as store_id, d.segment as segment from sales s left join stores d on s.store_id = d.store_id where d.segment = 'enterprise' order by s.amount asc limit 2",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { store_id: "store-000", segment: "enterprise" },
      { store_id: "store-000", segment: "enterprise" },
    ]);
  });

  it("executes IN subqueries as bounded semi joins", async () => {
    const storesPath = await storesFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select store_id, amount from sales where store_id in (select store_id from stores where segment = 'enterprise') order by amount asc limit 2",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { store_id: "store-000", amount: 0 },
      { store_id: "store-000", amount: 36.28 },
    ]);
  });

  it("executes ordered and limited IN subqueries before semi joins", async () => {
    const rightStoresPath = await rightStoresFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${rightStoresPath}`,
      "--sql",
      [
        "select distinct store_id",
        "from sales",
        "where store_id in (select store_id from stores order by store_id asc limit 1)",
        "order by store_id",
      ].join(" "),
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([{ store_id: "store-000" }]);
  });

  it("executes NOT IN subqueries as bounded anti joins", async () => {
    const storesPath = await storesFixturePath();
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select store_id, amount from sales where store_id not in (select store_id from stores) order by amount asc limit 2",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { store_id: "store-005", amount: 34.82 },
      { store_id: "store-006", amount: 35.55 },
    ]);
  });

  it("formats IN subquery output as JSON and writes it", async () => {
    const storesPath = await storesFixturePath();
    const dir = await mkdtemp(join(tmpdir(), "lakeql-cli-in-write-"));
    const output = join(dir, "semi");
    const sql =
      "select store_id, amount from sales where store_id in (select store_id from stores where segment = 'enterprise') order by amount asc limit 1";
    const json = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      sql,
      "--format",
      "json",
    ]);
    const write = await runCli([
      "write",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      sql,
      "--output",
      output,
    ]);

    expect(json).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(json.stdout)).toEqual([{ store_id: "store-000", amount: 0 }]);
    expect(write).toMatchObject({ exitCode: 0, stderr: "" });
    const body = JSON.parse(write.stdout) as { files: { path: string }[] };
    const store = memoryStore();
    for (const file of body.files) await store.put(file.path, await readFile(file.path));
    await expect(
      createParquetLake({ store }).path(`${output}/*.parquet`).toArray(),
    ).resolves.toEqual([{ amount: 0, store_id: "store-000" }]);
  });

  it("executes simple filtered CTE SQL", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "with recent as (select store_id, amount from input where amount > 900) select store_id, amount from recent order by amount desc limit 2",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { store_id: "store-006", amount: 999.27 },
      { store_id: "store-005", amount: 998.54 },
    ]);
  });

  it("executes aggregate CTE SQL", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "with totals as (select region, count(*) as rows, max(amount) as max_amount from input group by region) select region, rows from totals where max_amount > 990 order by region asc",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { region: "east", rows: 25 },
      { region: "north", rows: 25 },
      { region: "south", rows: 25 },
    ]);
  });

  it("executes computed DISTINCT CTE SQL", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "with enriched as (select distinct store_id, amount * 2 as doubled from input) select store_id, doubled from enriched order by doubled asc limit 1",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([{ store_id: "store-000", doubled: 0 }]);
  });

  it("executes aggregate scalar subqueries in WHERE", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, amount from input where amount = (select max(amount) as max_amount from input)",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([{ store_id: "store-006", amount: 999.27 }]);
  });

  it("executes LIMIT 1 scalar subqueries in WHERE", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, amount from input where amount >= (select amount from input order by amount desc limit 1)",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([{ store_id: "store-006", amount: 999.27 }]);
  });

  it("executes scalar subqueries in SELECT projections", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, (select max(amount) as max_amount from input) as max_amount from input order by amount asc limit 1",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([{ store_id: "store-000", max_amount: 999.27 }]);
  });

  it("executes scalar subqueries inside compound expressions", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      [
        "select store_id, amount from input",
        "where amount between (select min(amount) as min_amount from input) and (select max(amount) as max_amount from input)",
        "and amount in ((select min(amount) as min_amount from input), (select max(amount) as max_amount from input))",
        "and not (amount < (select min(amount) as min_amount from input))",
        "and (select max(amount) as max_amount from input) is not null",
        "and (select region from input where region = 'west' limit 1) like 'w%'",
        "and amount + (select min(amount) as min_amount from input) >= 0",
        "and case when amount >= (select min(amount) as min_amount from input) then (select region from input where region = 'west' limit 1) else 'x' end = 'west'",
        "order by amount asc",
      ].join(" "),
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { store_id: "store-000", amount: 0 },
      { store_id: "store-006", amount: 999.27 },
    ]);
  });

  it("formats aggregate, join, and subquery SQL as CSV and NDJSON", async () => {
    const storesPath = await storesFixturePath();
    const aggregate = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select region, count(*) as rows from input group by region order by region asc limit 1",
      "--format",
      "csv",
    ]);
    const join = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select s.store_id as store_id, d.segment as segment from sales s join stores d on s.store_id = d.store_id where d.segment = 'enterprise' order by s.amount asc limit 1",
      "--format",
      "csv",
    ]);
    const subquery = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select store_id, amount from sales where store_id in (select store_id from stores where segment = 'enterprise') order by amount asc limit 1",
    ]);

    expect(aggregate).toMatchObject({ exitCode: 0, stderr: "" });
    expect(aggregate.stdout).toBe("region,rows\neast,25\n");
    expect(join).toMatchObject({ exitCode: 0, stderr: "" });
    expect(join.stdout).toBe("store_id,segment\nstore-000,enterprise\n");
    expect(subquery).toMatchObject({ exitCode: 0, stderr: "" });
    expect(subquery.stdout).toBe('{"store_id":"store-000","amount":0}\n');
  });

  it("writes aggregate and join SQL results", async () => {
    const storesPath = await storesFixturePath();
    const dir = await mkdtemp(join(tmpdir(), "lakeql-cli-write-sql-"));
    const aggregateOutput = join(dir, "aggregate");
    const joinOutput = join(dir, "join");
    const aggregate = await runCli([
      "write",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select region, count(*) as rows from input group by region order by region asc limit 1",
      "--output",
      aggregateOutput,
    ]);
    const joined = await runCli([
      "write",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select s.store_id as store_id, d.segment as segment from sales s join stores d on s.store_id = d.store_id where d.segment = 'enterprise' order by s.amount asc limit 1",
      "--output",
      joinOutput,
    ]);

    expect(aggregate).toMatchObject({ exitCode: 0, stderr: "" });
    expect(joined).toMatchObject({ exitCode: 0, stderr: "" });
    const store = memoryStore();
    for (const body of [JSON.parse(aggregate.stdout), JSON.parse(joined.stdout)] as {
      files: { path: string }[];
    }[]) {
      for (const file of body.files) await store.put(file.path, await readFile(file.path));
    }
    await expect(
      createParquetLake({ store }).path(`${aggregateOutput}/*.parquet`).toArray(),
    ).resolves.toEqual([{ region: "east", rows: 25 }]);
    await expect(
      createParquetLake({ store }).path(`${joinOutput}/*.parquet`).toArray(),
    ).resolves.toEqual([{ store_id: "store-000", segment: "enterprise" }]);
  });

  it("rejects unsupported runtime SQL shapes with typed errors", async () => {
    const storesPath = await storesFixturePath();
    const aggregateJoin = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select count(*) as rows from sales s join stores d on s.store_id = d.store_id",
      "--format",
      "json",
    ]);
    const aggregateInSubquery = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--table",
      `stores=${storesPath}`,
      "--sql",
      "select count(*) as rows from sales where store_id in (select store_id from stores)",
      "--format",
      "json",
    ]);
    const emptyCte = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "with none as (select store_id from input where amount > 2000) select store_id from none",
      "--format",
      "json",
    ]);
    const scalarTooManyRows = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id from input where store_id = (select store_id from input limit 2)",
      "--format",
      "json",
    ]);
    const aggregateExplain = await runCli([
      "explain",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select count(*) as rows from input",
    ]);

    for (const result of [
      aggregateJoin,
      aggregateInSubquery,
      emptyCte,
      scalarTooManyRows,
      aggregateExplain,
    ]) {
      expect(result).toMatchObject({ exitCode: 1 });
      expect(result.stderr).toContain("LAKEQL_SQL_UNSUPPORTED");
    }
  });

  it("accepts select-first SQL with FROM while still using the --path source", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, amount from ignored_source where region = 'west' order by amount asc limit 1",
      "--format",
      "json",
    ]);

    expect(result.exitCode).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual([{ store_id: "store-000", amount: 0 }]);
  });

  it("executes SELECT DISTINCT through the core query engine", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select distinct region from input order by region asc",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { region: "east" },
      { region: "north" },
      { region: "south" },
      { region: "west" },
    ]);
  });

  it("executes computed projection SQL through the core evaluator", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, amount * 2 as doubled, case when amount > 10 then 'large' else 'small' end as bucket from input where amount < 20 order by amount asc limit 2",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { store_id: "store-000", doubled: 0, bucket: "small" },
    ]);
  });

  it("executes grouped aggregate SQL through the core engine", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select region, count(*) as rows, sum(amount) as total from input group by region order by region asc",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { region: "east", rows: 25, total: 12337.249999999996 },
      { region: "north", rows: 25, total: 12262.500000000002 },
      { region: "south", rows: 25, total: 12187.750000000002 },
      { region: "west", rows: 25, total: 11412 },
    ]);
  });

  it("executes grouped aggregate SQL over computed aliases", async () => {
    const bucketsPath = await bucketsFixturePath();
    const result = await runCli([
      "query",
      "--path",
      bucketsPath,
      "--sql",
      [
        "select case when amount < 10 then 'small' else 'large' end as bucket, count(*) as rows",
        "from input",
        "group by bucket",
        "order by bucket asc",
      ].join(" "),
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { bucket: "large", rows: 1 },
      { bucket: "small", rows: 2 },
    ]);
  });

  it("executes grouped aggregate expressions and COUNT DISTINCT SQL", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select region, max(amount * 2) as max_doubled, count(distinct store_id) as stores from input group by region order by region asc",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { region: "east", max_doubled: 1995.62, stores: 7 },
      { region: "north", max_doubled: 1997.08, stores: 7 },
      { region: "south", max_doubled: 1998.54, stores: 7 },
      { region: "west", max_doubled: 1921.6, stores: 7 },
    ]);
  });

  it("executes global aggregate SQL", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select count(*) as rows, max(amount) as max_amount from input",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([{ rows: 100, max_amount: 999.27 }]);
  });

  it("executes aggregate SQL with multiple group keys", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select region, store_id, count(*) as rows from input group by region, store_id order by region asc, store_id asc limit 5",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { region: "east", store_id: "store-000", rows: 3 },
      { region: "east", store_id: "store-001", rows: 4 },
      { region: "east", store_id: "store-002", rows: 4 },
      { region: "east", store_id: "store-003", rows: 3 },
      { region: "east", store_id: "store-004", rows: 3 },
    ]);
  });

  it("executes group-by-only SQL without requiring aggregate calls", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select region from input group by region order by region asc",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { region: "east" },
      { region: "north" },
      { region: "south" },
      { region: "west" },
    ]);
  });

  it("projects aggregate SQL output without leaking unselected group columns", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select count(*) as rows from input group by region order by region asc",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { rows: 25 },
      { rows: 25 },
      { rows: 25 },
      { rows: 25 },
    ]);
  });

  it("applies DISTINCT to aggregate SQL projection before limit", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select distinct count(*) as rows from input group by region order by region asc limit 2",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([{ rows: 25 }]);
  });

  it("rejects aggregate SQL selecting columns outside the grouping keys", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, count(*) as rows from input group by region",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 1 });
    expect(result.stderr).toContain("LAKEQL_SQL_UNSUPPORTED");
  });

  it("applies HAVING before aggregate ordering and limiting", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select region, count(*) as rows, max(amount) as max_amount from input group by region having max_amount > 980 order by region asc limit 2",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      { region: "east", rows: 25, max_amount: 997.81 },
      { region: "north", rows: 25, max_amount: 998.54 },
    ]);
  });

  it("queries a local Parquet path as CSV", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, amount where region = 'west' order by amount asc limit 2",
      "--format",
      "csv",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(result.stdout).toBe("store_id,amount\nstore-000,0\nstore-000,36.28\n");
  });

  it("explains, inspects, and reads schema for a local Parquet path", async () => {
    const explain = await runCli([
      "explain",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id where amount > 900 limit 1",
    ]);
    const inspect = await runCli(["inspect", "--path", fixturePath(SALES.file)]);
    const schema = await runCli(["schema", "--path", fixturePath(SALES.file)]);

    expect(explain.stdout).toContain("files planned: 1");
    expect(JSON.parse(inspect.stdout)).toMatchObject({
      rows: SALES.rows,
      rowGroups: 3,
      columns: 4,
    });
    expect(schema.exitCode).toBe(0);
    expect(JSON.parse(schema.stdout)).toMatchObject({
      rows: SALES.rows,
      columns: expect.arrayContaining([expect.objectContaining({ name: "amount" })]),
    });
  });

  it("describes a local Parquet path through SQL", async () => {
    const result = await runCli([
      "query",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "describe input",
      "--format",
      "json",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual([
      {
        path: fixturePath(SALES.file),
        rows: SALES.rows,
        columns: expect.arrayContaining([expect.objectContaining({ name: "amount" })]),
      },
    ]);
  });

  it("describes a named CLI table through SQL", async () => {
    const result = await runCli([
      "query",
      "--table",
      `sales=${fixturePath(SALES.file)}`,
      "--sql",
      "describe sales",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toMatchObject({
      path: "sales",
      rows: SALES.rows,
      columns: expect.arrayContaining([expect.objectContaining({ name: "store_id" })]),
    });
  });

  it("matches CLI explain, inspect, and schema snapshots", async () => {
    const explain = await runCli([
      "explain",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id where amount > 900 limit 1",
    ]);
    const inspect = JSON.parse(
      (await runCli(["inspect", "--path", fixturePath(SALES.file)])).stdout,
    ) as Record<string, unknown>;
    const schema = JSON.parse(
      (await runCli(["schema", "--path", fixturePath(SALES.file)])).stdout,
    ) as Record<string, unknown>;
    inspect.path = "<fixture:sales.parquet>";
    schema.path = "<fixture:sales.parquet>";

    expect(explain.stdout).toMatchInlineSnapshot(`
      "files planned: 1
      files skipped: 0
      projected columns: amount, store_id
      "
    `);
    expect(inspect).toMatchInlineSnapshot(`
      {
        "columns": 4,
        "path": "<fixture:sales.parquet>",
        "rowGroups": 3,
        "rows": 100,
      }
    `);
    expect(schema).toMatchInlineSnapshot(`
      {
        "columns": [
          {
            "name": "store_id",
            "type": "BYTE_ARRAY",
          },
          {
            "name": "date",
            "type": "BYTE_ARRAY",
          },
          {
            "name": "amount",
            "type": "DOUBLE",
          },
          {
            "name": "region",
            "type": "BYTE_ARRAY",
          },
        ],
        "path": "<fixture:sales.parquet>",
        "rows": 100,
      }
    `);
  });

  it("writes query results to local Parquet files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "lakeql-cli-"));
    const output = join(dir, "west");
    const result = await runCli([
      "write",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, region, amount where region = 'west' order by amount asc limit 2",
      "--output",
      output,
      "--max-rows-per-file",
      "1",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const body = JSON.parse(result.stdout) as { files: { path: string; rowCount: number }[] };
    expect(body.files.map((file) => file.rowCount)).toEqual([1, 1]);

    const store = memoryStore();
    for (const file of body.files) await store.put(file.path, await readFile(file.path));
    await expect(
      createParquetLake({ store }).path(`${output}/*.parquet`).toArray(),
    ).resolves.toEqual([
      { amount: 0, region: "west", store_id: "store-000" },
      { amount: 36.28, region: "west", store_id: "store-000" },
    ]);
  });

  it("writes output manifests for local write commands", async () => {
    const dir = await mkdtemp(join(tmpdir(), "lakeql-cli-manifest-"));
    const output = join(dir, "west");
    const manifestPath = join(dir, "manifest.json");
    const result = await runCli([
      "write",
      "--path",
      fixturePath(SALES.file),
      "--sql",
      "select store_id, region, amount where region = 'west' order by amount asc limit 2",
      "--output",
      output,
      "--max-rows-per-file",
      "1",
      "--manifest",
      manifestPath,
      "--job-id",
      "job_cli_write",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const body = JSON.parse(result.stdout) as {
      files: { path: string; rowCount: number }[];
      manifest: string;
    };
    expect(body.manifest).toBe(manifestPath);

    const store = memoryStore();
    await store.put(manifestPath, await readFile(manifestPath));
    const manifest = await readOutputManifest(store, manifestPath);
    expect(manifest).toMatchObject({
      jobId: "job_cli_write",
      entries: [
        {
          taskId: "job_cli_write-task-000000",
          outputPath: body.files[0]?.path,
          rowCount: 1,
        },
        {
          taskId: "job_cli_write-task-000000",
          outputPath: body.files[1]?.path,
          rowCount: 1,
        },
      ],
    });
    expect(manifest.planFingerprint).toMatch(/^fp_[0-9a-f]{16}$/u);
  });

  it("compacts a local Parquet file into rewritten output files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "lakeql-cli-compact-"));
    const output = join(dir, "sales");
    const result = await runCli([
      "compact",
      "--path",
      fixturePath(SALES.file),
      "--output",
      output,
      "--max-rows-per-file",
      "75",
    ]);

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    const body = JSON.parse(result.stdout) as { files: { path: string; rowCount: number }[] };
    expect(body.files.map((file) => file.rowCount)).toEqual([75, 25]);

    const store = memoryStore();
    for (const file of body.files) await store.put(file.path, await readFile(file.path));
    await expect(createParquetLake({ store }).path(`${output}/*.parquet`).count()).resolves.toBe(
      SALES.rows,
    );
  });

  it("returns typed failures for unsupported commands and bad arguments", async () => {
    await expect(runCli([])).resolves.toMatchObject({
      exitCode: 0,
      stdout: expect.stringContaining("usage:"),
    });
    await expect(runCli(["--help"])).resolves.toMatchObject({
      exitCode: 0,
      stdout: expect.stringContaining("usage:"),
    });
    await expect(runCli(["-h"])).resolves.toMatchObject({
      exitCode: 0,
      stdout: expect.stringContaining("usage:"),
    });
    await expect(runCli(["query", "--help"])).resolves.toMatchObject({
      exitCode: 0,
      stdout: expect.stringContaining("usage:"),
    });
    await expect(runCli(["nope"])).resolves.toMatchObject({ exitCode: 2 });
    await expect(
      runCli(["write", "--path", fixturePath(SALES.file), "--sql", "select id"]),
    ).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("--output"),
    });
    await expect(runCli(["query", "--path"])).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("LAKEQL_PARSE_ERROR"),
    });
    await expect(runCli(["query", "--nope"])).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("Unknown argument"),
    });
    await expect(
      runCli(["query", "--path", "/definitely/missing.parquet", "--sql", "select id"]),
    ).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("ENOENT"),
    });
    await expect(
      runCli(["query", "--path", fixturePath(SALES.file), "--format", "xml"]),
    ).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("--format"),
    });
    await expect(runCli(["query", "--table", "bad"])).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("--table must be name=path.parquet"),
    });
    await expect(runCli(["query", "--table", "1bad=/tmp/x.parquet"])).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("--table name must be a SQL identifier"),
    });
    await expect(
      runCli(["write", "--path", fixturePath(SALES.file), "--partition-by", " , "]),
    ).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("--partition-by must not be empty"),
    });
    await expect(
      runCli([
        "compact",
        "--path",
        fixturePath(SALES.file),
        "--output",
        "/tmp/nope",
        "--max-rows-per-file",
        "0",
      ]),
    ).resolves.toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("--max-rows-per-file must be a positive integer"),
    });
  });
});

async function storesFixturePath(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "lakeql-cli-join-"));
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
  const dir = await mkdtemp(join(tmpdir(), "lakeql-cli-right-join-"));
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

async function regionsFixturePath(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "lakeql-cli-region-"));
  const path = join(dir, "regions.parquet");
  const key = "tmp/regions.parquet";
  const store = memoryStore();
  await writeParquet(store, key, {
    columnData: [
      { name: "region", data: ["west", "east"], type: "STRING" },
      { name: "manager", data: ["Ada", "Grace"], type: "STRING" },
    ],
  });
  const bytes = await store.get(key);
  if (bytes === null) throw new Error("failed to write regions fixture");
  await writeFile(path, bytes);
  return path;
}

async function bucketsFixturePath(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "lakeql-cli-buckets-"));
  const path = join(dir, "buckets.parquet");
  const key = "tmp/buckets.parquet";
  const store = memoryStore();
  await writeParquet(store, key, {
    columnData: [{ name: "amount", data: [1, 2, 20], type: "DOUBLE" }],
  });
  const bytes = await store.get(key);
  if (bytes === null) throw new Error("failed to write buckets fixture");
  await writeFile(path, bytes);
  return path;
}
