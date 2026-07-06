import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { memoryStore } from "../packages/core/dist/index.js";
import { writeParquet } from "../packages/parquet/dist/index.js";

const repoRoot = fileURLToPath(new URL("..", import.meta.url));

export const cases = [
  {
    name: "projection-only scan",
    fixture: "sales.parquet",
    lakeql: "select store_id, amount from input",
    duckdb: (path) => `select store_id, amount from read_parquet('${sqlString(path)}')`,
  },
  {
    name: "selective predicate",
    fixture: "sales.parquet",
    lakeql: "select store_id, amount from input where region = 'west'",
    duckdb: (path) =>
      `select store_id, amount from read_parquet('${sqlString(path)}') where region = 'west'`,
  },
  {
    name: "nonselective predicate",
    fixture: "sales.parquet",
    lakeql: "select store_id, amount from input where amount >= 0",
    duckdb: (path) =>
      `select store_id, amount from read_parquet('${sqlString(path)}') where amount >= 0`,
  },
  {
    name: "filtered ordered scan",
    fixture: "sales.parquet",
    lakeql:
      "select store_id, region, amount from input where region = 'west' order by amount asc limit 5",
    duckdb: (path) =>
      `select store_id, region, amount from read_parquet('${sqlString(path)}') where region = 'west' order by amount asc limit 5`,
  },
  {
    name: "computed projection",
    fixture: "sales.parquet",
    lakeql:
      "select store_id, amount * 2 as doubled, case when amount > 10 then 'large' else 'small' end as bucket from input where amount < 20 order by amount asc limit 2",
    duckdb: (path) =>
      `select store_id, amount * 2 as doubled, case when amount > 10 then 'large' else 'small' end as bucket from read_parquet('${sqlString(path)}') where amount < 20 order by amount asc limit 2`,
  },
  {
    name: "wide schema projection",
    fixture: "wide.parquet",
    lakeql: "select c00, c31 from input where c00 >= 10 order by c31 desc limit 3",
    duckdb: (path) =>
      `select c00, c31 from read_parquet('${sqlString(path)}') where c00 >= 10 order by c31 desc limit 3`,
    lakeqlExpect: {
      filesRead: 1,
      rowGroupsRead: 1,
      rowsScanned: 24,
      workUnits: 1,
    },
  },
  {
    name: "null-heavy predicate",
    fixture: "types.parquet",
    lakeql: "select id, name, score from input where name is null order by id asc",
    duckdb: (path) =>
      `select id, name, score from read_parquet('${sqlString(path)}') where name is null order by id asc`,
  },
  {
    name: "string-heavy predicate",
    fixture: "sales.parquet",
    extraFiles: stringHeavyFixture,
    sourceAliases: { string_heavy: "string-heavy.parquet" },
    lakeql:
      "select id, category, payload from string_heavy where category = 'gamma' order by id asc limit 4",
    duckdb: (_path, files) =>
      `select id, category, payload from read_parquet('${sqlString(files["string-heavy.parquet"].path)}') where category = 'gamma' order by id asc limit 4`,
  },
  {
    name: "regex string functions",
    fixture: "sales.parquet",
    extraFiles: stringHeavyFixture,
    sourceAliases: { string_heavy: "string-heavy.parquet" },
    lakeql:
      "select id, regexp_replace(payload, 'event-00', 'hit-') as normalized from string_heavy where regexp_matches(payload, 'event-00[5-8]-') order by id asc",
    duckdb: (_path, files) =>
      `select id, regexp_replace(payload, 'event-00', 'hit-') as normalized from read_parquet('${sqlString(files["string-heavy.parquet"].path)}') where regexp_matches(payload, 'event-00[5-8]-') order by id asc`,
  },
  {
    name: "row-group pruning",
    fixture: "stats.parquet",
    lakeql: "select id, metric from input where metric > 199 order by id asc",
    duckdb: (path) =>
      `select id, metric from read_parquet('${sqlString(path)}') where metric > 199 order by id asc`,
    lakeqlExpect: {
      filesRead: 1,
      rowGroupsRead: 1,
      rowGroupsSkipped: 2,
      rowsMatched: 10,
      rowsScanned: 10,
      workUnits: 1,
    },
  },
  {
    name: "scalar subquery",
    fixture: "sales.parquet",
    lakeql:
      "select store_id, amount from input where amount = (select max(amount) as max_amount from input)",
    duckdb: (path) =>
      `select store_id, amount from read_parquet('${sqlString(path)}') where amount = (select max(amount) as max_amount from read_parquet('${sqlString(path)}'))`,
  },
  {
    name: "CTE materialization",
    fixture: "sales.parquet",
    lakeql:
      "with recent as (select store_id, amount from input where amount > 900) select store_id, amount from recent order by amount asc limit 2",
    duckdb: (path) =>
      `with recent as (select store_id, amount from read_parquet('${sqlString(path)}') where amount > 900) select store_id, amount from recent order by amount asc limit 2`,
  },
  {
    name: "aggregate CTE materialization",
    fixture: "sales.parquet",
    lakeql:
      "with totals as (select region, count(*) as rows, max(amount) as max_amount from input group by region) select region, rows from totals where max_amount > 990 order by region asc",
    duckdb: (path) =>
      `with totals as (select region, count(*) as rows, max(amount) as max_amount from read_parquet('${sqlString(path)}') group by region) select region, rows from totals where max_amount > 990 order by region asc`,
  },
  {
    name: "global aggregate",
    fixture: "sales.parquet",
    lakeql: "select count(*) as rows, max(amount) as max_amount from input",
    duckdb: (path) =>
      `select count(*) as rows, max(amount) as max_amount from read_parquet('${sqlString(path)}')`,
  },
  {
    name: "statistical aggregate",
    fixture: "sales.parquet",
    lakeql:
      "select var(amount) as amount_var, stddev(amount) as amount_stddev, var_pop(amount) as amount_var_pop, stddev_pop(amount) as amount_stddev_pop, median(amount) as amount_median, quantile_cont(amount, 0.75) as amount_p75, mode(region) as region_mode from input",
    duckdb: (path) =>
      `select var_samp(amount) as amount_var, stddev_samp(amount) as amount_stddev, var_pop(amount) as amount_var_pop, stddev_pop(amount) as amount_stddev_pop, median(amount) as amount_median, quantile_cont(amount, 0.75) as amount_p75, mode(region) as region_mode from read_parquet('${sqlString(path)}')`,
  },
  {
    name: "grouped aggregate",
    fixture: "sales.parquet",
    lakeql:
      "select region, count(*) as rows, max(amount) as max_amount from input group by region order by region asc",
    duckdb: (path) =>
      `select region, count(*) as rows, max(amount) as max_amount from read_parquet('${sqlString(path)}') group by region order by region asc`,
  },
  {
    name: "grouped expression aggregate",
    fixture: "sales.parquet",
    lakeql:
      "select region, max(amount * 2) as max_doubled, count(distinct store_id) as stores from input group by region order by region asc",
    duckdb: (path) =>
      `select region, max(amount * 2) as max_doubled, count(distinct store_id) as stores from read_parquet('${sqlString(path)}') group by region order by region asc`,
  },
  {
    name: "running aggregate window",
    fixture: "sales.parquet",
    lakeql:
      "select region, store_id, amount, sum(amount) over (partition by region order by amount asc, store_id asc) as running_amount from input order by region asc, amount asc, store_id asc limit 12",
    duckdb: (path) =>
      `select region, store_id, amount, sum(amount) over (partition by region order by amount asc, store_id asc) as running_amount from read_parquet('${sqlString(path)}') order by region asc, amount asc, store_id asc limit 12`,
  },
  {
    name: "qualify top rows per partition",
    fixture: "sales.parquet",
    lakeql:
      "select region, store_id, amount from input qualify row_number() over (partition by region order by amount desc, store_id asc) <= 2 order by region asc, amount desc, store_id asc",
    duckdb: (path) =>
      `select region, store_id, amount from read_parquet('${sqlString(path)}') qualify row_number() over (partition by region order by amount desc, store_id asc) <= 2 order by region asc, amount desc, store_id asc`,
  },
  {
    name: "rows frame moving window",
    fixture: "sales.parquet",
    lakeql:
      "select region, store_id, amount, sum(amount) over (partition by region order by amount asc, store_id asc rows between 1 preceding and current row) as moving_amount from input order by region asc, amount asc, store_id asc limit 12",
    duckdb: (path) =>
      `select region, store_id, amount, sum(amount) over (partition by region order by amount asc, store_id asc rows between 1 preceding and current row) as moving_amount from read_parquet('${sqlString(path)}') order by region asc, amount asc, store_id asc limit 12`,
  },
  {
    name: "groups frame peer window",
    fixture: "sales.parquet",
    lakeql:
      "select region, store_id, amount, count(*) over (partition by region order by floor(amount / 100) groups between current row and 1 following) as peer_rows from input order by region asc, amount asc, store_id asc limit 12",
    duckdb: (path) =>
      `select region, store_id, amount, count(*) over (partition by region order by floor(amount / 100) groups between current row and 1 following) as peer_rows from read_parquet('${sqlString(path)}') order by region asc, amount asc, store_id asc limit 12`,
  },
  {
    name: "range frame numeric offset window",
    fixture: "sales.parquet",
    lakeql:
      "select region, store_id, amount, count(*) over (partition by region order by amount range between 25 preceding and current row) as nearby_rows from input order by region asc, amount asc, store_id asc limit 12",
    duckdb: (path) =>
      `select region, store_id, amount, count(*) over (partition by region order by amount range between 25 preceding and current row) as nearby_rows from read_parquet('${sqlString(path)}') order by region asc, amount asc, store_id asc limit 12`,
  },
  {
    name: "range frame timestamp interval window",
    fixture: "sales.parquet",
    extraFiles: timestampWindowFixture,
    sourceAliases: { event_windows: "event-windows.parquet" },
    lakeql:
      "select account, id, amount, sum(amount) over (partition by account order by event_ts range between interval '1 day' preceding and current row) as day_amount from event_windows order by account asc, id asc",
    duckdb: (_path, files) =>
      `select account, id, amount, sum(amount) over (partition by account order by event_ts range between interval '1 day' preceding and current row) as day_amount from read_parquet('${sqlString(files["event-windows.parquet"].path)}') order by account asc, id asc`,
  },
  {
    name: "filtered aggregate window",
    fixture: "sales.parquet",
    lakeql:
      "select region, store_id, amount, sum(amount) filter (where amount >= 500) over (partition by region order by amount asc, store_id asc) as running_large_amount from input order by region asc, amount asc, store_id asc limit 12",
    duckdb: (path) =>
      `select region, store_id, amount, sum(amount) filter (where amount >= 500) over (partition by region order by amount asc, store_id asc) as running_large_amount from read_parquet('${sqlString(path)}') order by region asc, amount asc, store_id asc limit 12`,
  },
  {
    name: "named window clause",
    fixture: "sales.parquet",
    lakeql:
      "select region, store_id, amount, row_number() over ranked as rn from input window ranked as (partition by region order by amount desc, store_id asc) order by region asc, rn asc limit 12",
    duckdb: (path) =>
      `select region, store_id, amount, row_number() over ranked as rn from read_parquet('${sqlString(path)}') window ranked as (partition by region order by amount desc, store_id asc) order by region asc, rn asc limit 12`,
  },
  {
    name: "high-cardinality group aggregate",
    fixture: "sales.parquet",
    extraFiles: highCardinalityFixture,
    sourceAliases: { high_cardinality: "high-cardinality.parquet" },
    lakeql:
      "select entity_id, count(*) as rows, max(amount) as max_amount from high_cardinality group by entity_id order by entity_id asc limit 5",
    duckdb: (_path, files) =>
      `select entity_id, count(*) as rows, max(amount) as max_amount from read_parquet('${sqlString(files["high-cardinality.parquet"].path)}') group by entity_id order by entity_id asc limit 5`,
    lakeqlExpect: {
      filesRead: 1,
      rowGroupsRead: 1,
      rowsMatched: 80,
      rowsScanned: 80,
      workUnits: 1,
    },
  },
  {
    name: "many small files aggregate",
    fixture: "sales.parquet",
    extraFiles: smallFilesFixture,
    sourceAliases: { small_files: "small_files/*.parquet" },
    lakeql:
      "select bucket, count(*) as rows, max(amount) as max_amount from small_files group by bucket order by bucket asc",
    duckdb: (_path, files) =>
      `select bucket, count(*) as rows, max(amount) as max_amount from read_parquet([${Object.values(
        files,
      )
        .map((file) => `'${sqlString(file.path)}'`)
        .join(",")}]) group by bucket order by bucket asc`,
    lakeqlExpect: {
      filesRead: 5,
      rowGroupsRead: 5,
      rowGroupsSkipped: 0,
      rowsMatched: 10,
      rowsScanned: 10,
      workUnits: 5,
    },
  },
  {
    name: "bounded inner join",
    fixture: "sales.parquet",
    extraFiles: storesFixture,
    lakeql:
      "select s.store_id as store_id, s.amount as amount, d.segment as segment from sales s join stores d on s.store_id = d.store_id where d.segment = 'enterprise' order by s.amount asc limit 2",
    duckdb: (path, files) =>
      `select s.store_id as store_id, s.amount as amount, d.segment as segment from read_parquet('${sqlString(path)}') s join read_parquet('${sqlString(files.stores.path)}') d on s.store_id = d.store_id where d.segment = 'enterprise' order by s.amount asc limit 2`,
  },
  {
    name: "bounded left join",
    fixture: "sales.parquet",
    extraFiles: storesFixture,
    lakeql:
      "select s.store_id as store_id, d.segment as segment from sales s left join stores d on s.store_id = d.store_id where s.store_id = 'store-005' order by s.amount asc limit 1",
    duckdb: (path, files) =>
      `select s.store_id as store_id, d.segment as segment from read_parquet('${sqlString(path)}') s left join read_parquet('${sqlString(files.stores.path)}') d on s.store_id = d.store_id where s.store_id = 'store-005' order by s.amount asc limit 1`,
  },
  {
    name: "semi join subquery",
    fixture: "sales.parquet",
    extraFiles: storesFixture,
    lakeql:
      "select store_id, amount from sales where store_id in (select store_id from stores where segment = 'enterprise') order by amount asc limit 2",
    duckdb: (path, files) =>
      `select store_id, amount from read_parquet('${sqlString(path)}') where store_id in (select store_id from read_parquet('${sqlString(files.stores.path)}') where segment = 'enterprise') order by amount asc limit 2`,
  },
  {
    name: "anti join subquery",
    fixture: "sales.parquet",
    extraFiles: storesFixture,
    lakeql:
      "select store_id, amount from sales where store_id not in (select store_id from stores where segment = 'enterprise') order by amount asc limit 2",
    duckdb: (path, files) =>
      `select store_id, amount from read_parquet('${sqlString(path)}') where store_id not in (select store_id from read_parquet('${sqlString(files.stores.path)}') where segment = 'enterprise') order by amount asc limit 2`,
  },
];

async function storesFixture() {
  const store = memoryStore();
  await writeParquet(store, "stores.parquet", {
    columnData: [
      { name: "store_id", data: ["store-000", "store-000", "store-001"], type: "STRING" },
      { name: "region", data: ["west", "east", "east"], type: "STRING" },
      { name: "segment", data: ["enterprise", "wrong-region", "retail"], type: "STRING" },
    ],
  });
  return { stores: await storedFixtureFile(store, "stores.parquet") };
}

async function smallFilesFixture() {
  const store = memoryStore();
  const files = {};
  for (let fileIndex = 0; fileIndex < 5; fileIndex += 1) {
    const path = `small_files/part-${String(fileIndex).padStart(5, "0")}.parquet`;
    await writeParquet(store, path, {
      columnData: [
        { name: "id", data: [fileIndex * 2, fileIndex * 2 + 1], type: "INT32" },
        { name: "bucket", data: [`b${fileIndex % 2}`, `b${(fileIndex + 1) % 2}`], type: "STRING" },
        { name: "amount", data: [fileIndex * 10 + 1, fileIndex * 10 + 2], type: "DOUBLE" },
      ],
    });
    files[path] = await storedFixtureFile(store, path);
  }
  return files;
}

async function stringHeavyFixture() {
  const store = memoryStore();
  const rows = 48;
  await writeParquet(store, "string-heavy.parquet", {
    columnData: [
      { name: "id", data: sequence(rows), type: "INT32" },
      {
        name: "category",
        data: Array.from(
          { length: rows },
          (_value, index) => ["alpha", "beta", "gamma", "delta"][index % 4],
        ),
        type: "STRING",
      },
      {
        name: "payload",
        data: Array.from(
          { length: rows },
          (_value, index) => `event-${String(index).padStart(3, "0")}-${"x".repeat(24)}`,
        ),
        type: "STRING",
      },
    ],
  });
  return { "string-heavy.parquet": await storedFixtureFile(store, "string-heavy.parquet") };
}

async function timestampWindowFixture() {
  const store = memoryStore();
  await writeParquet(store, "event-windows.parquet", {
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
  return { "event-windows.parquet": await storedFixtureFile(store, "event-windows.parquet") };
}

async function highCardinalityFixture() {
  const store = memoryStore();
  const rows = 80;
  await writeParquet(store, "high-cardinality.parquet", {
    columnData: [
      {
        name: "entity_id",
        data: Array.from(
          { length: rows },
          (_value, index) => `entity-${String(index).padStart(3, "0")}`,
        ),
        type: "STRING",
      },
      {
        name: "amount",
        data: Array.from({ length: rows }, (_value, index) => index * 1.25),
        type: "DOUBLE",
      },
    ],
  });
  return {
    "high-cardinality.parquet": await storedFixtureFile(store, "high-cardinality.parquet"),
  };
}

async function storedFixtureFile(store, key) {
  const bytes = await store.get(key);
  if (bytes === null) throw new Error(`failed to write ${key}`);
  const dir = join(repoRoot, "bench/generated/duckdb", key.split("/").slice(0, -1).join("/"));
  await mkdir(dir, { recursive: true });
  const path = join(repoRoot, "bench/generated/duckdb", key);
  await writeFile(path, bytes);
  return { path, bytes };
}

function sequence(length) {
  return Array.from({ length }, (_value, index) => index);
}

function sqlString(value) {
  return value.replaceAll("'", "''");
}
