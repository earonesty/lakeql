import { readFile } from "node:fs/promises";
import { fixturePath, ICEBERG, SALES } from "lakeql-fixtures";
import { expect, it } from "vitest";
import {
  and,
  createLake,
  eq,
  gt,
  LakeqlError,
  loadIcebergTable,
  loadTable,
  memoryStore,
  planFiles,
  querySql,
  readParquetObjects,
  scanBatches,
  scanRows,
  writePartitionedParquet,
} from "./index.js";

it("re-exports the core surface", () => {
  const expr = and(eq("region", "west"), gt("amount", 100));
  expect(expr.kind).toBe("logical");
  expect(new LakeqlError("LAKEQL_PARSE_ERROR", "x").code).toBe("LAKEQL_PARSE_ERROR");
  expect(createLake).toBeTypeOf("function");
  expect(readParquetObjects).toBeTypeOf("function");
  expect(writePartitionedParquet).toBeTypeOf("function");
  expect(loadIcebergTable).toBeTypeOf("function");
  expect(loadTable).toBeTypeOf("function");
  expect(planFiles).toBeTypeOf("function");
  expect(querySql).toBeTypeOf("function");
  expect(scanBatches).toBeTypeOf("function");
  expect(scanRows).toBeTypeOf("function");
});

it("exports runtime driver subpaths", async () => {
  const cloudflare = await import("./cloudflare.js");
  const node = await import("./node.js");

  expect(cloudflare.createLake).toBeTypeOf("function");
  expect(cloudflare.writePartitionedParquet).toBeTypeOf("function");
  expect(cloudflare.loadIcebergTable).toBeTypeOf("function");
  expect(cloudflare.querySql).toBeTypeOf("function");
  expect(cloudflare.r2Store).toBeTypeOf("function");
  expect(node.createLake).toBeTypeOf("function");
  expect(node.writePartitionedParquet).toBeTypeOf("function");
  expect(node.loadIcebergTable).toBeTypeOf("function");
  expect(node.querySql).toBeTypeOf("function");
  expect(node.httpStore).toBeTypeOf("function");
  expect(node.s3Store).toBeTypeOf("function");
});

it("runs SQL through the public lake helper", async () => {
  const store = memoryStore();
  await store.put(SALES.file, await readFile(fixturePath(SALES.file)));
  const lake = createLake({ store });

  await expect(
    lake
      .sql("select store_id, amount from input where amount > $1 order by amount asc limit 2", {
        path: SALES.file,
        parameters: [900],
      })
      .toArray(),
  ).resolves.toEqual([
    { store_id: "store-002", amount: 923.79 },
    { store_id: "store-003", amount: 924.52 },
  ]);

  await expect(
    querySql(lake, `select store_id, amount from read_parquet('${SALES.file}') limit 1`).first(),
  ).resolves.toEqual({ store_id: "store-000", amount: 0 });
});

it("formats public SQL helper results", async () => {
  const store = memoryStore();
  await store.put(SALES.file, await readFile(fixturePath(SALES.file)));
  const lake = createLake({ store });
  const result = lake.sql(
    "select store_id, region, amount from input where region = $1 order by amount asc limit 2",
    { path: SALES.file, parameters: ["west"] },
  );

  await expect(result.count()).resolves.toBe(2);
  await expect(result.first()).resolves.toEqual({
    store_id: "store-000",
    region: "west",
    amount: 0,
  });
  const rows = [];
  for await (const row of result.rows()) rows.push(row);
  expect(rows).toEqual([
    { store_id: "store-000", region: "west", amount: 0 },
    { store_id: "store-000", region: "west", amount: 36.28 },
  ]);
  const batches = [];
  for await (const batch of result.batches()) batches.push(batch);
  expect(batches).toEqual([rows]);
  await expect(readStream(result.streamJson())).resolves.toBe(JSON.stringify(rows));
  await expect(readStream(result.streamNdjson())).resolves.toBe(
    '{"store_id":"store-000","region":"west","amount":0}\n{"store_id":"store-000","region":"west","amount":36.28}',
  );
  await expect(readStream(result.streamCsv())).resolves.toBe(
    "store_id,region,amount\nstore-000,west,0\nstore-000,west,36.28\n",
  );
});

it("runs SQL over read_parquet globs and named table joins", async () => {
  const store = memoryStore();
  await writePartitionedParquet(store, "sql/sales", {
    rows: [
      { store_id: "s1", amount: 10 },
      { store_id: "s2", amount: 20 },
    ],
  });
  await writePartitionedParquet(store, "sql/stores", {
    rows: [
      { store_id: "s1", segment: "retail", region: "west" },
      { store_id: "s3", segment: "enterprise", region: "east" },
    ],
  });
  await writePartitionedParquet(store, "sql/regions", {
    rows: [
      { region: "west", manager: "Ada" },
      { region: "east", manager: "Grace" },
    ],
  });
  const lake = createLake({ store });

  await expect(
    lake.sql("select store_id, amount from read_parquet('sql/sales/*.parquet')").toArray(),
  ).resolves.toEqual([
    { store_id: "s1", amount: 10 },
    { store_id: "s2", amount: 20 },
  ]);

  await expect(
    lake
      .sql(
        "select s.store_id as store_id, d.segment as segment from sales s left join stores d using (store_id)",
        {
          tables: {
            sales: "sql/sales/*.parquet",
            stores: "sql/stores/*.parquet",
          },
        },
      )
      .toArray(),
  ).resolves.toEqual([
    { store_id: "s1", segment: "retail" },
    { store_id: "s2", segment: null },
  ]);

  await expect(
    lake
      .sql(
        "select d.store_id as store_id, s.amount as amount, d.segment as segment from sales s right join stores d using (store_id) order by d.store_id",
        {
          tables: {
            sales: "sql/sales/*.parquet",
            stores: "sql/stores/*.parquet",
          },
        },
      )
      .toArray(),
  ).resolves.toEqual([
    { store_id: "s1", amount: 10, segment: "retail" },
    { store_id: "s3", amount: null, segment: "enterprise" },
  ]);

  await expect(
    lake
      .sql(
        [
          "select s.store_id as sales_store, d.store_id as dim_store, s.amount as amount, d.segment as segment",
          "from sales s full join stores d using (store_id)",
          "order by s.store_id asc nulls last, d.store_id asc nulls last",
        ].join(" "),
        {
          tables: {
            sales: "sql/sales/*.parquet",
            stores: "sql/stores/*.parquet",
          },
        },
      )
      .toArray(),
  ).resolves.toEqual([
    { sales_store: "s1", dim_store: "s1", amount: 10, segment: "retail" },
    { sales_store: "s2", dim_store: null, amount: 20, segment: null },
    { sales_store: null, dim_store: "s3", amount: null, segment: "enterprise" },
  ]);

  await expect(
    lake
      .sql(
        [
          "select s.store_id as store_id, d.segment as segment, r.manager as manager",
          "from sales s",
          "join stores d on s.store_id = d.store_id",
          "join regions r on d.region = r.region",
          "order by s.store_id",
        ].join(" "),
        {
          tables: {
            sales: "sql/sales/*.parquet",
            stores: "sql/stores/*.parquet",
            regions: "sql/regions/*.parquet",
          },
        },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "s1", segment: "retail", manager: "Ada" }]);

  await expect(
    lake
      .sql(
        [
          "select store_id",
          "from sales",
          "where store_id in (select store_id from stores order by store_id asc limit 1)",
        ].join(" "),
        {
          tables: {
            sales: "sql/sales/*.parquet",
            stores: "sql/stores/*.parquet",
          },
        },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "s1" }]);

  await expect(
    lake
      .sql(
        [
          "select s.store_id as store_id, r.manager as manager",
          "from sales s cross join regions r",
          "order by s.store_id asc, r.manager asc",
          "limit 3",
        ].join(" "),
        {
          tables: {
            sales: "sql/sales/*.parquet",
            regions: "sql/regions/*.parquet",
          },
        },
      )
      .toArray(),
  ).resolves.toEqual([
    { store_id: "s1", manager: "Ada" },
    { store_id: "s1", manager: "Grace" },
    { store_id: "s2", manager: "Ada" },
  ]);

  await expect(
    lake
      .sql("select * from sales cross join regions", {
        tables: {
          sales: "sql/sales/*.parquet",
          regions: "sql/regions/*.parquet",
        },
        joinMaxOutputRows: 3,
      })
      .toArray(),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it("runs SQL over named Iceberg table bindings", async () => {
  const store = memoryStore();
  await writeIcebergFixture(store);
  await writePartitionedParquet(store, "sql/labels", {
    rows: [
      { id: 0, label: "zero" },
      { id: 2, label: "two" },
      { id: 300, label: "filtered" },
    ],
  });
  const lake = createLake({ store });
  const icebergTables = {
    places: {
      metadataPath: ICEBERG.metadataFile,
    },
  };

  await expect(
    lake
      .sql("select id, nation from places where country = 'US' order by id", { icebergTables })
      .toArray(),
  ).resolves.toEqual([
    { id: 0, nation: "US" },
    { id: 2, nation: "US" },
    { id: 3, nation: "US" },
    { id: 200, nation: "US" },
    { id: 201, nation: "US" },
    { id: 202, nation: "US" },
    { id: 203, nation: "US" },
  ]);

  await expect(
    lake
      .sql(
        [
          "select p.id as id, p.nation as nation, l.label as label",
          "from places p join labels l using (id)",
          "where p.country = 'US'",
          "order by p.id",
        ].join(" "),
        {
          icebergTables,
          tables: { labels: "sql/labels/*.parquet" },
        },
      )
      .toArray(),
  ).resolves.toEqual([
    { id: 0, nation: "US", label: "zero" },
    { id: 2, nation: "US", label: "two" },
  ]);

  await expect(
    lake
      .sql(
        [
          "select p.id as left_id, q.id as right_id",
          "from places p join places q on p.id < q.id",
          "where p.id = 0 and q.id = 2",
        ].join(" "),
        { icebergTables },
      )
      .toArray(),
  ).resolves.toEqual([{ left_id: 0, right_id: 2 }]);

  await expect(
    lake
      .sql(
        [
          "select id",
          "from places",
          "where country = 'US'",
          "and id in (select id from labels order by id desc limit 1)",
        ].join(" "),
        {
          icebergTables,
          tables: { labels: "sql/labels/*.parquet" },
        },
      )
      .toArray(),
  ).resolves.toEqual([]);

  await expect(
    lake
      .sql("select id from places", {
        tables: { places: "sql/labels/*.parquet" },
        icebergTables,
      })
      .toArray(),
  ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });

  const leakedTempObjects = [];
  for await (const object of store.list("__lakeql_sql_iceberg/"))
    leakedTempObjects.push(object.path);
  expect(leakedTempObjects).toEqual([]);
});

it("runs aggregate, CTE, scalar subquery, and semi-join SQL helpers", async () => {
  const store = memoryStore();
  await store.put(SALES.file, await readFile(fixturePath(SALES.file)));
  await writeStoresFixture(store, "sql/stores");
  await writePartitionedParquet(store, "sql/buckets", {
    rows: [{ amount: 1 }, { amount: 2 }, { amount: 20 }],
  });
  const lake = createLake({ store });

  await expect(
    lake
      .sql(
        "select region, count(*) as rows from input group by region order by region asc limit 1",
        { path: SALES.file },
      )
      .toArray(),
  ).resolves.toEqual([{ region: "east", rows: 25 }]);

  await expect(
    lake
      .sql(
        [
          "select case when amount < 10 then 'small' else 'large' end as bucket, count(*) as rows",
          "from read_parquet('sql/buckets/*.parquet')",
          "group by bucket",
          "order by bucket asc",
        ].join(" "),
      )
      .toArray(),
  ).resolves.toEqual([
    { bucket: "large", rows: 1 },
    { bucket: "small", rows: 2 },
  ]);

  await expect(
    lake
      .sql(
        [
          "select amount, case",
          "when amount >= 20 then case when amount = 20 then 'exact-large' else 'large' end",
          "when amount < 2 then 'tiny'",
          "else 'small'",
          "end as bucket",
          "from read_parquet('sql/buckets/*.parquet')",
          "order by amount asc",
        ].join(" "),
      )
      .toArray(),
  ).resolves.toEqual([
    { amount: 1, bucket: "tiny" },
    { amount: 2, bucket: "small" },
    { amount: 20, bucket: "exact-large" },
  ]);

  await expect(
    lake
      .sql(
        "with totals as (select region, count(*) as rows, max(amount) as max_amount from input group by region) select region, rows from totals where max_amount > 990 order by region asc",
        { path: SALES.file },
      )
      .toArray(),
  ).resolves.toEqual([
    { region: "east", rows: 25 },
    { region: "north", rows: 25 },
    { region: "south", rows: 25 },
  ]);

  await expect(
    lake
      .sql(
        "select store_id, (select max(amount) as max_amount from input) as max_amount from input order by amount asc limit 1",
        { path: SALES.file },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "store-000", max_amount: 999.27 }]);

  await expect(
    lake
      .sql(
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
        { path: SALES.file },
      )
      .toArray(),
  ).resolves.toEqual([
    { store_id: "store-000", amount: 0 },
    { store_id: "store-006", amount: 999.27 },
  ]);

  await expect(
    lake
      .sql(
        "select store_id, amount from sales where store_id in (select store_id from stores where segment = 'enterprise') order by amount asc limit 1",
        { tables: { sales: SALES.file, stores: "sql/stores/*.parquet" } },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "store-000", amount: 0 }]);

  await expect(
    lake
      .sql(
        [
          "select distinct region from sales",
          "where region in (",
          "select region from sales",
          "group by region",
          "having count(*) > 20 and max(amount) > 900",
          ")",
          "order by region asc",
        ].join(" "),
        { tables: { sales: SALES.file } },
      )
      .toArray(),
  ).resolves.toEqual([
    { region: "east" },
    { region: "north" },
    { region: "south" },
    { region: "west" },
  ]);

  await expect(
    lake
      .sql(
        [
          "with west_stores as (select store_id, segment from stores where region = 'west')",
          "select s.store_id as store_id, w.segment as segment",
          "from sales s join west_stores w using (store_id)",
          "order by s.amount asc limit 1",
        ].join(" "),
        { tables: { sales: SALES.file, stores: "sql/stores/*.parquet" } },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "store-000", segment: "enterprise" }]);

  await expect(
    lake
      .sql(
        [
          "with enterprise as (select store_id from stores where segment = 'enterprise')",
          "select store_id, amount from sales",
          "where store_id in (select store_id from enterprise)",
          "order by amount asc limit 1",
        ].join(" "),
        { tables: { sales: SALES.file, stores: "sql/stores/*.parquet" } },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "store-000", amount: 0 }]);

  await expect(
    lake
      .sql(
        [
          "with matched as (",
          "select s.store_id as store_id, d.segment as segment",
          "from sales s join stores d using (store_id)",
          "where d.segment = 'enterprise'",
          ")",
          "select distinct store_id, segment from matched order by store_id",
        ].join(" "),
        { tables: { sales: SALES.file, stores: "sql/stores/*.parquet" } },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "store-000", segment: "enterprise" }]);

  await expect(
    lake
      .sql(
        [
          "with matched as (",
          "select store_id, amount from sales",
          "where store_id in (select store_id from stores where segment = 'enterprise')",
          ")",
          "select store_id, amount from matched order by amount asc limit 1",
        ].join(" "),
        { tables: { sales: SALES.file, stores: "sql/stores/*.parquet" } },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "store-000", amount: 0 }]);

  await expect(
    lake
      .sql(
        [
          "with enriched as (",
          "select store_id, amount, (select max(amount) as max_amount from sales) as max_amount",
          "from sales",
          ")",
          "select store_id, max_amount from enriched order by amount asc limit 1",
        ].join(" "),
        { tables: { sales: SALES.file } },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "store-000", max_amount: 999.27 }]);

  await expect(
    lake
      .sql(
        [
          "select store_id, amount",
          "from (",
          "select store_id, amount from (",
          "select store_id, amount from sales where amount > 900",
          ") high_sales where amount < 950",
          ") filtered",
          "order by amount asc limit 1",
        ].join(" "),
        { tables: { sales: SALES.file } },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "store-002", amount: 923.79 }]);

  await expect(
    lake
      .sql(
        [
          "with filtered as (",
          "select store_id, amount from (",
          "select store_id, amount from sales where amount > 900",
          ") high_sales where amount < 950",
          ")",
          "select store_id, amount from filtered order by amount asc limit 1",
        ].join(" "),
        { tables: { sales: SALES.file } },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "store-002", amount: 923.79 }]);

  await expect(
    lake
      .sql(
        [
          "with filtered as (",
          "with high_sales as (select store_id, amount from sales where amount > 900)",
          "select store_id, amount from high_sales where amount < 950",
          ")",
          "select store_id, amount from filtered order by amount asc limit 1",
        ].join(" "),
        { tables: { sales: SALES.file } },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "store-002", amount: 923.79 }]);
});

it("rejects unsupported SQL helper runtime shapes with typed errors", async () => {
  const store = memoryStore();
  await store.put(SALES.file, await readFile(fixturePath(SALES.file)));
  await writeStoresFixture(store, "sql/stores");
  const lake = createLake({ store });

  const unsupportedQueries = [
    lake.sql("select count(*) as rows from sales s join stores d on s.store_id = d.store_id", {
      tables: { sales: SALES.file, stores: "sql/stores/*.parquet" },
    }),
    lake.sql("select count(*) as rows from sales where store_id in (select store_id from stores)", {
      tables: { sales: SALES.file, stores: "sql/stores/*.parquet" },
    }),
    lake.sql(
      "with none as (select store_id from input where amount > 2000) select store_id from none",
      { path: SALES.file },
    ),
    lake.sql("select store_id from input where store_id = (select store_id from input limit 2)", {
      path: SALES.file,
    }),
    lake.sql("delete from input", { path: SALES.file }),
  ];

  for (const result of unsupportedQueries) {
    await expect(result.toArray()).rejects.toMatchObject({ code: "LAKEQL_SQL_UNSUPPORTED" });
  }
});

it("runs SQL helper join planning, wildcard projection, and null ordering branches", async () => {
  const store = memoryStore();
  await writePartitionedParquet(store, "sql/sales", {
    rows: [
      { store_id: "s1", region: "west", amount: 10 },
      { store_id: "s2", region: "east", amount: 20 },
      { store_id: "s3", region: "west", amount: 30 },
    ],
  });
  await writePartitionedParquet(store, "sql/stores", {
    rows: [
      { store_id: "s1", region: "west", segment: "retail" },
      { store_id: "s3", region: "east", segment: "wrong-region" },
    ],
  });
  await writePartitionedParquet(store, "sql/buckets", {
    rows: [
      { bucket: "small", min_amount: 0, max_amount: 20 },
      { bucket: "large", min_amount: 20, max_amount: 100 },
    ],
  });
  const lake = createLake({ store });
  const tables = {
    sales: "sql/sales/*.parquet",
    stores: "sql/stores/*.parquet",
    buckets: "sql/buckets/*.parquet",
  };

  await expect(
    lake
      .sql(
        [
          "select s.store_id as store_id, d.segment as segment from sales s join stores d on s.store_id = d.store_id",
          "where s.amount between 0 and 25",
          "and d.segment = 'retail'",
          "and s.region = d.region",
          "order by d.segment desc offset 0 limit 1",
        ].join(" "),
        { tables },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "s1", segment: "retail" }]);

  await expect(
    lake
      .sql(
        [
          "select s.store_id as store_id, d.segment as segment",
          "from sales s join stores d on s.store_id = d.store_id",
          "where d.segment = 'retail'",
          "order by s.store_id asc",
        ].join(" "),
        { tables, joinMaxRightRows: 1 },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "s1", segment: "retail" }]);

  await expect(
    lake
      .sql(
        [
          "select s.store_id as id, d.segment as segment",
          "from sales s join stores d on s.store_id = d.store_id",
          "order by id desc limit 1",
        ].join(" "),
        { tables },
      )
      .toArray(),
  ).resolves.toEqual([{ id: "s3", segment: "wrong-region" }]);

  const wildcardRows = await lake
    .sql(
      "select * from sales s left join stores d using (store_id) order by d.segment asc nulls first, s.store_id asc limit 2",
      { tables },
    )
    .toArray();
  expect(wildcardRows).toEqual([
    {
      amount: 20,
      region: "east",
      store_id: "s2",
      "s.amount": 20,
      "s.region": "east",
      "s.store_id": "s2",
      "d.region": null,
      "d.segment": null,
      "d.store_id": null,
    },
    {
      amount: 10,
      region: "west",
      store_id: "s1",
      "s.amount": 10,
      "s.region": "west",
      "s.store_id": "s1",
      "d.region": "west",
      "d.segment": "retail",
      "d.store_id": "s1",
    },
  ]);

  await expect(
    lake
      .sql(
        "select s.store_id as store_id, d.segment as segment from sales s left join stores d using (store_id) order by d.segment asc nulls last, s.store_id asc limit 2",
        { tables },
      )
      .toArray(),
  ).resolves.toEqual([
    { store_id: "s1", segment: "retail" },
    { store_id: "s3", segment: "wrong-region" },
  ]);

  await expect(
    lake
      .sql(
        [
          "select s.store_id as store_id, d.segment as segment",
          "from sales s left join stores d using (store_id)",
          "where d.segment = 'retail'",
          "order by s.store_id asc",
        ].join(" "),
        { tables, joinMaxRightRows: 1 },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "s1", segment: "retail" }]);

  await expect(
    lake
      .sql(
        [
          "select s.store_id as store_id, d.segment as segment",
          "from sales s left join stores d using (store_id)",
          "where s.region = 'west'",
          "order by s.store_id asc",
        ].join(" "),
        { tables },
      )
      .toArray(),
  ).resolves.toEqual([
    { store_id: "s1", segment: "retail" },
    { store_id: "s3", segment: "wrong-region" },
  ]);

  await expect(
    readStream(
      lake
        .sql(
          "select s.store_id as store_id, d.segment as segment from sales s left join stores d using (store_id) order by d.segment asc nulls first, s.store_id asc limit 1",
          { tables },
        )
        .streamCsv(),
    ),
  ).resolves.toBe("store_id,segment\ns2,\n");

  await expect(
    lake
      .sql("select distinct store_id from sales where store_id in (select store_id from stores)", {
        tables,
      })
      .toArray(),
  ).resolves.toEqual([{ store_id: "s1" }, { store_id: "s3" }]);

  await expect(
    lake
      .sql(
        [
          "select s.store_id as store_id",
          "from sales s",
          "where s.store_id in (",
          "select d.store_id from stores d",
          "where d.region <> s.region",
          ")",
          "order by s.store_id asc",
        ].join(" "),
        { tables },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "s3" }]);

  await expect(
    lake
      .sql(
        [
          "select s.store_id as store_id, b.bucket as bucket",
          "from sales s join buckets b",
          "on s.amount >= b.min_amount and s.amount < b.max_amount",
          "order by s.store_id",
        ].join(" "),
        { tables },
      )
      .toArray(),
  ).resolves.toEqual([
    { store_id: "s1", bucket: "small" },
    { store_id: "s2", bucket: "large" },
    { store_id: "s3", bucket: "large" },
  ]);

  await expect(
    lake
      .sql(
        [
          "select s.store_id as store_id",
          "from sales s",
          "where exists (",
          "select 1 from buckets b",
          "where s.amount >= b.min_amount",
          "and s.amount < b.max_amount",
          "and b.bucket = 'large'",
          ")",
          "order by s.store_id asc",
        ].join(" "),
        { tables },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "s2" }, { store_id: "s3" }]);

  await expect(
    lake
      .sql(
        [
          "select s.store_id as store_id, b.bucket as bucket",
          "from sales s left join buckets b",
          "on s.amount < b.max_amount and b.bucket = 'small'",
          "order by s.store_id asc",
        ].join(" "),
        { tables },
      )
      .toArray(),
  ).resolves.toEqual([
    { store_id: "s1", bucket: "small" },
    { store_id: "s2", bucket: null },
    { store_id: "s3", bucket: null },
  ]);

  await expect(
    lake
      .sql(
        [
          "select b.bucket as bucket, s.store_id as store_id",
          "from sales s right join buckets b",
          "on s.amount < b.max_amount and b.bucket = 'small'",
          "order by b.bucket asc",
        ].join(" "),
        { tables },
      )
      .toArray(),
  ).resolves.toEqual([
    { bucket: "large", store_id: null },
    { bucket: "small", store_id: "s1" },
  ]);

  await expect(
    lake
      .sql(
        [
          "select b.bucket as bucket, s.store_id as store_id",
          "from sales s right join buckets b",
          "on s.amount < b.max_amount and b.bucket = 'small'",
          "where b.bucket = 'small'",
          "order by b.bucket asc",
        ].join(" "),
        { tables },
      )
      .toArray(),
  ).resolves.toEqual([{ bucket: "small", store_id: "s1" }]);

  await expect(
    lake
      .sql(
        [
          "select s.store_id as store_id, b.bucket as bucket",
          "from sales s full join buckets b",
          "on s.amount < b.max_amount and b.bucket = 'small'",
          "order by s.store_id asc nulls last, b.bucket asc nulls last",
        ].join(" "),
        { tables },
      )
      .toArray(),
  ).resolves.toEqual([
    { store_id: "s1", bucket: "small" },
    { store_id: "s2", bucket: null },
    { store_id: "s3", bucket: null },
    { store_id: null, bucket: "large" },
  ]);

  await expect(
    lake
      .sql("select s.store_id as store_id from sales s join buckets b on s.amount < b.max_amount", {
        tables,
        joinMaxOutputRows: 3,
      })
      .toArray(),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it("covers SQL helper defaults, validation, empty results, and CSV escaping", async () => {
  const store = memoryStore();
  await store.put(SALES.file, await readFile(fixturePath(SALES.file)));
  await writePartitionedParquet(store, "sql/quoted", {
    rows: [{ id: 1, label: 'a,"b"', formula: "=sum(A1:A2)", plus: "+1", minus: "-1", at: "@cmd" }],
  });
  const lake = createLake({ store });

  await expect(
    lake
      .sql("select store_id, amount where region = 'west' order by amount asc offset 1 limit 1", {
        path: SALES.file,
      })
      .toArray(),
  ).resolves.toEqual([{ store_id: "store-000", amount: 36.28 }]);

  await expect(
    lake
      .sql("select store_id as id, amount where region = 'west' order by amount desc limit 1", {
        path: SALES.file,
      })
      .toArray(),
  ).resolves.toEqual([{ id: "store-003", amount: 960.8 }]);

  await expect(
    lake
      .sql("select distinct region from input order by region asc offset 1 limit 2", {
        path: SALES.file,
      })
      .toArray(),
  ).resolves.toEqual([{ region: "north" }, { region: "south" }]);

  await expect(
    lake
      .sql(
        "select region, max(amount) as max_amount from input where amount >= 0 group by region having max_amount > 0 order by region asc offset 1 limit 2",
        { path: SALES.file },
      )
      .toArray(),
  ).resolves.toEqual([
    { region: "north", max_amount: 998.54 },
    { region: "south", max_amount: 999.27 },
  ]);

  await expect(
    lake
      .sql(
        "select region from input group by region having count(*) > 20 and max(amount) > 900 order by region asc",
        { path: SALES.file },
      )
      .toArray(),
  ).resolves.toEqual([
    { region: "east" },
    { region: "north" },
    { region: "south" },
    { region: "west" },
  ]);

  await expect(
    lake
      .sql("select * from input group by region order by region asc limit 1", { path: SALES.file })
      .toArray(),
  ).resolves.toEqual([{ region: "east" }]);

  await expect(
    lake
      .sql(
        "select distinct region, max(amount) as max_amount from input group by region order by region asc offset 1 limit 1",
        { path: SALES.file },
      )
      .toArray(),
  ).resolves.toEqual([{ region: "north", max_amount: 998.54 }]);

  await expect(
    lake
      .sql(
        "select store_id from input where amount = (select amount from input where amount > 2000 limit 1)",
        { path: SALES.file },
      )
      .toArray(),
  ).resolves.toEqual([]);

  await expect(
    lake
      .sql(
        "select store_id, amount, row_number() over (order by amount asc) as rn from input qualify rn = 1",
        { path: SALES.file },
      )
      .toArray(),
  ).resolves.toEqual([{ store_id: "store-000", amount: 0, rn: 1 }]);

  await expect(
    lake
      .sql(
        "select store_id, amount, amount * 2 as doubled, row_number() over (order by amount asc) as rn from input qualify doubled > 100 and rn <= 10 order by amount asc",
        { path: SALES.file },
      )
      .toArray(),
  ).resolves.toEqual([
    { store_id: "store-006", amount: 71.83, doubled: 143.66, rn: 6 },
    { store_id: "store-000", amount: 72.56, doubled: 145.12, rn: 7 },
    { store_id: "store-001", amount: 73.29, doubled: 146.58, rn: 8 },
    { store_id: "store-002", amount: 74.02, doubled: 148.04, rn: 9 },
    { store_id: "store-000", amount: 108.84, doubled: 217.68, rn: 10 },
  ]);

  await expect(
    lake
      .sql("select amount, count(*) as rows from input group by region", { path: SALES.file })
      .toArray(),
  ).rejects.toMatchObject({ code: "LAKEQL_SQL_UNSUPPORTED" });
  await expect(lake.sql("describe input", { path: SALES.file }).toArray()).rejects.toMatchObject({
    code: "LAKEQL_PARSE_ERROR",
  });

  const empty = lake.sql("select store_id from input where amount > 2000", { path: SALES.file });
  await expect(empty.toArray()).resolves.toEqual([]);
  await expect(readStream(empty.streamCsv())).resolves.toBe("\n");
  const emptyBatches = [];
  for await (const batch of empty.batches()) emptyBatches.push(batch);
  expect(emptyBatches).toEqual([]);

  await expect(
    readStream(
      lake
        .sql("select id, label, formula, plus, minus, at from read_parquet('sql/quoted/*.parquet')")
        .streamCsv(),
    ),
  ).resolves.toBe('id,label,formula,plus,minus,at\n1,"a,""b""",=sum(A1:A2),+1,-1,@cmd\n');
  await expect(
    readStream(
      lake
        .sql("select id, label, formula, plus, minus, at from read_parquet('sql/quoted/*.parquet')")
        .streamCsv({ preventFormulae: true }),
    ),
  ).resolves.toBe('id,label,formula,plus,minus,at\n1,"a,""b""",\'=sum(A1:A2),\'+1,\'-1,\'@cmd\n');

  await writePartitionedParquet(store, "sql/bigint", {
    rows: [{ id: 1n }, { id: 1n }],
  });
  await expect(
    lake.sql("select distinct id from read_parquet('sql/bigint/*.parquet')").toArray(),
  ).resolves.toEqual([{ id: 1n }]);

  await expect(
    lake
      .sql("select *, amount * 2 as doubled from input order by amount asc limit 1", {
        path: SALES.file,
      })
      .toArray(),
  ).resolves.toEqual([
    {
      amount: 0,
      doubled: 0,
    },
  ]);

  await expect(lake.sql("bogus", { path: SALES.file }).toArray()).rejects.toMatchObject({
    code: "LAKEQL_PARSE_ERROR",
  });
  await expect(
    lake
      .sql(
        "select region, count(*) as rows, row_number() over (order by region) as rn from input group by region",
        { path: SALES.file },
      )
      .toArray(),
  ).rejects.toMatchObject({ code: "LAKEQL_SQL_UNSUPPORTED" });
});

it("keeps SQL CTE materialization scoped to one query", async () => {
  const store = memoryStore();
  await store.put(SALES.file, await readFile(fixturePath(SALES.file)));
  const lake = createLake({ store });
  const sql =
    "with recent as (select store_id, amount from input where amount > $1) select store_id, amount from recent order by amount asc limit 1";

  await expect(lake.sql(sql, { path: SALES.file, parameters: [900] }).toArray()).resolves.toEqual([
    { store_id: "store-002", amount: 923.79 },
  ]);
  await expect(lake.sql(sql, { path: SALES.file, parameters: [990] }).toArray()).resolves.toEqual([
    { store_id: "store-004", amount: 997.81 },
  ]);
  await expect(
    lake
      .sql(
        "with recent as (select store_id, amount from input where amount > 900) select amount, count(*) as rows from recent group by store_id",
        { path: SALES.file },
      )
      .toArray(),
  ).rejects.toMatchObject({ code: "LAKEQL_SQL_UNSUPPORTED" });

  const leakedTempObjects = [];
  for await (const object of store.list("__lakeql_sql_cte/")) leakedTempObjects.push(object.path);
  expect(leakedTempObjects).toEqual([]);
});

async function writeStoresFixture(store: ReturnType<typeof memoryStore>, prefix: string) {
  await writePartitionedParquet(store, prefix, {
    rows: [
      { store_id: "store-000", region: "west", segment: "enterprise" },
      { store_id: "store-000", region: "east", segment: "wrong-region" },
      { store_id: "store-001", region: "east", segment: "retail" },
    ],
  });
}

async function writeIcebergFixture(store: ReturnType<typeof memoryStore>) {
  await store.put(ICEBERG.metadataFile, await readFile(fixturePath(ICEBERG.metadataFile)));
  await store.put(ICEBERG.manifestListFile, await readFile(fixturePath(ICEBERG.manifestListFile)));
  for (const manifestFile of ICEBERG.manifestFiles) {
    await store.put(manifestFile, await readFile(fixturePath(manifestFile)));
  }
  for (const dataFile of ICEBERG.dataFiles) {
    await store.put(dataFile, await readFile(fixturePath(dataFile)));
  }
  await store.put(
    ICEBERG.equalityDeleteFile,
    await readFile(fixturePath(ICEBERG.equalityDeleteFile)),
  );
  await store.put(
    ICEBERG.positionDeleteFile,
    await readFile(fixturePath(ICEBERG.positionDeleteFile)),
  );
}

async function readStream(stream: ReadableStream<Uint8Array>): Promise<string> {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  const length = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(bytes);
}

it("loads, plans, and scans a Parquet table through the unified engine surface", async () => {
  const store = memoryStore();
  await store.put(SALES.file, await readFile(fixturePath(SALES.file)));

  const table = await loadTable({ format: "parquet", store, path: SALES.file });
  const plan = planFiles(table);
  const batches: unknown[][] = [];
  const rows: unknown[] = [];

  for await (const batch of scanBatches(plan, { batchSize: 25 })) {
    batches.push(batch);
  }
  for await (const row of scanRows(plan)) {
    rows.push(row);
  }

  expect(plan).toMatchObject({ format: "parquet", files: [{ path: SALES.file }] });
  expect(batches.map((batch) => batch.length)).toEqual([25, 15, 25, 15, 20]);
  expect(rows).toHaveLength(SALES.rows);
});

it("loads, plans, and scans an Iceberg table through the unified engine surface", async () => {
  const store = memoryStore();
  await writeIcebergFixture(store);

  const table = await loadTable({
    format: "iceberg",
    store,
    metadataPath: ICEBERG.metadataFile,
  });
  const plan = planFiles(table, {
    where: eq("country", "US"),
    select: ["id", "nation"],
  });
  const rows = [];
  const controller = new AbortController();

  for await (const row of scanRows(plan, {
    maxConcurrentReads: 1,
    maxElapsedMs: 10_000,
    signal: controller.signal,
  })) {
    rows.push(row);
  }

  expect(plan).toMatchObject({ format: "iceberg", plan: { deleteFilesPlanned: 1 } });
  expect(rows).toEqual([
    { id: 0, nation: "US" },
    { id: 2, nation: "US" },
    { id: 3, nation: "US" },
    { id: 200, nation: "US" },
    { id: 201, nation: "US" },
    { id: 202, nation: "US" },
    { id: 203, nation: "US" },
  ]);
});
