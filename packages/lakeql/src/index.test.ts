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
      { store_id: "s1", segment: "retail" },
      { store_id: "s3", segment: "enterprise" },
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

  const leakedTempObjects = [];
  for await (const object of store.list("__lakeql_sql_cte/")) leakedTempObjects.push(object.path);
  expect(leakedTempObjects).toEqual([]);
});

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
