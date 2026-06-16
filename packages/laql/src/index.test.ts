import { readFile } from "node:fs/promises";
import { fixturePath, ICEBERG, SALES } from "@laql/fixtures";
import { expect, it } from "vitest";
import {
  and,
  createLake,
  eq,
  gt,
  LaQLError,
  loadIcebergTable,
  loadTable,
  memoryStore,
  planFiles,
  readParquetObjects,
  scanBatches,
  scanRows,
  writePartitionedParquet,
} from "./index.js";

it("re-exports the core surface", () => {
  const expr = and(eq("region", "west"), gt("amount", 100));
  expect(expr.kind).toBe("logical");
  expect(new LaQLError("LAQL_PARSE_ERROR", "x").code).toBe("LAQL_PARSE_ERROR");
  expect(createLake).toBeTypeOf("function");
  expect(readParquetObjects).toBeTypeOf("function");
  expect(writePartitionedParquet).toBeTypeOf("function");
  expect(loadIcebergTable).toBeTypeOf("function");
  expect(loadTable).toBeTypeOf("function");
  expect(planFiles).toBeTypeOf("function");
  expect(scanBatches).toBeTypeOf("function");
  expect(scanRows).toBeTypeOf("function");
});

it("exports runtime driver subpaths", async () => {
  const cloudflare = await import("./cloudflare.js");
  const node = await import("./node.js");

  expect(cloudflare.createLake).toBeTypeOf("function");
  expect(cloudflare.writePartitionedParquet).toBeTypeOf("function");
  expect(cloudflare.loadIcebergTable).toBeTypeOf("function");
  expect(cloudflare.r2Store).toBeTypeOf("function");
  expect(node.createLake).toBeTypeOf("function");
  expect(node.writePartitionedParquet).toBeTypeOf("function");
  expect(node.loadIcebergTable).toBeTypeOf("function");
  expect(node.httpStore).toBeTypeOf("function");
  expect(node.s3Store).toBeTypeOf("function");
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
