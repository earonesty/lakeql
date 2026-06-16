import {
  createLake,
  eq,
  loadIcebergTable,
  r2Store,
} from "../../../packages/laql/src/cloudflare.js";

export interface Env {
  DATA: Parameters<typeof r2Store>[0];
  PARQUET_PATH?: string;
  ICEBERG_METADATA_PATH?: string;
}

const defaultParquetPath = "sales.parquet";
const defaultIcebergMetadataPath = "iceberg/warehouse/places/metadata/v2.metadata.json";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/parquet") return Response.json(await readParquetRows(env));
    if (url.pathname === "/iceberg") return Response.json(await planIcebergFiles(env));
    return new Response("not found\n", { status: 404 });
  },
};

async function readParquetRows(env: Env) {
  const lake = createLake({
    store: r2Store(env.DATA),
    budget: { maxOutputRows: 1000, maxConcurrentReads: 4, signal: AbortSignal.timeout(10_000) },
  });
  return await lake
    .path(env.PARQUET_PATH ?? defaultParquetPath)
    .limit(100)
    .toArray();
}

async function planIcebergFiles(env: Env) {
  const table = await loadIcebergTable({
    store: r2Store(env.DATA),
    metadataPath: env.ICEBERG_METADATA_PATH ?? defaultIcebergMetadataPath,
    maxConcurrentReads: 4,
    maxElapsedMs: 10_000,
  });
  const plan = table.planFiles({
    ref: "main",
    where: eq("country", "US"),
    readMode: "ignore-deletes",
  });
  return {
    snapshotId: plan.snapshotId,
    filesPlanned: plan.filesPlanned,
    filesSkipped: plan.filesSkipped,
    files: plan.files.map((file) => ({
      path: file.path,
      partition: file.partition,
      recordCount: file.recordCount,
    })),
  };
}
