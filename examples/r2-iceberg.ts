import { eq, loadIcebergTable, r2Store } from "../packages/laql/src/cloudflare.js";

export async function planR2Iceberg(bucket: Parameters<typeof r2Store>[0]) {
  const table = await loadIcebergTable({
    store: r2Store(bucket),
    metadataPath: "warehouse/places/metadata/v2.metadata.json",
    maxConcurrentReads: 4,
  });
  return table.planFiles({
    ref: "main",
    where: eq("country", "US"),
    readMode: "ignore-deletes",
  });
}
