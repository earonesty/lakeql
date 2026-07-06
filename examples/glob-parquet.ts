import { createLake, memoryStore } from "../packages/lakeql/src/node.js";

export async function queryParquetGlob(files: { path: string; bytes: Uint8Array }[]) {
  const store = memoryStore();
  for (const file of files) await store.put(file.path, file.bytes);
  const lake = createLake({ store });

  return await lake
    .sql("select store_id, amount from read_parquet('sales/*.parquet') order by amount asc limit 3")
    .toArray();
}
