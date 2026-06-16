import { createLake, httpStore } from "../packages/lakeql/src/node.js";

export async function queryHttpParquet(options: Parameters<typeof httpStore>[0]) {
  const lake = createLake({
    store: httpStore(options),
    budget: { maxOutputRows: 1000, maxConcurrentReads: 4 },
  });
  return await lake.path("sales.parquet").limit(100).toArray();
}
