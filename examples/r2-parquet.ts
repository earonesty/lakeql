import { createLake, r2Store } from "../packages/laql/src/cloudflare.js";

export async function queryR2Parquet(bucket: Parameters<typeof r2Store>[0]) {
  const lake = createLake({
    store: r2Store(bucket),
    budget: { maxOutputRows: 1000, maxConcurrentReads: 4 },
  });
  return await lake.path("sales.parquet").limit(100).toArray();
}

export default {
  async fetch(_request: Request, env: { DATA: Parameters<typeof r2Store>[0] }) {
    return Response.json(await queryR2Parquet(env.DATA));
  },
};
