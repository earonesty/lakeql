import { readFile } from "node:fs/promises";
import { createLake, gt, memoryStore } from "../packages/lakeql/src/node.js";

export async function queryLocalParquetWithBuilder(path: string) {
  const store = memoryStore();
  await store.put(path, await readFile(path));
  const lake = createLake({ store });

  return await lake
    .path(path)
    .select(["store_id", "amount"])
    .where(gt("amount", 900))
    .orderBy([{ column: "amount", direction: "asc" }])
    .limit(2)
    .toArray();
}
