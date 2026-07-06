import { readFile } from "node:fs/promises";
import { createLake, memoryStore } from "../packages/lakeql/src/node.js";

export async function queryLocalParquetWithSql(path: string) {
  const store = memoryStore();
  await store.put(path, await readFile(path));
  const lake = createLake({ store });

  return await lake
    .sql("select store_id, amount from input where amount > $1 order by amount asc limit 2", {
      path,
      parameters: [900],
    })
    .toArray();
}
