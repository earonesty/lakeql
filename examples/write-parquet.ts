import { memoryStore, writePartitionedParquet } from "../packages/lakeql/src/node.js";

export async function writeParquetResults() {
  const store = memoryStore();
  return await writePartitionedParquet(store, "out/sales", {
    rows: [
      { store_id: "store-001", region: "west", amount: 42 },
      { store_id: "store-002", region: "east", amount: 55 },
    ],
    partitionBy: ["region"],
  });
}
