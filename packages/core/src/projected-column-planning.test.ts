import { expect, it } from "vitest";
import { gt } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

it.each([
  "ordinary",
  "predicate",
  "late",
] as const)("rejects oversized %s column planning before opening the scanner", async (kind) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(2048);
  let opened = false;
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      scan() {
        throw Error("Unexpected row fallback");
      },
      scanVectorBatches() {
        opened = true;
        throw Error("Scanner must not open");
      },
    },
  });
  const name = "列".repeat(1000);
  let query = lake.path("table").select([kind === "predicate" ? "id" : name]);
  if (kind !== "ordinary") query = query.where(gt(kind === "predicate" ? name : "id", 0)).limit(1);
  await expect(query.run().toOwnedArray()).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
    details: { purpose: "read column name" },
  });
  expect(opened).toBe(false);
  expect(pool.usedBytes).toBe(0);
});
