import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { memorySpillAdapter } from "./runtime.js";

it.each([
  "bytes",
  "rows",
  "unsafe-size",
  "unsafe-count",
  "valid",
] as const)("validates restored sort run extent before scanning: %s", async (change) => {
  const pool = new MemoryBudget(1024 * 1024);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  let scans = 0;
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        scans++;
        yield [{ id: 3 }, { id: 1 }, { id: 2 }];
      },
    },
  });
  const spill = memorySpillAdapter();
  const first = await lake
    .path("table")
    .orderBy([{ column: "id" }])
    .sortWithState({ spill, spillId: "sort" });
  const snapshot = JSON.parse(new TextDecoder().decode(first.operatorState as Uint8Array));
  const run = snapshot.runs[0];
  if (change === "bytes") await spill.write(run.spillRef, new TextEncoder().encode('[{"id":1}]'));
  if (change === "rows") run.rowCount++;
  if (change === "unsafe-size") run.byteSize = Number.MAX_SAFE_INTEGER + 1;
  if (change === "unsafe-count") run.rowCount = Number.MAX_SAFE_INTEGER + 1;
  scans = 0;
  const pending = lake
    .path("table")
    .orderBy([{ column: "id" }])
    .sortWithState({
      spill,
      operatorState: new TextEncoder().encode(JSON.stringify(snapshot)),
    });
  if (change === "valid") {
    expect((await pending).rows).toEqual([
      { id: 1 },
      { id: 1 },
      { id: 2 },
      { id: 2 },
      { id: 3 },
      { id: 3 },
    ]);
    expect(scans).toBe(1);
  } else {
    await expect(pending).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });
    expect(scans).toBe(0);
  }
  expect(pool.usedBytes).toBe(0);
});
