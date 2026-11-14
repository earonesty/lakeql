import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { col, lit } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

const routes = [
  "row-projection",
  "row-ordered",
  "row-window",
  "vector-projection",
  "vector-window",
] as const;
it.each(
  routes.flatMap((route) => [false, true].map((abort) => ({ route, abort }))),
)("keeps expanded output live through a paused stream (%j)", async ({ route, abort }) => {
  const pool = new MemoryBudget(128 * 1024);
  const sibling = pool.reserve(37, "independent job");
  const controller = new AbortController();
  const store = memoryStore();
  await store.put("table", new Uint8Array([1]));
  let rowScans = 0;
  let vectorScans = 0;
  let closed = false;
  const scanner = {
    async *scan() {
      rowScans++;
      try {
        yield [
          { id: 0, text: "x" },
          { id: 1, text: "x" },
        ];
      } finally {
        closed = true;
      }
    },
    ...(route.startsWith("vector")
      ? {
          async *scanVectorBatches() {
            vectorScans++;
            try {
              yield { rowOffset: 0, batch: batchFromColumns({ id: [0, 1], text: ["x", "x"] }) };
            } finally {
              closed = true;
            }
          },
        }
      : {}),
  };
  const lake = new Lake({
    store,
    scanner,
    memoryPool: pool,
    budget: { signal: controller.signal },
  });
  const value = "a".repeat(10000);
  const expansion = {
    kind: "call" as const,
    fn: "replace",
    args: [col("text"), lit("x"), lit(value)],
  };
  let query = lake.path("table").batchSize(1);
  if (route.endsWith("window"))
    query = query.window("expanded", {
      fn: "first_value",
      args: [expansion],
      over: { partitionBy: [], orderBy: [{ expr: col("id") }] },
    });
  else query = query.project({ expanded: expansion });
  if (route === "row-ordered")
    query = query.orderBy([{ column: "id", direction: "desc" }]).limit(2);
  const stream = query.run().batches()[Symbol.asyncIterator]();
  try {
    const first = await stream.next();
    expect(first.value).toEqual([
      { id: route === "row-ordered" ? 1 : 0, text: "x", expanded: value },
    ]);
    // The payload alone needs 20,000 modeled bytes; an output directory or
    // planning charge cannot substitute for its retained allocation.
    expect(pool.usedBytes).toBeGreaterThanOrEqual(20000 + sibling.bytes);
    if (route.startsWith("vector")) {
      expect(vectorScans).toBeGreaterThan(0);
      expect(rowScans).toBe(0);
    } else expect(rowScans).toBeGreaterThan(0);
    if (abort) {
      controller.abort();
      await expect(stream.next()).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    }
  } finally {
    await stream.return?.();
  }
  expect(closed).toBe(true);
  expect(pool.usedBytes).toBe(sibling.bytes);
  sibling.release();
  expect(pool.usedBytes).toBe(0);
});

it("streams expanded vector projection under a cap smaller than the oversized projection", async () => {
  const pool = new MemoryBudget(128 * 1024);
  const store = memoryStore();
  await store.put("table", new Uint8Array([1]));
  const batch = batchFromColumns({ text: Array(1000).fill("x") });
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      scan() {
        throw new Error("unexpected row fallback");
      },
      async *scanVectorBatches() {
        yield { rowOffset: 0, batch };
      },
    },
  });
  const value = "a".repeat(10000);
  let count = 0;
  for await (const rows of lake
    .path("table")
    .batchSize(1)
    .project({
      expanded: { kind: "call", fn: "replace", args: [col("text"), lit("x"), lit(value)] },
    })
    .run()
    .batches()) {
    expect(rows).toEqual([{ text: "x", expanded: value }]);
    count += rows.length;
  }
  expect(count).toBe(1000);
  expect(pool.peakBytes).toBeLessThanOrEqual(128 * 1024);
  expect(pool.usedBytes).toBe(0);
});
