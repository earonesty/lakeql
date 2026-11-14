/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { env } from "cloudflare:test";
import { type Expr, Lake, MemoryBudget, SharedMemoryCache } from "lakeql-core";
import { expect, it } from "vitest";
import { type R2BucketLike, R2ObjectStore } from "../../r2/src/index.js";
import { parquetScanner } from "./index.js";

it.each([
  64, 128,
])("admits row fallback filters in concurrent Worker R2 queries at %i MiB", async (mib) => {
  const bindings = env as unknown as { BROTLI_FIXTURE: string; MEMORY_TEST_BUCKET: R2BucketLike };
  expect(bindings.BROTLI_FIXTURE.length).toBeGreaterThan(0);
  const store = new R2ObjectStore(bindings.MEMORY_TEST_BUCKET);
  const path = `brotli-${mib}.parquet`;
  await store.put(
    path,
    Uint8Array.from(atob(bindings.BROTLI_FIXTURE), (c) => c.charCodeAt(0)),
  );
  const pool = new MemoryBudget(mib * 1024 * 1024);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 65536 });
  try {
    const expected = Array.from({ length: 513 }, (_, id) => ({
      id,
      text: id % 7 === 0 ? null : `dictionary text ${id % 19} 猫`,
    }));
    const scanner = parquetScanner(store, {
      scanRangeCache: { maxBytes: 512 * 1024, coalesceBytes: 0, sharedCache: cache },
    });
    const lake = new Lake({
      store,
      memoryPool: pool,
      scanner: { scan: scanner.scan.bind(scanner) },
      budget: { maxAllocatedMemoryBytes: 2 * 1024 * 1024 },
    });
    for (const expand of [false, true]) {
      const predicate: Expr = {
        kind: "compare",
        op: "eq",
        left: {
          kind: "call",
          fn: "replace",
          args: [
            { kind: "column", name: "text" },
            { kind: "literal", value: "dictionary" },
            { kind: "literal", value: expand ? "x".repeat(2 * 1024 * 1024) : "accepted" },
          ],
        },
        right: { kind: "literal", value: "accepted text 1 猫" },
      };
      const selected = expected.filter((row) => row.text === "dictionary text 1 猫");
      await Promise.all(
        ["batches", "aggregate", "ordered", "topK", "sort"].map(async (route) => {
          let query = lake.path(path).where(predicate).batchSize(32);
          if (["ordered", "topK", "sort"].includes(route))
            query = query.orderBy([{ column: "id" }]);
          if (route === "topK") query = query.limit(2);
          const result = query.run();
          const output =
            route === "aggregate"
              ? result
                  .aggregateWithState([], { count: { op: "count" } })
                  .then((value) => value.rows)
              : route === "topK"
                ? result.topKWithState().then((value) => value.rows)
                : route === "sort"
                  ? result.sortWithState().then((value) => value.rows)
                  : result.toArray();
          if (expand)
            await expect(output).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
          else
            expect(await output).toEqual(
              route === "aggregate"
                ? [{ count: selected.length }]
                : route === "topK"
                  ? selected.slice(0, 2)
                  : selected,
            );
        }),
      );
    }
    cache.deleteWhere(() => true);
    expect(pool.usedBytes).toBe(0);
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
  } finally {
    cache.deleteWhere(() => true);
    await store.delete(path);
  }
});
