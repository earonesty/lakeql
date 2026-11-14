import { MemoryBudget, SharedMemoryCache } from "../packages/core/dist/index.js";
import { HttpObjectStore } from "../packages/http/dist/index.js";
import { createParquetLake } from "../packages/parquet/dist/index.js";
import { R2ObjectStore } from "../packages/r2/dist/index.js";

let pool, cache, lake;
let heldRows = [];
let heldOwners = [];
function releaseHeldOutput() {
  heldRows = [];
  for (const owner of heldOwners) owner.release();
  heldOwners = [];
}
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/configure") {
      releaseHeldOutput();
      cache?.deleteWhere(() => true);
      const previousUsedBytes = pool?.usedBytes ?? 0;
      pool = new MemoryBudget(Number(url.searchParams.get("mib")) * 1024 * 1024);
      cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 8 * 1024 * 1024 });
      lake = createParquetLake({
        store:
          env.STORE_BASE === undefined
            ? new R2ObjectStore(env.DATA)
            : new HttpObjectStore({ baseUrl: env.STORE_BASE }),
        memoryPool: pool,
        scanRangeCache: { maxBytes: 8 * 1024 * 1024, sharedCache: cache },
        budget: { maxAllocatedMemoryBytes: pool.maxBytes },
      });
      return Response.json({ configured: true, previousUsedBytes });
    }
    if (url.pathname === "/held") {
      return Response.json({
        jobs: heldRows.length,
        rows: heldRows.reduce((sum, rows) => sum + rows.length, 0),
        lastIds: heldRows.map((rows) => rows.at(-1)?.id),
      });
    }
    if (url.pathname === "/release") {
      releaseHeldOutput();
      return Response.json({ released: true });
    }
    if (url.pathname === "/evict") {
      cache.deleteWhere(() => true);
      return Response.json({ usedBytes: pool.usedBytes, peakBytes: pool.peakBytes });
    }
    if (url.searchParams.get("retainPrevious") !== "1") releaseHeldOutput();
    const concurrency = Number(url.searchParams.get("jobs"));
    const reports = await Promise.all(
      Array.from({ length: concurrency }, async () => {
        const result = lake.path("stress.parquet").batchSize(1024).run();
        try {
          let ids = 0,
            characters = 0,
            count = 0;
          if (url.searchParams.get("mode") === "stream") {
            for await (const row of result.rows()) {
              ids += row.id;
              characters += row.payload.length;
              count++;
            }
          } else {
            const owner =
              url.searchParams.get("mode") === "owned" ? await result.toOwnedArray() : undefined;
            const rows = owner === undefined ? await result.toArray() : owner.value;
            if (owner !== undefined) heldOwners.push(owner);
            heldRows.push(rows);
            for (const row of rows) {
              ids += row.id;
              characters += row.payload.length;
            }
            count = rows.length;
          }
          return { status: "ok", rows: count, ids, characters, ranges: result.stats.rangeRequests };
        } catch (error) {
          if (error?.code !== "LAKEQL_BUDGET_EXCEEDED") throw error;
          return {
            status: "budget",
            code: error.code,
            allocation: error.details,
            rowsReturned: result.stats.rowsReturned,
            ranges: result.stats.rangeRequests,
          };
        }
      }),
    );
    return Response.json({ reports, usedBytes: pool.usedBytes, peakBytes: pool.peakBytes });
  },
};
