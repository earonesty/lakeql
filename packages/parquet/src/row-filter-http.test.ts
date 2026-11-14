import { createServer } from "node:http";
import {
  type Expr,
  Lake,
  MemoryBudget,
  SharedMemoryCache,
  withObjectStoreReadControls,
} from "lakeql-core";
import { expect, it } from "vitest";
import { brotliFixtureRows, createBrotliFixture } from "../../../fixtures/src/brotli.js";
import { HttpObjectStore } from "../../http/src/index.js";
import { parquetScanner } from "./index.js";

it.each(
  [false, true].flatMap((ignoreRange) =>
    [false, true].map((projection) => ({ ignoreRange, projection })),
  ),
)("admits row expressions over HTTP: ignored=$ignoreRange, projection=$projection", async ({
  ignoreRange,
  projection,
}) => {
  const bytes = createBrotliFixture();
  const expected = brotliFixtureRows();
  let active = 0;
  let peakReads = 0;
  let reads = 0;
  const server = createServer((request, response) => {
    if (request.method === "HEAD") {
      response.writeHead(200, { "Content-Length": bytes.length }).end();
      return;
    }
    const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
    if (!match) {
      response.writeHead(400).end();
      return;
    }
    reads++;
    peakReads = Math.max(peakReads, ++active);
    const start = ignoreRange ? 0 : Number(match[1]);
    const end = ignoreRange ? bytes.length : Math.min(bytes.length, Number(match[2]) + 1);
    setTimeout(() => {
      response.writeHead(ignoreRange ? 200 : 206, {
        "Content-Length": end - start,
        ...(ignoreRange ? {} : { "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}` }),
        ETag: '"brotli-fixture"',
      });
      response.end(bytes.subarray(start, end));
      active--;
    }, 1);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    if (!address || typeof address === "string") throw Error("Missing listener");
    const store = withObjectStoreReadControls(
      new HttpObjectStore({ baseUrl: `http://127.0.0.1:${address.port}/` }),
      { maxConcurrentReads: 2 },
    );
    for (const cap of [64, 128]) {
      const pool = new MemoryBudget(cap * 1024 * 1024);
      const cache = new SharedMemoryCache({ maxBytes: 512 * 1024, memoryBudget: pool });
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
        const selected = expected
          .filter((row) => row.text === "dictionary text 1 猫")
          .map((row) => (projection ? { ...row, projected: "accepted text 1 猫" } : row));
        await Promise.all(
          ["batches", "aggregate", "ordered", "topK", "sort", "distinct", "orderedDistinct"].map(
            async (route) => {
              let query = lake
                .path("brotli.parquet")
                .where(
                  projection
                    ? {
                        kind: "compare",
                        op: "eq",
                        left: { kind: "column", name: "text" },
                        right: { kind: "literal", value: "dictionary text 1 猫" },
                      }
                    : predicate,
                )
                .batchSize(32);
              if (projection) query = query.project({ projected: predicate.left });
              if (["ordered", "topK", "sort", "orderedDistinct"].includes(route))
                query = query.orderBy([{ column: "id" }]);
              if (route === "topK") query = query.limit(2);
              if (route === "distinct" || route === "orderedDistinct")
                query = query.select(["text"]).distinct();
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
                  route === "distinct" || route === "orderedDistinct"
                    ? [
                        projection
                          ? { text: "dictionary text 1 猫", projected: "accepted text 1 猫" }
                          : { text: "dictionary text 1 猫" },
                      ]
                    : route === "aggregate"
                      ? [{ count: selected.length }]
                      : route === "topK"
                        ? selected.slice(0, 2)
                        : selected,
                );
            },
          ),
        );
      }
      expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
      cache.deleteWhere(() => true);
      expect(pool.usedBytes).toBe(0);
    }
    expect(reads).toBeGreaterThan(0);
    expect(peakReads).toBeLessThanOrEqual(2);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
}, 30000);
