import { createServer } from "node:http";
import { parquetWriteBuffer } from "hyparquet-writer";
import { Lake, MemoryBudget, memorySpillAdapter } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake, ParquetScanAdapter } from "./index.js";

it.each([
  206, 200,
])("validates aggregate and sort checkpoint restore before HTTP %i reads", async (status) => {
  const bytes = Buffer.from(
    parquetWriteBuffer({
      columnData: [
        { name: "key", type: "INT32", data: [1, 2, 1] },
        { name: "value", type: "INT32", data: [2, 3, 4] },
      ],
    }),
  );
  let requests = 0;
  const server = createServer((request, response) => {
    requests++;
    const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
    if (status === 206 && match) {
      const start = Number(match[1]);
      const end = Math.min(Number(match[2]), bytes.length - 1);
      response
        .writeHead(206, {
          "content-range": `bytes ${start}-${end}/${bytes.length}`,
          "content-length": end - start + 1,
        })
        .end(bytes.subarray(start, end + 1));
    } else response.writeHead(200, { "content-length": bytes.length }).end(bytes);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    if (address === null || typeof address === "string") throw Error("No server port");
    const pool = new MemoryBudget(4 * 1024 * 1024);
    const lake = createParquetLake({
      memoryPool: pool,
      store: new HttpObjectStore({
        baseUrl: `http://127.0.0.1:${address.port}`,
        maxFullObjectCacheBytes: 0,
      }),
    });
    const spec = { total: { op: "sum" as const, column: "value" } };
    const first = await lake.path("fixture.parquet").run().aggregateWithState(["key"], spec);
    expect(first.rows).toEqual([
      { key: 1, total: 6 },
      { key: 2, total: 3 },
    ]);
    expect(requests).toBeGreaterThan(0);
    requests = 0;
    await expect(
      lake
        .path("fixture.parquet")
        .run()
        .aggregateWithState(["key"], spec, { operatorState: first.operatorState, maxGroups: 1 }),
    ).rejects.toMatchObject({ code: "LAKEQL_GROUP_LIMIT_EXCEEDED" });
    expect(requests).toBe(0);
    expect(pool.usedBytes).toBe(0);
    const duplicate = JSON.parse(new TextDecoder().decode(first.operatorState as Uint8Array));
    duplicate.groups.push(duplicate.groups[0]);
    await expect(
      lake
        .path("fixture.parquet")
        .run()
        .aggregateWithState(["key"], spec, {
          operatorState: new TextEncoder().encode(JSON.stringify(duplicate)),
        }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });
    expect(requests).toBe(0);
    expect(pool.usedBytes).toBe(0);
    const resumed = await lake
      .path("fixture.parquet")
      .run()
      .aggregateWithState(["key"], spec, { operatorState: first.operatorState, maxGroups: 2 });
    expect(resumed.rows).toEqual([
      { key: 1, total: 12 },
      { key: 2, total: 6 },
    ]);
    expect(requests).toBeGreaterThan(0);
    expect(pool.usedBytes).toBe(0);
    const bufferedSpec = { median: { op: "median" as const, column: "value" } };
    const buffered = await lake.path("fixture.parquet").run().aggregateWithState([], bufferedSpec);
    const limited = createParquetLake({
      memoryPool: pool,
      budget: { maxBufferedRows: 2 },
      store: new HttpObjectStore({
        baseUrl: `http://127.0.0.1:${address.port}`,
        maxFullObjectCacheBytes: 0,
      }),
    });
    requests = 0;
    await expect(
      limited.path("fixture.parquet").run().aggregateWithState([], bufferedSpec, {
        operatorState: buffered.operatorState,
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    expect(requests).toBe(0);
    expect(pool.usedBytes).toBe(0);
    const spill = memorySpillAdapter();
    const continuationSpec = {
      n: { op: "count" as const },
      total: { op: "sum" as const, column: "value" },
      median: { op: "median" as const, column: "value" },
      quantile: { op: "quantile" as const, column: "value", quantile: 0.5 },
      distinct: { op: "count_distinct" as const, column: "value" },
      approximate: { op: "approx_count_distinct" as const, column: "value" },
      mode: { op: "mode" as const, column: "value" },
    };
    let previousSpill: string | undefined;
    for (let copies = 1; copies <= 3; copies++) {
      const spillId = `aggregate-continuation-${copies}`;
      const continued = await lake
        .path("fixture.parquet")
        .batchSize(1)
        .run()
        .aggregateWithState(["key"], continuationSpec, {
          spill,
          spillId,
          ...(previousSpill ? { operatorState: { spillRef: previousSpill } } : {}),
        });
      expect(continued.rows).toEqual([
        {
          key: 1,
          n: 2 * copies,
          total: 6 * copies,
          median: 3,
          quantile: 3,
          distinct: 2,
          approximate: 2,
          mode: 2,
        },
        {
          key: 2,
          n: copies,
          total: 3 * copies,
          median: 3,
          quantile: 3,
          distinct: 1,
          approximate: 1,
          mode: 3,
        },
      ]);
      expect(continued.operatorSpill?.id).toBe(spillId);
      expect(await spill.read(spillId)).toEqual(continued.operatorState);
      expect(pool.usedBytes).toBe(0);
      previousSpill = spillId;
    }
    const restoreLimited = createParquetLake({
      memoryPool: pool,
      budget: { maxBufferedRows: 1 },
      store: new HttpObjectStore({
        baseUrl: `http://127.0.0.1:${address.port}`,
        maxFullObjectCacheBytes: 0,
      }),
    });
    if (!previousSpill) throw new Error("Missing aggregate checkpoint");
    requests = 0;
    await expect(
      restoreLimited
        .path("fixture.parquet")
        .run()
        .aggregateWithState(["key"], continuationSpec, {
          spill,
          operatorState: { spillRef: previousSpill },
        }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    expect(requests).toBe(0);
    expect(pool.usedBytes).toBe(0);
    // A fresh run on the same pool must not inherit restored counters or charges.
    const fresh = await lake.path("fixture.parquet").run().aggregateWithState(["key"], spec);
    expect(fresh.rows).toEqual([
      { key: 1, total: 6 },
      { key: 2, total: 3 },
    ]);
    expect(pool.usedBytes).toBe(0);
    const rowStore = new HttpObjectStore({
      baseUrl: `http://127.0.0.1:${address.port}`,
      maxFullObjectCacheBytes: 0,
    });
    const rowAdapter = new ParquetScanAdapter(rowStore, { scanRangeCache: { maxBytes: 0 } });
    const rowLake = new Lake({
      store: rowStore,
      memoryPool: pool,
      scanner: { scan: rowAdapter.scan.bind(rowAdapter) },
    });
    const rowOnly = await rowLake
      .path("fixture.parquet")
      .batchSize(1)
      .run()
      .aggregate(["key"], continuationSpec);
    expect(rowOnly).toEqual([
      { key: 1, n: 2, total: 6, median: 3, quantile: 3, distinct: 2, approximate: 2, mode: 2 },
      { key: 2, n: 1, total: 3, median: 3, quantile: 3, distinct: 1, approximate: 1, mode: 3 },
    ]);
    expect(pool.usedBytes).toBe(0);
    const sorted = await lake
      .path("fixture.parquet")
      .orderBy([{ column: "value" }])
      .sortWithState({ spill, spillId: "sorted" });
    const sortState = JSON.parse(new TextDecoder().decode(sorted.operatorState as Uint8Array));
    sortState.runs[0].rowCount++;
    requests = 0;
    await expect(
      lake
        .path("fixture.parquet")
        .orderBy([{ column: "value" }])
        .sortWithState({
          spill,
          operatorState: new TextEncoder().encode(JSON.stringify(sortState)),
        }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });
    expect(requests).toBe(0);
    expect(pool.usedBytes).toBe(0);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
