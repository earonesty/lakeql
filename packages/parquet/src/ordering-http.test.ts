import { createServer } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import {
  gte,
  Lake,
  memoryStore,
  SharedMemoryCache,
  withObjectStoreReadControls,
} from "lakeql-core";
import { expect, it } from "vitest";
import { httpStore } from "../../http/src/index.js";
import { createLake } from "../../lakeql/src/sql.js";
import { writeParquet } from "./index.js";
import { ParquetScanAdapter } from "./scan-adapter.js";

it.each([
  false,
  true,
])("shares bounded ordering across row and deferred HTTP reads (ignoreRange=%s)", async (ignoreRange) => {
  const source = memoryStore();
  const rows = Array.from({ length: 512 }, (_, id) => ({
    id,
    score: (id * 17) % 23,
    payload: `${id}:`.padEnd(128, "x"),
  }));
  await writeParquet(source, "fixture.parquet", {
    rowGroupSize: [128, 128, 128, 128],
    columnData: [
      { name: "id", data: rows.map((r) => r.id) },
      { name: "score", data: rows.map((r) => r.score) },
      { name: "payload", data: rows.map((r) => r.payload) },
    ],
  });
  const bytes = await source.get("fixture.parquet");
  const ranges: string[] = [];
  let active = 0,
    peak = 0,
    requests = 0;
  const server = createServer((request, response) => {
    requests++;
    ranges.push(request.headers.range ?? "missing");
    active++;
    peak = Math.max(peak, active);
    response.once("close", () => {
      active--;
    });
    void delay(2).then(() => {
      const range = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
      if (range === null) {
        response.writeHead(400).end();
        return;
      }
      const start = ignoreRange ? 0 : Number(range[1]);
      const end = ignoreRange ? bytes.length : Math.min(bytes.length, Number(range[2]) + 1);
      response.writeHead(ignoreRange ? 200 : 206, {
        "content-length": end - start,
        ...(ignoreRange ? {} : { "content-range": `bytes ${start}-${end - 1}/${bytes.length}` }),
        etag: '"fixture"',
      });
      response.end(bytes.subarray(start, end));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw Error("missing server address");
  try {
    const store = withObjectStoreReadControls(
      httpStore({
        baseUrl: `http://127.0.0.1:${address.port}`,
        objects: [{ path: "fixture.parquet", size: bytes.length }],
        maxFullObjectCacheBytes: 0,
      }),
      { maxConcurrentReads: 2 },
    );
    const scanner = new ParquetScanAdapter(store, {
      scanRangeCache: {
        maxBytes: 256 * 1024,
        sharedCache: new SharedMemoryCache({ maxBytes: 256 * 1024 }),
      },
    });
    const rowLake = new Lake({
      store,
      scanner: { scan: scanner.scan.bind(scanner) },
      budget: { maxBufferedRows: 13, maxMemoryBytes: 64 * 1024 },
    });
    const deferredLake = new Lake({
      store,
      scanner,
      budget: { maxBufferedRows: 13, maxMemoryBytes: 64 * 1024 },
    });
    const expected = [...rows]
      .sort((a, b) => b.score - a.score)
      .slice(3, 13)
      .map(({ score, payload }) => ({ score, payload }));
    const run = (lake: Lake) =>
      lake
        .path("fixture.parquet")
        .batchSize(31)
        .select(["score", "payload"])
        .where(gte("id", 0))
        .orderBy([{ column: "score", direction: "desc" }])
        .offset(3)
        .limit(10)
        .run();
    const results = await Promise.all([run(rowLake).toArray(), run(deferredLake).toArray()]);
    expect(results).toEqual([expected, expected]);
    expect(requests).toBeGreaterThan(0);
    expect(peak).toBe(2);
    const before = requests;
    expect(await run(deferredLake).toArray()).toEqual(expected);
    // Planning probes can remain; cached data ranges must not be refetched.
    expect(ranges.slice(before)).toEqual(["bytes=0-1", "bytes=0-1", "bytes=0-1"]);
    const controller = new AbortController();
    controller.abort("cancelled ordering");
    const cancelledStore = withObjectStoreReadControls(store, { signal: controller.signal });
    const cancelled = new Lake({
      store: cancelledStore,
      scanner: new ParquetScanAdapter(cancelledStore),
    });
    const beforeCancel = requests;
    await expect(run(cancelled).toArray()).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    expect(requests).toBe(beforeCancel);
    expect(await run(deferredLake).toArray()).toEqual(expected);
    const scalar = "(SELECT MAX(score) FROM input)";
    const sqlRows = async (repeat: boolean, memoMaxBytes?: number) => {
      const lake = createLake({ store, scanRangeCache: { maxBytes: 0 } });
      return lake
        .sql(`SELECT id, ${scalar} AS a${repeat ? `, ${scalar} AS b` : ""} FROM input LIMIT 2`, {
          tables: { input: "fixture.parquet" },
          ...(memoMaxBytes === undefined ? {} : { memoMaxBytes }),
        })
        .toArray();
    };
    const beforeSingle = requests;
    expect(await sqlRows(false)).toEqual([
      { id: 0, a: 22 },
      { id: 1, a: 22 },
    ]);
    const singleReads = requests - beforeSingle;
    const beforeRepeated = requests;
    const repeatedRows = [
      { id: 0, a: 22, b: 22 },
      { id: 1, a: 22, b: 22 },
    ];
    expect(await sqlRows(true)).toEqual(repeatedRows);
    expect(requests - beforeRepeated).toBe(singleReads);
    const beforeDisabled = requests;
    expect(await sqlRows(true, 0)).toEqual(repeatedRows);
    expect(requests - beforeDisabled).toBeGreaterThan(singleReads);
    const first = await run(rowLake).topKWithState();
    const resumed = await new Lake({ store, scanner: { async *scan() {} } })
      .path("fixture.parquet")
      .select(["score", "payload"])
      .orderBy([{ column: "score", direction: "desc" }])
      .offset(3)
      .limit(10)
      .topKWithState({ operatorState: first.operatorState });
    expect(resumed.rows).toEqual(expected);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
