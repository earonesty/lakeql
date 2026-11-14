import { createServer } from "node:http";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, materializeBatchRows, withObjectStoreReadControls } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it("automatically windows HTTP reads under pressure and preserves concurrent progress", async () => {
  const values = Array.from({ length: 256 }, (_, i) => i);
  const bytes = new Uint8Array(
    parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      pageSize: 64,
      rowGroupSize: [values.length],
      columnData: [{ name: "id", type: "INT32", encoding: "PLAIN", data: values }],
    }),
  );
  const ranges: number[] = [];
  let fullGets = 0;
  let active = 0;
  let peakReads = 0;
  let holdNextRead: (() => void) | undefined;
  let heldReadClosed: (() => void) | undefined;
  const server = createServer((request, response) => {
    if (request.method === "HEAD") {
      response.writeHead(200, { "Content-Length": bytes.length }).end();
      return;
    }
    const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
    if (match === null) {
      fullGets++;
      response.writeHead(400).end();
      return;
    }
    const start = Number(match[1]);
    const end = Number(match[2]) + 1;
    ranges.push(end - start);
    active++;
    peakReads = Math.max(peakReads, active);
    if (holdNextRead !== undefined) {
      const notify = holdNextRead;
      holdNextRead = undefined;
      response.writeHead(206, {
        "Content-Length": end - start,
        "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}`,
      });
      response.write(bytes.subarray(start, start + 1));
      response.once("close", () => {
        active--;
        heldReadClosed?.();
      });
      notify();
      return;
    }
    setTimeout(() => {
      response.writeHead(206, {
        "Content-Length": end - start,
        "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}`,
      });
      response.end(bytes.subarray(start, end));
      active--;
    }, 1);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  try {
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("missing listener");
    const store = withObjectStoreReadControls(
      new HttpObjectStore({
        baseUrl: `http://127.0.0.1:${address.port}/`,
      }),
      { maxConcurrentReads: 2 },
    );
    const source = await asyncBufferFromStore(store, "pages.parquet");
    const metadata = await readParquetMetadataFromFile(source);
    async function run(budget: MemoryBudget, limit = values.length, signals: AbortSignal[] = []) {
      const rows = [];
      try {
        for await (const owner of readOwnedParquetVectorBatches(
          source,
          metadata,
          budget,
          {
            columns: ["id"],
            batchSize: 8,
          },
          signals,
        )) {
          rows.push(...materializeBatchRows(owner.value.batch));
          owner.release();
          if (rows.length >= limit) break;
        }
        expect(rows).toEqual(values.slice(0, rows.length).map((id) => ({ id })));
        expect(rows.length).toBe(limit);
      } finally {
        expect(budget.usedBytes).toBe(0);
      }
    }
    ranges.length = 0;
    await run(new MemoryBudget(65536));
    const fullChunkRequests = ranges.length;
    expect(Math.max(...ranges)).toBeGreaterThan(512);
    ranges.length = 0;
    const tight = new MemoryBudget(512);
    await run(tight);
    expect(Math.max(...ranges)).toBeLessThanOrEqual(256);
    expect(ranges.length).toBeGreaterThan(fullChunkRequests);
    const pageRequests = ranges.length;
    const exact = tight.peakBytes;
    await run(new MemoryBudget(exact));
    await expect(run(new MemoryBudget(exact - 1))).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
    ranges.length = 0;
    await run(new MemoryBudget(512), 8);
    expect(ranges.length).toBeLessThan(pageRequests);
    peakReads = 0;
    const root = new MemoryBudget(1024);
    await Promise.all([run(root.child(512)), run(root.child(512))]);
    expect(root.usedBytes).toBe(0);
    expect(root.peakBytes).toBeLessThanOrEqual(1024);
    expect(peakReads).toBe(2);
    const controller = new AbortController();
    const started = new Promise<void>((resolve) => {
      holdNextRead = resolve;
    });
    const closed = new Promise<void>((resolve) => {
      heldReadClosed = resolve;
    });
    const canceled = root.child(512);
    const rejection = expect(run(canceled, values.length, [controller.signal])).rejects.toThrow();
    await started;
    await expect.poll(() => canceled.usedBytes).toBeGreaterThan(0);
    const survivor = run(root.child(512));
    controller.abort();
    await rejection;
    await closed;
    await survivor;
    expect(canceled.usedBytes).toBe(0);
    expect(root.usedBytes).toBe(0);
    expect(root.peakBytes).toBeLessThanOrEqual(root.maxBytes);
    expect(fullGets).toBe(0);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
