import { createServer } from "node:http";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, materializeBatchRows } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { acquireIndexedParquetSource } from "./indexed-source.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it.each([
  "abort",
  "disconnect",
])("cleans pending indexed data reads while a sibling completes (%s)", async (failure) => {
  const values = Array.from({ length: 40 }, (_, id) => id);
  const bytes = new Uint8Array(
    parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: [20, 20],
      columnData: [{ name: "id", type: "INT32", encoding: "PLAIN", data: values }],
    }),
  );
  const held = Promise.withResolvers<void>();
  const closed = Promise.withResolvers<void>();
  let hold = true;
  let disconnect = () => {};
  const server = createServer((request, response) => {
    const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
    if (!match) {
      response.writeHead(400).end();
      return;
    }
    const start = Number(match[1]),
      end = Number(match[2]) + 1;
    response.writeHead(206, {
      "Content-Length": end - start,
      "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}`,
    });
    // Discovery and footer reads finish; stall only the first data-range body.
    if (hold && start > 1 && end < bytes.length) {
      hold = false;
      response.write(bytes.subarray(start, start + 1));
      response.once("close", () => closed.resolve());
      disconnect = () => response.destroy();
      held.resolve();
    } else response.end(bytes.subarray(start, end));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Missing listener");
    const store = new HttpObjectStore({ baseUrl: `http://127.0.0.1:${address.port}/` });
    const file = await asyncBufferFromStore(store, "fixture");
    const root = new MemoryBudget(131072);
    const run = async (budget: MemoryBudget, signals: AbortSignal[] = []) => {
      const source = await acquireIndexedParquetSource(file, budget, signals);
      const rows = [];
      try {
        for await (const owner of readOwnedParquetVectorBatches(
          file,
          source,
          budget,
          { columns: ["id"], batchSize: 4, canStopEarly: true },
          signals,
        )) {
          try {
            rows.push(...materializeBatchRows(owner.value.batch));
          } finally {
            owner.release();
          }
        }
        expect(rows).toEqual(values.map((id) => ({ id })));
      } finally {
        source.dispose();
      }
    };
    const controller = new AbortController();
    const canceled = root.child(65536);
    const rejection = expect(run(canceled, [controller.signal])).rejects.toThrow();
    await held.promise;
    expect(canceled.usedBytes).toBeGreaterThan(0);
    const survivor = run(root.child(65536));
    if (failure === "abort") controller.abort();
    else disconnect();
    await rejection;
    await closed.promise;
    await survivor;
    expect(canceled.usedBytes).toBe(0);
    expect(root.usedBytes).toBe(0);
    expect(root.peakBytes).toBeLessThanOrEqual(root.maxBytes);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
