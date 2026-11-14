import { createServer } from "node:http";
import { parquetWriteBuffer } from "hyparquet-writer";
import {
  cachedObjectStore,
  MemoryBudget,
  uriObjectStore,
  withObjectStoreReadControls,
} from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { createParquetLake } from "./index.js";

it.each([
  0,
  1024 * 1024,
])("propagates factory cache limit %i through wrappers to HTTP probes", async (maxBytes) => {
  const bytes = new Uint8Array(
    parquetWriteBuffer({ columnData: [{ name: "id", type: "INT32", data: [1, 2, 3] }] }),
  );
  let requests = 0;
  const server = createServer((_request, response) => {
    requests++;
    response.writeHead(200, { "content-length": bytes.length }).end(bytes);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    if (address === null || typeof address === "string") throw Error("No server address");
    const pool = new MemoryBudget(4 * 1024 * 1024);
    const http = new HttpObjectStore({ baseUrl: `http://127.0.0.1:${address.port}` });
    const store = withObjectStoreReadControls(
      cachedObjectStore(
        uriObjectStore(http, { scheme: "s3", authority: "bucket", prefix: "data" }),
        { maxBytes: 0 },
      ),
      { maxConcurrentReads: 2 },
    );
    const lake = createParquetLake({ store, memoryPool: pool, cache: { maxBytes } });
    const query = () => lake.path("s3://bucket/data/file.parquet").toArray();
    expect(await query()).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    if (maxBytes > 0) {
      expect(requests).toBe(1);
      expect(pool.usedBytes).toBeGreaterThan(0);
    } else {
      expect(requests).toBeGreaterThan(1);
      expect(pool.usedBytes).toBe(0);
    }
    requests = 0;
    expect(await query()).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    if (maxBytes > 0) expect(requests).toBe(0);
    else expect(requests).toBeGreaterThan(1);
    const cleanup = pool.reserve(pool.maxBytes, "reclaim all unpinned caches");
    cleanup.release();
    expect(pool.usedBytes).toBe(0);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
