import { createServer } from "node:http";
import { MemoryBudget, ObjectStoreJsonCache } from "lakeql-core";
import { expect, it } from "vitest";
import { httpStore } from "./index.js";

it.each([
  206, 200,
])("reads owned JSON cache entries over real HTTP %s responses", async (status) => {
  const value = { files: [{ path: "雪.parquet", size: 12 }], text: "abc".repeat(1024) };
  const bytes = Buffer.from(JSON.stringify({ value }));
  const ranges: string[] = [];
  const server = createServer((request, response) => {
    if (request.method === "HEAD") {
      response.writeHead(200, { "content-length": bytes.length }).end();
      return;
    }
    const header = request.headers.range ?? "";
    ranges.push(header);
    const match = /^bytes=(\d+)-(\d+)$/.exec(header);
    if (status === 206 && match) {
      const start = Number(match[1]);
      const end = Math.min(Number(match[2]), bytes.length - 1);
      response
        .writeHead(206, {
          "content-length": end - start + 1,
          "content-range": `bytes ${start}-${end}/${bytes.length}`,
        })
        .end(bytes.subarray(start, end + 1));
    } else response.writeHead(200, { "content-length": bytes.length }).end(bytes);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    if (address === null || typeof address === "string") throw Error("No server port");
    const cache = new ObjectStoreJsonCache({
      store: httpStore({ baseUrl: `http://127.0.0.1:${address.port}` }),
      prefix: "cache",
    });
    const pool = new MemoryBudget(65536);
    const result = await cache.getOwnedEntry("key", pool);
    expect(result?.value).toEqual({ value });
    expect(pool.usedBytes).toBe(result?.bytes);
    expect(pool.peakBytes).toBeGreaterThan(pool.usedBytes);
    result?.release();
    expect(pool.usedBytes).toBe(0);
    expect(ranges.length).toBeGreaterThan(0);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
