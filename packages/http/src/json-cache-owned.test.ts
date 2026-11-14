import { createServer } from "node:http";
import { MemoryBudget, MemoryScope, ObjectStoreJsonCache } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "./index.js";

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
    const pool = new MemoryBudget(65536);
    const store = new HttpObjectStore({
      baseUrl: `http://127.0.0.1:${address.port}`,
      memoryBudget: pool,
    });
    const cache = new ObjectStoreJsonCache({ store, prefix: "cache" });
    const result = await cache.getOwnedEntry("key", pool);
    expect(result?.value).toEqual({ value });
    expect(pool.usedBytes).toBeGreaterThanOrEqual(result?.bytes ?? 0);
    expect(pool.peakBytes).toBeGreaterThan(pool.usedBytes);
    result?.release();
    if (status === 200) {
      expect(pool.usedBytes).toBeGreaterThan(0);
      expect(ranges).toHaveLength(1);
    }
    store.clearFullObjectCache();
    expect(pool.usedBytes).toBe(0);
    expect(ranges.length).toBeGreaterThan(0);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});

it("writes owned standard JSON through real HTTP and reads it back", async () => {
  let stored = Buffer.alloc(0);
  const server = createServer(async (request, response) => {
    if (request.method === "PUT") {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(Buffer.from(chunk));
      stored = Buffer.concat(chunks);
      response.writeHead(204).end();
    } else response.writeHead(200, { "content-length": stored.length }).end(stored);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    if (address === null || typeof address === "string") throw Error("No server port");
    const pool = new MemoryBudget(65536);
    const scope = new MemoryScope(pool);
    const value = {
      date: new Date("2026-11-14T00:00:00Z"),
      bytes: new Uint8Array([0, 255]),
      text: "雪😀",
    };
    const input = scope.allocate(1024, "test input", () => ({ value }));
    const store = new HttpObjectStore({
      baseUrl: `http://127.0.0.1:${address.port}`,
      maxFullObjectCacheBytes: 0,
    });
    const cache = new ObjectStoreJsonCache({ store, prefix: "cache" });
    await cache.setOwnedEntry("key", input);
    expect(stored.toString()).toBe(JSON.stringify({ value }));
    scope.dispose();
    expect(pool.usedBytes).toBe(0);
    const result = await cache.getOwnedEntry("key", pool);
    expect(result?.value).toEqual(JSON.parse(JSON.stringify({ value })));
    result?.release();
    expect(pool.usedBytes).toBe(0);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
