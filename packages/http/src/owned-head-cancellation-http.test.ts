import { createServer } from "node:http";
import { MemoryBudget, MemoryScope, withObjectStoreReadControls } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "./index.js";

it.each([
  "headers",
  "body",
])("releases the HTTP metadata slot when cancelled during %s", async (phase) => {
  let entered!: () => void;
  let closed!: () => void;
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const disconnected = new Promise<void>((resolve) => {
    closed = resolve;
  });
  const requests: string[] = [];
  const server = createServer((request, response) => {
    requests.push(request.url ?? "");
    if (request.url === "/pending") {
      response.on("close", closed);
      if (phase === "body") {
        response.writeHead(206, { "Content-Length": "2", "Content-Range": "bytes 0-1/10" });
        response.write(new Uint8Array([1]));
      }
      entered();
      return;
    }
    response.writeHead(206, { "Content-Length": "2", "Content-Range": "bytes 0-1/10" });
    response.end(new Uint8Array([1, 2]));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const pool = new MemoryBudget(65536);
  const pendingScope = new MemoryScope(pool);
  const nextScope = new MemoryScope(pool);
  try {
    const address = server.address();
    if (!address || typeof address === "string") throw Error("Missing server address");
    const store = withObjectStoreReadControls(
      new HttpObjectStore({
        baseUrl: `http://127.0.0.1:${address.port}`,
        maxFullObjectCacheBytes: 0,
      }),
      { maxConcurrentReads: 1 },
    );
    const head = store.headOwned;
    if (head === undefined) throw Error("Missing owned metadata reader");
    const pending = head("pending", pendingScope);
    const rejected = expect(pending).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    await started;
    if (phase === "body") await expect.poll(() => pool.usedBytes).toBeGreaterThan(0);
    const next = head("next", nextScope);
    await Promise.resolve();
    expect(requests).toEqual(["/pending"]);
    pendingScope.dispose();
    await rejected;
    await disconnected;
    const owner = await next;
    expect(owner?.value.size).toBe(10);
    expect(requests).toEqual(["/pending", "/next"]);
    owner?.release();
    nextScope.dispose();
    expect(pool.usedBytes).toBe(0);
  } finally {
    pendingScope.dispose();
    nextScope.dispose();
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
