import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { gzipSync } from "node:zlib";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { ownedResponseBody } from "./response-body.js";

it("bounds actual decoded fixture bytes over identity, gzip and chunked HTTP responses", async () => {
  const bytes = await readFile(
    new URL(
      "../../../fixtures/external/parquet-testing/data/nested_lists.snappy.parquet",
      import.meta.url,
    ),
  );
  const compressed = gzipSync(bytes);
  expect(compressed.length).toBeLessThan(bytes.length);
  const server = createServer((request, response) => {
    if (request.url === "/gzip") {
      response.writeHead(200, { "Content-Encoding": "gzip", "Content-Length": compressed.length });
      response.end(compressed);
    } else if (request.url === "/chunked") {
      response.writeHead(200);
      response.write(bytes.subarray(0, 17));
      setImmediate(() => response.end(bytes.subarray(17)));
    } else {
      response.writeHead(200, { "Content-Length": bytes.length });
      response.end(bytes);
    }
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    if (!address || typeof address === "string") throw Error("Missing server address");
    for (const transport of ["identity", "gzip", "chunked"]) {
      for (const maxBytes of [bytes.length, bytes.length - 1]) {
        const response = await fetch(`http://127.0.0.1:${address.port}/${transport}`);
        if (transport === "gzip")
          expect(Number(response.headers.get("content-length"))).toBe(compressed.length);
        const budget = new MemoryBudget(bytes.length * 2);
        const scope = new MemoryScope(budget);
        try {
          if (maxBytes < bytes.length) {
            await expect(ownedResponseBody(response, maxBytes, scope)).rejects.toMatchObject({
              code: "LAKEQL_BUDGET_EXCEEDED",
              details: { limit: maxBytes, actual: bytes.length },
            });
            expect(budget.usedBytes).toBe(0);
          } else {
            const output = await ownedResponseBody(response, maxBytes, scope);
            const retained = output.retain();
            scope.dispose();
            try {
              expect(retained.value).toEqual(new Uint8Array(bytes));
              expect(budget.usedBytes).toBe(retained.bytes);
              expect(retained.bytes).toBeLessThanOrEqual(maxBytes);
            } finally {
              retained.release();
            }
          }
        } finally {
          scope.dispose();
        }
        expect(budget.usedBytes).toBe(0);
        expect(budget.peakBytes).toBeLessThanOrEqual(bytes.length * 2);
        expect(response.body?.locked).toBe(false);
      }
    }
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
