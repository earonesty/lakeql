import { createServer } from "node:http";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "../../http/src/index.js";
import { readOwnedParquetMetadata } from "./metadata-cache.js";
import { decodeOwnedFullMetadata } from "./owned-full-metadata.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it.each([
  false,
  true,
])("bounds exact-footer retries over HTTP, ignored ranges=%s", async (ignoreRange) => {
  const bytes = new Uint8Array(
    parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      columnData: [
        { name: "id", type: "INT32", data: Array.from({ length: 100000 }, (_, i) => i) },
      ],
    }),
  );
  const footerBytes = new DataView(bytes.buffer).getUint32(bytes.length - 8, true) + 8;
  const measurement = new MemoryScope(new MemoryBudget(Number.MAX_SAFE_INTEGER));
  const expected = decodeOwnedFullMetadata(bytes.buffer, measurement);
  const capacity = footerBytes + measurement.budget.peakBytes;
  measurement.dispose();
  expect(capacity).toBeLessThan(65536);
  const requests: { start: number; end: number; sent: number }[] = [];
  const server = createServer((request, response) => {
    const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
    if (!match) {
      response.writeHead(400).end();
      return;
    }
    const start = Number(match[1]),
      end = Number(match[2]) + 1;
    const payload = ignoreRange ? bytes : bytes.subarray(start, end);
    requests.push({ start, end, sent: payload.length });
    response.writeHead(ignoreRange ? 200 : 206, {
      "Content-Length": payload.length,
      ...(ignoreRange ? {} : { "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}` }),
      ETag: '"metadata-retry"',
    });
    response.end(payload);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  try {
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Missing listener");
    const store = new HttpObjectStore({ baseUrl: `http://127.0.0.1:${address.port}/` });
    const file = await asyncBufferFromStore(store, "fixture.parquet");
    for (const cap of [capacity, capacity - 1]) {
      requests.length = 0;
      const budget = new MemoryBudget(cap);
      if (cap === capacity) {
        const result = await readOwnedParquetMetadata("fixture.parquet", file, budget, undefined);
        try {
          expect(result.owner.value).toEqual(expected);
        } finally {
          result.owner.release();
        }
      } else {
        await expect(
          readOwnedParquetMetadata("fixture.parquet", file, budget, undefined),
        ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
      }
      expect(requests.map(({ start, end }) => [start, end])).toEqual([
        [bytes.length - cap, bytes.length],
        [bytes.length - footerBytes, bytes.length],
      ]);
      expect(requests.reduce((sum, request) => sum + request.sent, 0)).toBe(
        ignoreRange ? 2 * bytes.length : cap + footerBytes,
      );
      expect(budget.peakBytes).toBeLessThanOrEqual(cap);
      expect(budget.usedBytes).toBe(0);
    }
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
