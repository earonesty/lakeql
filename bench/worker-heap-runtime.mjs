import assert from "node:assert/strict";
import { createServer } from "node:http";
import { createRequire } from "node:module";

const require = createRequire(import.meta.resolve("@cloudflare/vitest-pool-workers"));
const { Miniflare, Runtime, serializeConfig, kInspectorSocket } = require("miniflare");

/** Local backends only. V8 flags limit managed heap, not total Worker memory. */
export async function createWorkerHeapRuntime(script, bytes, heapMiB) {
  if (heapMiB === undefined) {
    const mf = new Miniflare({
      name: "heap-stress",
      modules: true,
      script,
      compatibilityDate: "2026-06-13",
      compatibilityFlags: ["nodejs_compat"],
      r2Buckets: ["DATA"],
      inspectorPort: 0,
    });
    try {
      await mf.ready;
      await (await mf.getR2Bucket("DATA")).put("stress.parquet", bytes);
      return mf;
    } catch (error) {
      await mf.dispose();
      throw error;
    }
  }
  assert.ok(Number.isSafeInteger(heapMiB) && heapMiB >= 32 && heapMiB <= 256);
  const server = createServer((request, response) => {
    const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? "");
    if (!match) {
      response.writeHead(400).end();
      return;
    }
    const start = Number(match[1]),
      end = Math.min(Number(match[2]) + 1, bytes.length);
    if (start >= bytes.length) {
      response.writeHead(416).end();
      return;
    }
    response.writeHead(206, {
      "Content-Length": end - start,
      "Content-Range": `bytes ${start}-${end - 1}/${bytes.length}`,
      ETag: '"heap-fixture"',
    });
    response.end(bytes.subarray(start, end));
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  const loopbackAddress = `127.0.0.1:${address.port}`;
  const runtime = new Runtime();
  const controller = new AbortController();
  let runtimeStderr = "";
  const dispose = async () => {
    controller.abort();
    await runtime.dispose();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  };
  try {
    const ports = await runtime.updateConfig(
      serializeConfig({
        v8Flags: [`--max-heap-size=${heapMiB}`],
        services: [
          { name: "loopback", external: { address: loopbackAddress, http: {} } },
          {
            name: "heap-stress",
            worker: {
              modules: [{ name: "worker.mjs", esModule: script }],
              compatibilityDate: "2026-06-13",
              compatibilityFlags: ["nodejs_compat"],
              bindings: [{ name: "STORE_BASE", text: "http://fixture/" }],
              globalOutbound: { name: "loopback" },
            },
          },
        ],
        sockets: [
          { name: "entry", address: "127.0.0.1:0", http: {}, service: { name: "heap-stress" } },
        ],
      }),
      {
        entryAddress: "127.0.0.1:0",
        loopbackAddress,
        inspectorAddress: "127.0.0.1:0",
        requiredSockets: ["entry", kInspectorSocket],
        handleRuntimeStdio(stdout, stderr) {
          stdout.resume();
          stderr.on("data", (chunk) => {
            runtimeStderr = (runtimeStderr + chunk.toString()).slice(-16384);
          });
        },
      },
      ["heap-stress"],
      controller.signal,
    );
    assert.ok(ports?.get("entry") && ports?.get(kInspectorSocket), runtimeStderr);
    return {
      getInspectorURL: async () => new URL(`http://127.0.0.1:${ports.get(kInspectorSocket)}`),
      dispatchFetch: (url) =>
        fetch(
          `http://127.0.0.1:${ports.get("entry")}${new URL(url).pathname}${new URL(url).search}`,
        ),
      runtimeStderr: () => runtimeStderr,
      dispose,
    };
  } catch (error) {
    await dispose();
    throw error;
  }
}
