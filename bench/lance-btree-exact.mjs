import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { createServer } from "node:http";
import { resolve, sep } from "node:path";
import { SharedMemoryCache } from "../packages/core/dist/index.js";
import { httpStore } from "../packages/http/dist/index.js";
import { openLanceDataset } from "../packages/lance/dist/index.js";

const options = parseArguments(process.argv.slice(2));
const local = options.dataset === undefined ? undefined : await localRangeServer(options.dataset);
const baseUrl = options.baseUrl ?? local?.baseUrl;
if (baseUrl === undefined) throw new Error("provide --dataset or --base-url");
const indexCache = new SharedMemoryCache({
  maxBytes: options.indexCacheBytes,
  policy: "latency",
});

try {
  const trials = [];
  for (let trial = 1; trial <= options.trials; trial += 1) {
    const startedAt = performance.now();
    const dataset = await openLanceDataset({
      store: httpStore({ baseUrl }),
      path: options.path,
      version: options.version,
      indexCache,
      budget: {
        maxBytes: 4 * 1024 * 1024,
        maxRangeRequests: options.maxRangeRequests,
        maxElapsedMs: options.maxElapsedMs,
        maxMemoryBytes: 16 * 1024 * 1024,
        maxConcurrentReads: 8,
        maxOutputRows: 8,
        maxRowsDecoded: 5_000,
      },
    });
    const result = await dataset.lookupRows({
      snapshotId: dataset.snapshotId,
      index: options.index,
      values: [options.key],
      select: options.select,
    });
    const group = result.groups[0];
    if (group === undefined || group.rows.length !== options.expectedRows) {
      throw new Error(
        `exact lookup returned ${group?.rows.length ?? 0} rows; expected ${options.expectedRows}`,
      );
    }
    trials.push({
      trial,
      wallMs: performance.now() - startedAt,
      ...result.stats,
    });
  }
  console.log(
    `${JSON.stringify(
      {
        baseUrl,
        path: options.path,
        version: options.version,
        index: options.index,
        key: options.key,
        select: options.select,
        maxRangeRequests: options.maxRangeRequests,
        indexCacheBytes: options.indexCacheBytes,
        fullObjectGets: local?.fullObjectGets ?? null,
        trials,
      },
      null,
      2,
    )}\n`,
  );
  if (local !== undefined && local.fullObjectGets !== 0) {
    throw new Error(`benchmark made ${local.fullObjectGets} full-object GET requests`);
  }
} finally {
  await local?.close();
}

function parseArguments(arguments_) {
  if (arguments_[0] === "--") arguments_ = arguments_.slice(1);
  const values = new Map();
  for (let index = 0; index < arguments_.length; index += 2) {
    const name = arguments_[index];
    const value = arguments_[index + 1];
    if (name === undefined || !name.startsWith("--") || value === undefined) {
      throw new Error("arguments must be --name value pairs");
    }
    values.set(name.slice(2), value);
  }
  const dataset = values.get("dataset");
  const key = values.get("key");
  if (key === undefined) throw new Error("--key is required");
  const trials = Number(values.get("trials") ?? "3");
  const maxRangeRequests = Number(values.get("max-range-requests") ?? "27");
  const expectedRows = Number(values.get("expected-rows") ?? "1");
  const maxElapsedMs = Number(values.get("max-elapsed-ms") ?? "10000");
  const indexCacheBytes = Number(values.get("index-cache-bytes") ?? String(8 * 1024 * 1024));
  for (const [name, value] of [
    ["trials", trials],
    ["max-range-requests", maxRangeRequests],
    ["expected-rows", expectedRows],
    ["max-elapsed-ms", maxElapsedMs],
    ["index-cache-bytes", indexCacheBytes],
  ]) {
    if (
      !Number.isSafeInteger(value) ||
      value < (name === "expected-rows" || name === "index-cache-bytes" ? 0 : 1)
    ) {
      throw new Error(`--${name} must be a valid integer`);
    }
  }
  return {
    ...(dataset === undefined ? {} : { dataset }),
    ...(values.has("base-url") ? { baseUrl: values.get("base-url") } : {}),
    path: values.get("path") ?? (dataset === undefined ? "" : "dataset.lance"),
    version: values.get("version") ?? "1",
    index: values.get("index") ?? "parcel_key_btree",
    key,
    select: (values.get("select") ?? "parcel_key").split(","),
    trials,
    maxRangeRequests,
    expectedRows,
    maxElapsedMs,
    indexCacheBytes,
  };
}

async function localRangeServer(datasetPath) {
  const root = resolve(datasetPath);
  let fullObjectGets = 0;
  const server = createServer(async (request, response) => {
    try {
      const relative = decodeURIComponent(
        new URL(request.url ?? "/", "http://localhost").pathname.slice(1),
      );
      const prefix = "dataset.lance/";
      if (!relative.startsWith(prefix)) {
        response.writeHead(404).end();
        return;
      }
      const path = resolve(root, relative.slice(prefix.length));
      if (path !== root && !path.startsWith(`${root}${sep}`)) {
        response.writeHead(400).end();
        return;
      }
      const metadata = await stat(path);
      if (request.method === "HEAD") {
        response.writeHead(200, { "content-length": metadata.size }).end();
        return;
      }
      const match = /^bytes=(\d+)-(\d+)$/u.exec(request.headers.range ?? "");
      if (match === null) {
        fullObjectGets += 1;
        response.writeHead(400).end();
        return;
      }
      const start = Number(match[1]);
      const end = Number(match[2]);
      response.writeHead(206, {
        "accept-ranges": "bytes",
        "content-length": end - start + 1,
        "content-range": `bytes ${start}-${end}/${metadata.size}`,
      });
      createReadStream(path, { start, end }).pipe(response);
    } catch {
      response.writeHead(404).end();
    }
  });
  await new Promise((resolveListen) => server.listen(0, "127.0.0.1", resolveListen));
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("benchmark server failed");
  return {
    baseUrl: `http://127.0.0.1:${address.port}/`,
    get fullObjectGets() {
      return fullObjectGets;
    },
    close: () => new Promise((resolveClose) => server.close(resolveClose)),
  };
}
