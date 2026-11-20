import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { createServer } from "node:http";
import { resolve, sep } from "node:path";
import { SharedMemoryCache } from "../packages/core/dist/index.js";
import { httpStore } from "../packages/http/dist/index.js";
import { openLanceDataset } from "../packages/lance/dist/index.js";

// Full-text search over an official Lance INVERTED index through HTTP byte ranges. The local
// server rejects any request without a Range header, so a full-object GET fails the run.
//   node bench/lance-fts.mjs --dataset packages/lance/fixtures/fts-inverted-v2.0.lance \
//     --version 5 --index text_idx --query "quick brown fox" --kind match --limit 10
const options = parseArguments(process.argv.slice(2));
const local = options.dataset === undefined ? undefined : await localRangeServer(options.dataset);
const baseUrl = options.baseUrl ?? local?.baseUrl;
if (baseUrl === undefined) throw new Error("provide --dataset or --base-url");
const indexCache = new SharedMemoryCache({
  maxBytes: options.indexCacheBytes,
  policy: "latency",
});
const query =
  options.kind === "phrase"
    ? { kind: "phrase", text: options.query, slop: options.slop }
    : {
        kind: "match",
        text: options.query,
        operator: options.operator,
        fuzziness: options.fuzziness,
      };

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
        maxBytes: options.maxBytes,
        maxRangeRequests: options.maxRangeRequests,
        maxElapsedMs: options.maxElapsedMs,
        maxMemoryBytes: 64 * 1024 * 1024,
        maxConcurrentReads: 8,
        maxOutputRows: options.limit,
        maxRowsDecoded: 100_000,
      },
    });
    const result = await dataset.searchRows({
      snapshotId: dataset.snapshotId,
      index: options.index,
      query,
      select: options.select,
      limit: options.limit,
    });
    trials.push({
      trial,
      // Observed, not assumed: an uncached repeat (cache size 0) is not warm.
      cache: result.stats.cacheHits > 0 ? "warm" : trial === 1 ? "cold" : "repeat-uncached",
      wallMs: performance.now() - startedAt,
      rows: result.rows.length,
      totalMatches: result.totalMatches,
      topScore: result.scores[0] ?? null,
      search: result.search,
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
        query,
        select: options.select,
        limit: options.limit,
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
  const text = values.get("query");
  if (text === undefined) throw new Error("--query is required");
  const integer = (name, fallback, minimum = 1) => {
    const value = Number(values.get(name) ?? fallback);
    if (!Number.isSafeInteger(value) || value < minimum) {
      throw new Error(`--${name} must be a valid integer`);
    }
    return value;
  };
  const kind = values.get("kind") ?? "match";
  if (kind !== "match" && kind !== "phrase") throw new Error("--kind must be match or phrase");
  const fuzziness = values.get("fuzziness") ?? "0";
  const dataset = values.get("dataset");
  return {
    ...(dataset === undefined ? {} : { dataset }),
    ...(values.has("base-url") ? { baseUrl: values.get("base-url") } : {}),
    path: values.get("path") ?? (dataset === undefined ? "" : "dataset.lance"),
    version: values.get("version") ?? "1",
    index: values.get("index") ?? "text_idx",
    query: text,
    kind,
    operator: values.get("operator") ?? "or",
    fuzziness: fuzziness === "auto" ? "auto" : Number(fuzziness),
    slop: integer("slop", 0, 0),
    limit: integer("limit", 10),
    select: (values.get("select") ?? "id").split(","),
    trials: integer("trials", 3),
    maxBytes: integer("max-bytes", 8 * 1024 * 1024),
    maxRangeRequests: integer("max-range-requests", 256),
    maxElapsedMs: integer("max-elapsed-ms", 10000),
    indexCacheBytes: integer("index-cache-bytes", 8 * 1024 * 1024, 0),
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
