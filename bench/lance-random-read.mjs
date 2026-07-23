import { createReadStream } from "node:fs";
import { readFile, stat, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { resolve, sep } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { httpStore } from "../packages/http/dist/index.js";
import { openLanceDataset } from "../packages/lance/dist/index.js";

const options = parseArguments(process.argv.slice(2));
const manifest = JSON.parse(await readFile(resolve(options.manifest), "utf8"));
const local = options.dataset === undefined ? undefined : await localRangeServer(options.dataset);
const baseUrl = options.baseUrl ?? local?.baseUrl;
if (baseUrl === undefined) throw new Error("provide --dataset or --base-url");

try {
  const trials = [];
  for (let trial = 0; trial < options.trials; trial += 1) {
    const wallStart = performance.now();
    const dataset = await openLanceDataset({
      store: httpStore({ baseUrl }),
      path: options.path,
      budget: {
        maxBytes: 16 * 1024 * 1024,
        maxRangeRequests: 512,
        maxElapsedMs: 3_000,
        maxMemoryBytes: 32 * 1024 * 1024,
        maxConcurrentReads: 8,
        maxOutputRows: 32,
        maxRowsDecoded: 32,
      },
    });
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: manifest.rowIds,
      select: manifest.select,
    });
    if (!isDeepStrictEqual(result.rows, manifest.rows)) {
      const mismatch = result.rows.findIndex(
        (row, index) => !isDeepStrictEqual(row, manifest.rows[index]),
      );
      throw new Error(
        `Lance benchmark row ${mismatch} disagrees with the recorded projection: ${JSON.stringify({
          expected: manifest.rows[mismatch],
          actual: result.rows[mismatch],
        })}`,
      );
    }
    trials.push({
      trial: trial + 1,
      wallMs: performance.now() - wallStart,
      ...result.stats,
    });
  }
  const output = {
    source: manifest.source,
    producer: manifest.producer,
    storageVersion: manifest.storageVersion,
    datasetRows: manifest.rowCount,
    requestedRows: manifest.rowIds.length,
    baseUrl,
    path: options.path,
    fullObjectGets: local?.fullObjectGets ?? null,
    trials,
  };
  const serialized = `${JSON.stringify(output, null, 2)}\n`;
  if (options.output !== undefined) await writeFile(resolve(options.output), serialized);
  console.log(serialized);
  if (local !== undefined && local.fullObjectGets !== 0) {
    throw new Error(`benchmark made ${local.fullObjectGets} full-object GET requests`);
  }
} finally {
  await local?.close();
}

function parseArguments(arguments_) {
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
  return {
    ...(dataset === undefined ? {} : { dataset }),
    ...(values.has("base-url") ? { baseUrl: values.get("base-url") } : {}),
    manifest: values.get("manifest") ?? `${dataset ?? "."}/benchmark.json`,
    ...(values.has("output") ? { output: values.get("output") } : {}),
    path: values.get("path") ?? (dataset === undefined ? "" : "dataset.lance"),
    trials: Number(values.get("trials") ?? "3"),
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
