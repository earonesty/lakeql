import { mkdir, open, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { dirname, join, relative, sep } from "node:path";

export async function timed(fn) {
  const startMemoryBytes = process.memoryUsage().rss;
  let peakMemoryBytes = startMemoryBytes;
  const sampleMemory = () => {
    peakMemoryBytes = Math.max(peakMemoryBytes, process.memoryUsage().rss);
  };
  const sampler = setInterval(sampleMemory, 1);
  sampler.unref();
  const start = performance.now();
  try {
    const value = await fn();
    sampleMemory();
    return {
      value,
      ms: performance.now() - start,
      peakMemoryBytes,
      peakMemoryDeltaBytes: Math.max(0, peakMemoryBytes - startMemoryBytes),
    };
  } finally {
    clearInterval(sampler);
  }
}

export function positiveIntegerEnv(name, fallback) {
  const value = process.env[name];
  if (value === undefined) return fallback;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

export function optionalPositiveIntegerEnv(name) {
  const value = process.env[name];
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

export function formatBytes(bytes) {
  const mib = bytes / 1024 / 1024;
  return `${mib.toFixed(1)} MiB`;
}

export function formatOptionalBytes(bytes) {
  return bytes === undefined ? "not run" : formatBytes(bytes);
}

export function formatOptionalMs(ms) {
  return ms === undefined ? "not run" : ms.toFixed(1);
}

export function summarizeSamples(samples, peakMemorySamples) {
  const sorted = [...samples].sort((a, b) => a - b);
  const sum = samples.reduce((acc, value) => acc + value, 0);
  return {
    minMs: sorted[0] ?? 0,
    medianMs: percentile(sorted, 0.5),
    p95Ms: percentile(sorted, 0.95),
    maxMs: sorted.at(-1) ?? 0,
    meanMs: sum / Math.max(samples.length, 1),
    peakRssBytes: Math.max(0, ...peakMemorySamples),
  };
}

export function fileStore(root) {
  const counters = newStoreCounters();
  return {
    counters,
    takeCounters() {
      const snapshot = { ...counters };
      resetStoreCounters(counters);
      return snapshot;
    },
    resetCounters() {
      resetStoreCounters(counters);
    },
    async get(path) {
      try {
        counters.get += 1;
        const bytes = new Uint8Array(await readFile(join(root, path)));
        counters.bytesFetched += bytes.byteLength;
        return bytes;
      } catch (error) {
        if (error?.code === "ENOENT") return null;
        throw error;
      }
    },
    async getRange(path, range) {
      counters.getRange += 1;
      const fullPath = join(root, path);
      const bytes = new Uint8Array(range.length);
      const handle = await open(fullPath, "r");
      try {
        let offset = 0;
        while (offset < bytes.byteLength) {
          const { bytesRead } = await handle.read(
            bytes,
            offset,
            bytes.byteLength - offset,
            range.offset + offset,
          );
          if (bytesRead === 0) break;
          offset += bytesRead;
        }
        if (offset !== bytes.byteLength) {
          throw new Error(`short range read for ${path}: ${range.offset}+${range.length}`);
        }
      } finally {
        await handle.close();
      }
      counters.bytesFetched += bytes.byteLength;
      return bytes;
    },
    async put(path, body) {
      counters.put += 1;
      const fullPath = join(root, path);
      await mkdir(dirname(fullPath), { recursive: true });
      const bytes = body instanceof Uint8Array ? body : await streamBytes(body);
      await writeFile(fullPath, bytes);
    },
    async delete(path) {
      counters.delete += 1;
      await rm(join(root, path), { force: true });
    },
    async *list(prefix) {
      counters.list += 1;
      for (const path of await listFiles(join(root, prefix))) {
        const objectPath = relative(root, path).split(sep).join("/");
        const info = await stat(path);
        yield { path: objectPath, size: info.size, lastModified: info.mtime };
      }
    },
    async head(path) {
      counters.head += 1;
      try {
        const info = await stat(join(root, path));
        return { size: info.size, lastModified: info.mtime };
      } catch (error) {
        if (error?.code === "ENOENT") return null;
        throw error;
      }
    },
  };
}

export function newStoreCounters() {
  return {
    get: 0,
    getRange: 0,
    head: 0,
    put: 0,
    delete: 0,
    list: 0,
    bytesFetched: 0,
  };
}

export function queryStats(queryId) {
  return {
    queryId,
    elapsedMs: 0,
    manifestsRead: 0,
    manifestsSkipped: 0,
    filesPlanned: 0,
    filesRead: 0,
    filesSkipped: 0,
    rowGroupsRead: 0,
    rowGroupsSkipped: 0,
    columnsRead: [],
    bytesRequested: 0,
    rangeRequests: 0,
    rowsDecoded: 0,
    rowsMatched: 0,
    rowsReturned: 0,
    cacheHits: 0,
    cacheMisses: 0,
  };
}

function resetStoreCounters(counters) {
  counters.get = 0;
  counters.getRange = 0;
  counters.head = 0;
  counters.put = 0;
  counters.delete = 0;
  counters.list = 0;
  counters.bytesFetched = 0;
}

async function streamBytes(body) {
  const chunks = [];
  let total = 0;
  for await (const chunk of body) {
    chunks.push(chunk);
    total += chunk.byteLength;
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

async function listFiles(root) {
  const out = [];
  try {
    const entries = await readdir(root, { withFileTypes: true });
    for (const entry of entries) {
      const path = join(root, entry.name);
      if (entry.isDirectory()) out.push(...(await listFiles(path)));
      else out.push(path);
    }
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  return out.sort();
}

function percentile(sorted, p) {
  if (sorted.length === 0) return 0;
  const index = Math.min(sorted.length - 1, Math.floor(p * sorted.length));
  return sorted[index];
}

/** Add a benchmark timeout while preserving fetch's caller-signal precedence. */
export function fetchWithTimeout(input, init, timeoutMs = 30000) {
  const caller =
    init?.signal !== undefined ? init.signal : input instanceof Request ? input.signal : undefined;
  const timeout = AbortSignal.timeout(timeoutMs);
  return fetch(input, { ...init, signal: caller ? AbortSignal.any([caller, timeout]) : timeout });
}
