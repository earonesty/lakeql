const DEFAULT_SOURCE_URL =
  "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/plotly/2015_flights.parquet";
const DEFAULT_DATASET_SIZE = 25238218;
const DATASET_PREFIX = "/compare-data/";

let stats = { requests: 0, bytes: 0 };

self.addEventListener("install", (event) => {
  event.waitUntil(self.skipWaiting());
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("message", (event) => {
  const port = event.ports[0];
  if (event.data?.type === "resetStats") {
    stats = { requests: 0, bytes: 0 };
    port?.postMessage({ ok: true });
    return;
  }
  if (event.data?.type === "getStats") {
    port?.postMessage(stats);
  }
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin || !url.pathname.includes(DATASET_PREFIX)) return;
  event.respondWith(proxyDataset(event.request, url));
});

async function proxyDataset(request, url) {
  const sourceUrl = datasetSourceUrl(url);
  const datasetSize = datasetSizeHint(url);
  const isHead = request.method === "HEAD";
  const range = request.headers.get("range");
  const headers = new Headers();
  if (range) headers.set("range", range);

  const upstream = await fetch(sourceUrl, { headers, method: isHead ? "HEAD" : "GET" });
  const body = isHead ? undefined : await upstream.arrayBuffer();
  const totalSize = contentRangeTotal(upstream.headers.get("content-range")) ?? datasetSize;
  const responseSize =
    body?.byteLength ??
    numberHeader(upstream.headers.get("content-length")) ??
    totalSize ??
    DEFAULT_DATASET_SIZE;
  stats = {
    requests: stats.requests + 1,
    bytes: stats.bytes + (body?.byteLength ?? 0),
  };

  const responseHeaders = new Headers({
    "accept-ranges": "bytes",
    "content-length": String(responseSize),
    "content-type": "application/octet-stream",
    "cache-control": "public, max-age=300",
  });

  const parsed = range && body ? parseRange(range, body.byteLength, totalSize) : undefined;
  if (parsed) {
    responseHeaders.set("content-range", `bytes ${parsed.start}-${parsed.end}/${totalSize ?? "*"}`);
  }

  return new Response(body, {
    status: range ? 206 : upstream.status,
    statusText: range ? "Partial Content" : upstream.statusText,
    headers: responseHeaders,
  });
}

function datasetSourceUrl(url) {
  const raw = url.searchParams.get("source");
  if (!raw) return DEFAULT_SOURCE_URL;
  const parsed = new URL(raw);
  if (parsed.protocol !== "https:" && parsed.origin !== self.location.origin) {
    throw new Error("Benchmark source must be an HTTPS URL or same-origin fixture.");
  }
  return parsed.href;
}

function datasetSizeHint(url) {
  const raw = url.searchParams.get("size");
  if (!raw) return undefined;
  const value = Number(raw);
  return Number.isFinite(value) && value > 0 ? value : undefined;
}

function numberHeader(value) {
  if (value === null) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : undefined;
}

function contentRangeTotal(value) {
  if (value === null) return undefined;
  const match = /\/(\d+)$/u.exec(value);
  if (!match) return undefined;
  return Number(match[1]);
}

function parseRange(range, byteLength, totalSize) {
  const match = /^bytes=(\d+)-(\d+)?$/u.exec(range);
  if (!match) return undefined;
  const start = Number(match[1]);
  const requestedEnd = match[2] === undefined ? start + byteLength - 1 : Number(match[2]);
  const last = totalSize === undefined ? start + byteLength - 1 : totalSize - 1;
  return { start, end: Math.min(requestedEnd, start + byteLength - 1, last) };
}
