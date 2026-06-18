const SOURCE_URL = "https://raw.githubusercontent.com/plotly/datasets/master/2015_flights.parquet";
const DATASET_SIZE = 25238218;
const DATASET_PATH = "/compare-data/2015_flights.parquet";

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
  if (url.origin !== self.location.origin || !url.pathname.endsWith(DATASET_PATH)) return;
  event.respondWith(proxyDataset(event.request));
});

async function proxyDataset(request) {
  const isHead = request.method === "HEAD";
  const range = request.headers.get("range");
  const headers = new Headers();
  if (range) headers.set("range", range);

  const upstream = await fetch(SOURCE_URL, { headers, method: isHead ? "HEAD" : "GET" });
  const body = isHead ? undefined : await upstream.arrayBuffer();
  stats = {
    requests: stats.requests + 1,
    bytes: stats.bytes + (body?.byteLength ?? 0),
  };

  const responseHeaders = new Headers({
    "accept-ranges": "bytes",
    "content-length": String(body?.byteLength ?? DATASET_SIZE),
    "content-type": "application/octet-stream",
    "cache-control": "public, max-age=300",
  });

  const parsed = range && body ? parseRange(range, body.byteLength) : undefined;
  if (parsed) {
    responseHeaders.set("content-range", `bytes ${parsed.start}-${parsed.end}/${DATASET_SIZE}`);
  }

  return new Response(body, {
    status: range ? 206 : upstream.status,
    statusText: range ? "Partial Content" : upstream.statusText,
    headers: responseHeaders,
  });
}

function parseRange(range, byteLength) {
  const match = /^bytes=(\d+)-(\d+)?$/u.exec(range);
  if (!match) return undefined;
  const start = Number(match[1]);
  const requestedEnd = match[2] === undefined ? start + byteLength - 1 : Number(match[2]);
  return { start, end: Math.min(requestedEnd, start + byteLength - 1, DATASET_SIZE - 1) };
}
