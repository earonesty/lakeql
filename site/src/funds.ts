import { eq } from "lakeql-core";
import { httpStore } from "lakeql-http";
import { createParquetLake } from "lakeql-parquet";
import "./funds.css";
import "./funds-detail.css";

declare const __LAKEQL_VERSION__: string;

const DATASET_BASE = "https://api.tigzig.com/mf/v1/download/";
const DATASET_KEY = "amfi_nav_master.parquet";
const MANIFEST_URL = "https://api.tigzig.com/mf/v1/downloads/manifest";
const SEARCH_URL = "https://api.tigzig.com/mf/v1/search";
const SOURCE_URL = "https://api.tigzig.com/mf/v1/schemes";

interface FundRow {
  scheme_code: number;
  date: string;
  nav: number;
  scheme_name: string;
}

interface SearchResult {
  scheme_code: number;
  scheme_name: string;
  isin: string | null;
  category_sub: string | null;
  is_active: boolean;
}

interface Manifest {
  generated_at: string;
  total_rows: number;
  files: Record<string, { size_bytes: number; size_human: string; row_count: number }>;
}

interface WireStats {
  bytes: number;
  requests: number;
  fullRequests: number;
}

let datasetBytes = 178_528_643;
let activeRun = 0;
let searchTimer: number | undefined;
let searchAbort: AbortController | undefined;

function element<T extends Element = HTMLElement>(id: string): T {
  const found = document.getElementById(id);
  if (!found) throw new Error(`Missing #${id}`);
  return found as T;
}

function measuredStore(): { store: ReturnType<typeof httpStore>; stats: WireStats } {
  const stats: WireStats = { bytes: 0, requests: 0, fullRequests: 0 };
  const measuredFetch: typeof fetch = async (input, init) => {
    const headers = new Headers(init?.headers);
    if (!headers.has("range")) stats.fullRequests += 1;
    const response = await fetch(input, init);
    stats.requests += 1;
    const contentLength = Number(response.headers.get("content-length") ?? 0);
    if (Number.isFinite(contentLength)) stats.bytes += contentLength;
    return response;
  };
  return {
    store: httpStore({ baseUrl: DATASET_BASE, fetch: measuredFetch }),
    stats,
  };
}

async function loadFund(schemeCode: number, announcedName?: string): Promise<void> {
  const run = ++activeRun;
  setLoading(true, announcedName);
  const { store, stats } = measuredStore();
  const lake = createParquetLake({
    store,
    budget: { maxOutputRows: 12_000, maxConcurrentReads: 4 },
    scanRangeCache: { maxBytes: 8 * 1024 * 1024 },
  });
  const started = performance.now();

  try {
    const rows = (await lake
      .path(DATASET_KEY)
      .select(["scheme_code", "date", "nav", "scheme_name"])
      .where(eq("scheme_code", schemeCode))
      .orderBy([{ column: "date" }])
      .limit(12_000)
      .toArray()) as FundRow[];
    if (run !== activeRun) return;
    if (rows.length === 0) throw new Error(`No NAV history found for scheme ${schemeCode}.`);

    const elapsed = performance.now() - started;
    const name = rows.at(-1)?.scheme_name ?? announcedName ?? `Scheme ${schemeCode}`;
    renderFund(rows, name, stats, elapsed);
    updateLocation(schemeCode);
  } catch (error) {
    if (run !== activeRun) return;
    showError(error);
  } finally {
    if (run === activeRun) setLoading(false);
  }
}

function renderFund(rows: FundRow[], name: string, stats: WireStats, elapsed: number): void {
  const first = rows[0];
  const latest = rows.at(-1);
  if (!first || !latest) return;

  element("fund-name").textContent = name;
  element("fund-range").textContent = `${formatDate(first.date)} — ${formatDate(latest.date)}`;
  element("latest-nav").textContent = latest.nav.toLocaleString("en-IN", {
    minimumFractionDigits: 4,
    maximumFractionDigits: 4,
  });
  const years = Math.max(0, (Date.parse(latest.date) - Date.parse(first.date)) / 31_557_600_000);
  element("history-years").textContent = `${years.toFixed(1)} years`;
  const change = ((latest.nav / first.nav - 1) * 100).toFixed(1);
  element("nav-change").textContent = `${Number(change) >= 0 ? "+" : ""}${change}%`;
  element("observation-count").textContent = rows.length.toLocaleString("en-IN");

  element("bytes-read").textContent = formatBytes(stats.bytes);
  element("request-count").textContent = String(stats.requests);
  element("full-requests").textContent = String(stats.fullRequests);
  element("scan-ratio").textContent =
    `${Math.max(0, (1 - stats.bytes / datasetBytes) * 100).toFixed(2)}%`;
  element("query-time").textContent =
    elapsed < 1_000 ? `${Math.round(elapsed)} ms` : `${(elapsed / 1_000).toFixed(2)} s`;
  element("proof-file-size").textContent = formatBytes(datasetBytes);

  const query = queryText(latest.scheme_code);
  element("query-code").textContent = query;
  const source = element<HTMLAnchorElement>("source-record");
  source.href = `${SOURCE_URL}/${latest.scheme_code}/nav`;
  source.textContent = `Open scheme ${latest.scheme_code} at TigZig ↗`;
  renderChart(rows);
  renderRecentRows(rows.slice(-8).reverse());
  element("fund-error").hidden = true;
}

function renderChart(rows: FundRow[]): void {
  const left = 54;
  const right = 930;
  const top = 40;
  const bottom = 340;
  const sample = sampleRows(rows, 420);
  const values = sample.map((row) => row.nav);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const point = (row: FundRow, index: number): [number, number] => [
    left + (index / Math.max(1, sample.length - 1)) * (right - left),
    bottom - ((row.nav - min) / span) * (bottom - top),
  ];
  const points = sample.map(point);
  const line = points
    .map(([x, y], index) => `${index === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`)
    .join(" ");
  const area = `${line} L${right},${bottom} L${left},${bottom} Z`;
  element<SVGPathElement>("chart-line").setAttribute("d", line);
  element<SVGPathElement>("chart-area").setAttribute("d", area);
  element("chart-empty").hidden = true;

  const labels = element<SVGGElement>("chart-labels");
  labels.replaceChildren();
  addSvgLabel(labels, left, 28, max.toFixed(2), "start");
  addSvgLabel(labels, left, 365, rows[0]?.date.slice(0, 4) ?? "", "start");
  addSvgLabel(labels, right, 365, rows.at(-1)?.date.slice(0, 4) ?? "", "end");
  addSvgLabel(labels, left, 133, (max - span / 3).toFixed(2), "start");
  addSvgLabel(labels, left, 233, (max - (span * 2) / 3).toFixed(2), "start");
  addSvgLabel(labels, left, 333, min.toFixed(2), "start");
}

function addSvgLabel(
  host: SVGGElement,
  x: number,
  y: number,
  text: string,
  anchor: "start" | "end",
): void {
  const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
  label.setAttribute("x", String(x));
  label.setAttribute("y", String(y));
  label.setAttribute("text-anchor", anchor);
  label.textContent = text;
  host.append(label);
}

function sampleRows(rows: FundRow[], maximum: number): FundRow[] {
  if (rows.length <= maximum) return rows;
  const sampled: FundRow[] = [];
  for (let index = 0; index < maximum; index += 1) {
    const sourceIndex = Math.round((index / (maximum - 1)) * (rows.length - 1));
    const row = rows[sourceIndex];
    if (row) sampled.push(row);
  }
  return sampled;
}

function renderRecentRows(rows: FundRow[]): void {
  const body = element<HTMLTableSectionElement>("fund-rows");
  body.replaceChildren();
  for (const row of rows) {
    const tr = document.createElement("tr");
    const date = document.createElement("td");
    const nav = document.createElement("td");
    const code = document.createElement("td");
    date.textContent = row.date;
    nav.textContent = row.nav.toLocaleString("en-IN", {
      minimumFractionDigits: 4,
      maximumFractionDigits: 4,
    });
    code.textContent = String(row.scheme_code);
    tr.append(date, nav, code);
    body.append(tr);
  }
}

async function searchFunds(rawQuery: string): Promise<void> {
  const query = rawQuery.trim();
  const host = element("search-results");
  if (query.length < 2) {
    host.hidden = true;
    host.replaceChildren();
    element("search-state").textContent = "search";
    return;
  }
  searchAbort?.abort();
  searchAbort = new AbortController();
  element("search-state").textContent = "finding";
  try {
    const url = new URL(SEARCH_URL);
    url.searchParams.set("q", query);
    url.searchParams.set("limit", "7");
    const response = await fetch(url, { signal: searchAbort.signal });
    if (!response.ok) throw new Error(`Search returned ${response.status}`);
    const payload = (await response.json()) as { results?: SearchResult[] };
    renderSearchResults(payload.results ?? []);
  } catch (error) {
    if ((error as { name?: string }).name === "AbortError") return;
    host.hidden = false;
    host.textContent = "Search is unavailable. Enter an AMFI scheme code instead.";
  } finally {
    element("search-state").textContent = "search";
  }
}

function renderSearchResults(results: SearchResult[]): void {
  const host = element("search-results");
  host.replaceChildren();
  host.hidden = false;
  if (results.length === 0) {
    host.textContent = "No matching schemes.";
    return;
  }
  for (const result of results) {
    const button = document.createElement("button");
    button.type = "button";
    const name = document.createElement("strong");
    const meta = document.createElement("span");
    name.textContent = result.scheme_name;
    meta.textContent = [
      String(result.scheme_code),
      result.category_sub,
      result.is_active ? "active" : "matured",
    ]
      .filter(Boolean)
      .join(" · ");
    button.append(name, meta);
    button.addEventListener("click", () => {
      host.hidden = true;
      element<HTMLInputElement>("fund-search-input").value = result.scheme_name;
      void loadFund(result.scheme_code, result.scheme_name);
    });
    host.append(button);
  }
}

async function loadManifest(): Promise<void> {
  try {
    const response = await fetch(MANIFEST_URL);
    if (!response.ok) return;
    const manifest = (await response.json()) as Manifest;
    const file = manifest.files[DATASET_KEY];
    if (!file) return;
    datasetBytes = file.size_bytes;
    element("hero-rows").textContent = `${(file.row_count / 1_000_000).toFixed(1)} million`;
    element("hero-size").textContent = file.size_human;
    element("file-rows").textContent = file.row_count.toLocaleString("en-IN");
    element("file-size").textContent = file.size_human;
    element("proof-file-size").textContent = file.size_human;
    element("file-generated").textContent = new Date(manifest.generated_at).toLocaleString(
      undefined,
      {
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
      },
    );
  } catch {
    // The page remains useful with the last-known public manifest values.
  }
}

function setLoading(loading: boolean, name?: string): void {
  document.body.classList.toggle("is-querying", loading);
  element("run-state").textContent = loading ? "querying" : "live result";
  if (loading) {
    element("chart-empty").hidden = false;
    element("chart-empty").textContent = "Pruning 37 million rows by scheme code…";
    element("fund-error").hidden = true;
    if (name) element("fund-name").textContent = name;
  }
}

function showError(error: unknown): void {
  const host = element("fund-error");
  const code = (error as { code?: string }).code;
  const message = error instanceof Error ? error.message : String(error);
  host.textContent = `${code ? `${code}: ` : ""}${message}`;
  host.hidden = false;
  element("chart-empty").hidden = false;
  element("chart-empty").textContent = "The live query did not complete.";
}

function queryText(schemeCode: number): string {
  return `lake.path("${DATASET_KEY}")\n  .where(eq("scheme_code", ${schemeCode}))\n  .select(["date", "nav", "scheme_name"])\n  .orderBy([{ column: "date" }])\n  .toArray()`;
}

function updateLocation(schemeCode: number): void {
  const url = new URL(window.location.href);
  url.searchParams.set("scheme", String(schemeCode));
  history.replaceState(null, "", url);
}

function formatDate(value: string): string {
  return new Date(`${value}T00:00:00Z`).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

function setupInteractions(): void {
  const input = element<HTMLInputElement>("fund-search-input");
  input.addEventListener("input", () => {
    window.clearTimeout(searchTimer);
    searchTimer = window.setTimeout(() => {
      if (/^\d{4,7}$/u.test(input.value.trim())) {
        element("search-results").hidden = true;
        void loadFund(Number(input.value.trim()));
      } else {
        void searchFunds(input.value);
      }
    }, 280);
  });
  input.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    const code = Number(input.value.trim());
    if (Number.isSafeInteger(code)) void loadFund(code);
  });

  for (const button of document.querySelectorAll<HTMLButtonElement>("[data-scheme]")) {
    button.addEventListener("click", () => {
      const code = Number(button.dataset.scheme);
      if (Number.isSafeInteger(code)) void loadFund(code, button.textContent ?? undefined);
    });
  }

  element<HTMLButtonElement>("copy-query").addEventListener("click", async (event) => {
    const button = event.currentTarget as HTMLButtonElement;
    await navigator.clipboard.writeText(element("query-code").textContent ?? "");
    button.textContent = "copied";
    window.setTimeout(() => {
      button.textContent = "copy";
    }, 1_200);
  });
}

for (const tag of document.querySelectorAll<HTMLElement>(".fund-brand small")) {
  tag.textContent = `v${__LAKEQL_VERSION__}`;
}
setupInteractions();
void loadManifest();
const initialScheme = Number(new URL(window.location.href).searchParams.get("scheme") ?? 122639);
void loadFund(Number.isSafeInteger(initialScheme) ? initialScheme : 122639);
