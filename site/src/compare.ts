import { sql } from "@codemirror/lang-sql";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { Prec } from "@codemirror/state";
import { EditorView, keymap } from "@codemirror/view";
import * as duckdb from "@duckdb/duckdb-wasm";
import duckdbWorkerEh from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import duckdbWorkerMvp from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbWasmEh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbWasmMvp from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import { tags } from "@lezer/highlight";
import { basicSetup } from "codemirror";
import { isTimestampValue, type ObjectStore, type QueryBudget, type QueryStats } from "lakeql-core";
import { httpStore } from "lakeql-http";
import { createParquetLake } from "lakeql-parquet";
import { parseSql } from "lakeql-sql";
import { applyAst } from "./apply-ast.js";
import "./styles.css";

declare const __LAKEQL_VERSION__: string;

type Engine = "lakeql" | "duckdb";
type DuckCacheMode = "fresh" | "cached";
type LakeCacheMode = "fresh" | "cached";
type DatasetKind = "tabular" | "spatial" | "window";
type Row = Record<string, unknown>;
type Lake = ReturnType<typeof createParquetLake>;

const DEFAULT_SOURCE_URL =
  "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/plotly/2015_flights.parquet";
const SPATIAL_SOURCE_URL =
  "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/bench/spatial.parquet";
const WINDOW_SOURCE_URL =
  "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/bench/window-events.parquet";
const DEFAULT_DATASET_KEY = "2015_flights.parquet";
const DEFAULT_DATASET_SIZE = 25_238_218;
const SPATIAL_DATASET_SIZE = 22_342;
const WINDOW_DATASET_SIZE = 4_854;
const SCAN_RANGE_CACHE_BYTES = 32 * 1024 * 1024;
const BYTES_PER_MB = 1024 * 1024;
const DEFAULT_MEMORY_BUDGET_MB = 256;
const MEMORY_BUDGETS_MB = [16, 32, 64, 128, 256] as const;
const PENDING_RUN_KEY = "lakeql_compare_pending_run";

const EXAMPLES = [
  {
    name: "Delay Preview",
    sql: `select "ARRIVAL_DELAY"
from flights.parquet
limit 20`,
  },
  {
    name: "Three Columns",
    sql: `select "DEPARTURE_DELAY", "ARRIVAL_DELAY", "DISTANCE"
from flights.parquet
limit 20`,
  },
  {
    name: "Top Delays",
    sql: `select "DEPARTURE_DELAY", "ARRIVAL_DELAY", "DISTANCE"
from flights.parquet
where "DEPARTURE_DELAY" > 120
order by "DEPARTURE_DELAY" desc
limit 10`,
  },
  {
    name: "Long Flights",
    sql: `select "DISTANCE", count() as flights, avg("ARRIVAL_DELAY") as avg_arrival_delay
from flights.parquet
where "DISTANCE" > 2500
group by "DISTANCE"
order by flights desc
limit 10`,
  },
];

const SPATIAL_EXAMPLES = [
  {
    name: "BBox Intersects",
    sql: `select id, name
from spatial.parquet
where st_intersects(geometry, st_bbox(-118.245, 34.050, -118.242, 34.055))
limit 20`,
  },
  {
    name: "DWithin Near",
    sql: `select id, name
from spatial.parquet
where st_dwithin(geometry, st_point(-118.2437, 34.0522), 0.001)
order by id
limit 20`,
  },
  {
    name: "Within Window",
    sql: `select id, name
from spatial.parquet
where st_within(geometry, st_bbox(-118.245, 34.050, -118.242, 34.055))
order by id
limit 20`,
  },
  {
    name: "Contains Window",
    sql: `select id, name
from spatial.parquet
where st_contains(st_bbox(-118.245, 34.050, -118.242, 34.055), geometry)
order by id
limit 20`,
  },
];

const WINDOW_EXAMPLES = [
  {
    name: "Running Sum",
    sql: `select account, event_id, event_ts, amount,
  sum(amount) over (
    partition by account
    order by event_ts, event_id
    rows between unbounded preceding and current row
  ) as running_amount
from window_events.parquet
order by account, event_ts, event_id
limit 24`,
  },
  {
    name: "Moving Avg",
    sql: `select account, event_id, amount,
  avg(amount) over (
    partition by account
    order by event_ts, event_id
    rows between 2 preceding and current row
  ) as moving_avg
from window_events.parquet
order by account, event_ts, event_id
limit 24`,
  },
  {
    name: "Peer Rank",
    sql: `select region, account, event_id, score,
  rank() over (partition by region order by score desc) as region_rank,
  dense_rank() over (partition by region order by score desc) as dense_region_rank
from window_events.parquet
order by region, region_rank, account, event_id
limit 24`,
  },
  {
    name: "Top Event",
    sql: `select region, account, event_id, amount, score,
  row_number() over (
    partition by region
    order by score desc, amount desc, event_id asc
  ) as rn
from window_events.parquet
qualify rn <= 3
order by region, rn`,
  },
  {
    name: "Interval Range",
    sql: `select account, event_id, amount,
  sum(amount) filter (where category = 'purchase') over (
    partition by account
    order by event_ts
    range between interval '2 days' preceding and current row
  ) as purchase_2d
from window_events.parquet
order by account, event_ts, event_id
limit 24`,
  },
];

interface DatasetConfig {
  sourceUrl: string;
  key: string;
  size?: number;
  name: string;
  host: string;
  kind: DatasetKind;
}

const dataset = datasetConfigFromLocation();
const examples =
  dataset.kind === "spatial"
    ? SPATIAL_EXAMPLES
    : dataset.kind === "window"
      ? WINDOW_EXAMPLES
      : EXAMPLES;

let engine: Engine = "lakeql";
let duckCacheMode: DuckCacheMode = "cached";
let lakeCacheMode: LakeCacheMode = "cached";
let memoryBudgetMb = DEFAULT_MEMORY_BUDGET_MB;
let activeExample = 0;
let view: EditorView;
let lakeRuntime: { lake: Lake; cacheMode: LakeCacheMode; memoryBudgetMb: number } | undefined;
let duckState:
  | Promise<{
      db: duckdb.AsyncDuckDB;
      conn: duckdb.AsyncDuckDBConnection;
      initMs: number;
      fileName: string;
    }>
  | undefined;
let duckFreshRunId = 0;
let initialSqlText: string | undefined;

const highlight = HighlightStyle.define([
  { tag: tags.keyword, color: "#c6f24e", fontWeight: "700" },
  { tag: [tags.string, tags.special(tags.string)], color: "#6ad7e5" },
  { tag: [tags.number, tags.bool, tags.null], color: "#e3b341" },
  { tag: [tags.lineComment, tags.blockComment], color: "#5b636d", fontStyle: "italic" },
  { tag: [tags.propertyName, tags.variableName], color: "#e7ebef" },
  { tag: tags.operator, color: "#8b949e" },
]);

const surfaceTheme = EditorView.theme(
  {
    "&": { color: "#e7ebef", backgroundColor: "transparent", height: "100%" },
    ".cm-content": { caretColor: "#c6f24e" },
    ".cm-scroller": { overflow: "auto" },
    ".cm-gutters": {
      backgroundColor: "transparent",
      borderRight: "1px solid #15181d",
      color: "#5b636d",
    },
    ".cm-activeLine, .cm-activeLineGutter": { backgroundColor: "rgba(255,255,255,0.02)" },
    "&.cm-focused .cm-selectionBackground, ::selection": {
      backgroundColor: "rgba(198,242,78,0.16)",
    },
    ".cm-cursor, .cm-dropCursor": { borderLeftColor: "#c6f24e" },
  },
  { dark: true },
);

const runKeymap = Prec.highest(
  keymap.of([
    {
      key: "Mod-Enter",
      run: () => {
        void run();
        return true;
      },
    },
  ]),
);

function mountEditor(): void {
  const parent = document.getElementById("editor");
  if (!parent) return;
  if (view) view.destroy();
  view = new EditorView({
    parent,
    doc: initialSqlText ?? examples[activeExample].sql,
    extensions: [
      basicSetup,
      sql(),
      syntaxHighlighting(highlight),
      surfaceTheme,
      runKeymap,
      EditorView.lineWrapping,
    ],
  });
}

interface Stats {
  bytes: number;
  requests: number;
}

function knownSizeStore(inner: ObjectStore): ObjectStore {
  const store: ObjectStore = {
    get: inner.get.bind(inner),
    getRange: inner.getRange.bind(inner),
    put: inner.put.bind(inner),
    delete: inner.delete.bind(inner),
    list: inner.list.bind(inner),
    async head(path) {
      if (path === dataset.key && dataset.size !== undefined) return { size: dataset.size };
      return inner.head(path);
    },
  };
  return store;
}

function datasetProxyUrl(): string {
  const url = new URL(`compare-data/${encodeURIComponent(dataset.key)}`, window.location.href);
  url.searchParams.set("source", dataset.sourceUrl);
  if (dataset.size !== undefined) url.searchParams.set("size", String(dataset.size));
  return url.href;
}

function duckDatasetProxyUrl(fileName: string): string {
  if (fileName === dataset.key) return datasetProxyUrl();
  const url = new URL(datasetProxyUrl());
  url.searchParams.set("duckdb_run", fileName);
  return url.href;
}

function datasetProxyBase(): string {
  const url = datasetProxyUrl();
  const key = encodeURIComponent(dataset.key);
  const index = url.indexOf(key);
  return index >= 0 ? url.slice(0, index) : url;
}

let proxyReady: Promise<void> | undefined;

function prepareCompareProxy(): Promise<void> {
  if (proxyReady) return proxyReady;
  proxyReady = (async () => {
    if (!("serviceWorker" in navigator)) {
      throw new Error("This browser does not support the file-read counter.");
    }
    await navigator.serviceWorker.register(new URL("compare-sw.js", window.location.href), {
      scope: "./",
    });
    await navigator.serviceWorker.ready;
  })();
  return proxyReady;
}

async function ensureCompareProxy(
  options: { navigateOnUncontrolled?: boolean } = {},
): Promise<void> {
  await prepareCompareProxy();
  if (navigator.serviceWorker.controller) return;
  if (options.navigateOnUncontrolled) {
    navigateForProxyControl();
    return new Promise(() => {});
  }
  throw new Error("The file-read counter is not active yet. Reload and try again.");
}

function navigateForProxyControl(): void {
  savePendingRunState();
  window.location.assign(window.location.href);
}

async function resetProxyStats(): Promise<void> {
  await ensureCompareProxy();
  await serviceWorkerRequest("resetStats");
}

async function serviceWorkerRequest(type: "resetStats"): Promise<{ ok: true }>;
async function serviceWorkerRequest(type: "getStats"): Promise<Stats>;
async function serviceWorkerRequest(
  type: "resetStats" | "getStats",
): Promise<{ ok: true } | Stats> {
  const controller = navigator.serviceWorker.controller;
  if (!controller)
    throw new Error("The file-read counter is not active yet. Reload and try again.");
  const channel = new MessageChannel();
  return new Promise((resolve) => {
    channel.port1.onmessage = (event) => resolve(event.data);
    controller.postMessage({ type }, [channel.port2]);
  });
}

function createLakeRuntime(cacheMode: LakeCacheMode): {
  lake: Lake;
  cacheMode: LakeCacheMode;
  memoryBudgetMb: number;
} {
  const store = knownSizeStore(httpStore({ baseUrl: datasetProxyBase(), fetch: benchmarkFetch }));
  const budgetBytes = memoryBudgetBytes();
  const budget: QueryBudget = { maxMemoryBytes: budgetBytes };
  const scanRangeCache = { maxBytes: Math.min(SCAN_RANGE_CACHE_BYTES, budgetBytes) };
  const lake =
    cacheMode === "cached"
      ? createParquetLake({
          store,
          budget,
          cache: { maxBytes: budgetBytes, policy: "balanced" },
          scanRangeCache,
        })
      : createParquetLake({ store, budget, scanRangeCache });
  return { lake, cacheMode, memoryBudgetMb };
}

function benchmarkFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const request = new Request(input, init);
  const url = new URL(request.url);
  if (url.origin === window.location.origin && url.pathname.includes("/compare-data/")) {
    url.searchParams.set("source", dataset.sourceUrl);
    if (dataset.size !== undefined) url.searchParams.set("size", String(dataset.size));
    return fetch(new Request(url, request));
  }
  return fetch(request);
}

async function runLakeql(
  sqlText: string,
): Promise<{ rows: Row[]; ms: number; stats: Stats; lakeStats: QueryStats }> {
  await resetProxyStats();
  if (
    lakeCacheMode === "fresh" ||
    !lakeRuntime ||
    lakeRuntime.cacheMode !== lakeCacheMode ||
    lakeRuntime.memoryBudgetMb !== memoryBudgetMb
  ) {
    lakeRuntime = createLakeRuntime(lakeCacheMode);
  }
  const { lake } = lakeRuntime;
  const started = performance.now();
  const ast = { ...parseSql(sqlText), source: dataset.key };
  const aggregates =
    ast.aggregates && Object.keys(ast.aggregates).length > 0 ? ast.aggregates : undefined;
  const grouped = (ast.groupBy?.length ?? 0) > 0;
  let rows: Row[];
  let lakeStats: QueryStats;

  if (!aggregates && !grouped) {
    const result = applyAst(lake.path(dataset.key), ast).run();
    rows = (await result.toArray()) as Row[];
    lakeStats = result.stats;
  } else {
    let base = lake.path(dataset.key);
    if (ast.where) base = base.where(ast.where);
    const result = base.run();
    rows = (await result.aggregate(ast.groupBy ?? [], aggregates ?? {}, {
      ...(ast.orderBy !== undefined ? { orderBy: ast.orderBy } : {}),
      ...(ast.limit !== undefined ? { limit: ast.limit } : {}),
      ...(ast.offset !== undefined ? { offset: ast.offset } : {}),
    })) as Row[];
    lakeStats = result.stats;
  }

  return {
    rows,
    ms: performance.now() - started,
    stats: await serviceWorkerRequest("getStats"),
    lakeStats,
  };
}

async function initDuckDb(fileName: string) {
  if (duckState) return duckState;
  duckState = (async () => {
    await ensureCompareProxy();
    const started = performance.now();
    const bundles: duckdb.DuckDBBundles = {
      mvp: { mainModule: duckdbWasmMvp, mainWorker: duckdbWorkerMvp },
      eh: { mainModule: duckdbWasmEh, mainWorker: duckdbWorkerEh },
    };
    const bundle = await duckdb.selectBundle(bundles);
    const worker = new Worker(bundle.mainWorker ?? duckdbWorkerMvp);
    const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    await db.registerFileURL(
      fileName,
      duckDatasetProxyUrl(fileName),
      duckdb.DuckDBDataProtocol.HTTP,
      true,
    );
    await db.collectFileStatistics(fileName, true);
    const conn = await db.connect();
    if (dataset.kind === "spatial") {
      await conn.query("INSTALL spatial; LOAD spatial;");
    }
    return { db, conn, initMs: performance.now() - started, fileName };
  })();
  return duckState;
}

async function resetDuckDb(): Promise<void> {
  if (!duckState) return;
  const state = await duckState.catch(() => undefined);
  await state?.conn.close().catch(() => undefined);
  await state?.db.terminate().catch(() => undefined);
  duckState = undefined;
}

async function runDuckDb(
  sqlText: string,
): Promise<{ rows: Row[]; ms: number; initMs: number; stats: Stats }> {
  await resetProxyStats();
  const fileName = duckCacheMode === "fresh" ? `${duckFreshRunId++}-${dataset.key}` : dataset.key;
  if (duckCacheMode === "fresh") await resetDuckDb();
  const state = await initDuckDb(fileName);
  if (state.fileName !== fileName) {
    await resetDuckDb();
  }
  const { conn, initMs } = state.fileName === fileName ? state : await initDuckDb(fileName);
  const duckSql = duckSqlForDataset(sqlText, fileName);
  const started = performance.now();
  const table = await conn.query(duckSql);
  return {
    rows: arrowTableToRows(table),
    ms: performance.now() - started,
    initMs,
    stats: await serviceWorkerRequest("getStats"),
  };
}

function arrowTableToRows(table: unknown): Row[] {
  const maybeRows = (table as { toArray?: () => unknown[] }).toArray?.();
  if (!maybeRows) return [];
  return maybeRows.map((row) => {
    if (row && typeof row === "object" && "toJSON" in row) {
      return (row as { toJSON: () => Row }).toJSON();
    }
    return row as Row;
  });
}

async function run(): Promise<void> {
  const runBtn = document.getElementById("run");
  runBtn?.classList.add("is-busy");
  hideError();
  const text = view.state.doc.toString();

  try {
    await ensureCompareProxy({ navigateOnUncontrolled: true });
    if (engine === "lakeql") {
      const { rows, ms, stats, lakeStats } = await runLakeql(text);
      renderResult(rows);
      setGauges({
        rows: rows.length,
        ms,
        initMs: 0,
        requests: stats.requests,
        bytes: stats.bytes,
        rowGroups: rowGroupSummary(lakeStats),
        scanRows: lakeStats.rowsDecoded,
        scanRowsLabel: "scan rows",
      });
    } else {
      const { rows, ms, initMs, stats } = await runDuckDb(text);
      renderResult(rows);
      setGauges({
        rows: rows.length,
        ms,
        initMs,
        requests: stats.requests,
        bytes: stats.bytes,
        scanRowsLabel: "scan rows",
      });
    }
  } catch (error) {
    showError(error);
    setGauge("g-rows", "0");
  } finally {
    runBtn?.classList.remove("is-busy");
  }
}

interface PendingRunState {
  engine: Engine;
  duckCacheMode: DuckCacheMode;
  lakeCacheMode: LakeCacheMode;
  memoryBudgetMb: number;
  activeExample: number;
  sql: string;
  controlNavs: number;
}

function currentRunState(controlNavs = 0): PendingRunState {
  return {
    engine,
    duckCacheMode,
    lakeCacheMode,
    memoryBudgetMb,
    activeExample,
    sql: view?.state.doc.toString() ?? initialSqlText ?? examples[activeExample].sql,
    controlNavs,
  };
}

function savePendingRunState(): void {
  const previous = readPendingRunState();
  sessionStorage.setItem(
    PENDING_RUN_KEY,
    JSON.stringify(currentRunState((previous?.controlNavs ?? 0) + 1)),
  );
}

function takePendingRunState(): PendingRunState | undefined {
  const raw = sessionStorage.getItem(PENDING_RUN_KEY);
  if (!raw) return undefined;
  sessionStorage.removeItem(PENDING_RUN_KEY);
  return parsePendingRunState(raw);
}

function readPendingRunState(): PendingRunState | undefined {
  const raw = sessionStorage.getItem(PENDING_RUN_KEY);
  return raw ? parsePendingRunState(raw) : undefined;
}

function parsePendingRunState(raw: string): PendingRunState | undefined {
  try {
    const parsed = JSON.parse(raw) as Partial<PendingRunState>;
    if (parsed.engine !== "lakeql" && parsed.engine !== "duckdb") return undefined;
    return {
      engine: parsed.engine,
      duckCacheMode: parsed.duckCacheMode === "cached" ? "cached" : "fresh",
      lakeCacheMode: parsed.lakeCacheMode === "cached" ? "cached" : "fresh",
      memoryBudgetMb: parseMemoryBudgetMb(parsed.memoryBudgetMb),
      activeExample:
        typeof parsed.activeExample === "number" &&
        parsed.activeExample >= 0 &&
        parsed.activeExample < examples.length
          ? parsed.activeExample
          : 0,
      sql: typeof parsed.sql === "string" ? parsed.sql : examples[0].sql,
      controlNavs: typeof parsed.controlNavs === "number" ? parsed.controlNavs : 0,
    };
  } catch {
    return undefined;
  }
}

function applyPendingRunState(state: PendingRunState): void {
  engine = state.engine;
  duckCacheMode = state.duckCacheMode;
  lakeCacheMode = state.lakeCacheMode;
  memoryBudgetMb = state.memoryBudgetMb;
  activeExample = state.activeExample;
  initialSqlText = state.sql;
}

function syncControlsFromState(): void {
  document.querySelectorAll<HTMLButtonElement>(".switch__opt").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.engine === engine);
  });
  document
    .querySelectorAll<HTMLButtonElement>("#query-picker button[data-example]")
    .forEach((button) => {
      button.classList.toggle("is-active", Number(button.dataset.example) === activeExample);
    });
  const duckSelect = document.getElementById("duck-cache-mode") as HTMLSelectElement | null;
  if (duckSelect) duckSelect.value = duckCacheMode;
  const lakeSelect = document.getElementById("lake-cache-mode") as HTMLSelectElement | null;
  if (lakeSelect) lakeSelect.value = lakeCacheMode;
  const memorySelect = document.getElementById("memory-budget") as HTMLSelectElement | null;
  if (memorySelect) memorySelect.value = String(memoryBudgetMb);
}

async function resumePendingRun(state: PendingRunState | undefined): Promise<void> {
  if (!state) return;
  if (!navigator.serviceWorker.controller && state.controlNavs > 0) {
    throw new Error("The file-read counter is not active after navigation. Try the run again.");
  }
  await ensureCompareProxy({ navigateOnUncontrolled: true });
  await run();
}

function setGauges(input: {
  rows: number;
  ms: number;
  initMs: number;
  requests: number | undefined;
  bytes: number | undefined;
  rowGroups?: string;
  scanRows?: number;
  scanRowsLabel?: string;
}): void {
  setGauge("g-rows", String(input.rows));
  setGauge("g-ms", input.ms < 10 ? input.ms.toFixed(1) : Math.round(input.ms).toString());
  setGauge("g-init", input.initMs > 0 ? Math.round(input.initMs).toString() : "0");
  setGauge(
    "g-reqs",
    input.requests === undefined || !Number.isFinite(input.requests)
      ? "n/a"
      : String(input.requests),
  );
  setGauge(
    "g-bytes",
    input.bytes === undefined || !Number.isFinite(input.bytes) ? "n/a" : formatBytes(input.bytes),
  );
  setGauge("g-rowgroups", input.rowGroups ?? "n/a");
  setGauge(
    "g-scan-rows",
    input.scanRows === undefined || !Number.isFinite(input.scanRows)
      ? "n/a"
      : formatCount(input.scanRows),
  );
  setGaugeLabel("g-scan-rows-label", input.scanRowsLabel ?? "scan rows");
  setGauge("g-engine", engine === "lakeql" ? "lakeql" : "duckdb");
}

function setGauge(id: string, value: string): void {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = value;
  el.classList.remove("flash");
  void el.offsetWidth;
  el.classList.add("flash");
}

function setGaugeLabel(id: string, value: string): void {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function renderResult(rows: Row[]): void {
  const host = document.getElementById("result");
  if (!host) return;
  if (rows.length === 0) {
    host.innerHTML = `<div class="result__empty">0 rows matched.</div>`;
    return;
  }
  const cols = Object.keys(rows[0]);
  const head = cols.map((c) => `<th>${escapeHtml(c)}</th>`).join("");
  const body = rows
    .map((r) => {
      const cells = cols
        .map((c) => {
          const v = r[c];
          const numeric =
            (typeof v === "number" && !isEpochMillisTimestampColumn(c, v)) || typeof v === "bigint";
          return `<td class="${numeric ? "num" : ""}">${escapeHtml(formatCell(c, v))}</td>`;
        })
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");
  host.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function formatCell(column: string, v: unknown): string {
  if (v === null || v === undefined) return ".";
  if (typeof v === "bigint") return v.toString();
  if (isTimestampValue(v)) return v.toJSON();
  if (v instanceof Date) return v.toISOString();
  if (column === "FL_DATE" && typeof v === "number") return new Date(v).toISOString().slice(0, 10);
  if (typeof v === "number" && isEpochMillisTimestampColumn(column, v)) {
    return new Date(v).toISOString();
  }
  if (typeof v === "number") return Number.isInteger(v) ? String(v) : v.toFixed(2);
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function isEpochMillisTimestampColumn(column: string, value: number): boolean {
  if (!Number.isInteger(value) || !Number.isFinite(value)) return false;
  if (value < -8_640_000_000_000_000 || value > 8_640_000_000_000_000) return false;
  const normalized = column.toLowerCase();
  return (
    normalized === "timestamp" ||
    normalized.endsWith("_ts") ||
    normalized.endsWith("_time") ||
    normalized.endsWith("_at") ||
    normalized.includes("timestamp")
  );
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

function formatCount(count: number): string {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(count);
}

function memoryBudgetBytes(): number {
  return memoryBudgetMb * BYTES_PER_MB;
}

function parseMemoryBudgetMb(value: unknown): number {
  return typeof value === "number" &&
    MEMORY_BUDGETS_MB.includes(value as (typeof MEMORY_BUDGETS_MB)[number])
    ? value
    : DEFAULT_MEMORY_BUDGET_MB;
}

function rowGroupSummary(stats: QueryStats): string {
  return `${stats.rowGroupsRead}/${stats.rowGroupsRead + stats.rowGroupsSkipped}`;
}

function showError(error: unknown): void {
  const el = document.getElementById("error");
  if (!el) return;
  const message = error instanceof Error ? error.message : String(error);
  el.innerHTML = `<b>error</b> ${escapeHtml(message)}`;
  el.hidden = false;
}

function hideError(): void {
  const el = document.getElementById("error");
  if (el) el.hidden = true;
}

function escapeHtml(s: string): string {
  return s.replace(
    /[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c] as string,
  );
}

function setupEngineSwitch(): void {
  const opts = Array.from(document.querySelectorAll<HTMLButtonElement>(".switch__opt"));
  const glide = document.getElementById("engine-glide");
  function moveGlide(active: HTMLButtonElement): void {
    if (!glide) return;
    glide.style.width = `${active.offsetWidth}px`;
    glide.style.transform = `translateX(${active.offsetLeft - 3}px)`;
  }

  opts.forEach((opt) => {
    opt.addEventListener("click", () => {
      if (opt.classList.contains("is-active")) return;
      opts.forEach((o) => {
        o.classList.remove("is-active");
      });
      opt.classList.add("is-active");
      engine = opt.dataset.engine as Engine;
      moveGlide(opt);
      setGauge("g-engine", engine === "lakeql" ? "lakeql" : "duckdb");
    });
  });

  const active = opts.find((o) => o.classList.contains("is-active"));
  if (active) requestAnimationFrame(() => moveGlide(active));
}

function setupExamples(): void {
  const host = document.getElementById("query-picker");
  if (!host) return;
  host.innerHTML = examples
    .map(
      (example, i) =>
        `<button type="button" class="${i === activeExample ? "is-active" : ""}" data-example="${i}">${escapeHtml(
          example.name,
        )}</button>`,
    )
    .join("");
  host.addEventListener("click", (event) => {
    const button = (event.target as Element).closest<HTMLButtonElement>("button[data-example]");
    if (!button) return;
    activeExample = Number(button.dataset.example);
    initialSqlText = undefined;
    host.querySelectorAll("button").forEach((b) => {
      b.classList.remove("is-active");
    });
    button.classList.add("is-active");
    mountEditor();
  });
}

function setupDuckCacheMode(): void {
  const select = document.getElementById("duck-cache-mode") as HTMLSelectElement | null;
  select?.addEventListener("change", () => {
    duckCacheMode = select.value as DuckCacheMode;
    if (duckCacheMode === "fresh") void resetDuckDb();
  });
}

function setupLakeCacheMode(): void {
  const select = document.getElementById("lake-cache-mode") as HTMLSelectElement | null;
  select?.addEventListener("change", () => {
    lakeCacheMode = select.value as LakeCacheMode;
    lakeRuntime = undefined;
  });
}

function setupMemoryBudget(): void {
  const select = document.getElementById("memory-budget") as HTMLSelectElement | null;
  select?.addEventListener("change", () => {
    memoryBudgetMb = parseMemoryBudgetMb(Number(select.value));
    lakeRuntime = undefined;
  });
}

function setupVersion(): void {
  const tag = document.getElementById("version-tag");
  if (tag) tag.textContent = `v${__LAKEQL_VERSION__}`;
}

function datasetConfigFromLocation(): DatasetConfig {
  const params = new URLSearchParams(window.location.search);
  const kind = datasetKindParam(params.get("kind"));
  const sourceUrl =
    sourceUrlParam(params.get("source")) ??
    (kind === "spatial"
      ? SPATIAL_SOURCE_URL
      : kind === "window"
        ? WINDOW_SOURCE_URL
        : DEFAULT_SOURCE_URL);
  return {
    sourceUrl,
    key:
      nonEmptyParam(params.get("key")) ??
      sourceKey(sourceUrl) ??
      (kind === "spatial"
        ? "spatial.parquet"
        : kind === "window"
          ? "window-events.parquet"
          : DEFAULT_DATASET_KEY),
    size:
      positiveNumberParam(params.get("size")) ??
      (kind === "spatial"
        ? SPATIAL_DATASET_SIZE
        : kind === "window"
          ? WINDOW_DATASET_SIZE
          : DEFAULT_DATASET_SIZE),
    name:
      nonEmptyParam(params.get("name")) ??
      (kind === "spatial"
        ? "Spatial Parquet on R2"
        : kind === "window"
          ? "Window function fixture"
          : "Plotly 2015 flights"),
    host: nonEmptyParam(params.get("host")) ?? "Cloudflare R2",
    kind,
  };
}

function datasetKindParam(value: string | null): DatasetKind {
  return value === "spatial" || value === "window" ? value : "tabular";
}

function sourceUrlParam(value: string | null): string | undefined {
  if (!value) return undefined;
  try {
    const url = new URL(value);
    return url.protocol === "https:" || url.origin === window.location.origin
      ? url.href
      : undefined;
  } catch {
    return undefined;
  }
}

function nonEmptyParam(value: string | null): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function positiveNumberParam(value: string | null): number | undefined {
  if (!value) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function sourceKey(sourceUrl: string): string | undefined {
  const name = new URL(sourceUrl).pathname.split("/").filter(Boolean).at(-1);
  return name?.endsWith(".parquet") ? name : undefined;
}

function duckSqlForDataset(sqlText: string, fileName: string): string {
  const escaped = fileName.replace(/'/g, "''");
  let out = sqlText
    .replace(/\bfrom\s+flights\.parquet\b/giu, `from '${escaped}'`)
    .replace(/\bfrom\s+spatial\.parquet\b/giu, `from '${escaped}'`)
    .replace(/\bfrom\s+window_events\.parquet\b/giu, `from '${escaped}'`)
    .replace(/\bfrom\s+window-events\.parquet\b/giu, `from '${escaped}'`)
    .replace(new RegExp(`\\bfrom\\s+${escapeRegExp(dataset.key)}\\b`, "giu"), `from '${escaped}'`);
  if (dataset.kind === "spatial") {
    out = out
      .replace(/\bst_dwithin\s*\(\s*geometry\s*,/giu, "ST_DWithin(ST_GeomFromWKB(geometry),")
      .replace(/\bst_intersects\s*\(\s*geometry\s*,/giu, "ST_Intersects(ST_GeomFromWKB(geometry),")
      .replace(/\bst_within\s*\(\s*geometry\s*,/giu, "ST_Within(ST_GeomFromWKB(geometry),")
      .replace(/\bst_contains\s*\(/giu, "ST_Contains(")
      .replace(/,\s*geometry\s*\)/giu, ", ST_GeomFromWKB(geometry))")
      .replace(
        /\bst_point\s*\(\s*([+-]?(?:\d+\.?\d*|\.\d+))\s*,\s*([+-]?(?:\d+\.?\d*|\.\d+))\s*\)/giu,
        "ST_GeomFromText('POINT($1 $2)')",
      )
      .replace(
        /\bst_bbox\s*\(\s*([^,\s]+)\s*,\s*([^,\s]+)\s*,\s*([^,\s]+)\s*,\s*([^)]+?)\s*\)/giu,
        "ST_GeomFromText('POLYGON((' || $1 || ' ' || $2 || ',' || $3 || ' ' || $2 || ',' || $3 || ' ' || $4 || ',' || $1 || ' ' || $4 || ',' || $1 || ' ' || $2 || '))')",
      );
  }
  return out;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function renderDatasetConfig(): void {
  setText("dataset-name", dataset.name);
  setText("dataset-size", dataset.size === undefined ? "probed" : formatBytes(dataset.size));
  setText("dataset-host", dataset.host);
  const source = document.getElementById("dataset-source") as HTMLAnchorElement | null;
  if (source) source.href = dataset.sourceUrl;
  document.body.dataset.datasetKind = dataset.kind;
  document.querySelectorAll<HTMLElement>("[data-kind]").forEach((element) => {
    const active = element.dataset.kind === dataset.kind;
    element.classList.toggle("is-active", active);
    if (active) element.setAttribute("aria-current", "page");
    else element.removeAttribute("aria-current");
  });
}

function setText(id: string, value: string): void {
  const element = document.getElementById(id);
  if (element) element.textContent = value;
}

const pendingRunState = takePendingRunState();
if (pendingRunState) applyPendingRunState(pendingRunState);

document.getElementById("run")?.addEventListener("click", () => void run());
setupVersion();
renderDatasetConfig();
setupExamples();
setupDuckCacheMode();
setupLakeCacheMode();
setupMemoryBudget();
syncControlsFromState();
setupEngineSwitch();
mountEditor();
void prepareCompareProxy().catch(showError);
void resumePendingRun(pendingRunState).catch(showError);
