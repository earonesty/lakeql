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
import type { ObjectStore, QueryBudget, QueryStats } from "lakeql-core";
import { httpStore } from "lakeql-http";
import { createParquetLake } from "lakeql-parquet";
import { parseSql } from "lakeql-sql";
import "./styles.css";

declare const __LAKEQL_VERSION__: string;

type Engine = "lakeql" | "duckdb";
type DuckCacheMode = "fresh" | "cached";
type LakeCacheMode = "fresh" | "cached";
type Row = Record<string, unknown>;
type Lake = ReturnType<typeof createParquetLake>;

const DATASET_KEY = "2015_flights.parquet";
const DATASET_PROXY_PATH = "compare-data/2015_flights.parquet";
const DATASET_SIZE = 25_238_218;
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

let engine: Engine = "lakeql";
let duckCacheMode: DuckCacheMode = "fresh";
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
    }>
  | undefined;
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
    doc: initialSqlText ?? EXAMPLES[activeExample].sql,
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
      if (path === DATASET_KEY) return { size: DATASET_SIZE };
      return inner.head(path);
    },
  };
  return store;
}

function applyAst(builder: ReturnType<Lake["path"]>, ast: ReturnType<typeof parseSql>) {
  let next = builder;
  if (ast.select) next = next.select(ast.select);
  if (ast.where) next = next.where(ast.where);
  if (ast.orderBy) next = next.orderBy(ast.orderBy);
  if (ast.offset !== undefined) next = next.offset(ast.offset);
  if (ast.limit !== undefined) next = next.limit(ast.limit);
  return next;
}

type OrderTerm = { column: string; direction?: "asc" | "desc" };

function sortRows(rows: Row[], terms: OrderTerm[]): Row[] {
  return [...rows].sort((a, b) => {
    for (const term of terms) {
      const dir = term.direction === "desc" ? -1 : 1;
      const av = a[term.column];
      const bv = b[term.column];
      if (av === bv) continue;
      if (av === null || av === undefined) return 1;
      if (bv === null || bv === undefined) return -1;
      return (av < bv ? -1 : 1) * dir;
    }
    return 0;
  });
}

function datasetProxyUrl(): string {
  return new URL(DATASET_PROXY_PATH, window.location.href).href;
}

function datasetProxyBase(): string {
  const url = datasetProxyUrl();
  return url.slice(0, url.length - DATASET_KEY.length);
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
  const store = knownSizeStore(httpStore({ baseUrl: datasetProxyBase() }));
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
  const ast = { ...parseSql(sqlText), source: DATASET_KEY };
  const aggregates =
    ast.aggregates && Object.keys(ast.aggregates).length > 0 ? ast.aggregates : undefined;
  const grouped = (ast.groupBy?.length ?? 0) > 0;
  let rows: Row[];
  let lakeStats: QueryStats;

  if (!aggregates && !grouped) {
    const result = applyAst(lake.path(DATASET_KEY), ast).run();
    rows = (await result.toArray()) as Row[];
    lakeStats = result.stats;
  } else {
    let base = lake.path(DATASET_KEY);
    if (ast.where) base = base.where(ast.where);
    const result = base.run();
    rows = (await result.aggregate(ast.groupBy ?? [], aggregates ?? {})) as Row[];
    lakeStats = result.stats;
    if (ast.orderBy) rows = sortRows(rows, ast.orderBy);
    const offset = ast.offset ?? 0;
    if (ast.limit !== undefined) rows = rows.slice(offset, offset + ast.limit);
    else if (offset > 0) rows = rows.slice(offset);
  }

  return {
    rows,
    ms: performance.now() - started,
    stats: await serviceWorkerRequest("getStats"),
    lakeStats,
  };
}

async function initDuckDb() {
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
    await db.registerFileURL(DATASET_KEY, datasetProxyUrl(), duckdb.DuckDBDataProtocol.HTTP, true);
    await db.collectFileStatistics(DATASET_KEY, true);
    const conn = await db.connect();
    return { db, conn, initMs: performance.now() - started };
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
  if (duckCacheMode === "fresh") await resetDuckDb();
  const { conn, initMs } = await initDuckDb();
  const duckSql = sqlText.replace(/\bfrom\s+flights\.parquet\b/giu, `from '${DATASET_KEY}'`);
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
    sql: view?.state.doc.toString() ?? initialSqlText ?? EXAMPLES[activeExample].sql,
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
        parsed.activeExample < EXAMPLES.length
          ? parsed.activeExample
          : 0,
      sql: typeof parsed.sql === "string" ? parsed.sql : EXAMPLES[0].sql,
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
          const numeric = typeof v === "number" || typeof v === "bigint";
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
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  if (column === "FL_DATE" && typeof v === "number") return new Date(v).toISOString().slice(0, 10);
  if (typeof v === "number") return Number.isInteger(v) ? String(v) : v.toFixed(2);
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
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
  host.innerHTML = EXAMPLES.map(
    (example, i) =>
      `<button type="button" class="${i === activeExample ? "is-active" : ""}" data-example="${i}">${escapeHtml(
        example.name,
      )}</button>`,
  ).join("");
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

const pendingRunState = takePendingRunState();
if (pendingRunState) applyPendingRunState(pendingRunState);

document.getElementById("run")?.addEventListener("click", () => void run());
setupVersion();
setupExamples();
setupDuckCacheMode();
setupLakeCacheMode();
setupMemoryBudget();
syncControlsFromState();
setupEngineSwitch();
mountEditor();
void prepareCompareProxy().catch(showError);
void resumePendingRun(pendingRunState).catch(showError);
