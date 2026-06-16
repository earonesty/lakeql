import { javascript } from "@codemirror/lang-javascript";
import { json } from "@codemirror/lang-json";
import { sql } from "@codemirror/lang-sql";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { type Extension, Prec } from "@codemirror/state";
import { EditorView, keymap } from "@codemirror/view";
import { tags } from "@lezer/highlight";
import { basicSetup } from "codemirror";
import {
  and,
  between,
  col,
  eq,
  fn,
  gt,
  gte,
  ilike,
  isIn,
  isNull,
  like,
  lit,
  lt,
  lte,
  ne,
  not,
  type ObjectStore,
  or,
} from "lakeql-core";
import { httpStore } from "lakeql-http";
import { createParquetLake } from "lakeql-parquet";
import { parseSql } from "lakeql-sql";
import "./styles.css";

type Mode = "sql" | "js" | "json";

const DEFAULTS: Record<Mode, string> = {
  sql: `select region, sum(amount) as revenue, count() as orders
from sales.parquet
where amount > 0
group by region
order by revenue desc`,
  js: `lake.path("sales.parquet")
  .select(["store_id", "region", "amount"])
  .where(gt("amount", 50))
  .orderBy([{ column: "amount", direction: "desc" }])
  .limit(10)
  .toArray()`,
  json: `{
  "version": 1,
  "from": "sales.parquet",
  "select": ["store_id", "region", "amount"],
  "where": { "gt": ["amount", 50] },
  "orderBy": [{ "column": "amount", "direction": "desc" }],
  "limit": 10
}`,
};

const docs: Record<Mode, string> = { ...DEFAULTS };
let mode: Mode = "sql";

// ---- editor ---------------------------------------------------------------

const highlight = HighlightStyle.define([
  { tag: tagKeyword(), color: "#c6f24e", fontWeight: "700" },
  { tag: tagStrings(), color: "#6ad7e5" },
  { tag: tagNumbers(), color: "#e3b341" },
  { tag: tagComment(), color: "#5b636d", fontStyle: "italic" },
  { tag: tagProp(), color: "#e7ebef" },
  { tag: tagOperator(), color: "#8b949e" },
]);

function tagKeyword() {
  return tags.keyword;
}
function tagStrings() {
  return [tags.string, tags.special(tags.string)];
}
function tagNumbers() {
  return [tags.number, tags.bool, tags.null];
}
function tagComment() {
  return [tags.lineComment, tags.blockComment];
}
function tagProp() {
  return [tags.propertyName, tags.variableName];
}
function tagOperator() {
  return tags.operator;
}

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

function languageFor(m: Mode): Extension {
  if (m === "sql") return sql();
  if (m === "json") return json();
  return javascript();
}

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

let view: EditorView;

function mountEditor() {
  const parent = document.getElementById("editor");
  if (!parent) return;
  if (view) view.destroy();
  view = new EditorView({
    parent,
    doc: docs[mode],
    extensions: [
      basicSetup,
      languageFor(mode),
      syntaxHighlighting(highlight),
      surfaceTheme,
      runKeymap,
      EditorView.lineWrapping,
    ],
  });
}

// ---- object store with live byte/request instrumentation ------------------

interface Stats {
  bytes: number;
  requests: number;
}

function countingStore(inner: ObjectStore): { store: ObjectStore; stats: Stats } {
  const stats: Stats = { bytes: 0, requests: 0 };
  const store: ObjectStore = {
    async get(path) {
      stats.requests += 1;
      const out = await inner.get(path);
      if (out) stats.bytes += out.byteLength;
      return out;
    },
    async getRange(path, range) {
      stats.requests += 1;
      const out = await inner.getRange(path, range);
      stats.bytes += out.byteLength;
      return out;
    },
    put: inner.put?.bind(inner),
    list: inner.list?.bind(inner),
    async head(path) {
      stats.requests += 1;
      return inner.head(path);
    },
  };
  return { store, stats };
}

function resolveSource(raw: string): { baseUrl: string; key: string } {
  const url = new URL(raw.trim(), window.location.href);
  const key = url.pathname.split("/").pop() || "data.parquet";
  const baseUrl = url.href.slice(0, url.href.length - key.length);
  return { baseUrl, key };
}

// ---- run pipelines --------------------------------------------------------

type Lake = ReturnType<typeof createParquetLake>;
type Row = Record<string, unknown>;

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

// Aggregated rows come back fully materialized, so order/limit are applied here
// rather than through the streaming builder.
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

async function runSql(text: string, lake: Lake, key: string): Promise<Row[]> {
  const ast = { ...parseSql(text), source: key };
  const aggregates =
    ast.aggregates && Object.keys(ast.aggregates).length > 0 ? ast.aggregates : undefined;
  const grouped = (ast.groupBy?.length ?? 0) > 0;

  if (!aggregates && !grouped) {
    return (await applyAst(lake.path(key), ast).toArray()) as Row[];
  }
  if (ast.having) {
    throw new Error("HAVING is supported by the engine but not yet wired into this playground.");
  }
  let base = lake.path(key);
  if (ast.where) base = base.where(ast.where);
  let rows = (await base.groupBy(ast.groupBy ?? []).aggregate(aggregates ?? {})) as Row[];
  if (ast.orderBy) rows = sortRows(rows, ast.orderBy);
  const offset = ast.offset ?? 0;
  if (ast.limit !== undefined) rows = rows.slice(offset, offset + ast.limit);
  else if (offset > 0) rows = rows.slice(offset);
  return rows;
}

// A lake whose source is fixed to the resolved key, so the query text's
// table name is cosmetic and always points at the configured URL.
function boundLake(lake: Lake, key: string) {
  return {
    path: (_name?: string) => lake.path(key),
    query: (q: Record<string, unknown>) => lake.query({ ...q, from: key } as never),
  };
}

const jsScope = {
  and,
  between,
  col,
  eq,
  fn,
  gt,
  gte,
  ilike,
  isIn,
  isNull,
  like,
  lit,
  lt,
  lte,
  ne,
  not,
  or,
};

async function runJs(code: string, lake: Lake, key: string): Promise<Row[]> {
  const scope: Record<string, unknown> = { ...jsScope, lake: boundLake(lake, key) };
  const names = Object.keys(scope);
  // The playground evaluates the visitor's own builder expression in a scope
  // limited to the lake and expression helpers — no globals are passed in.
  const factory = new Function(...names, `return (async () => (${code}))();`);
  let result: unknown = await factory(...names.map((n) => scope[n]));
  if (result && typeof (result as { toArray?: unknown }).toArray === "function") {
    result = await (result as { toArray: () => Promise<Row[]> }).toArray();
  }
  if (!Array.isArray(result)) {
    throw new Error("Expression must resolve to rows — end with `.toArray()`.");
  }
  return result as Row[];
}

async function run(): Promise<void> {
  const runBtn = document.getElementById("run");
  runBtn?.classList.add("is-busy");
  hideError();
  docs[mode] = view.state.doc.toString();

  const sourceInput = document.getElementById("source-url") as HTMLInputElement;
  const { baseUrl, key } = resolveSource(sourceInput.value || "./sales.parquet");
  const { store, stats } = countingStore(httpStore({ baseUrl }));
  const lake = createParquetLake({ store });

  const started = performance.now();
  try {
    const text = docs[mode];
    let rows: Row[];
    if (mode === "sql") {
      rows = await runSql(text, lake, key);
    } else if (mode === "json") {
      const parsed = JSON.parse(text) as Record<string, unknown>;
      rows = (await lake.query({ ...parsed, from: key } as never).toArray()) as Row[];
    } else {
      rows = await runJs(text, lake, key);
    }
    const ms = performance.now() - started;
    renderResult(rows);
    setGauges(rows.length, stats.bytes, stats.requests, ms);
  } catch (error) {
    showError(error);
    setGauges(0, stats.bytes, stats.requests, performance.now() - started);
  } finally {
    runBtn?.classList.remove("is-busy");
  }
}

// ---- rendering ------------------------------------------------------------

function renderResult(rows: Row[]): void {
  const host = document.getElementById("result");
  if (!host) return;
  if (rows.length === 0) {
    host.innerHTML = `<div class="result__empty">0 rows — the query ran, the predicate matched nothing.</div>`;
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
          return `<td class="${numeric ? "num" : ""}">${escapeHtml(formatCell(v))}</td>`;
        })
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");
  host.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function formatCell(v: unknown): string {
  if (v === null || v === undefined) return "·";
  if (typeof v === "bigint") return v.toString();
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function setGauges(rows: number, bytes: number, requests: number, ms: number): void {
  setGauge("g-rows", String(rows));
  setGauge("g-bytes", formatBytes(bytes));
  setGauge("g-reqs", String(requests));
  setGauge("g-ms", ms < 10 ? ms.toFixed(1) : Math.round(ms).toString());
}

function setGauge(id: string, value: string): void {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = value;
  el.classList.remove("flash");
  void el.offsetWidth;
  el.classList.add("flash");
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

function showError(error: unknown): void {
  const el = document.getElementById("error");
  if (!el) return;
  const code = (error as { code?: string })?.code;
  const message = error instanceof Error ? error.message : String(error);
  el.innerHTML = `${code ? `<b>${escapeHtml(code)}</b> ` : "<b>error</b> "}${escapeHtml(message)}`;
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

// ---- wiring ---------------------------------------------------------------

function setupSwitch(): void {
  const opts = Array.from(document.querySelectorAll<HTMLButtonElement>(".switch__opt"));
  const glide = document.getElementById("switch-glide");

  function moveGlide(active: HTMLButtonElement): void {
    if (!glide) return;
    glide.style.width = `${active.offsetWidth}px`;
    glide.style.transform = `translateX(${active.offsetLeft - 3}px)`;
  }

  opts.forEach((opt) => {
    opt.addEventListener("click", () => {
      if (opt.classList.contains("is-active")) return;
      // Persist the current editor's text under the OUTGOING mode before switching.
      docs[mode] = view.state.doc.toString();
      opts.forEach((o) => {
        o.classList.remove("is-active");
      });
      opt.classList.add("is-active");
      mode = opt.dataset.mode as Mode;
      moveGlide(opt);
      mountEditor();
    });
  });

  const active = opts.find((o) => o.classList.contains("is-active"));
  if (active) requestAnimationFrame(() => moveGlide(active));
}

function setupCopy(): void {
  const btn = document.getElementById("copy-install");
  btn?.addEventListener("click", async () => {
    await navigator.clipboard.writeText("npm install lakeql");
    const prev = btn.textContent;
    btn.textContent = "copied";
    setTimeout(() => {
      btn.textContent = prev;
    }, 1400);
  });
}

document.getElementById("run")?.addEventListener("click", () => void run());
setupSwitch();
setupCopy();
mountEditor();
// First scan so visitors see live data and gauges immediately.
void run();
