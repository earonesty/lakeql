# DuckDB-WASM Parity Plan — expand the footprint without bloating core

Goal: grow lakeql's addressable footprint by closing the parity gaps that block the
**most common duckdb-wasm browser use cases**, while keeping the "dependency-light,
no-WASM, runtime-agnostic, never-wrong" posture that is our actual advantage.

The bar is *not* "everything DuckDB does." It is: **the browser jobs people
reach for duckdb-wasm for today should work in lakeql**, and every new dependency
should be **lazily loaded only when a query needs it** — never in the static
edge graph.

> Guiding rule: adopt solved problems, keep correctness and resource limits
> non-negotiable, and **pay for a module only when you use it**.

---

## Why this plan exists — how people actually use duckdb-wasm

Three dominant patterns across the ecosystem:

1. **"Bring your own file" ad-hoc analysis** — upload/drag a CSV/JSON/Parquet,
   run SQL, no server. Data journalism, SQL teaching, "explore this export."
2. **Interactive dashboards / BI** — data loaded once as **Apache Arrow**, then
   queried on every filter change; results flow zero-copy into charting libs
   (Observable Plot, Perspective, Arquero). Arrow is duckdb-wasm's data protocol
   for *both* ingest and query results.
3. **Remote lake querying at the edge** — range-read Parquet on S3/GCS/R2,
   schema previews, Parquet-inspector extensions.

**lakeql already wins #3** — and beats duckdb-wasm there: no 4 GB/tab WASM ceiling,
real spill, resumable bookmarks, Iceberg-native reads. The footprint problem is
that **#1 and #2 — the majority of browser duckdb-wasm usage — are currently
impossible in lakeql**: it reads only Parquet + Iceberg and emits only
rows/NDJSON/CSV/Parquet.

---

## Design principle: smart-load everything optional

Today lakeql's loading strategy is **partial and inconsistent**:

- ✅ Lazy where it counts: `avsc` is `await import()`-ed only when an Iceberg
  table actually has Avro manifests (`packages/iceberg/src/index.ts`); the
  Cloudflare driver is dynamically imported in the workerd entry.
- ✅ Package-level separation: `lakeql-parquet`, `lakeql-iceberg`, `lakeql-sql`
  are separate packages, so you only bundle what you import.
- ✅ **Geo is now lazy (was the eager anti-pattern, fixed):** `@turf/*` + `h3-js`
  used to be statically imported into `lakeql-core`'s evaluator, so **every**
  consumer bundled turf + h3-js (~7.5 MB) even for a pure `SELECT … FROM parquet`.
  They now live in `packages/core/src/geo-backend.ts`, reached only via a dynamic
  `import()` triggered when a query actually uses a spatial function needing exact
  geometry/H3. Verified: a parquet-only consumer bundle contains zero static
  turf/h3 references — they sit in a separate async chunk.

**The principle for all parity work below:** follow the `avsc` precedent, not the
geo anti-pattern. Every new format reader and every new output encoder is either
(a) its own package, or (b) behind a dynamic `import()` triggered only by the API
call / function / format that needs it. The core static graph must not grow.

### Companion cleanup — de-bundle geo from core ✅ DONE

`@turf/*` + `h3-js` were lifted out of `lakeql-core`'s static graph into a
dynamically-imported `geo-backend.ts`. The evaluator holds an injectable backend
slot; the query executor scans each query's expressions and `await import()`s the
backend only when a backend-requiring spatial function (`st_intersects/disjoint/
contains/within`, `h3_within/cell/parent`) is present — the `avsc` pattern. Pure
spatial helpers (`st_point`, `st_bbox`, `st_distance`, `st_area`, `h3_in`, …) need
no backend and keep working with zero extra deps. Also set `"sideEffects": false`
on `lakeql-core` + `lakeql` so the umbrella's barrel re-exports tree-shake. This is
the proof-of-concept for the smart-load principle that the new format/output
modules below must follow.

(The standalone `lakeql-geo` builder package keeps its own turf import for the
fluent API; it is not re-exported by the edge `index` entry, so it never enters
the default bundle.)

---

## Top 10 parity gaps, ranked by adoption impact

### Tier 1 — table stakes (unlock whole use-case categories; do first)

Each is **independent of the lake engine** and is the highest-ROI work in the repo.

1. **Read CSV** — `read_csv`-style with type sniffing, header detection,
   delimiter/quote options. The #1 browser job. Ship as **`lakeql-csv`**
   (own package). *Status: ✅ initial opt-in package implemented: headered /
   headerless CSV, delimiter override/detection, quoted fields, null handling,
   type sniffing, browser-friendly inputs, query integration, ingest budgets, and
   typed rejection for malformed/ragged CSV.*

2. **Read JSON / NDJSON** — auto-structure detection, arrays + line-delimited.
   Second-most-common ingest. Ship as **`lakeql-json`** (own package).
   *Status: ✅ initial opt-in package implemented: JSON arrays, single objects,
   NDJSON, browser-friendly inputs, query integration, ingest budgets, nested
   JSON cell preservation, and typed rejection for malformed or unsupported row
   shapes.*

3. **Ingest in-memory JS data** — register a JS array / `File` / `Blob` /
   `ArrayBuffer` / Arrow table as a queryable table (the duckdb-wasm
   `registerFileBuffer` / `insertJSONFromArray` loop). This is the core browser
   "bring your own data" interaction. *Status: ✅ row-array ingest implemented
   via `createInMemoryLake` / `inMemoryRowsScanner`; `File` / `Blob` /
   `ArrayBuffer` are covered through the CSV/JSON opt-in packages; Arrow table
   ingest is available through the opt-in `lakeql-arrow` package.*

4. **Apache Arrow output** — return results as an Arrow table / IPC stream
   (`.toArrow()`, `streamArrow()`). Arrow is the interop format: zero-copy into
   Plot/Perspective/Arquero, Transferable across workers, 10–100× faster than JS
   objects. **Must be lazy** — `apache-arrow` lives in a separate **`lakeql-arrow`**
   package, never in core. *Status: ✅ initial opt-in package implemented:
   row/query/batch conversion to Arrow tables, IPC payloads, and readable IPC
   streams. `apache-arrow` is scoped to `lakeql-arrow`; core does not import it.*

### Tier 2 — SQL depth (blocks porting real analytical queries)

5. **Window functions** — `ROW_NUMBER`, `RANK`, `LAG`/`LEAD`, running sums,
   `OVER (PARTITION BY … ORDER BY …)`. Core of analytics/dashboards; the most
   conspicuous absence. *Status: ✅ supported and tested: ranking/value/exact
   aggregate windows, explicit `ROWS` / `RANGE` / `GROUPS` frames, `EXCLUDE`,
   named windows, null-treatment modifiers, `FILTER`, `DISTINCT`, `QUALIFY`,
   stateful execution, Workers runtime, and Parquet work-unit fan-out are covered
   by DuckDB reference cases, deterministic seeded differential cases, unit tests,
   bundle checks, and workerd tests.*

6. **Broader JOINs** — N-way, `RIGHT` / `FULL` / `CROSS`, non-equi predicates.
   Real queries join 3+ tables / dimension tables. *Status: ⚠️ bounded two-table
   inner/left equi-join only.*

7. **Richer function library** — regex (`regexp_matches`/`regexp_replace`),
   statistical aggregates (`stddev`, `var`, `median`, `quantile`, `mode`), more
   date/string functions, list/struct accessors. A missing function silently
   breaks a ported query. *Status: ⚠️ regex matches/replace, ~30 other scalar
   fns, basic aggs, variance/stddev sample/pop aggs, budgeted exact `median`,
   budgeted exact continuous `quantile_cont`, and budgeted exact `mode`;
   discrete quantile aliases and list/struct accessors remain future work.*

### Tier 3 — ergonomics & retention

8. **Parameterized / prepared statements** — `$1` / named params bound per
   filter change without string-building SQL. Standard duckdb-wasm dashboard
   pattern; also the safe one. *Status: ⚠️ positional `$1` SQL parameters can be
   bound through the parser API and compile to scalar literals before execution;
   named parameters and reusable prepared statement objects remain future work.*

9. **`DESCRIBE` / `SUMMARIZE` / `SAMPLE` as SQL** — the "preview an unknown file"
   workflow (Parquet-inspector extensions, first look at any dataset). *Status:
   ⚠️ partial: CLI `inspect` and `schema` are available, and `DESCRIBE <table>`
   is exposed through the SQL statement parser / CLI query path; `SUMMARIZE` and
   SQL `SAMPLE` remain future work.*

10. **OPFS / persistent local cache** — duckdb-wasm persists tables in OPFS so
    reloads are instant. lakeql's edge cache story is strong but lacks a
    browser-persistence tier for repeated local querying. Ship as an optional
    cache adapter (smart-loaded), not core. *Status: ✅ initial opt-in
    `lakeql-opfs` package implemented: OPFS-backed byte and JSON
    `CacheAdapter` implementations for browser persistence without adding OPFS
    APIs to core.*

---

## Sequencing

- **Tier 1 (#1–4) + geo de-bundle** is the strategic move: it converts lakeql from
  "edge lake engine" into "drop-in duckdb-wasm alternative for the browser," and
  establishes the smart-load principle on a real case (`lakeql-arrow`) before more
  modules arrive.
- **After Tier 1, do not drift into SQL bikeshedding by default.** The highest-ROI
  follow-up work is broader runtime proof and measured performance: portable
  work-unit fan-out/fan-in across browser/Worker/edge deployments, persistent
  caches, benchmark lanes, profiling, and planning memoization.
- **Tier 2 (#5–7)** matters when it blocks real analytical workloads, but it should
  be pulled by observed query gaps rather than generic DuckDB syntax parity.
- **Tier 3 (#8–10)** is retention polish. Add ergonomic SQL statements only when
  they unlock a broader workflow such as unknown-file preview, safe dashboard
  parameters, or persistent local sessions.

## Confirmed packaging decisions

- Arrow output ships as **`lakeql-arrow`** (separate package; `apache-arrow` never
  enters the core static graph).
- Geo de-bundle target is the existing **`lakeql-geo`** package (opt-in function
  pack; turf + h3-js leave `lakeql-core`).

## Non-negotiables

- **Never silently wrong:** unsupported CSV/JSON shapes, Arrow types we can't map,
  etc. get a typed `LAKEQL_*` rejection — same discipline as Parquet/Iceberg today.
- **Resource budgets apply to ingest too:** in-memory + CSV/JSON ingestion must
  respect `maxBytes` / `maxBufferedRows` / spill, not load unbounded files into a
  tab.
- **Core stays lean:** if a feature adds a dependency, it is lazily loaded or in
  its own package. The edge "query Parquet on R2" bundle must not grow because we
  added a CSV reader.
