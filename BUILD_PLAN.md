# BUILD_PLAN.md

Concrete, feature-complete plan to close every open item in [`NEEDED.md`](./NEEDED.md).
Each workstream lists the gap, the chosen approach (including external packages where
hand-rolling is the wrong call), the files it touches, and an acceptance bar.

The north star from `NEEDED.md` is unchanged: **a small, strict, trustworthy read core
that either reads correctly or rejects precisely**, runnable inside Cloudflare Workers and
similar constrained runtimes. Every package choice below is filtered through "does it run on
`workerd` and stay small?" — Node-only tooling is allowed **only** as dev/CI dependencies for
fixture generation and reference comparison, never in the shipped bundle.

---

## 0. Dependency decisions (research outcome)

| Need | Decision | Package | Bundle / runtime | Why |
|---|---|---|---|---|
| S3 SigV4 signing | **Replace hand-rolled signer** | `aws4fetch` | ~2.5 KB gzipped, uses `fetch` + `SubtleCrypto` | Purpose-built for Workers/edge; Cloudflare's own R2 docs recommend it. Smaller and more battle-tested than the bespoke signer; `@smithy/signature-v4` is heavier and drags Node-isms. |
| S3 list XML parsing | **Replace regex parser** | `fast-xml-parser` | ~25 KB min, pure JS, Workers-compatible | Correctly handles entities, repeated `<Contents>`, optional fields, and pagination tokens that the current regex mishandles. `txml` is a smaller alt (kept as fallback if size budget is tight). |
| Reference-engine comparison (CI only) | **Adopt as dev dep** | `@duckdb/node-api` | Node-only, not bundled | DuckDB reads both Parquet and Iceberg; use it to assert LaQL's planned/read rows match a reference engine on the same fixtures. |
| External Iceberg fixtures | **Generate + vendor** | PyIceberg + Spark (PySpark 3.5) via Docker | build-time only | PyIceberg writes equality deletes (Spark cannot); Spark/PySpark writes position deletes, partition/schema evolution. Mirrors DuckDB-iceberg's own `test_data_generator` approach. |
| Bounded concurrency / cancellation | **Native + tiny glue** | `AbortController`/`AbortSignal` (native) + internal semaphore | 0 KB | This is glue, not a protocol — keep it internal. `p-limit` is the drop-in fallback if the semaphore proves fiddly. |
| Metadata validation (future) | **Defer; watch** | `valibot` if needed | tree-shakeable, tiny | Only if hand-rolled JSON validators keep growing. Not part of this plan's required scope. |

Keep delegated (no change): `hyparquet`/`hyparquet-writer` (Parquet), `avsc` (Avro OCF — already
wired into the runtime read path), `h3-js` (H3).

---

## Milestone 1 — Trust the Iceberg Reader

### 1.1 External multi-engine fixtures (`NEEDED.md` §1, §9)
**Gap:** only `apache/parquet-testing` is fetched; no Spark/PyIceberg/Trino/DuckDB Iceberg fixtures.

**Build:**
- Add `fixtures/external/generate-iceberg/` containing a Dockerized generator:
  - `Dockerfile` pinning `pyspark==3.5.*`, `pyiceberg[pyarrow]`, and the Iceberg Spark runtime jar.
  - `generate.py` producing a labeled matrix of warehouses under `fixtures/external/iceberg-reference/<engine>/<case>/`:
    - v1 table (Spark), v2 table (Spark), v2 + **position deletes** (Spark),
      v2 + **equality deletes** (PyIceberg — Spark can't write these),
      partition evolution, schema evolution, snapshot/time-travel history (≥3 snapshots).
  - `manifest.json` per case recording engine, version, expected row count, and a SHA-256 of every file.
- Commit generated outputs (they are small) and a top-level `fixtures/external/CHECKSUMS.txt`.
- Extend `fixtures/src/fetch-external.ts` to also verify checksums of vendored Iceberg fixtures, not just clone `parquet-testing`.

**Acceptance:** `pnpm fixtures:external` validates checksums offline; the conformance lane discovers each `<case>/metadata.json` and loads it through `@laql/iceberg` with no errors except the deliberately-unsupported cases.

### 1.2 Snapshot / time-travel proof against external metadata (§2)
**Build:** in `packages/iceberg/src/conformance.conformance.test.ts`, for each external case with history,
assert `loadIcebergTable` + snapshot selection (`as-of`, snapshot-id) plans the file set that matches
the recorded per-snapshot row counts in `manifest.json`.

**Acceptance:** every external snapshot resolves to the exact expected file/row set.

### 1.3 Strict unsupported-feature rejection — extend coverage (§1)
**Status:** delete-file rejection (`LAQL_UNSUPPORTED_DELETE_FILES`) and `format-version >= 3`
(`LAQL_CATALOG_ERROR`) already exist. **Build remaining detectors:**
- Reject (with specific codes) on read when metadata contains: sort orders/transforms LaQL can't honor,
  unsupported partition transforms, unknown manifest content types, or unknown table-format features.
- Add a `docs/unsupported.md` enumerating each detected-and-rejected feature with its error code.

**Acceptance:** a fixture exercising each unsupported feature throws the documented code; none are silently ignored.

### 1.4 Compatibility matrix (§1, §7)
**Build:** `docs/compatibility.md` — a generated table (Parquet primitives/nested/logical types,
Iceberg v1/v2/v3, manifest lists vs direct refs, position/equality deletes, deletion vectors,
partition/schema evolution, catalog kinds) with cells: ✅ supported+tested / ⚠️ supported / 🚫 detected+rejected.
Generate it from a single `compatibility.json` source of truth so it can't drift; link it near the top of the README.

**Acceptance:** matrix renders from `compatibility.json`; a test asserts every 🚫 row maps to a real rejection test and every ✅ row maps to a passing conformance test.

---

## Milestone 2 — Trust the Parquet Scanner

### 2.1 Expanded Parquet type matrix (§2)
**Build:** add fixtures (generated + from `parquet-testing`) covering int8/16/32/64, unsigned ints,
float/double, decimal (int32/int64/fixed/byte-array backings), date, time, timestamp (millis/micros/nanos,
UTC and local), binary, fixed-len byte arrays, booleans, and null-heavy columns. Decode through `@laql/parquet`
and compare values against DuckDB (see 2.4).

### 2.2 Nested-type posture — decide and document (§2)
**Research note:** `hyparquet` assembles nested **lists** but does **not** assemble **struct** columns
(returns sub-column data). **Build:**
- Decide posture: support lists/maps; for structs, either assemble in `@laql/parquet` glue or **detect and reject**
  via a typed error. Document the decision in `docs/parquet-types.md` and reflect it in the compatibility matrix.

**Acceptance:** nested fixtures either read correctly or reject with a documented code — never silently flatten.

### 2.3 Predicate + row-group pruning verified against reference (§2)
**Build:** parametric predicate suite across nulls, booleans, strings, ints/floats, dates/timestamps, decimals,
and `and`/`or`/`not`/`in`/`between`/`like`. For each, assert (a) LaQL output rows equal DuckDB output rows and
(b) row groups pruned match an expected manifest (so pruning correctness, not just result correctness, is proven).

### 2.4 Reference-engine comparison harness (§2, §9)
**Build:** `packages/<core|parquet>/test/reference/` helper wrapping `@duckdb/node-api`:
`expectMatchesDuckDb(sql, fixturePath, predicate)`. Runs under a `LAQL_REFERENCE=1` lane (Node only).

**Acceptance:** Parquet and Iceberg read paths produce row-for-row identical results to DuckDB on every fixture in the matrix.

### 2.5 Streaming + cancellation tests (§2, §3)
Covered by Milestone 3 resource work; add Parquet-specific streaming assertions (batch boundaries, bounded buffer).

---

## Milestone 3 — Trust the Worker Story

### 3.1 Resource limits — finish the option surface (§3)
**Status:** `QueryBudget` already has `maxBytes`, `maxFiles`, `maxRowsDecoded`, `maxRangeRequests`,
`maxElapsedMs`, `maxBufferedRows`, `maxMemoryBytes`. **Build the two missing controls:**
- `maxConcurrentReads` — a small internal semaphore in `@laql/core` gating all object-store `getRange`/`get`
  calls; thread it through the scan/plan paths in `packages/parquet` and `packages/iceberg`.
- `signal?: AbortSignal` — accept on `planFiles`/`scanBatches`/`scanRows`/`loadTable`; check at every await
  boundary (before each range read, between batches) and reject with `LAQL_ABORTED`. `maxElapsedMs` becomes
  an internal `AbortSignal.timeout` feeding the same path.

**Files:** `packages/core/src/query.ts` (budget + semaphore), `packages/core/src/store.ts` (signal-aware reads),
`packages/{parquet,iceberg}/src/index.ts` (plumb through), `packages/core/src/errors.ts` (`LAQL_ABORTED`).

**Acceptance:** tests prove (a) no more than `maxConcurrentReads` in-flight reads (instrument the store),
(b) aborting mid-scan stops further range reads and rejects with `LAQL_ABORTED`, (c) `maxElapsedMs` triggers the same.

### 3.2 Range reads as default — confirm + document (§3)
**Status:** already default in http/s3/r2 adapters. **Build:** a contract test asserting every adapter issues
`Range` requests for metadata/row-group reads; document memory behavior in `docs/cloudflare-workers.md`
(streaming model, peak-buffer = `maxBufferedRows` × row size, no full-dataset load).

### 3.3 Cloudflare Worker demo (§7)
**Build:** `examples/worker/` — a deployable Worker reading a Parquet file and an Iceberg table from R2,
with `wrangler.toml`, run under `@cloudflare/vitest-pool-workers` in the existing `test:workerd` lane.

### 3.4 Benchmark report (§6)
**Build:** `bench/` harness reporting, per scenario, **bytes fetched, object requests, wall time, rows scanned,
rows returned, peak memory where measurable**. Scenarios: single Parquet cold, single Parquet warm-metadata,
Hive-partitioned, Iceberg v1, Iceberg v2+deletes, many-small-files, large-row-groups+selective-predicate.
Instrument via the object-store adapter (count requests/bytes). Output `bench/REPORT.md`.

**Acceptance:** `pnpm bench` produces a reproducible report; numbers committed as a baseline.

### 3.5 R2 + HTTP recipes (§7)
**Status:** recipe docs exist. **Build:** make them executable — move snippets into `examples/` and assert in
`packages/laql/src/recipes.test.ts` (extend existing) so docs can't rot.

---

## Milestone 4 — Trust the Extension Surface

### 4.1 Unify the engine contract (§4)
**Gap:** naming is fragmented; `planRowGroups` is missing.
**Build a stable surface in `@laql/core` (or a new `@laql/engine` facade) exposing:**
- `loadTable(...)` — re-export wrapping `loadIcebergTable` (+ Parquet-file table).
- `planFiles(table, opts)` — standalone, delegating to the `IcebergTable` method.
- `planRowGroups(file, predicate, opts)` — **new**: surface the Parquet row-group pruning already done internally
  as a first-class, testable function returning the selected row-group indices + bytes.
- `scanBatches(plan, opts)` / `scanRows(plan, opts)` — unified async generators that dispatch to
  `readParquetObjectBatches` / `scanPlannedIcebergRows` under one signature.
- Re-export object-store + catalog adapter interfaces and `LaQLError`.

**Acceptance:** a doc example builds a query end-to-end using only these named exports, with no imports from
package internals. Old entrypoints kept as thin aliases (no breaking change).

### 4.2 Catalog adapter interface + conformance (§5)
**Build:**
- Promote the catalog interface to a documented contract (`load`, `commit`, `listTables`…) with a `docs/catalogs.md`.
- REST catalog conformance lane: spin up a reference REST catalog (Iceberg's `iceberg-rest-fixture` image, or
  Lakekeeper) in CI via Docker; run load/commit/conflict tests against it.
- Stub adapters/notes for Glue and Nessie behind the same interface (interface proven, impls optional).

### 4.3 Commit-boundary + conflict tests (§5)
**Status:** read v1/v2, append v2-only, reject ≥v3 are implemented. **Build:** explicit tests for
optimistic-concurrency conflict (stale base snapshot → `conditionalPut` fails → typed retry/error) and
stale-metadata detection on every supported commit path.

### 4.4 Package READMEs (§7, §9)
**Build:** per-package `README.md` stating exactly what each package owns and its public surface
(several LICENSE/README stubs already exist untracked — fill them in).

---

## S3 hardening (cross-cutting, `NEEDED.md` §8 highest priority)

This is the highest-risk hand-rolled surface and is scheduled first within Milestone 3 work.

**Build:**
1. Replace the bespoke SigV4 signer (`packages/s3/src/index.ts:~150–191`) with `aws4fetch`'s `AwsClient.sign`,
   keeping the `ObjectStore` adapter shape. Preserve path-encoding/traversal guards.
2. Replace the regex `parseListObjectsV2` (`~193–229`) with `fast-xml-parser`, handling continuation tokens,
   `IsTruncated`, missing optional fields, and entity decoding.
3. Add the **AWS SigV4 test-suite vectors** (`aws-sig-v4-test-suite`, Apache-2.0) as `packages/s3/test/sigv4-vectors/`
   and assert canonical-request + signature match — this guards correctness regardless of whether we keep
   `aws4fetch` or revert.
4. Add an **S3-compatible provider test** lane: run against MinIO in Docker (CI) for real list/get/range/put.

**Acceptance:** all AWS test vectors pass; MinIO round-trips (get, getRange, list with >1000 keys/pagination, put,
conditional put) pass; bundle size delta documented.

---

## SQL parser (§8, lower priority)

**Status:** hand-rolled but deliberately tiny/bounded, documented (`docs/sql-dialect.md`), CLI-only (not exported
from `laql`). **No replacement now.** Action: add a guard test asserting the parser stays within its documented
subset (rejects joins/subqueries/CTEs with a clear error), and a tripwire note: if SQL becomes public-facing or the
subset grows, adopt `node-sql-parser`/`pgsql-ast-parser` or a generated grammar.

---

## CI / Release quality (§9)

**Status:** CI runs lint, typecheck, build, fixture-determinism, coverage (90%), workerd, and the conformance
*job* — but the conformance job runs over an **empty** `fixtures/external/`.

**Build (extend `.github/workflows/ci.yml`):**
- New `external` job: restore vendored external fixtures (cache), run the conformance lane so it actually exercises
  real-engine output (Milestone 1).
- New `reference` job: install `@duckdb/node-api`, run the `LAQL_REFERENCE=1` comparison lane (Milestone 2).
- New `providers` job: MinIO + Iceberg REST fixture services, run S3 and catalog conformance (S3 hardening, 4.2).
- A separate `fixtures-regen` workflow (manual/scheduled) rebuilds the Dockerized PyIceberg/Spark fixtures and
  opens a PR if checksums change — keeps generation reproducible without putting Docker on the hot PR path.
- Changelog discipline: enforce a Changeset on PRs touching `packages/**` (changeset bot / CI check).

**Acceptance:** PR CI proves correctness against real external engines, not just self-generated fixtures.

---

## Sequencing & exit criteria

1. **S3 hardening** (unblocks trustworthy object reads; highest risk).
2. **M1 Iceberg fixtures + matrix + rejection** (the core trust promise).
3. **M2 Parquet matrix + reference harness** (correctness proof).
4. **M3 resource limits + worker demo + benchmarks** (the edge promise).
5. **M4 contract unification + catalog conformance + package docs** (extension surface).

**Done = the `NEEDED.md` "90% Love It" statements are each backed by a passing CI lane:**
small enough for the edge (M3 benchmarks), reads normal Parquet + simple Iceberg correctly (M1/M2 reference lanes),
tells you exactly why when it can't (rejection tests + `docs/unsupported.md` + matrix), no surprise memory/network
(resource-limit + benchmark lanes), buildable-on without internals (M4 unified contract example).
