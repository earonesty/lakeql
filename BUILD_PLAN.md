# LaQL build plan

Companion to [README.md](README.md) (the product spec). The spec says *what*; this says *in
what order, with what tests, against what fixtures*. The very-high-level phase list in the
spec is expanded here into buildable milestones.

---

## Ground rules

```txt
runtime targets   Node >= 22, Cloudflare Workers (workerd), browsers, Deno, Bun.
                  core never imports a runtime API (see spec: Runtime model).

language          TypeScript strict, ESM-only, isolatedModules, verbatimModuleSyntax.
                  noExplicitAny is a lint ERROR. No casts to silence the checker —
                  fix the types or expand the fixture.

package manager   pnpm workspaces. workspace:* for internal deps.

lint/format       biome (single tool for both). `pnpm lint` must be clean.

tests             vitest. v8 coverage. 90% gate on lines, branches,
                  functions, statements — enforced in CI, not aspirational.

builds            tsc -b with project references. Declarations + sourcemaps shipped.

errors            every failure path throws LaQLError with a spec'd LAQL_* code.
                  New failure modes get a new code in @laql/core, not a bare Error.
```

Definition of done for any feature:

```txt
1. fixture exists that exercises it (generated, committed, reproducible)
2. unit tests + fixture tests pass
3. coverage gate still green
4. biome clean, tsc -b clean
5. error paths throw typed LaQLError codes, each with a test
6. spec (README.md) updated if behavior diverged
```

---

## Repo layout

```txt
packages/core      AST, expression builders, planner, evaluator, engine, errors, types
packages/parquet   hyparquet reader/writer adapters, pruning, projection
packages/iceberg   icebird/iceberg-js adapters, snapshots, manifests, commits
packages/http      HTTP range-read ObjectStore
packages/r2        Cloudflare R2 ObjectStore
packages/s3        S3-compatible ObjectStore
packages/geo       h3_* and st_* functions (h3-js, @turf) — optional at runtime
packages/sql       SQL dialect -> AST compiler
packages/cli       laql command line
packages/laql      umbrella: batteries-included entry + runtime driver subpaths
fixtures/          @laql/fixtures: deterministic generators + committed data
```

`@lakeql/react` from the spec is deferred until the HTTP server mode exists; it is not
scaffolded.

---

## Fixture strategy

Fixtures are **generated, committed, and reproducible**:

```txt
generated    fixtures/src/generate.ts produces every file in fixtures/data/
             deterministically — no clocks, no RNG. Same code -> same bytes.

committed    fixtures/data/ is in git. Tests never generate data on the fly;
             they read committed bytes, so a test failure is a code change,
             not a generator drift.

reproducible CI regenerates and `git diff --exit-code`s fixtures/data.
             Generator changes therefore show up as reviewable data diffs.

external     conformance inputs we don't own (apache/parquet-testing files,
             Iceberg reference warehouses) are fetched into fixtures/external/
             (gitignored) by a fetch script; CI caches them. Used by the
             conformance suite only — the main suite never depends on the network.
```

Fixture inventory grows with the phases:

```txt
phase 1   sales.parquet   (multi row group, string/double columns)        [done]
          types.parquet   (int32/int64-past-2^53/bool/nullable/double)    [done]
phase 2   stats.parquet           (row groups with disjoint min/max ranges)
          hive/ directory layout  (date=*/country=* partitions, small files)
          wide.parquet            (30+ columns, for projection assertions)
phase 3   iceberg-v2 warehouse    (metadata.json chain, manifest list, manifests,
                                   2 snapshots, schema evolution: add/rename/drop)
          iceberg-deletes         (position delete files; later: deletion vectors,
                                   equality deletes)
phase 5   groupby.parquet         (known group cardinalities incl. a >maxGroups case)
          bookmark replay logs    (golden bookmark JSON at fixed positions)
phase 6   write golden files      (expected parquet output bytes for fixed input)
phase 7   geo.parquet             (GeoJSON column + bbox columns)
          h3.parquet              (h3_7/h3_8 columns aligned with partition layout)
```

## Test taxonomy

```txt
unit          pure logic: builders, planner phases, predicate classification,
              expression eval. Live next to source (src/*.test.ts).

fixture       read/process committed fixtures end-to-end through public APIs.
              The parquet round-trip suite is the template.

conformance   external files (parquet-testing, Iceberg reference tables) decoded
              and compared against expected JSON. Separate vitest project tag;
              runs in CI nightly + on demand, not on every push.

runtime       same fixture suite executed in workerd via
              @cloudflare/vitest-pool-workers (phase 4+). The runtime-agnostic
              claim is CI-enforced, not aspirational. Browser/Deno/Bun smoke
              jobs follow once workerd is green.

property      fast-check generators for expression eval (eval(ast) == eval
              (normalize(ast))), bookmark round-trips, and SQL->AST->SQL echo.
              Added per-phase where invariants are crisp.

benchmark     vitest bench over fixture scans (bytes requested, range request
              count, wall time). Tracked from phase 2 so pruning regressions
              are visible; not a CI gate initially.
```

## Coverage policy

```txt
gate          90% lines / branches / functions / statements, repo-wide,
              enforced by `pnpm coverage` in CI.

what counts   packages/*/src/**, excluding *.test.ts and bin entries.

no gaming     placeholder modules stay tiny so they don't pad the denominator.
              Coverage exclusions require a comment saying why.
```

---

## Phases

Phases ship in order; each leaves `main` releasable. Research items are listed with the
phase that needs them — resolve them at phase start, not before.

### Phase 0 — scaffold  [done]

Monorepo, biome, vitest + 90% gate, tsc -b project references, CI, fixtures package
generating real Parquet via hyparquet-writer, core error model + expression builders +
ObjectStore contract + MemoryObjectStore, parquet read adapter over ObjectStore with
fixture round-trip tests.

### Phase 1 — core read path

Scope: scan/filter/project/limit over plain Parquet paths, streaming-first.

```txt
deliverables
  - expression evaluator over rows (all spec operators; scalar functions:
    string, numeric, date families)
  - logical plan: from/select/where/limit/offset as AST -> operator pipeline
  - AsyncIterable<Row> execution; rows() / toArray() / first() / count()
  - streamNdjson() / streamJson() as ReadableStream<Uint8Array>
  - lake.path("...*.parquet") with glob over ObjectStore.list
  - JSON query API v1 parse + validate (version field, typed errors)
  - JS value mapping per spec (int64 bigint; JSON output safe-number-or-string)

fixtures   wide.parquet; reuse sales/types
tests      unit (evaluator: every operator x null handling), fixture (end-to-end
           queries with expected row sets), property (evaluator vs naive
           reference impl on random rows)
research   none — all mechanisms verified in phase 0
exit       query a multi-file glob from MemoryObjectStore and stream NDJSON;
           every spec operator evaluated with SQL three-valued null semantics
```

### Phase 2 — pruning

Scope: the spec's reason to exist — skip files, row groups, columns.

```txt
deliverables
  - predicate split + classification (PredicatePlan: partition / fileStats /
    rowGroupStats / residual)
  - parquet row-group stats pruning + dictionary filtering where available
  - column projection driven by select+where analysis
  - hive partition discovery (lake.hive) + partition pruning
  - explain(): text + JSON with skipped/planned counts per spec
  - QueryStats wired through every read

fixtures   stats.parquet, hive/ layout
tests      unit (classifier: each predicate shape -> expected class), fixture
           (assert filesSkipped/rowGroupsSkipped exact numbers — pruning tests
           must count, not just pass), benchmark baseline
research   hyparquet dictionary-filter API surface
exit       a selective query over hive/ reads strictly fewer bytes than a full
           scan, and explain() proves it with exact counts
```

### Phase 3 — Iceberg reads

```txt
deliverables
  - icebird adapter: metadata.json chain, snapshot by id/timestamp/branch/tag
  - manifest list + manifest pruning using partition predicates
  - schema evolution mapping (field-id based projection)
  - delete handling, phased easy-first per spec: position deletes ->
    deletion vectors -> equality deletes; strict mode throws
    LAQL_UNSUPPORTED_DELETE_FILES for whatever isn't supported yet
  - readMode: strict | ignore-deletes | ignore-unsupported-deletes
  - static metadata.json + R2/S3-layout catalogs; iceberg-rest via iceberg-js

fixtures   iceberg-v2 warehouse (generated: avsc manifests + metadata chain),
           iceberg-deletes; external Iceberg reference tables in conformance
tests      fixture (snapshot pinning: same query, two snapshots, different
           rows), conformance, unit (manifest pruning math)
research   icebird equality-delete coverage (flagged in spec); manifest
           generation via avsc for our own fixtures
exit       time-travel query against the fixture warehouse with position
           deletes applied and manifests pruned (counted in explain)
```

### Phase 4 — runtime drivers + Worker ergonomics

```txt
deliverables
  - @laql/http httpStore (Range requests, etag capture)
  - @laql/r2 r2Store; @laql/s3 s3Store (SigV4)
  - createLake config surface; budgets (LAQL_BUDGET_EXCEEDED with the spec's
    actionable message); policy layer (columns/limits/rowFilter/context)
  - laql/cloudflare, laql/node driver subpaths in the umbrella package
  - workerd test lane: fixture suite green under vitest-pool-workers
  - cache adapters: memoryCache + cacheApiCache; footer/metadata caches

fixtures   reuse all; add a policy fixture config
tests      runtime matrix lane goes live; unit (SigV4 vectors, Range header
           edges); budget/policy fixture tests
research   pin the non-Iceberg consistency story (etag-pinned plans) — spec
           gap flagged earlier; decide before bookmarks ship in phase 5
exit       same fixture suite passes on Node and workerd; an R2-backed Worker
           example streams NDJSON under a budget
```

### Phase 5 — aggregation, sort, bookmarks

```txt
deliverables
  - group by + aggregates (count/sum/avg/min/max/count_distinct/first/last/any)
    with maxGroups -> LAQL_GROUP_LIMIT_EXCEEDED
  - top-k order by; order-by-after-limit; spill adapter interface (impl later)
  - slice API: run({ slice }) -> SliceResult with bookmark
  - bookmark serialization: position-only first, then serialized operator
    state (group hash table, top-k heap); plan fingerprint + LAQL_BOOKMARK_STALE
  - resumableBatches({ bookmarkEvery }); lake.resume(bookmark)
  - HMAC-signed pagination tokens (LAQL_BOOKMARK_INVALID on forgery)

fixtures   groupby.parquet, golden bookmark JSON
tests      property (run-to-completion == run-sliced-and-resumed, any slice
           boundary — THE invariant of the product), unit (fingerprint
           stability), fixture (kill/resume at every row-group edge)
research   plan-compat fingerprint vs engine version (decision flagged in
           spec discussion); operator-state format versioning
exit       a query sliced at arbitrary points yields byte-identical output to
           an unsliced run, across simulated process restarts
```

### Phase 6 — writes

```txt
deliverables
  - hyparquet-writer adapter: writeParquet with schema/compression/rowGroupSize
    (research: zstd = pluggable compressor; snappy default)
  - partitioned directory writes (hive layout, maxRows/maxBytes per file)
  - Iceberg append commit: data file metadata, avsc manifests, commit via
    iceberg-js requirements/updates (we own the commit protocol logic)
  - commit conflict retry -> LAQL_ICEBERG_COMMIT_CONFLICT; DO coordinator recipe
  - insert validation (required/unique/ranges/enum -> LAQL_VALIDATION_ERROR)
  - resumable writes: multipart state in bookmarks; CTAS as slice chain ending
    in one commit

fixtures   write golden files; round-trip (write -> read back through phase 1-3)
tests      golden-byte comparisons, round-trips, commit-conflict simulation
           against a fake REST catalog
research   iceberg-js updateTable requirements semantics against a real
           catalog (conformance lane)
exit       append to the fixture warehouse, read it back with time travel, and
           survive a mid-write resume
```

### Phase 7 — geo/H3, SQL, indexes, CLI, joins

Additive tracks, parallelizable, each gated the same way:

```txt
geo/h3    h3_* + st_* functions; h3_within -> partition pruning rewrite;
          bbox-column pushdown (geo.parquet, h3.parquet fixtures)
sql       dialect -> AST; every README SQL example becomes a parser test;
          property: AST -> SQL -> AST round-trip
indexes   sidecar formats (minmax/bloom/h3/bbox) + planner integration
cli       wire commands to real engine; snapshot-test CLI output
joins     broadcast/lookup with maxRightRows; planner rejects unsafe joins
docs      docs/ tree + recipes from the spec, each recipe runnable against
          fixtures
```

---

## CI

Single workflow (`.github/workflows/ci.yml`), every push/PR:

```txt
pnpm install --frozen-lockfile
biome check            (lint + format, zero tolerance)
tsc -b                 (typecheck + build)
fixture reproducibility (regenerate, git diff --exit-code)
vitest coverage        (90% gate fails the build)
```

Phase 4 adds the workerd lane; phase 3 adds the nightly conformance lane.
Release flow when packages are ready to publish: changesets + npm provenance
(`--provenance`), placeholders published immediately to hold the names.
