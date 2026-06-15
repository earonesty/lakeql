# LaQL build plan

Companion to [VISION.md](VISION.md) (the product spec). The spec says *what*; this says *in
what order, with what tests, against what fixtures*. This plan turns the responsibility
boundary in the spec into buildable milestones: caller intent and infrastructure go in;
bounded deterministic lake work comes out.

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
6. spec (VISION.md) updated if behavior diverged
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

`@laql/react` from the spec is deferred until the HTTP server mode exists; it is not
scaffolded.

---

## Scope boundary

The library owns mechanics. Milestones should move responsibility into `laql` only when it
is generic lake-query machinery:

```txt
metadata reads, snapshot/manifest/file/row-group planning
bounded task construction
predicate/projection execution
row and batch streaming
Parquet output file writing
bookmark/checkpoint generation
limit enforcement
retry-safe task state transitions
task/output manifest aggregation
Iceberg commit protocol mechanics
```

The caller owns intent, policy, and infrastructure:

```txt
source/table selection, credentials, snapshot/release choice
selected fields, predicates, transforms, output schema, partitioning strategy
budgets, stale timeout, retry limit, concurrency
object store, queue, state/checkpoint store, lock/commit adapter
destination prefix, manifest format, append/overwrite/promotion rules
job id, idempotency key, cancellation/requeue/completion behavior
progress, metrics, logs, audit and error hooks
```

Tests should reflect the boundary: fixtures provide caller intent and fake substrates;
assertions verify that `laql` turns them into deterministic bounded work without baking in
domain-specific policy.

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
          iceberg-deletes         (position deletes, deletion vectors,
                                   equality deletes)
phase 5   task-manifest golden JSON
          output-manifest golden JSON
          retry replay logs       (same task retried at each transition)
phase 6   groupby.parquet         (known group cardinalities incl. a >maxGroups case)
          bookmark replay logs    (golden bookmark JSON at fixed positions)
phase 7   write golden files      (expected parquet output bytes for fixed input)
phase 8   geo.parquet             (GeoJSON column + bbox columns)
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
              (normalize(ast))), bookmark round-trips, deterministic task
              manifests, retry replay idempotence, and SQL->AST->SQL echo.
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

## Execution queue

The end goal is still **finish every phase in this file**. Do not treat that as one
unbounded work item. Work through the queue below in order, committing and pushing each
verified slice before starting the next slice.

Slices may be large when the code naturally moves together. A slice is acceptable when it
has all of these properties:

```txt
1. its scope is named before editing starts
2. its exit check is concrete enough to run in this repo
3. it leaves the branch releasable
4. `pnpm check` passes
5. the verified diff is committed and pushed
```

If a slice cannot pass `pnpm check`, either fix it in the same slice or revert only that
slice's own changes before choosing a smaller next slice. Do not mark the overall goal
blocked just because later queue items remain.

If the current queue item has multiple remaining tracks, consume them left-to-right as
written unless a later track is a strict prerequisite for an earlier one. "All phases" is
the goal; "current queue item, next listed track, largest verifiable slice" is the work
selection rule.

The goal is blocked only when the next listed track cannot be advanced after the failing
condition has been reproduced, the repo state has been inspected, and no smaller
verifiable slice inside that same track can move forward. Missing later-phase work is not
a blocker; it is just the next queue item.

### Queue order

```txt
Q0  plan hygiene
    - keep this queue current after each substantial commit
    - move newly discovered requirements into the earliest dependent queue item

Q1  phase 1/2 closure: plain Parquet reads and pruning
    - audit current read/pruning implementation against phase 1 and phase 2 exits
    - add any missing fixture/golden coverage for row-group streaming, task inputs,
      projection+where column reads, and exact explain counts
    - exit: selective hive query proves fewer bytes/ranges than full scan, and stable
      task input goldens cover the same query twice

Q2  phase 3 closure: Iceberg reads
    - finish metadata-chain and manifest-reference behavior
    - add manifest-list/manifest pruning counters if missing from explain/plans
    - lock deterministic data-file/task order with goldens
    - add conformance harness shape for external Iceberg reference warehouses
    - exit: time-travel fixture query applies deletes, prunes manifests, and produces
      stable planned files and row groups

Q3  phase 4 closure: runtime drivers and Worker ergonomics
    - audit HTTP/R2/S3 stores, Range and etag behavior, cache adapters, policy, budgets,
      substrate hooks, and umbrella runtime subpaths
    - expand workerd fixture lane where needed
    - landed: paginated R2/S3 listings; R2-backed Worker NDJSON streaming under budget;
      core query clock/id/metrics substrate hooks; Worker queue/checkpoint substrate
      bookmark handoff coverage; etag-keyed Parquet footer cache invalidation; workerd
      metadata cache reuse coverage
    - remaining: none known after audit; advance to Q4 unless a final Q3 proof gap is found
    - exit: Node and workerd fixture lanes pass with caller-provided queue/checkpoint
      substrate and budget/policy coverage

Q4  phase 5 closure: task manifests, bookmarks, queue-safe retry
    - complete deterministic task and output manifest formats
    - complete bookmark validation/signing/resume flows for reads and write tasks
    - complete retry-state machine coverage across every transition
    - landed: deterministic task/output manifest goldens; signed read/write bookmarks;
      slice/resume run-to-completion invariants; checkpoint replay idempotence across
      every queue-visible task transition
    - remaining: none known after audit; advance to Q5 unless a final Q4 proof gap is found
    - exit: sliced/resumed execution is byte-identical to unsliced execution, and every
      replayed task transition produces one logical completion

Q5  phase 6 closure: aggregation, sort, bounded memory operators
    - finish aggregate operators and maxGroups failure behavior
    - finish resumable top-k/sort state and spill-backed global operators
    - finish memory/spill/output-row budget enforcement
    - landed: aggregate output-row budgets apply to finished groups instead of input
      rows; grouped aggregate scans request predicate, group, and aggregate columns;
      aggregate/top-k/sort resume from serialized bytes and spill refs; operator-level
      spill budget typed failures
    - remaining: none known after audit; advance to Q6 unless a final Q5 proof gap is found
    - exit: aggregation and bounded sort resume across process restarts without exceeding
      configured budgets except through typed failures

Q6  phase 7 closure: writes
    - finish Parquet write options, deterministic output planning, partition policies,
      output-manifest fan-in, Iceberg append commit behavior, validation, and CTAS chain
    - add fake REST conflict/retry coverage and real-catalog conformance lane shape
    - landed: direct and partitioned Parquet write round-trips/goldens; deterministic
      task/idempotency output paths; output-manifest fan-in; CTAS through checkpoints;
      Iceberg append and REST conflict coverage; partitioned write tasks resume from
      running, output-written, manifest-recorded, and complete checkpoints; retry proof
      leaves one physical output file set and one checkpoint fan-in entry set; Iceberg append
      time-travel readback scans real appended Parquet rows
    - remaining: none known after audit; advance to Q7 unless a final Q6 proof gap is found
    - exit: append to fixture warehouse, read back through time travel, survive mid-write
      resume, and prove retries create one logical output manifest entry/file set

Q7  phase 8 closure: additive tracks
    - finish geo/H3 pushdown, SQL examples and round-trips, sidecar indexes, CLI snapshots,
      joins, and runnable docs/recipes
    - landed: VISION read, geospatial bbox, and nested H3 SQL examples parse and round-trip;
      CLI help/explain/inspect/schema outputs have inline snapshots; sidecar indexes prune
      object planning before scan/explain; geo/H3 fixture row-group pushdown is proven
      for bbox, h3_in, and exact-radius h3_within predicates
    - remaining order:
      1. audit join coverage
      2. make docs/recipes runnable against fixtures
    - exit: every VISION SQL example parses/runs where applicable, CLI snapshots are stable,
      index planning is covered, and docs recipes run against fixtures

Q8  final release hardening
    - update BUILD_PLAN phase status markers
    - run full local gate plus conformance lanes available in the repo
    - verify package exports, docs, and examples
    - exit: no known unchecked phase deliverables remain
```

### Selecting the next slice

For each work session:

```txt
1. run `git status --short`
2. read the first queue item whose exit is not satisfied
3. choose the largest coherent slice inside that queue item that can be verified now
4. state the slice and exit check
5. implement, test with `pnpm check`, commit, push
6. repeat from step 1
```

If the current queue item appears complete, prove it with tests or docs before advancing.
If proof is missing, the proof is the next slice.

## Phases

Phases ship in order; each leaves `main` releasable. Research items are listed with the
phase that needs them — resolve them at phase start, not before.

### Phase 0 — scaffold  [done]

Monorepo, biome, vitest + 90% gate, tsc -b project references, CI, fixtures package
generating real Parquet via hyparquet-writer, core error model + expression builders +
ObjectStore contract + MemoryObjectStore, parquet read adapter over ObjectStore with
fixture round-trip tests.

### Phase 1 — core read path

Scope: scan/filter/project/limit over plain Parquet paths, streaming-first and
row-group-aware.

```txt
deliverables
  - expression evaluator over rows (all spec operators; scalar functions:
    string, numeric, date families)
  - logical plan: from/select/where/limit/offset as AST -> operator pipeline
  - parquet row-group streaming; batches are bounded by row group and caller
    batch size, never whole-file materialization
  - AsyncIterable<Row> execution; rows() / batches() / toArray() / first() / count()
  - streamNdjson() / streamJson() as ReadableStream<Uint8Array>
  - lake.path("...*.parquet") with glob over ObjectStore.list
  - JSON query API v1 parse + validate (version field, typed errors)
  - JS value mapping per spec (int64 bigint; JSON output safe-number-or-string)
  - per-query budget checks for rows decoded, bytes read, range requests, and
    elapsed runtime where the engine can observe them

fixtures   wide.parquet; reuse sales/types
tests      unit (evaluator: every operator x null handling), fixture (end-to-end
           queries with expected row sets), row-group streaming fixture tests
           that prove peak buffered rows stay bounded, property (evaluator vs
           naive reference impl on random rows)
research   none — all mechanisms verified in phase 0
exit       query a multi-file glob from MemoryObjectStore and stream NDJSON;
           every spec operator evaluated with SQL three-valued null semantics;
           no public read path requires holding a full Parquet file in memory
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
  - task input model for bounded plain-Parquet scans:
    path, etag/version, row-group ranges, projected columns, residual predicate

fixtures   stats.parquet, hive/ layout
tests      unit (classifier: each predicate shape -> expected class), fixture
           (assert filesSkipped/rowGroupsSkipped exact numbers — pruning tests
           must count, not just pass), task input golden tests, benchmark baseline
research   hyparquet dictionary-filter API surface
exit       a selective query over hive/ reads strictly fewer bytes than a full
           scan, explain() proves it with exact counts, and planned task inputs
           are stable for the same object versions and query
```

### Phase 3 — Iceberg reads

```txt
deliverables
  - icebird adapter: metadata.json chain, snapshot by id/timestamp/branch/tag
  - manifest list + manifest pruning using partition predicates
  - deterministic data-file planning from Iceberg manifests, including file
    sequence/order, delete-file association, partition values, and stats
  - schema evolution mapping (field-id based projection)
  - delete handling per spec: position deletes, deletion vectors, equality
    deletes; strict mode throws LAQL_UNSUPPORTED_DELETE_FILES for unknown delete
    formats
  - readMode: strict | ignore-deletes | ignore-unsupported-deletes
  - static metadata.json + R2/S3-layout catalogs; iceberg-rest via iceberg-js

fixtures   iceberg-v2 warehouse (generated: avsc manifests + metadata chain),
           iceberg-deletes; external Iceberg reference tables in conformance
tests      fixture (snapshot pinning: same query, two snapshots, different
           rows), conformance, unit (manifest pruning math), golden planned
           data-file/task order
research   manifest generation via avsc for our own fixtures
exit       time-travel query against the fixture warehouse with position
           deletes applied and manifests pruned (counted in explain); the same
           snapshot and query yield the same planned files and row groups
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
  - substrate interfaces for caller-supplied queue, checkpoint/state store,
    lock/commit coordinator, clock/id generator, metrics/log hooks

fixtures   reuse all; add a policy fixture config
tests      runtime matrix lane goes live; unit (SigV4 vectors, Range header
           edges); budget/policy fixture tests; fake substrate tests that prove
           core remains runtime-agnostic
research   pin the non-Iceberg consistency story for etag-pinned plain
           Parquet plans before bookmarks ship in phase 5
exit       same fixture suite passes on Node and workerd; an R2-backed Worker
           example streams NDJSON under a budget with caller-provided queue and
           checkpoint adapters
```

### Phase 5 — task manifests, bookmarks, queue-safe retry

```txt
deliverables
  - deterministic TaskManifest:
    job id, plan fingerprint, snapshot/object versions, ordered task list,
    task ids, row-group/file ranges, projected columns, partition values,
    output role
  - deterministic OutputManifest:
    task id, output path, partition values, row count, byte size,
    content hash/etag, Iceberg data file metadata when applicable
  - slice API: run({ slice }) -> SliceResult with bookmark
  - bookmark serialization for position-only scans and write tasks:
    fileIndex, rowGroup, rowOffset, task id, output manifest cursor,
    plan fingerprint + LAQL_BOOKMARK_STALE
  - retry-safe task state machine:
    planned -> running -> output-written -> manifest-recorded -> complete
    with idempotency key checks and stale timeout handling
  - checkpoint adapter contract; in-memory fake for tests
  - queue-safe requeue contract: retry limit, stale detection, cancellation,
    completion/fan-in hooks remain caller policy but engine exposes state
    transitions and typed outcomes
  - resumableBatches({ bookmarkEvery }); lake.resume(bookmark)
  - HMAC-signed pagination tokens (LAQL_BOOKMARK_INVALID on forgery)

fixtures   task-manifest golden JSON, output-manifest golden JSON, retry replay
           logs, golden bookmark JSON
tests      property (run-to-completion == run-sliced-and-resumed, any slice
           boundary — THE invariant of the product), property (same query and
           snapshot -> same task manifest), property (replaying a completed or
           partially completed task is idempotent), unit (fingerprint stability),
           fixture (kill/resume at every row-group and task transition)
research   plan-compat fingerprint vs engine version (decision flagged in
           spec discussion); task/output manifest format versioning
exit       a query sliced at arbitrary points yields byte-identical output to
           an unsliced run, across simulated process restarts; replaying every
           queue-visible transition produces exactly one logical task completion
```

### Phase 6 — aggregation, sort, bounded memory operators

```txt
deliverables
  - group by + aggregates (count/sum/avg/min/max/count_distinct/first/last/any)
    with maxGroups -> LAQL_GROUP_LIMIT_EXCEEDED
  - serialized operator state for group hash table, top-k heap, approximate
    sketches; bookmark/checkpoint format versioning
  - top-k order by; order-by-after-limit; spill adapter interface
  - spill-backed operator contract for large global sort/hash join operators
  - budget enforcement for maxGroups, max memory, spill bytes, output rows

fixtures   groupby.parquet, bookmark replay logs at operator-state boundaries
tests      property (sliced/resumed aggregation == unsliced aggregation), unit
           (operator state serialization), fixture (>maxGroups typed failure),
           memory-budget tests with deterministic fake allocator/counters
research   operator-state serialization format details
exit       aggregations and bounded sorts resume across process restarts and
           never exceed configured group/memory budgets without typed failure
```

### Phase 7 — writes

```txt
deliverables
  - hyparquet-writer adapter: writeParquet with schema/compression/rowGroupSize
    (research: zstd = pluggable compressor; snappy default)
  - partitioned directory writes (hive layout, maxRows/maxBytes per file)
  - deterministic output path planning from job id/idempotency key, task id,
    partition values, file ordinal, and output policy
  - output manifest writer and aggregator; task-level output manifest entries
    feed final commit/fan-in
  - Iceberg append commit: data file metadata, avsc manifests, commit via
    iceberg-js requirements/updates (we own the commit protocol logic)
  - commit conflict retry -> LAQL_ICEBERG_COMMIT_CONFLICT; DO coordinator recipe
  - insert validation (required/unique/ranges/enum -> LAQL_VALIDATION_ERROR)
  - resumable writes: multipart state in bookmarks; CTAS as slice chain ending
    in one commit

fixtures   write golden files; round-trip (write -> read back through phase 1-3)
tests      golden-byte comparisons, round-trips, commit-conflict simulation
           against a fake REST catalog, output-manifest aggregation tests,
           overwrite/append policy fixture tests
research   iceberg-js updateTable requirements semantics against a real
           catalog (conformance lane)
exit       append to the fixture warehouse, read it back with time travel, and
           survive a mid-write resume; retrying any write task produces one
           logical output manifest entry and one committed file set
```

### Phase 8 — geo/H3, SQL, indexes, CLI, joins

Additive tracks, parallelizable, each gated the same way:

```txt
geo/h3    h3_* + st_* functions; h3_within -> partition pruning rewrite;
          bbox-column pushdown (geo.parquet, h3.parquet fixtures)
sql       dialect -> AST; every VISION SQL example becomes a parser test;
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
