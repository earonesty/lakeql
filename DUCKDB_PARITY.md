# DuckDB-WASM Parity TODO

This plan is for the users who land on the browser comparison page, point both
engines at the same public R2/S3/HTTP Parquet data, and ask whether LakeQL is a
credible replacement for DuckDB-WASM in remote lake workloads.

The target user is not primarily uploading local files into a browser sandbox.
They already have data in object storage, or they are building an API/dashboard
over remote Parquet or Iceberg. The adoption bar is:

```txt
Bring your R2/S3/HTTP lake, keep the SQL that matters, avoid the WASM tax.
```

LakeQL should win on startup cost, byte-range behavior, memory ceilings,
spill/bookmark mechanics, Iceberg awareness, and edge runtime fit. The remaining
work is about removing the moments where a DuckDB-WASM user sees the performance
advantage, tries a normal analytical query, and immediately hits an unsupported
syntax or data-shape wall.

## Product Boundary

Do not chase full DuckDB compatibility. DuckDB is a full embedded analytical
database. LakeQL is a lake query engine for bounded remote file work.

The parity work here is intentionally narrower:

- Remote Parquet and Iceberg reads over object storage.
- SQL portability for common analytical queries.
- Correct nested/logical type handling for common lake files.
- Fast unknown-file inspection.
- Browser/Worker cache behavior that makes cold and warm range reads visible.
- No silent wrong answers.

Local file upload, in-memory JS arrays, CSV/JSON playground workflows, and Arrow
interop are useful, and several are already implemented as opt-in packages. They
are not the top differentiators for the R2/S3/HTTP comparison path.

## Non-Negotiables

- Unsupported behavior must be detected and rejected with typed `LakeqlError`
  codes rather than approximated.
- New physical operators and planner rules should be broadly applicable. Avoid
  query-specific branches.
- Operator limits must be expressed as vector-shape or execution capabilities,
  not product-scope shortcuts.
- Keep the remote Parquet bundle lean. Optional dependencies belong behind
  package boundaries or lazy imports.
- Preserve bounded execution: memory, rows, files, row groups, range requests,
  spill, and output limits remain part of the contract.

## Priority 1: SQL Portability For Remote Lake Queries

This is the largest adoption blocker. If users copy ordinary DuckDB analytical
SQL and the parser/compiler rejects it, the performance story stops mattering.

Current state:

- Window functions and `QUALIFY` are supported and tested.
- Basic filtering, projection, grouping, sorting, bounded inner/left/right/full
  equi-join chains, bounded inner/left/right/full non-equality `JOIN ON`
  predicates, and bounded `CROSS JOIN`/comma join forms exist.
- `read_parquet('path')` table-function sources map onto normal LakeQL path
  planning.
- Named Iceberg SQL table bindings can materialize through the unified Iceberg
  engine scan path, with explicit metadata path, snapshot/ref, and read-mode
  options. Source-specific conjunctive predicates are pushed into Iceberg
  planning when the table binding appears once in the SQL AST.
- Scalar subqueries, ordered/limited `IN (select ...)`, uncorrelated `EXISTS`,
  correlated equality `IN`/`EXISTS`, bounded non-equality correlated
  `EXISTS`, nested derived tables, and
  single-binding CTEs used as outer sources, join inputs, or `IN` subquery
  sources compile to existing scalar, semi/anti join, and CTE materialization
  plans. CTE bodies can contain bounded joins, `IN` subquery plans, scalar
  subqueries, and nested single-binding CTEs.
- Searched `CASE`, simple `CASE <expr> WHEN ...`, and nested `CASE` forms are
  supported. Statically obvious mixed literal result types are rejected with a
  typed diagnostic instead of surfacing later as a generic materialization
  failure.
- `GROUP BY` can reference computed projection aliases, with DuckDB reference
  coverage for common aggregate queries.
- Runtime join and `IN` subquery sorting resolve projected output aliases before
  falling back to source columns, so `ORDER BY` keeps DuckDB-style alias
  behavior even on manually planned row paths.
- Inner and cross join chains pre-filter conjunctive `WHERE` predicates when
  all referenced columns prove the predicate belongs to one qualified input
  alias, reducing bounded join build/probe work without changing results.
- Join row paths push source projections down to each input side when selected
  columns, join keys, predicates, ordering, and computed projections can all be
  proven against qualified input aliases.
- Unsupported broad SQL syntax is detected and rejected.
- Recursive CTEs are explicitly rejected until there is a bounded execution
  design.

TODO:

- Broaden source-specific predicate pushdown across repeated Iceberg bindings
  and nested scopes without changing query semantics.
- Revisit outer-join planner optimizations after the bounded execution
  semantics stay covered by DuckDB reference tests.
- Extend join-chain predicate pushdown only where outer-join null-extension
  semantics remain proven.
- Broaden remaining subquery support for common analytical correlation forms
  beyond bounded predicate `EXISTS`.
- Tighten remaining alias resolution edge cases so `HAVING` and `QUALIFY`
  behave the way DuckDB users expect across nested scopes.

Acceptance criteria:

- A representative suite of DuckDB-authored remote Parquet queries compiles and
  matches DuckDB reference results.
- Unsupported SQL emits an error that points to the unsupported construct and,
  when possible, the closest supported rewrite.

## Priority 2: Struct And Nested Data Support

Remote lake files often contain nested records. Rejecting Parquet structs is a
clear signal that LakeQL may not handle a real production lake, even when the
query only touches a subset of fields.

Current state:

- Lists and maps are supported and tested.
- Struct columns are detected and rejected to avoid silent flattening.

TODO:

- Add a first-class struct vector shape to the execution engine.
- Preserve struct values through projection, filtering, grouping keys where
  meaningful, JSON output, and Arrow output where the opt-in Arrow package is
  used.
- Support nested field references in SQL and AST paths, such as
  `payload.user_id` or equivalent quoted-path behavior.
- Push projection into Parquet reads so selecting one nested field does not
  require materializing the entire struct when the underlying reader can avoid it.
- Define equality/order semantics deliberately. Struct ordering should be
  rejected unless there is a stable SQL-compatible comparison contract.
- Keep schema mismatch behavior explicit for multi-file scans with nested fields:
  missing nested fields should produce null only where the schema reconciliation
  contract says that is valid.

Acceptance criteria:

- DuckDB-authored Parquet files with struct columns can be queried for nested
  fields with reference-result coverage.
- Unsupported struct operations fail with typed errors rather than falling back
  to object comparison or lossy flattening.

## Priority 3: Date And Time Function Coverage

Time columns are central to remote analytics. Users will group by day, truncate
to hour, extract month, compare intervals, and format timestamps. Missing
date/time functions make LakeQL feel incomplete even when scans are fast.

Current state:

- Date, time, and timestamp logical decoding is supported and tested.
- Timestamp micros/nanos preserve precision.
- Window `RANGE` over timestamp intervals works.

TODO:

- Add `date_trunc` for timestamp/date values.
- Add `extract` / `date_part` for year, quarter, month, day, hour, minute,
  second, day of week, and epoch-derived fields.
- Add `strftime` or a deliberately scoped formatting function compatible with
  common DuckDB usage.
- Add epoch conversion helpers such as `epoch_ms`, `epoch_us`, and `epoch_ns`
  with clear precision rules.
- Add interval construction and arithmetic helpers where they compose with the
  existing timestamp/interval model.
- Normalize timestamp display across LakeQL and DuckDB-WASM in comparison
  tooling so equal values are visually equal.

Acceptance criteria:

- Common partition/time-rollup queries can be ported from DuckDB without JS-side
  timestamp manipulation.
- Function behavior is covered by DuckDB reference tests for timezone-neutral
  cases and explicit LakeQL tests for precision-preserving timestamp values.

## Priority 4: Unknown Remote File Inspection

DuckDB-WASM is often used as a quick remote data inspector. LakeQL should make it
easy to inspect a public Parquet object before running a full query.

Current state:

- CLI schema/inspect paths exist.
- `DESCRIBE` is partially available through SQL statement parsing and CLI query
  paths.
- The compare page exposes fixed benchmark queries but not a general inspector
  workflow.

TODO:

- Make `DESCRIBE <source>` work consistently in browser, CLI, and API surfaces.
- Add `SUMMARIZE <source>` for per-column count/null/min/max/basic stats where
  the values can be computed from metadata or bounded scans.
- Add SQL `SAMPLE` support with an explicit bounded strategy.
- Add a remote schema preview API that reports file size, row count, row groups,
  columns, logical types, and supported/unsupported feature flags.
- Surface unsupported file features early, before executing a query.
- Expose the same inspection path in the compare page for custom `source`
  values.

Acceptance criteria:

- A user can paste a public R2/S3/HTTP Parquet URL and get schema, row-group, and
  unsupported-feature information without writing code.
- Inspection has clear byte/range metrics so users see LakeQL's remote-read
  behavior directly.

## Priority 5: Multi-File And Schema Evolution Polish

DuckDB users commonly point at directories, globs, and partitioned datasets.
LakeQL has multi-file planning, but this area needs to feel boring and robust.

Current state:

- Multi-file Parquet planning for prefixes/globs exists.
- Hive partition pruning and missing-column null fill are implemented.
- Schema compatibility checks exist.

TODO:

- Expand reference coverage for mixed-schema directories generated by DuckDB and
  Spark.
- Make schema reconciliation diagnostics more actionable: identify the file,
  column, physical type, logical type, and accepted alternatives.
- Ensure partition column typing is predictable across string, numeric, date, and
  timestamp-looking partition values.
- Support nested-field schema evolution once struct support lands.
- Add compare-page or bench coverage for multi-file remote datasets, not only
  single-object fixtures.
- Keep file expansion bounded by explicit limits with clear errors when a glob or
  prefix is too broad.

Acceptance criteria:

- A partitioned remote Parquet dataset can be queried with predictable schema
  behavior and measured range-read metrics.
- Schema mismatches fail before partial execution when they cannot be reconciled.

## Priority 6: Join Depth And Physical Join Operators

Joins matter for analytical lake workloads: facts plus dimensions, event data
plus accounts, geography plus lookup tables. The important gap is not every SQL
join variant; it is reliable multi-table planning with bounded memory.

Current state:

- Bounded two-table inner/left equi-join support exists.

TODO:

- Generalize join planning to N-way joins with explicit physical operators.
- Add a broadcast/hash join capability model so small dimension tables can join
  against larger remote fact scans safely.
- Add planner rules for join ordering based on available file stats, row counts,
  limits, and explicit user budgets.
- Add spill-aware hash table behavior or a typed rejection when join state would
  exceed the declared budget.
- Keep non-equi joins behind a separate physical capability rather than
  overloading the equi-join path.

Acceptance criteria:

- Common star-schema queries can run with multiple dimension tables.
- Query plans report join strategy, estimated/materialized rows, and memory
  budget decisions.

## Priority 7: Remote Cache Story

DuckDB-WASM can feel fast after startup because data and metadata may be reused
inside its runtime. LakeQL's advantage is stronger if users can see cold vs warm
remote range behavior and persistent browser caching.

Current state:

- Shared bounded scan cache exists.
- OPFS cache adapters exist as an opt-in package.
- The compare page reports request and byte counts through the service worker
  proxy.

TODO:

- Add a persistent-cache mode to the compare page using `lakeql-opfs`.
- Separate metrics for object bytes, metadata bytes, decoded page/vector cache
  hits, and service-worker proxy bytes.
- Show cold, warm-memory, and warm-persistent runs distinctly.
- Add cache invalidation based on object identity: URL, size, ETag/Last-Modified
  where available.
- Document a browser/Worker caching recipe for public R2/S3/HTTP Parquet.

Acceptance criteria:

- The compare page can demonstrate that repeated remote queries avoid redundant
  range reads without hiding correctness or stale-object assumptions.

## Priority 8: Function Coverage For Analytical Portability

After SQL shape and nested data, missing functions are the next most common
porting failure. The goal is not a huge standard library; it is the set that
shows up in object-store analytics.

Current state:

- Regex match/replace, common scalar functions, basic aggregates, variance,
  standard deviation, median, continuous quantile, and mode have coverage.
- Discrete quantile aliases and list/struct accessors remain incomplete.

TODO:

- Add list accessors and list transforms needed for nested Parquet values.
- Add struct accessors alongside struct vector support.
- Add `quantile_disc` and DuckDB-compatible aliases where semantics are clear.
- Add histogram-style summaries if they can be implemented with explicit memory
  bounds.
- Add more string normalization helpers commonly used on partition and event
  data.
- Keep every aggregate state budgeted and serializable where it participates in
  work-unit fan-out.

Acceptance criteria:

- Function gaps discovered by the reference query corpus become tracked TODO
  items with tests, not one-off parser exceptions.

## Priority 9: Prepared Query Objects

This is less urgent than SQL/data-shape parity, but it matters for dashboards and
APIs that rerun the same query with different filters.

Current state:

- Positional SQL parameters can be bound through parser APIs.
- Named parameters and reusable prepared objects are not a product-level API.

TODO:

- Add a prepared query object that compiles SQL once and binds values repeatedly.
- Support named parameters with validation.
- Preserve typed literals for timestamps, decimals, binary, and intervals.
- Expose plan diagnostics before execution.
- Ensure prepared plans respect changing budgets and object-store metadata.

Acceptance criteria:

- A dashboard can bind filters without string-building SQL.
- Parameter binding errors are typed and identify the missing or incompatible
  parameter.

## Sequencing

1. SQL table functions and portability fixes that unblock common remote queries.
2. Struct vectors and nested field references.
3. Date/time function coverage.
4. `DESCRIBE`, `SUMMARIZE`, `SAMPLE`, and remote schema preview.
5. Multi-file/schema evolution hardening.
6. N-way joins and explicit join physical operators.
7. Persistent remote cache story in the compare page.
8. Function-library gaps pulled from real DuckDB reference queries.
9. Prepared query objects.

The first four items create the strongest perception shift: LakeQL stops looking
like a fast narrow benchmark and starts looking like a practical remote-lake
replacement for DuckDB-WASM.
