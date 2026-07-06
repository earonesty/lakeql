# Performance

Lakeql keeps reads bounded by batches and row groups. Projection is derived from `select`, `where`, and `orderBy`, so scans only request needed physical columns where the Parquet adapter can do so.

Use `explain()` to inspect planned files and projected columns:

```ts
const explanation = await lake.path("sales.parquet").select(["amount"]).explain();
console.log(explanation.text);
console.log(explanation.json.predicatePlan);
```

The JSON predicate plan classifies pruning candidates as partition, file stats, row-group stats, or residual predicates.
For window queries, `explain()` also reports the number of window expressions,
the number of shared window sort groups, whether `QUALIFY` is present, and the
execution path used by the query API.

Parquet row-group pruning currently uses footer min/max statistics. hyparquet 1.26.0
does not expose a public dictionary-filter API; dictionary pruning should be added
when that API becomes available without breaking row-group skip accounting.

Budgets can cap files, bytes, decoded rows, returned rows, range requests, elapsed time, buffered rows, and serialized operator memory.

## Work-unit fan-out

Parquet task manifests can be split into row-group-sized work units and reduced
with bounded fan-in. This keeps the deployment boundary data-only: the same JSON
work-unit payload can run in a browser, Cloudflare Worker, Supabase Edge function,
or Node process, then merge vector aggregate partials in task order.

Window fan-out uses the same portable boundary for partitioned window work:
`windowParquetTask()` projects each row group into JSON work-unit rows,
`windowParquetTasks()` repartitions rows by the evaluated window `PARTITION BY`
keys, evaluates each bucket with the core window backend, and restores source
order from the transported row ordinal. Normal Parquet window queries select this
topology automatically when every window in the query has the same non-empty
`PARTITION BY`; `explain().json.windowPlan` reports the selected execution path,
bucket count, and unavailable reason for global or incompatible window specs.
Call the explicit helpers when a deployment needs to choose bucket count,
concurrency, or transport boundaries directly.

Vector aggregate optimizations must preserve that boundary. "Single-pass"
aggregate work means updating several aggregate states while scanning one decoded
vector batch inside an already-bounded work unit; it does not mean collapsing
work units, removing queue/yield points, or replacing portable partial-state
fan-in.

The regression proof for the explicit 10M-row claim is:

```sh
pnpm bench:workunits:10m
```

That benchmark fails if planning or fan-out performs full-object reads, if the
planner selects more than the expected row groups/work units, if fan-out exceeds
its range/byte/decoded-row guards, or if the active fan-out wave exceeds the
configured row budget. It also fails if aggregate fan-out stops using warm
metadata-cache hits for planned work units.

The same report includes a numeric fan-out lane over the same transported work
units with only `count(*)`, `sum(metric)`, and `max(metric)`. Use it to separate
numeric vector aggregate cost from the full lane's string `count_distinct(bucket)`
cost before adding new fast paths.

It also includes a distinct-only lane for `count_distinct(bucket)` over the same
work units. Use the numeric and distinct lanes together to decide whether the
remaining gap is local range-read overhead, Parquet decode/assembly, or distinct
hashing before adding another specialized execution path.

High-cardinality exact distinct uses sorted partial runs at the portable
work-unit boundary once a batch/sample is large and distinct enough. Low-cardinality
partials still use the cheaper local `Set` path. This keeps partials JSON-only
while avoiding a second full round of JS `Set.add` during fan-in. The current
profile shape after that change is:

- high-cardinality local UTF-8 distinct run construction inside each work unit;
- Parquet string decode/assembly;
- sorted-run final count/snapshot merge;
- JSON boundary serialization for portable partials.

The old dominant path, fan-in insertion of every partial key into one JS `Set`,
should no longer dominate high-cardinality profiles. If it does, inspect whether
the partials are below the sorted-run threshold or a budgeted query forced the
checked `Set` path.

The default 10M work-unit dataset uses low-cardinality bucket strings. To avoid
overfitting optimizations to repeated values, run:

```sh
pnpm bench:workunits:10m:high-cardinality
```

That variant sets `LAKEQL_WORKUNIT_BUCKET_CARDINALITY=100000`, so the selected
tail rows exercise high-cardinality distinct state and fan-in behavior. Add the
`:duckdb` suffix for the same native DuckDB lane comparison.

Do not run work-unit benchmarks with different dataset configurations in
parallel: they share `bench/generated/workunits`, and a config change regenerates
that fixture.

Pass a shared `planningCache` when repeated queries can reuse object expansion
for a source pattern. Object-store glob listing is not an atomic snapshot, so the
cache is explicit runtime policy: uncached planning sees current store state,
while cached planning trades freshness for a stable expansion during the cache
window. The 10M work-unit benchmark uses this cache to prove warm planning avoids
re-listing and re-heading the same 100 files before splitting row-group work
units.

Pass a shared Parquet `metadataCache` when repeated planning or fan-out execution
can reuse footer statistics. The task payloads remain data-only JSON, but warm
planning can avoid footer range reads entirely, and aggregate fan-out can reuse
the same cached metadata instead of rereading footers for each row-group work
unit. Scan work-unit runners can also attach the same runtime-local cache without
serializing it into the work-unit envelope.

Cold Parquet metadata planning uses a bounded 64 KiB initial footer range read per
file instead of the upstream decoder's larger default tail read. The 10M
work-unit benchmark guards cold planning at 8 MiB across 100 files and guards
warm planning at zero object reads.

For local wall-time context, `pnpm bench:workunits:10m:duckdb` adds a comparison
against Node DuckDB's native engine. The report times DuckDB's matching full
aggregate, numeric-only aggregate, and distinct-only aggregate separately, so the
lakeql ratios identify whether the gap is broad fan-out overhead, numeric vector
work, or distinct hashing. Treat Node DuckDB as a strong local CPU baseline, not
the DuckDB-WASM product comparison: browser WASM needs a separate lane for
download/compile/init time, HTTP range behavior, warm queries, and browser memory.

For bounded top-k sorts, use `orderBy(...).limit(...).topKWithState()` to
serialize or spill the retained heap state between slices. For full global sorts,
`sortWithState()` builds sorted runs capped by `maxBufferedRows`, merges those runs
for output, and can persist each run plus the compact run manifest through a
`SpillAdapter`:

```ts
const result = await lake
  .path("events.parquet")
  .orderBy([{ column: "created_at" }])
  .sortWithState({ spill, spillId: "sort-events" });
```

Plain `orderBy().toArray()` remains available for small direct reads.
