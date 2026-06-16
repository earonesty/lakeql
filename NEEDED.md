# Needed to Reach 90% "Love It"

LaQL should not try to look like a finished warehouse engine by adding surface area first. It should become a small, strict, trustworthy execution core that a nicer DX layer can safely build on.

The target promise:

> A tiny TypeScript execution core for reading Parquet and planning Iceberg tables from object storage in constrained runtimes.

That promise becomes lovable when users believe it will either read correctly or reject precisely.

## 1. Compatibility Before Features

Still needed:

- Add real external fixtures from Spark, PyIceberg, Trino, and DuckDB/Iceberg where possible. Today only `apache/parquet-testing` is fetched (`fixtures/src/fetch-external.ts`); Iceberg reference warehouses must be dropped in manually under `fixtures/external/iceberg-reference/`.
- Keep generated local fixtures, but do not treat them as the compatibility proof.
- Add a public compatibility matrix covering:
  - Parquet primitive types
  - Parquet nested types
  - timestamps, dates, decimals, binary, fixed
  - Iceberg format v1/v2/v3
  - manifest lists and direct manifest refs
  - position deletes, equality deletes, deletion vectors
  - partition evolution
  - schema evolution
  - REST catalog, object-store metadata, and future catalog adapters

Already in place (keep tested, do not regress):

- Unsupported Iceberg delete files are detected and rejected with a specific error (`LAQL_UNSUPPORTED_DELETE_FILES`) in strict read mode (`packages/iceberg/src/index.ts`). Iceberg `format-version` >= 3 is rejected with `LAQL_CATALOG_ERROR`. Extend the same discipline to remaining Parquet/Iceberg features.
- Never silently ignore table metadata that could change query results.

## 2. Read Path Correctness

- Prove snapshot and time-travel planning against real Iceberg metadata.
- Validate partition pruning against evolved schemas and partition specs.
- Validate row-group pruning for type coercions and edge values.
- Expand predicate tests across:
  - nulls
  - booleans
  - strings
  - ints/floats
  - dates/timestamps
  - decimals
  - `and`, `or`, `not`
  - `in`, `between`, `like`
- Add tests that compare planned/read rows against a reference engine for the same fixture data.
- Keep delete support honest:
  - position deletes supported and tested
  - equality deletes supported and tested
  - deletion vectors detected; support only when actually implemented

## 3. Resource Discipline

Already in place (keep tested, do not regress):

- Range reads are the default in every object-store adapter (`getRange` in `packages/http`, `packages/s3`, `packages/r2`).
- Budget options exist in `packages/core/src/query.ts` (`QueryBudget`): `maxBytes`, `maxFiles`, `maxRowsDecoded`, `maxRangeRequests`, `maxElapsedMs`, `maxBufferedRows`, `maxMemoryBytes`.
- Scans stream batches incrementally (`readParquetObjectBatches`, `scanPlannedIcebergRows` are async generators) — no full-dataset load.

Still needed:

- Add a max-concurrent-object-reads option (not implemented today).
- Add a real `AbortSignal` / timeout (only the `maxElapsedMs` budget and stream `.cancel()` exist now).
- Document memory behavior for Cloudflare Workers and similar constrained runtimes.
- Add tests for bounded concurrency and cancellation (current coverage is thin).

## 4. Engine Contract

Stabilize the low-level core around a few durable contracts. Naming is currently fragmented and must be unified:

- `loadTable` — exists as `loadIcebergTable` (`packages/iceberg/src/index.ts`).
- `planFiles` — exists only as a method on the `IcebergTable` class, not a standalone contract.
- `planRowGroups` — does not exist; needs to be added.
- `scanBatches` / `scanRows` — split across packages (`readParquetObjectBatches` in parquet, `scanPlannedIcebergRows` in iceberg); needs a unified naming/contract.
- object-store adapters — exist (`packages/core/src/store.ts`).
- catalog adapters — exist (`IcebergCommitCatalog`, object-store and REST implementations).
- typed error objects — exist (`packages/core/src/errors.ts`, `LaQLError`).

The core should expose enough for other DX layers to build SQL, dashboards, APIs, or reactive views without depending on internal fixture-shaped data.

## 5. Catalog and Object Store Trust

Already in place (keep tested, do not regress):

- Object-store metadata loading is the simplest path (`loadIcebergTableFromObjectStore`).
- A REST catalog implementation exists (`IcebergRestCatalog`); a catalog adapter interface exists (`IcebergCommitCatalog`).
- Commit behavior is already explicit and enforced: reads accept Iceberg v1/v2 (>=v3 rejected with `LAQL_CATALOG_ERROR`); append requires format-version 2 (`LAQL_VALIDATION_ERROR` otherwise).

Still needed:

- Harden REST catalog support with conformance-style tests.
- Generalize the catalog adapter interface for Glue/Nessie/custom catalogs.
- Add conflict and stale-metadata tests for every supported commit path.

## 6. Performance Proof

Add repeatable benchmarks for:

- single Parquet file cold read
- single Parquet file warm metadata read
- Hive-partitioned dataset
- Iceberg v1 table
- Iceberg v2 table with deletes
- many small files
- large row groups with selective predicates

Benchmarks should report:

- bytes fetched
- object requests
- wall time
- peak memory where measurable
- rows scanned
- rows returned

The goal is not to beat DuckDB. The goal is to prove LaQL is small, predictable, and suitable inside edge/serverless limits.

## 7. Developer Experience for the Core

- README starts with the honest scope and a working edge example.
- Add a compatibility matrix near the top.
- Add "unsupported but detected" docs.
- Add recipes for:
  - local Parquet
  - HTTP-hosted Parquet
  - S3
  - R2
  - Iceberg object-store metadata
  - Iceberg REST catalog
  - Cloudflare Worker
- Keep the fluent API pleasant, but do not let DX hide unsupported semantics.

## 8. Do Not Hand-Roll Solved Problems

Replace or harden custom implementations where the risk is protocol correctness, security, or endless syntax edge cases.

Highest priority:

- Replace or heavily conformance-test S3 SigV4 signing and XML parsing in `packages/s3`.
  - Both are hand-rolled today (Web Crypto AWS4-HMAC-SHA256; regex-based XML), with no AWS test vectors and no S3-compatible provider tests (all mocked). This is the highest-risk hand-rolled surface.
  - Prefer AWS/Smithy signing primitives if bundle size allows.
  - Candidate: `@smithy/signature-v4`.
  - Use a real XML parser if S3 list parsing grows beyond the current small response shape.
  - If custom signing remains, add AWS-compatible test vectors and S3-compatible provider tests.

Lower priority:

- The hand-rolled SQL parser in `packages/sql` currently meets the "custom parser is acceptable" bar: the subset is deliberately tiny and bounded (`MAX_TOKENS`/`MAX_PARSE_DEPTH`, no joins/subqueries/CTEs), it is documented (`docs/sql-dialect.md`), and it is not exported from the `laql` aggregate package (CLI-only). Revisit (use `node-sql-parser` / `pgsql-ast-parser` or a generated grammar) only if SQL becomes a public-facing surface or the subset grows.

Keep delegated to libraries:

- Parquet read/write should stay on `hyparquet` and `hyparquet-writer`.
- Avro object-container decoding stays on `avsc` (already done — the runtime read path decodes through `avsc`, including 64-bit longs as BigInt).
- H3 should stay on `h3-js`.

Keep internal as core engine work:

- query planning
- expression evaluation
- row batching/scanning contracts
- Iceberg planning glue around metadata, snapshots, manifests, and deletes
- object-store abstraction

Maybe replace later:

- Geospatial predicates can remain simple if documented as approximate/simple.
- If serious geometry support becomes part of the promise, use Turf or another real geometry library.
- Manual JSON validators are acceptable for now, but `zod`, `valibot`, or `arktype` may be worth adopting if validation complexity keeps growing.

Rule of thumb: hand-roll glue, not file formats, security protocols, or broad languages.

## 9. Release Quality

- CI already runs: typecheck, format/lint, build, unit tests, workerd tests, coverage (90% gate), and fixture determinism (`.github/workflows/ci.yml`).
- CI runs the conformance *job* (`pnpm test:conformance`) but does **not** fetch external fixtures (`pnpm fixtures:external` is never invoked, and the lane is opt-in via `LAQL_CONFORMANCE=1` over an empty `fixtures/external/`). Wire external compatibility fixtures into CI so this lane actually exercises real engine output.
- Add a small changelog discipline.
- Add package-level READMEs that say what each package owns.
- Add examples that are executable, not just snippets.

## Suggested Milestones

### Milestone 1: Trust the Iceberg Reader

- Real Spark/PyIceberg v1 and v2 fixtures.
- Compatibility matrix.
- Strict unsupported-feature errors.
- Snapshot/time-travel tests against external fixtures.

### Milestone 2: Trust the Parquet Scanner

- Expanded Parquet type matrix.
- Nested type posture decided and documented.
- Predicate and row-group pruning verified against reference outputs.
- Streaming and cancellation tests.

### Milestone 3: Trust the Worker Story

- Cloudflare Worker demo.
- Resource limit options.
- Request/byte/memory benchmark report.
- R2 and HTTP recipes.

### Milestone 4: Trust the Extension Surface

- Stable object-store and catalog adapter interfaces.
- Catalog conformance tests.
- Clear commit support boundaries.
- Better package docs.

## Definition of "90% Love It"

Users can love this when they can say:

- "It is small enough to run where DuckDB or JVM engines are too heavy."
- "It reads normal Parquet and simple Iceberg tables correctly."
- "When it cannot support a table feature, it tells me exactly why."
- "It does not surprise me with memory or network usage."
- "I can build my own nicer API on top of this core without depending on internals."

That is the lane. Own it tightly.
