# hyparquet reader contracts

`hyparquet@1.26.0.patch` preserves the unsigned low word when assembling a signed
96-bit physical value and adds an optional `timestampFromInt96` parser. The hook
runs before Julian-day interpretation, allowing LakeQL to share its exact ordinary
and Spark-writer decoding contract across public and owned readers. It applies to
PLAIN values and dictionary entries, including nested columns; it is not tied to
a query or fixture. Callers without the hook retain upstream timestamp conversion.

The patch is pinned in the root pnpm configuration and lockfile. Dependency upgrades
must revalidate the hook and the physical carrier fix. The public `lakeql` bundle
inlines hyparquet because workspace patches do not propagate to npm consumers;
its third-party notice includes the upstream license. The package-install gate
checks the real Spark fixture in a clean npm consumer, without workspace patches.
That gate requires `pnpm fixtures:external`, as does external conformance.

Evidence: `bench/int96-spark-oracle.mjs` checks raw dictionary bytes against documented
Spark microseconds, both LakeQL reader paths, and DuckDB. DuckDB is diagnostic for
the extreme value, not the expected-value oracle. Temporal fixture tests exercise
ordinary INT96, Spark INT96, and annotated FLBA12 timestamps. External conformance
checks all successful public/owned rows and independent date fixture expectations.

The same patch recognizes MAP schemas whose repeated key/value group contains
only a key field. Assembly supplies null values without exposing the physical
wrapper, while still rejecting a missing value column when the schema declares
one. Empty unannotated REPEATED leaves become empty arrays; OPTIONAL leaves and
optional LIST containers retain null semantics. Collection-shape fixture tests,
the complete external corpus, and the clean npm install gate cover these rules.

## Bounded writer statistics

`hyparquet-writer@0.15.6.patch` prevents truncated string maxima from being
published as upper bounds. Optional extrema are omitted when they exceed the
writer's 16-code-unit encoding bound or contain surrogate code units whose UTF-8
order is not established by the writer's JS comparator. Non-null pages without
valid extrema disable that column's optional column index; null counts and offset
indexes remain. Short representable bounds retain normal pruning. The writer is
inlined into the published bundle and the clean-install smoke test verifies a
long-string equality query that otherwise loses matching rows.
