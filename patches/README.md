# hyparquet INT96 parser contract

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
