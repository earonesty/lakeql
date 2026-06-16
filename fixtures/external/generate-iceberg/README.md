# Iceberg Reference Fixture Generator

This directory builds the vendored fixtures under `fixtures/external/iceberg-reference/`.
The generator is intentionally Dockerized because Spark, PyIceberg, Java, and Arrow are
dev/CI-only dependencies and must not enter the runtime packages.

```sh
fixtures/external/generate-iceberg/run.sh
pnpm fixtures:external -- --update-checksums
pnpm test:conformance
```

Generated case directories contain:

- an Iceberg warehouse written by Spark or PyIceberg
- `manifest.json`, consumed by the conformance lane
- a case-level `expectedRecordCount` for the latest generated snapshot
- per-case SHA-256 entries in `manifest.json` for every generated file except `manifest.json`
- small Parquet data/delete files and Iceberg metadata files

The expected case matrix is:

- Spark format v1 table
- Spark format v2 table
- Spark format v2 table with position deletes
- PyIceberg format v2 table with equality deletes
- Spark partition evolution
- Spark schema evolution
- Spark snapshot/time-travel history with at least three snapshots
