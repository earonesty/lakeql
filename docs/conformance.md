# Conformance

The main test and fixture lanes are deterministic and do not use the network.
External conformance inputs are opt-in under `fixtures/external/`.

Fetch Apache parquet-testing files with:

```sh
pnpm fixtures:external
```

Generate the Dockerized Spark/PyIceberg Iceberg fixture matrix with:

```sh
pnpm fixtures:iceberg
pnpm fixtures:external -- --update-checksums
```

Iceberg conformance inputs are discovered under
`fixtures/external/iceberg-reference/`. Put reference warehouse directories there.
The preferred layout is one `manifest.json` per case:

```json
{
  "engine": "spark",
  "case": "v2-position-deletes",
  "metadataPath": "metadata/v3.metadata.json",
  "expectedRecordCount": 8,
  "files": [
    {
      "path": "metadata/v3.metadata.json",
      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
  ],
  "snapshots": [
    {
      "snapshotId": 1,
      "expectedRecordCount": 10,
      "expectedFiles": ["data/part-000.parquet"]
    },
    {
      "asOfTimestampMs": 1767225600000,
      "expectedRecordCount": 8
    }
  ]
}
```

The conformance lane loads every file under `iceberg-reference/` into an object store,
loads each case metadata file through `@laql/iceberg`, and verifies any declared
snapshot row-count or file-list expectations. Cases without `manifest.json` are still
discovered by `metadata.json` and `*.metadata.json` as a compatibility fallback.

Vendored Iceberg reference fixtures are verified by
`fixtures/external/CHECKSUMS.txt`. The checksum file uses SHA-256 lines relative to
`fixtures/external/`:

```txt
<sha256>  iceberg-reference/<engine>/<case>/<file>
```

Verify the vendored Iceberg fixture checksums without network access:

```sh
pnpm fixtures:external -- --verify-only
```

Then run the conformance lane:

```sh
pnpm test:conformance
```

Local conformance skips missing external fixtures by default. Dedicated CI can require the vendored
Iceberg matrix with:

```sh
LAQL_REQUIRE_EXTERNAL_ICEBERG=1 pnpm test:conformance
```

That mode fails if `fixtures/external/iceberg-reference/` has no discovered reference cases.
It also requires the generated matrix to include exactly the expected named case coverage:

- `v1-table`
- `v2-table`
- `v2-position-deletes`
- `v2-equality-deletes`
- `partition-evolution`
- `schema-evolution`
- `snapshot-history`

Every strict-mode case must be described by `manifest.json`. The `snapshot-history` case must include
at least three snapshot expectations, each with a snapshot id or as-of timestamp, expected record
count, and expected file list. Every strict-mode manifest must include a top-level
`expectedRecordCount` for the current case result and `files` entries with SHA-256 checksums for
every generated case file except `manifest.json`; conformance verifies those per-case hashes before
loading metadata.

Run the DuckDB-backed reference lane for row-for-row Parquet comparisons:

```sh
pnpm test:reference
```

Run S3-compatible provider checks against MinIO by setting the endpoint and credentials:

```sh
LAQL_MINIO_ENDPOINT=http://127.0.0.1:9000 \
LAQL_MINIO_ACCESS_KEY=minioadmin \
LAQL_MINIO_SECRET_KEY=minioadmin \
LAQL_REST_CATALOG_URL=http://127.0.0.1:8181 \
pnpm test:providers
```

The CI provider job starts MinIO and `apache/iceberg-rest-fixture` before running this lane.
Set `LAQL_REQUIRE_PROVIDERS=1` when the lane should fail instead of skipping missing provider
endpoints; CI sets this for the provider job.

Pass `--force` to replace an existing external checkout:

```sh
pnpm fixtures:external -- --force
```
