# Compatibility Matrix

This file is generated from `docs/compatibility.json`. Run `pnpm docs:compatibility` after editing the source of truth.

Legend: supported+tested = covered by tests; supported = implemented with narrower coverage; detected+rejected = fails with a typed `LakeqlError`; planned = not yet a compatibility promise.

| Area | Feature | Status | Notes |
| --- | --- | --- | --- |
| Parquet | Primitive scalar columns | supported+tested | Covered by local fixtures and hyparquet decoding. |
| Parquet | Projection | supported+tested | Column projection is threaded into Parquet reads. |
| Parquet | Row-group pruning | supported+tested | Predicate pruning uses row-group statistics where available. |
| Parquet | Deployment-neutral work-unit fan-out | supported+tested | Parquet task manifests split into portable JSON row-group work units, fan aggregate partials back in, and can reuse runtime metadata caches without changing the work-unit payload. |
| Parquet | Lists and maps | supported+tested | Delegated to hyparquet and compared against DuckDB-authored LIST and MAP Parquet values. |
| Parquet | Struct assembly | detected+rejected | Rejected with LAKEQL_UNSUPPORTED_PARQUET_FEATURE to avoid silent flattening. |
| Parquet | Decimal/date/time/timestamp logical values | supported+tested | Reference-engine comparison covers DuckDB-authored DECIMAL(9,2), TIME, DATE, and TIMESTAMP logical decoding. |
| Parquet | Unsigned integer and byte-array values | supported+tested | Reference-engine comparison covers DuckDB-authored unsigned integer logical values, binary payloads, and fixed-length byte arrays. |
| Parquet | Wide decimal backing variants | detected+rejected | Decimals above precision 15 are rejected with LAKEQL_UNSUPPORTED_PARQUET_FEATURE to avoid lossy JS number decoding. |
| Parquet | Timestamp micros/nanos | detected+rejected | Sub-millisecond timestamp units are rejected with LAKEQL_UNSUPPORTED_PARQUET_FEATURE to avoid truncating precision. |
| Parquet | Null-heavy scalar and nested rows | supported+tested | Reference-engine comparison covers null-heavy DuckDB-authored scalar, date/timestamp, list, map, and binary values. |
| Iceberg | Format v1 reads | supported+tested | Metadata load and planning are covered. |
| Iceberg | Format v2 reads | supported+tested | Metadata load, planning, and append boundaries are covered. |
| Iceberg | Format v3+ reads | detected+rejected | Rejected with LAKEQL_CATALOG_ERROR. |
| Iceberg | Snapshot-id, ref, and as-of planning | supported+tested | Local metadata tests cover selection behavior. |
| Iceberg | Manifest lists and direct manifest references | supported+tested | Snapshots can hydrate manifests from inline/direct metadata refs or manifest-list files. |
| Iceberg | Position deletes | supported+tested | Delete metadata and planned row scanning are covered by fixtures. |
| Iceberg | Equality deletes | supported+tested | Delete metadata and planned row scanning are covered by fixtures. |
| Iceberg | Deletion vectors | detected+rejected | Rejected with LAKEQL_UNSUPPORTED_DELETE_FILES. |
| Iceberg | Non-identity partition transforms | detected+rejected | Rejected with LAKEQL_UNSUPPORTED_ICEBERG_FEATURE. |
| Iceberg | Non-empty sort orders | detected+rejected | Rejected with LAKEQL_UNSUPPORTED_ICEBERG_FEATURE. |
| Iceberg | Unknown manifest-list content values | detected+rejected | Rejected with LAKEQL_UNSUPPORTED_ICEBERG_FEATURE. |
| Iceberg | Partition/schema evolution against external engines | supported+tested | Strict external conformance loads Spark partition-evolution and schema-evolution fixture metadata. |
| Catalogs | Object-store metadata | supported+tested | loadIcebergTableFromObjectStore is the stable path. |
| Catalogs | REST catalog load | supported | Load/list/append contract is documented. Provider conformance covers create/list/load against a reference REST catalog; append and 409 conflict request handling are covered by unit tests. |
| Catalogs | Glue and Nessie | planned | Live adapters are not implemented yet; typed catalog stubs are available and reject operations explicitly. |
| Catalogs | Glue and Nessie catalog stubs | supported+tested | Stubs satisfy IcebergCatalog and fail load/list/commit operations with LAKEQL_CATALOG_ERROR. |
| Object storage | HTTP range reads | supported+tested | Adapter uses Range for getRange. |
| Object storage | R2 range reads | supported+tested | Adapter maps to R2 ranged get. |
| Object storage | S3 SigV4 and ListObjectsV2 | supported+tested | Signing delegates to aws4fetch; XML parsing uses fast-xml-parser. |
| SQL | DESCRIBE metadata statement | supported+tested | parseSqlStatement recognizes DESCRIBE and the CLI query path returns Parquet schema metadata for local paths or named tables. |
| Browser parity | In-memory JavaScript row arrays | supported+tested | createInMemoryLake registers JS row arrays behind the normal Lake runtime with query-time budgets and task planning. |
| Browser parity | CSV ingest | supported+tested | lakeql-csv is an opt-in package for headered/headerless CSV, delimiter options, quoted fields, type sniffing, null handling, and ingest budgets. |
| Browser parity | JSON and NDJSON ingest | supported+tested | lakeql-json is an opt-in package for JSON arrays, single objects, NDJSON records, browser binary inputs, and ingest budgets. |
| Browser parity | Apache Arrow output | supported+tested | lakeql-arrow is an opt-in package that converts rows, query results, and vector Batches to Arrow tables or IPC without adding apache-arrow to core. |
| Browser parity | OPFS persistent cache | supported+tested | lakeql-opfs is an opt-in package exposing OPFS-backed byte and JSON CacheAdapter implementations for browser persistence without adding OPFS APIs to core. |
