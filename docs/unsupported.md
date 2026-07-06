# Unsupported But Detected

Lakeql rejects table features that can change query results when it cannot honor
them. Callers can catch these failures by `LakeqlError.code`.

| Feature | Detection point | Error code | Notes |
| --- | --- | --- | --- |
| Iceberg format-version 3 or newer | Metadata load | `LAKEQL_CATALOG_ERROR` | Reads are limited to Iceberg v1 and v2 metadata. |
| Iceberg advertised table feature flags | Metadata load | `LAKEQL_UNSUPPORTED_ICEBERG_FEATURE` | Feature flags are rejected until each advertised behavior is explicitly supported. |
| Iceberg non-identity partition transforms | Metadata load | `LAKEQL_UNSUPPORTED_ICEBERG_FEATURE` | `identity` and `void` partition fields are accepted; bucket/truncate/time transforms are not silently planned. |
| Iceberg non-empty sort orders | Metadata load | `LAKEQL_UNSUPPORTED_ICEBERG_FEATURE` | Sorted table metadata is rejected until sort-order semantics are part of planning coverage. |
| Iceberg unknown manifest-list content values | Manifest-list hydration | `LAKEQL_UNSUPPORTED_ICEBERG_FEATURE` | Data and delete manifests are recognized; future content types are rejected. |
| Unknown Iceberg delete-file content | Strict file planning | `LAKEQL_UNSUPPORTED_DELETE_FILES` | Future delete formats are not silently ignored. |
| Iceberg deletion vectors | Strict file planning | `LAKEQL_UNSUPPORTED_DELETE_FILES` | Vectors are detected as delete metadata, but vector decoding is not implemented. |
| Parquet decimals above precision 15 | Parquet schema validation | `LAKEQL_UNSUPPORTED_PARQUET_FEATURE` | Prevents lossy JS number decoding for wide decimal values. |
| Unsafe Iceberg manifest paths | Manifest validation | `LAKEQL_CATALOG_ERROR` | Absolute paths and traversal outside the table root are rejected. |
| Parquet struct columns | Parquet schema validation | `LAKEQL_UNSUPPORTED_PARQUET_FEATURE` | Struct groups are rejected before scan/planning so nested data is not silently flattened. |
| Unsupported SQL syntax outside the documented subset | SQL AST mapping | `LAKEQL_SQL_UNSUPPORTED` | Broad join forms, unsupported subqueries, nested or recursive CTEs, simple `CASE <expr>` forms, and broad SQL execution are intentionally out of scope. |

Use `ignore-deletes` or `ignore-unsupported-deletes` only when the application explicitly accepts
raw file scans that may not reflect logical table deletes. These read modes do not bypass metadata
feature checks.
