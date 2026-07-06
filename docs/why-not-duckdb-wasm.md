# Why Not DuckDB WASM

DuckDB WASM is a strong embedded analytical database. Lakeql is narrower: it is designed for TypeScript applications that need object-store-native Parquet and Iceberg planning, typed errors, resumable queue-sized work, and caller-owned storage/runtime adapters.

Choose Lakeql when you need:

- deterministic task manifests and bookmarks
- direct ObjectStore integration for HTTP, R2, S3, or in-memory tests
- conservative pruning and explain output in application code
- queue-safe slices for Workers or serverless jobs
- a small TypeScript API for object-storage queries

Choose DuckDB when you need broad SQL joins over many local tables or mature analytical execution beyond Lakeql's current constrained broadcast and lookup join helpers.
