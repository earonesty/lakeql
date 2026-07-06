---
"lakeql-core": minor
"lakeql-parquet": minor
"lakeql-sql": minor
---

Add multi-file Parquet planning for prefixes and globs, including bounded file expansion, Hive partition pruning, schema compatibility checks, missing-column null fill, and SQL `read_parquet('...')` sources. Empty glob and prefix matches now fail with `LAKEQL_NO_FILES_MATCHED`, and `*` is segment-local; use `**` for recursive matches.
