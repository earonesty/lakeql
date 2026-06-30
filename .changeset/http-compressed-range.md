---
"lakeql-http": patch
---

Detect static hosts that serve byte ranges over compressed assets and fall back to cached full-object slicing so Parquet footer reads use decoded byte offsets.
