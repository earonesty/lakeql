---
"lakeql": patch
---

Reuse covering cached and in-flight Parquet byte ranges, merge adjacent known
column chunks without padding, and avoid overfetch for metadata tail probes.
Unify local and shared range-cache retention while preserving byte/entry limits,
priority, expiry and object-version isolation. Reuse versioned planned object
identities instead of repeating head probes during deferred scans.
