---
"lakeql": patch
---

Reuse covering cached and in-flight Parquet byte ranges, merge adjacent known
column chunks without padding, and avoid overfetch for metadata tail probes.
Unify local and shared range-cache retention while preserving byte/entry limits,
priority, expiry and object-version isolation. Reuse versioned planned object
identities instead of repeating head probes during deferred scans.

Reject incomplete physical range responses before serving owners or waiters.
Revalidate cached plan identities at query boundaries, and invalidate byte-store
cache entries when fresh head/list observations identify an overwritten object.

Isolate unversioned raw, metadata and decoded cache products by read observation;
fence late byte-read completions across invalidation; stop validation workers
from scheduling more I/O after a peer fails.
