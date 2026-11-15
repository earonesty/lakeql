# Deferred payload retention

Ordered top-k and filtered LIMIT queries can first select narrow row references,
then fetch projected payload columns. Their common materialization helper now
checks retained payload bytes as rows arrive, using the existing `maxMemoryBytes`
and `maxBufferedRows` limits. No new option or execution strategy is introduced.

The materialization byte metric is the sum of two serialized lists:

- Selected references, including their source identity, row index and ranking keys.
- Retained `[referenceKey, row]` entries, including currently materialized payload.

The helper measures the selected reference list once. A shared running counter
updates only the changed result entry, with UTF-8 sizing and the existing JSON-safe
representation for binary and other scalar values. Limit checks happen before the
entry is retained. A rejected payload stops the scan; subsequent payload windows
are not opened. Discarded offset references are released before materialization.
Only the selected references and result entries are charged during this phase;
the ranking phase retains its own existing checks.

A query that previously passed because only narrow ranking keys were charged can
now fail when its selected payload exceeds the operator allowance. Reducing LIMIT
or projected columns, or increasing the allowance, changes that outcome.

These remain serialized operator-state limits, not whole-runtime memory limits.
The scanner has already decoded the current batch when rows are charged. Reader
buffers, caches, decoded batches and caller-held output have separate lifetimes;
this change does not establish a combined heap ceiling or change range concurrency,
row-window sizing, or decoder allocation behavior.

## Verification

Core regressions cover ordered and filtered LIMIT paths, exact byte caps and one
byte below, offsets, Unicode and binary values, empty results, zero limits, and
iterator closure without consuming the next payload row after failure. The wide-row
failure tests reproduced the missing enforcement before the fix.

Real Parquet fixtures exercise both HTTP 206 range responses and HTTP 200 responses
that ignore ranges, plus concurrent native R2 jobs. They check oversized rejection,
success under a sufficient allowance, healthy reuse after failure, and HTTP
cancellation during payload materialization. Existing warm-cache and read-concurrency
checks remain in place. The fixtures do not measure peak heap usage.

Validated locally with build, lint, types, 961 regular tests (97 skipped), 10 native
Worker tests, 81 reference comparisons and coverage. Coverage thresholds pass
(91.16% statements, 84.72% branches).
