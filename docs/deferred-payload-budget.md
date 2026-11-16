# Streaming deferred payloads

This change is stacked on PR #25; the two should be reviewed together. Eligible projected ordered
top-k and filtered LIMIT queries select narrow references, then materialize payload
columns through the existing asynchronous batch API. The payload phase no longer
collects the complete selected result before yielding it.

## Retention and backpressure

The query retains bounded selected references and one output window. Ranking still
needs up to `offset + limit` references; its row and byte limits are enforced as
references are admitted. Filtered reference accounting is incremental too.

When a byte allowance is configured, payload materialization starts with one row.
It measures output-entry size and decoded bytes per row, then sizes subsequent
windows using the remaining allowance. Physical reads coalesce nearby references
within a measured byte allowance and the existing row-span ceiling. A window may
read files in physical order; the emitted rows preserve result order and stable ties.

The producer yields the completed window and opens no next payload window until
the consumer requests another batch. On resumption it releases completed entries
and references. Consumer return, cancellation and failures stop future reads.
Computed projections are applied before output retention is charged.

If widths increase unexpectedly, an oversized window is discarded and retried at
a smaller size. Reads and decoding performed by retries remain in query statistics
and count against their normal limits. A single row that cannot fit alongside the
remaining references and its decoded input still raises `LAKEQL_BUDGET_EXCEEDED`.
No user tuning flag is required.

## What the byte allowance measures

The live charge combines serialized remaining references, serialized
`[referenceKey, projectedRow]` entries, and the current decoded batch's existing
physical-engine byte estimate. Counters update changed entries and released
references; there is no repeated scan of all retained output rows. Decoded overlap
is checked when the scanner supplies a batch and while output is materialized.

This is deterministic modeled operator accounting, not an allocator or whole-heap
ceiling. Decoding can allocate before returning a batch. Encoded reader buffers,
caches and caller-held results have separate policies and lifetimes. In particular,
the HTTP store's range-ignoring 200 fallback can still buffer an entire object;
small request windows do not bound that response allocation.

`rows()` and `batches()` support bounded consumption. `toArray()` intentionally
collects caller-visible output and can therefore allocate more than the operator
allowance. Streaming can expose a valid prefix before a later read or budget error.

## Verification and cost

Core tests cover output larger than the live allowance, exact live-byte boundaries,
Unicode/binary values, offsets, computed projection charges, shrinking windows,
consumer backpressure/return, paused cancellation, cross-file ordering and ties,
empty results and zero limits. Real Parquet fixtures cover HTTP 206 and
range-ignoring HTTP 200 responses, payload-phase/paused cancellation, recovery and
concurrent native R2 jobs.

The warm HTTP ordering fixture now makes four tiny `bytes=0-1` probes instead of
three: the additional probe establishes the first payload width. It still makes
zero warm data-range reads. Variable-width retries may add reads or decoding; the
fixtures establish correctness and request behavior, not a throughput or peak-heap
claim.

Final local validation passes build, lint, types, 971 regular tests (97 skipped),
10 native Worker/R2 tests, 81 reference comparisons and coverage thresholds
(91.19% statements, 84.76% branches).

## Self-review

A second review of the stack reproduced and fixed three additional cases:

- Wildcard projections cannot be pruned to a known deferred column list. They now
  use the full-column execution path, preserving all columns, including with
  computed projections. Real HTTP 206/200 regressions cover both forms.
- Row iteration with LIMIT zero now returns before opening a scan or spending a
  file/read budget.
- A decoded width of zero is a valid measurement, not a perpetually unknown sample.
  The all-null regression coalesces 32 rows into two payload scans instead of 32.

The review also traced cleanup across yields and errors, retry termination and
read accounting, stable ordering and decoded/output overlap. The remaining
whole-reader/heap limits described above are unchanged; this is not an independent
external review or a claim of a process-memory ceiling.

An additional 64 deterministic checks compared the compiled public query API with
an independent stable full-sort oracle across variable widths, null/binary values,
two files, output batch sizes and decoder batch boundaries; all passed. Selection
still uses the scanner's existing ranking/predicate input-batch policy. The decoded
overlap charge in this change applies to payload materialization, not a new global
reservation for every scanner or encoded reader allocation.
