# Unified bounded ordering

PR [#21](https://github.com/earonesty/lakeql/pull/21) is closed. Its branch
`perf/memory-bounded-execution` is preserved for reference. This replacement starts
from main at `b54d4ed`, which already contains the recent #19/#20 improvements.

## What changed

One priority queue in `query-sort.ts` now serves bounded row selection, deferred
ranking and sorted-run merging. It replaces two worst-candidate scans and the
separate merge heap. Restored top-k candidates enter the same selector as new
input. Earlier equal-key rows survive replacement, and equal-key sort runs merge
in run order. Vector top-k keeps its existing bounded sort-and-truncate algorithm;
its redundant final sort and repeated normalization are removed. Deferred key
comparisons borrow their ordering terms instead of copying the array each time.

The three production files have 133 added and 154 removed lines. There is no new
execution backend, reader, allocation framework, decoder or format implementation.
No source or raw experiment archive from #21 is imported.

## Bounds and compatibility

Selection retains at most `offset + limit` candidates, with one ordinal per
candidate and fixed queue bookkeeping. There is no full-input key cache or upfront
allocation based on the requested limit. Zero-capacity selection retains nothing.
Existing `maxBufferedRows` and serialized `maxMemoryBytes` checks remain in place;
serialization is skipped when its limit is absent. This preserves their existing
meaning; serialized-size accounting is not a total JavaScript or Worker RAM cap.
The 64/128 MiB Worker fixture settings below are serialized budgets, not proof of
whole-runtime caps. No such guarantee is claimed.

Readers, cache capacities, physical vector-shape placement, cancellation and
parallel-range scheduling are unchanged. HTTP servers that honor ranges and those
that return whole-object 200 responses are tested separately. Local kernel timings
are not used to claim cloud throughput.

## Verification and measurements

- `pnpm check`: build, lint, types, package consumers, compatibility, fixture
  checks, 922 regular tests (97 skipped), 8 Worker tests, 7 conformance cases
  (one skipped), 81 reference cases and coverage pass. The external Parquet corpus
  is absent from this checkout; its optional conformance lane is skipped.
- The final native-R2 addition passes all 9 Worker tests with serialized file
  startup. It writes real Parquet to a native emulated R2 bucket, checks four
  concurrent jobs using both row and deferred ranking at 64/128 MiB serialized
  budgets, and verifies zero warm data-range requests.
- The HTTP fixture checks independent full-sort parity, ties, offsets, a 13-row
  candidate ceiling, checkpoint restore, both 206/200 behavior and two simultaneous
  reads. Warm queries issue exactly three `bytes=0-1` planning probes and no cached
  data-range requests. Pre-read cancellation makes no HTTP request and does not
  prevent healthy-query reuse. This is actual localhost HTTP, distinct from native R2.
- Unit tests verify stable full-sort equivalence across distributions/capacities,
  invalid limits, empty/unequal runs, released candidate references after finish,
  and logarithmic comparison growth on adverse replacement input. Existing query
  tests cover null/multiple keys, serialized and spilled checkpoints, budget errors
  and sort continuation.
- `pnpm bench:ordering`: Node v24.18.1, 8,192 values; alternating implementations,
  two warmups and six measured samples. At k=512, descending input uses 137,801
  comparisons versus 3,932,671 for the removed scan; observed medians are 0.59 ms
  versus 4.45 ms. Mixed input uses 35,740 versus 3,936,120 comparisons (0.20 ms
  versus 4.41 ms). Small-k overhead remains: k=8 descending is 0.27 ms versus
  0.13 ms, and mixed is 0.15 ms versus 0.10 ms. These are selection-kernel results,
  not end-to-end query speedups or peak-heap measurements.

The tradeoff is one shared logarithmic implementation in place of duplicated
linear scans, with stable ordering and bounded candidate metadata. Small-k kernel
regressions are retained explicitly; no tuning flags or second selector are added.
Whole-runtime memory budgeting remains a separate architectural question, rather
than an exhaustive prerequisite program for this change.
