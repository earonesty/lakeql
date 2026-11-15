# Running counters and shared memoization

Two shared mechanisms replace repeated work without introducing another execution
framework.

`RunningCounter` owns retained item and byte totals. Buffer owners charge additions,
release removals and reset on flush. It also supplies JSON-list framing around
individually measured values, preserving existing serialized-size limits. DISTINCT,
ordered rows, deferred references, sort runs, window buffers and row aggregate
states use it. Restored row aggregate lists initialize their counters once; limit
checks then inspect totals and the touched group, rather than snapshotting all
groups. Snapshots remain the checkpoint representation. Vector group-by directories
also track additions, updates, merges, replacements, deletions and clear operations.
Buffered vector aggregates share the list-size arithmetic; scalar states contribute
their fixed modeled charge. Previously unbudgeted buffers initialize their byte
measurement once when a byte budget is enabled. Caller-supplied plain group Maps
retain the inspection fallback because they do not provide mutation hooks.

`SharedMemoryCache.getOrCompute(key, compute, measure)` shares pending computations
and completed results through the existing capacity, priority and TTL policy.
Pending entries include a modeled key/entry charge. Result charges include that
charge plus the caller's measured result size. Overflow declines retention;
failures can be retried. Expiry/eviction may permit recomputation, and a completed
old promise cannot resurrect or replace a cleared/newer entry.

SQL roots own a memo and clear it in `finally`. Library and CLI share scalar
subquery resolution and correlated lookup preparation instead of maintaining two
copies. Canonical plan keys include typed literals and actual nested dependencies,
not generated placeholder IDs or cyclic parser registries. Correlated preparation
reuses a lookup table containing distinct correlation keys. `memoMaxBytes` controls
SQL memo capacity; library defaults use the configured query byte limit, then cache
capacity, then 64 MiB. General callers can use query-owned caches or a cache TTL.

## Bounds

Byte metrics are explicit: existing operator limits remain serialized-size limits;
the SQL memo measures serialized result content plus modeled entry/key storage.
They do not establish a total JavaScript/Worker heap cap. Memo capacity bounds what
the cache retains, not allocations inside `compute` or values held by active callers.
The memo is shared among subqueries of a SQL execution; it is not a new global RAM
pool shared with every reader and operator.

## Evidence

- `pnpm check` passes build, lint, types, package consumers, compatibility, fixture
  checks; the final counter integration separately passes lint, types, build,
  940 regular/coverage tests (97 skipped), 9 Worker tests, 81 references
  and 7 conformance cases (one skipped). The optional external Parquet corpus is
  absent in this checkout.
- Unit tests cover pending/completed reuse, capacity pressure, oversized results,
  failure/retry, TTL, clear/replacement races, typed keys and cyclic dependencies.
  Counter tests cover Unicode, removals, reset, invalid adjustments, exact byte
  boundaries and one-byte/one-row-below restore failures. Vector group tests cover
  shared ownership, aliases, replacement, deletion, restore and merge; a 1,000-group
  regression verifies that budget checks do not revisit existing groups.
- Real Parquet tests use actual HTTP 206 and range-ignoring HTTP 200 responses,
  and a native emulated R2 bucket. With range caching disabled, two equal scalar
  subqueries require the same reads as one; disabling memoization adds reads.
- `pnpm bench:query-state` measures public in-memory row paths with 1,000 rows and
  a 64 MiB serialized limit. One warmup/five samples; encoding-work instrumentation
  is separate from timing. A diagnostic comparison replaces only `query.js` with
  main's `6f04828` implementation while retaining current dependencies:

| Path | Main module ms | Counter module ms | Encoded characters before → after |
| --- | ---: | ---: | ---: |
| Grouped sum | 1034.66 | 12.70 | 43,014,866 → 223,753 |
| Median | 27.20 | 4.60 | 5,677,661 → 89,322 |
| DISTINCT | 117.26 | 2.17 | 63,854,990 → 126,780 |
| Ordered buffer | 411.58 | 2.17 | 57,848,990 → 114,780 |

These local diagnostic timings are not cloud-throughput or peak-heap measurements.
Scaling tests independently require near-linear accounting work when input doubles.
