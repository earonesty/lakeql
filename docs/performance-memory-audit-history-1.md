# Performance and memory audit: historical execution log 1

[Current requirements and evidence](performance-memory-audit.md).


### Broad verification and measured kernel results

The broad verification checkpoint passed repository-wide lint and typechecking, 938
standard tests (97 skipped), eight workerd tests, and 81 reference tests.
Skipped tests are not treated as verified. These checks do not establish a
complete shared allocation bound; the integration gaps below remain open.

Run `node bench/performance-memory.mjs` after building to compare the changed
kernels with baseline `b54d4ed`. The harness reads baseline kernel source from Git,
uses the current build for unchanged dependencies, asserts matching outputs, warms
up once, and reports the median of three runs. This isolates algorithm changes
rather than comparing two independently built releases.

Measured on November 12, 2026 with Node v24.18.1:

| Kernel | Rows | Baseline ms | Updated ms |
| --- | ---: | ---: | ---: |
| Group by with 64 MiB serialized budget | 2,000 | 222.403 | 1.818 |
| Group by with 64 MiB serialized budget | 5,000 | 1,424.142 | 3.632 |
| Group by with 64 MiB serialized budget | 10,000 | 5,606.793 | 9.457 |
| Row top-k with k = 1,000 | 20,000 | 213.268 | 5.727 |
| RANGE frame endpoint lookup | 10,000 | 162.312 | 2.175 |

Group-by inputs have one group per row. Top-k uses descending unique scores to
exercise replacement. RANGE inputs use sorted numeric keys and one-unit bounds.
These are CPU kernel measurements, not cloud-query latency, throughput under
concurrency, or measured peak memory. Real R2/S3 validation remains required;
the existing public R2 benchmark fixtures provide a read-only route for that work.

### Integration status

Work is on `perf/memory-bounded-execution`, based on audited upstream `b54d4ed`.
The existing `QueryBudget.maxMemoryBytes` explicitly measures deterministic
serialized operator bytes; the allocation pool must remain a separate contract.

The internal `memory-budget.ts` foundation now supports atomic hierarchical
reservations, per-query children of a shared pool, peak/available-byte counters,
and reference-counted reservation handles. Failed admission creates no partial
reservation; admission may evict reusable cache entries before failing. Releasing a cache owner can leave the same allocation charged through
reader handles. These handles require explicit lifecycle integration; they do not
track JavaScript references automatically.

Five reservation tests pass, covering competing jobs at a 64 MiB boundary,
per-query limits, pinned ownership, repeated release, replacement overlap,
failure cleanup, invalid byte counts, and zero capacity. They test admission
without allocating equivalent payloads and are not runtime-memory or fixture
evidence.

`SharedMemoryCache` now accepts a shared `memoryBudget`, offers `acquire` leases
for readers retaining data across eviction, and offers `setAllocated` to reserve
before invoking an allocation factory. Object-store defensive cache copies use
that factory. Cache admission can evict entries to fit current pool availability;
pinned entries stay charged after eviction. Failed factories release reservations.
The existing `set` API accounts for retention of an already allocated payload and
does not retroactively make that allocation bounded.

Automatic reclamation now lets mandatory query reservations evict reusable cache
payloads from the shared pool. Reclaimers are registered while a cache is nonempty
and detached when it empties. Pinned allocations remain charged; admission fails
if live owners prevent sufficient reclamation. Requests exceeding a query's total
capacity fail without evicting warm caches. `tryReserve` lets optional optimizations
decline admission; it does not suppress validation errors or reclaimer failures.
Admission is synchronous and fails rather than waiting on memory held by the
requesting query. Async read scheduling and fairness remain separate work.

Validation after automatic reclamation: 36 tests pass across memory admission, object-store cache, Parquet
range-cache, and decoded-column-cache suites. Core TypeScript compilation passes.
The tests include pre-allocation admission, rejected factories, eviction with a
live reader, query/cache competition, and failure cleanup. Existing range-cache
behavior remains covered, but this is not evidence of cloud-provider operation.

### Allocation integration inventory

An internal `MemoryScope` now reserves before synchronous/asynchronous allocation
and releases outstanding scope ownership on disposal. Independently retained
owners keep the shared allocation charged after the producing scope ends.
Pending asynchronous work remains charged until it settles, even after disposal;
otherwise cancellation could admit a new allocation while an old network read
still owns memory. Ten focused scope/pool tests pass, including real allocation
and touching of two 32 MiB buffers under a 64 MiB allowance, failure cleanup,
disposal during pending I/O, and retained ownership. Core typechecking passes.
This scope is not yet wired into the complete query lifecycle.

HTTP inspection identified another required boundary: `head` uses a ranged GET
probe rather than HTTP HEAD, and the driver can consume and privately cache a
full response when a server ignores ranges or serves compressed ranges. That
fallback previously used unbounded `arrayBuffer()` consumption. The driver now
consumes decoded bytes incrementally, cancels oversized responses, and retains
fallback payloads in a bounded cache with automatic eviction. Both response and
fallback-cache limits default to 16 MiB and are explicitly configurable; this is a
per-response/provider contract, not a total engine-memory guarantee. It intentionally
changes the behavior of previously unbounded full-object reads above that limit.
Geometric buffer growth keeps bookkeeping constant-sized even for tiny network
chunks; growth and final compaction still require overlapping allocation charges.
Fifteen HTTP tests and HTTP/core compilation pass. Tests cover exact/over limits,
missing or misleading lengths, decoded compression expansion, ignored ranges,
size probes, cache eviction/disablement, and 10,000 one-byte chunks.
Shared response/fallback ownership must still be integrated before claiming a
total allocation cap. Live cloud reports
now distinguish data-range operations from the additional wire-level probe.

| Boundary | Current ownership and remaining work |
| --- | --- |
| Core query budgets | Serialized operator limits remain unchanged. Introduce query allocation scopes and propagate them through operator execution. |
| Object-store cache | Defensive insertion copies reserve through the supplied pool. Returned copies and source read buffers still need query ownership and pre-allocation admission. |
| Shared cache readers | Immediate borrows remain available for compatibility. Migrate retained decoded vectors/batches and asynchronous consumers to leases before claiming complete bounds. |
| Parquet scanner construction | `createParquetLake` constructs object, scan-range, and decoded caches. Unify their allocation pool and query propagation while respecting explicitly supplied shared caches. |
| Parquet vector pages | `vector-batches.ts` reads compressed bodies and creates decoded vectors, validity masks, and slices. Reserve compressed input, decode expansion, and overlapping output buffers; release at actual consumption boundaries. |
| Read control | The semaphore limits requests, not resident bytes. Add byte admission without holding scarce request slots while waiting for memory. |
| Cache pressure | Query reservations reclaim reusable cache space automatically, with lifecycle registration and pin-aware charges. Async scheduling and fairness remain open; avoid waits that require the requesting query to free its own state. |

The group-by accounting finding now has an implementation: engine-created group
maps maintain byte totals on insertion, replacement, removal, aggregate updates,
and merges. Aggregate updates apply only the changed state's byte delta. Weak
owner references avoid retaining an entire query through a detached group.
Externally constructed plain maps retain the full-scan estimator. Group state
must be mutated through the group update APIs, not by writing aggregate internals.
The accounting preserves the existing serialized/estimated metric; bookkeeping
heap overhead still needs inclusion in the separate allocation contract.

Twenty group-by tests pass, including a full-scan oracle checked after repeated
updates with budgets toggled, JSON restore, merges, removals, replacements, and
failed aggregate updates. Exact-limit and one-byte-under decisions match the
oracle. A 10,000-group regression verifies that budgeted updates do not iterate
the group map. Core compilation and 103 tests across query, physical execution,
aggregate memory, and group-by pass. This proves the local accounting change,
not cloud operation or a measured end-to-end speedup.

Row top-k now uses a generic stable bounded heap with O(log k) candidate updates.
Its explicit encounter ordinal corrects a previous tie behavior: replacing a tied
worst candidate could discard an earlier row. The new result matches stable full
sorting. This is an intentional correction, not byte-for-byte preservation of
the old incidental tie order. Restored rows precede newly encountered rows.

Row ordered collection, sort-run accumulation, and DISTINCT now use shared
incremental serialized-list accounting. Optional serialization is skipped when
there is no serialized-byte limit. Removed candidates and new candidates are
measured individually; the retained buffer is not serialized on every row.
Window row buffers now also use incremental accounting for collection and run
flushing. Restored rows are measured once; unbudgeted collection skips optional
size calculation. Custom window-runner estimators keep their existing full-buffer
semantics unless they explicitly supply an additive serialized item estimator.
Remaining row accounting work includes late-materialized row references and row
aggregate snapshots. Sort-run byte accounting still follows
the existing current-run limit; all retained runs need coverage under the separate
allocation pool before this can be called a total memory cap.

New tests compare bounded selection against stable sorting across capacities,
null placement, directions, and multiple keys; bound comparison growth; and check
serialized sizes for nested, escaped, binary, bigint, and undefined values.
Row-query tests cover exact limits and one-byte-over failures for replacement and
DISTINCT. These are in-process execution tests, not cloud-read fixtures.
Core compilation and 79 tests across query execution, bounded selection,
buffer accounting, and window runner pass. The exact top-k budget test uses
`topKWithState` to exercise the row path explicitly; the ordinary query planner
can choose late materialization, whose reference buffers have a different charge.

Range-cache prefetch now calls an internal range-population operation and does
not create a defensive output copy that is immediately discarded. Demand reads
still return independent copies. Shared cache hits acquire a lease across the
async handoff and release it after copying; prefetch releases its lease without
copying. Tests cover both local and shared cache implementations, duplicate and
coalesced ranges, shared failures and retry, output mutation, and eviction during
the async handoff. Pending reads and returned copies still need allocation-pool
admission; this change does not claim to close those ownership gaps.

A loopback HTTP fixture test serves the committed sales Parquet file with HEAD
and 206 byte-range responses. Three coalesced ranges fetch exactly 384 bytes,
including an overlapping demand read without an extra request, and observe two
concurrent requests under a cap of two. Remote decoding returns all 100 expected
rows and matches local decoding; no full-object GET occurs. Both shared and local
range caches pass. This proves HTTP protocol operation with real Parquet data,
not R2/S3 deployment behavior, production latency, or cloud cancellation semantics.

`node bench/cloud-range-validation.mjs` now validates the production HTTP adapter
against the existing public R2 `bench/window-events.parquet` fixture. It forbids
full-object reads, requires 206 responses, and checks all 144 selected rows against
the independent fixture generation contract. On November 12, 2026:

- Coalesced prefetch plus an overlapping demand read issued exactly four data-range
  requests and fetched 4,096 data bytes, both at a concurrency cap of one and at a cap of four.
  The HTTP adapter's size probe is a separate two-byte ranged GET, so the full wire
  operation comprises five range requests and 4,098 response bytes.
  Observed peak concurrency was respectively one and four. Repeating cached
  prefetch issued no additional request.
- At both 64 MiB and 128 MiB **serialized operator** budgets, cold scans issued
  eight data-range requests and fetched 9,507 data bytes; warm scans issued three data-range requests and
  fetched 6,902 bytes with a deliberately small 4 KiB scan cache. All rows matched.
- Cold elapsed times were 715 ms and 646 ms, and warm times 587 ms and 569 ms.
  These single live observations demonstrate operation, not a controlled latency
  comparison or proof that either configuration bounds total runtime memory.

Review also identified and corrected a read semaphore handoff race. A released
slot now transfers directly to its queued recipient instead of briefly appearing
free to a new caller. Cancellation after transfer passes the slot to the next
waiter. Queue removal is O(1) through linked waiters, and release handles are
idempotent. Deterministic tests cover newcomer overtaking, cancellation at the
handoff boundary, and cancellation of 250 entries in a 500-reader FIFO queue.
After the fix, 32 read-control/range-cache tests pass and the live R2 validator
passes again with the same request counts, byte counts, row results, and concurrency
peaks. Core, HTTP, and Parquet compilation passes.

`vectorTopKBatch` no longer re-sorts the already ordered indices from
`vectorTopKIndices`, eliminating comparator-time normalization on that redundant
pass. Existing vector sorting tests cover the retained ordering behavior.

RANGE offset endpoints now use binary search over the sorted partition, with
constant auxiliary memory. This handles row-dependent offsets without assuming
monotonic endpoint movement. Tests compare numeric frames to a separate linear
oracle across directions, ties, and changing offsets, check timestamp intervals,
empty/unbounded frames and invalid inputs, and bound order-key accesses over
10,000 rows. This avoids the memory tradeoff of caching all RANGE keys. GROUPS
frames and peer-span discovery are still open; binary-searching `samePeer` is not
automatically valid because its representation-based equality need not coincide
with sort equivalence for every scalar representation.

GROUPS and peer lookup, the remaining row-accounting paths, concurrent-job cloud
and cancellation validation, dynamic read
admission, and end-to-end accounting remain open. The new cache option bounds
charged cache payloads only; it is not yet a whole-query or whole-runtime cap.

### Owned HTTP allocation path

`ObjectStore` now has an optional `getRangeOwned(path, range, scope)` capability.
The HTTP implementation reserves engine-created response buffers before allocation
against the supplied scope. Growth charges old and replacement buffers together;
if geometric growth cannot fit, it tries the exact required capacity. Returned
views retain the full backing allocation charge, including when a server ignores
a small Range request and returns a full object. Fetch-provided chunks remain
runtime-owned and are outside this accounting contract.

`MemoryScope.adopt` transfers an independent handle into a scope using the same
budget. Working HTTP buffers remain charged while a stream read is pending even
if its scope is disposed; they are released when that read settles. URI mapping
and read-control wrappers preserve the optional capability. Read controls release
an owned result if cancellation occurs before its handoff to the caller.

Verification covers replacement-overlap rejection, exact-growth fallback, short
views over larger backing buffers, retained ownership, pending-read disposal,
full-response fallback charging, cancelled handoff, disposed-scope rejection, and
cross-budget adoption rejection. HTTP, scope, semaphore, and URI tests are run
alongside HTTP/core compilation.

This capability is not yet wired into query execution or Parquet range caches.
The owned HTTP path also does not yet reuse the legacy full-response cache: that
cache must acquire ownership-aware storage before reuse can preserve allocation
accounting. Other adapters and cache wrappers still need the capability. The
legacy byte-returning API limits individual response consumption but transfers
untracked final payload ownership to its caller. None of these changes yet prove
a total 64–128 MiB query or Worker memory bound.

`SharedMemoryCache.setOwned` can now retain an existing allocation without a
second payload charge or copy. `acquireOwned` returns an independent retained
handle; ordinary borrowed leases also pin owned entries across eviction.
Admission requires the allocation and cache to use the same budget identity.
Cross-budget promotion is deliberately rejected until an explicit ownership
transfer contract exists; silently moving query-owned charges into another pool
would invalidate both limits. Pressure reclamation, replacement, incompatible
budget rejection, oversized admission, and pinned eviction have dedicated tests.
This supplies the cache primitive needed for owned reads; scanner integration and
cross-job pool ownership remain outstanding.

The object-store cache wrapper now preserves `getRangeOwned` when its underlying
adapter supports it. Cache hits pin the source while reserving and allocating a
defensive output copy in the caller's scope; misses preserve the adapter's owned
output and use the existing pre-allocation cache admission for a separate cached
copy. This preserves mutation isolation and write invalidation. Tests verify
warm-read reuse, output mutation isolation, write invalidation, rejected cache
admission, and pin cleanup when output-copy admission fails under pressure.
The 19 object-store cache tests pass, with core compilation also passing.

The Parquet store-buffer bridge exposes optional `sliceOwned` when its adapter
supports owned ranges. Complete ArrayBuffer-backed responses reuse the original
allocation and ownership. Subarray or shared-buffer responses reserve a contiguous
output while the source remains charged, then release the source; failed output
admission also releases the source. Physical-read statistics, read-budget checks,
and cancellation checks are shared with the ordinary slice path. Four bridge
tests cover zero-copy reuse, copy overlap, failed admission, and capability absence.
Parquet compilation passes. Existing real-fixture loopback HTTP tests require
execution outside the filesystem sandbox because its loopback listen is denied.
The decoder and range-cache callers still need to consume this capability and
release ownership at their actual last use; exposing it alone is not an execution
memory cap.

The in-memory reference adapter now implements `getRangeOwned` with reservation
before the output copy. Its pre-existing object backing is caller/store-owned and
is not newly charged as query allocation. Both ordinary and owned range methods
validate safe integer bounds and explicitly copy bytes, including Uint8Array
subclasses whose `slice` method returns a view. Tests verify isolation, exact
admission, malformed ranges, empty endpoint ranges, and released ownership.
The memory-store, object-store-cache, and Parquet bridge suites pass 38 tests;
Parquet/core compilation passes. This establishes reference-adapter behavior,
not filesystem or cloud transport equivalence.

### Decoder ownership review and integration checkpoint

A read buffer cannot be released merely because `dataPageValues` returned.
`vector-batches.ts` constructs compressed page views over complete column chunks,
keeps dictionaries across pages, and creates typed vector slices with `subarray`.
Decoded vectors may also enter `DecodedColumnCache`, and yielded batches can be
retained by callers beyond the producer's next iteration. The ownership contract
must therefore cover the output batch, dictionary lifetime, and cache retention,
including shared backing allocations. A `finally` around a page read alone would
under-account live buffers. Object/column fallback decoding through hyparquet
needs the same analysis rather than an assumption that returning from decode ends
all input ownership. This is the next integration boundary; no query-wide cap is
claimed by the currently optional read capabilities.

After the owned adapter/cache/bridge changes, the full default test run passes
973 tests with 97 skipped (88 passing files, 7 skipped). Full repository typecheck
and lint pass. The default suite includes the committed-fixture loopback HTTP
range tests; it was run with loopback access enabled. These checks verify current
behavior, but skipped tests and allocation paths not yet integrated remain outside
this evidence.

`MemoryScope.retainBacking` now composes ownership for an output referencing
multiple existing allocations, such as a page vector plus a dictionary. It retains
backing handles without new budget charges; callers must reserve genuinely new
output allocations separately. Producer disposal can then release producer handles
while consumer or cache ownership keeps the allocations charged. Partial retention
failure rolls back acquired handles. Cross-budget composition is rejected.
Repeated identical handles are deduplicated; distinct handles aliasing one
allocation still charge the budget once, although the composite `bytes` reports a
conservative sum (relevant to cache admission). Tests cover producer/consumer/cache
lifetimes, exact-cap operation, duplicate handles, failure rollback, and incompatible
budgets. The scope/cache suites pass 26 tests, and core compilation passes.
This is the output-ownership primitive; decoder and operator propagation remain
required before it provides end-to-end allocation bounds.

### Owned Parquet range cache

`cachedRangeBuffer` now preserves an owned source's `sliceOwned` and adds
`prefetchOwned`. These paths coalesce requests, deduplicate in-flight reads within
a budget, retain cache backing without copying it, and reserve defensive demand
outputs while their sources remain pinned. Prefetch avoids discarded output
copies. Independent waiters share an I/O ownership stage: closing one caller's
scope does not uncharge or invalidate another caller's pending read. Failure and
last-waiter cleanup release pending ownership. Local owned caches participate in
budget pressure reclamation; a supplied shared cache admits ownership only when
its budget matches. Different budget identities do not share charged ownership.

Tests verify overlapping prefetch/demand deduplication, warm reuse, defensive
mutation isolation, failed output admission and retry, pressure eviction, and
independent waiter disposal. The range-cache and bridge suites pass 21 tests and
Parquet compilation passes. This is distinct from the existing real HTTP fixture
coverage of the legacy path: owned-cache cloud and decoder integration still need
their own evidence. Owned outputs currently preserve requested coalescing under
pressure rather than dynamically reducing it; measured admission remains open.

### Real HTTP fixture validation of owned range caching

The committed SALES Parquet fixture now exercises the production `HttpObjectStore`
owned response reader, read controls, owned store-buffer bridge, and owned range
cache through a loopback HTTP server. Tests run with shared read-allocation pools
of 64 MiB and 128 MiB and two consumer scopes. Three coalesced 128-byte reads serve
prefetch plus overlapping demand with observed concurrency two; warm prefetch
adds no request. The adapter's metadata probe is excluded from these three data
requests. Every request is ranged and no full GET occurs. Decoding through owned
slices yields the same 100 rows as direct local fixture reads. All tracked read
allocations are released after consumer disposal and cache eviction. Both owned
cases and both legacy HTTP cases pass (four tests); full typecheck passes.

The test retains read outputs for the decode stage and checks their charged peak
against the configured pool. It does not charge decoded JS rows, prove a total
Worker heap cap, or establish public-cloud latency; those remain separate evidence
requirements. The transport exercises real HTTP 206 responses and bounded parallel
requests rather than substituting local-file performance for network behavior.

Owned demand reads now reserve their mandatory output before starting I/O. They
keep existing cached/in-flight coalesced windows, but reduce a new read to its
exact requested range when the configured window exceeds current shared-pool
headroom. This permits small requests under pressure without requiring a caller
to tune coalescing. Empty requests perform no I/O. If exact input plus output still
cannot fit, admission fails and releases pending ownership. Prefetch scheduling
and concurrency admission still need dynamic byte-weighted scheduling.

A shared-pool pressure test exposed a reclaimer propagation bug: a child could
have headroom while its parent lacked it, leaving a child cache unreclaimable by
sibling work. Reclaimers now register through ancestors and detach through the
same chain. Tests cover a sibling reclaiming cache storage and exact demand under
a competing allocation with a tighter parent cap. After rebuilding package
outputs, the budget/cache/owned-range suites pass 31 tests; the four real HTTP
fixture cases also pass with unchanged coalescing and parallelism assertions.
Parquet/core compilation and targeted lint pass.

Owned prefetch now waits for all launched reads to settle before reporting a
failure. Previously `Promise.all` could reject while a sibling request still held
in-flight buffers, allowing callers to mistake task rejection for completed
cleanup. The deterministic regression test fails one request while another is
held pending, disposes the caller, verifies the pending allocation remains
charged, then completes the read and checks final cleanup. All six owned-range
cache tests pass, along with Parquet compilation and targeted lint. This does not
yet abort transport I/O on scope disposal; it makes the completion boundary honest
while preserving live-buffer accounting.

Memory scopes now expose a lazily created disposal AbortSignal. Owned HTTP range
requests pass it to Fetch (including full-response fallback), and owned response
readers cancel pending stream reads on disposal. Buffers stay charged through the
cancellation callback until the read unwinds; listeners are removed on completion.
Scope-induced transport/body errors normalize to `LAKEQL_ABORTED`. Tests cover
requests waiting for headers, streams waiting for a chunk, charge retention during
cancellation, and signals first accessed after disposal. All 22 HTTP-response and
scope tests pass, with HTTP/core compilation and targeted lint passing. Shared
range-cache stages still need last-active-waiter cancellation propagation; one
consumer must not abort I/O another consumer is still using.

Shared owned range reads now track active waiters separately from unsettled
promises. Disposing one consumer leaves the transport alive for others. When the
last active waiter leaves, the shared I/O scope is disposed and its HTTP signal
aborts; pending buffers stay charged until their promises settle. A new request
can immediately start without joining the cancelled operation, and old cleanup
cannot delete that replacement request. Scope cancellation reports
`LAKEQL_ABORTED`. A deterministic two-waiter test verifies single cancellation,
retry before old cleanup completes, and zero leaked charges. The seven owned-range
unit tests and four HTTP fixture tests pass together, with Parquet compilation
passing. Whole-query signal propagation and decoded-output ownership remain open.

Read-control cancellation now reaches owned I/O while it is pending. Each controlled
owned read creates a scope linked to both the caller scope and the query/read-control
signal, then transfers a successful result into caller ownership before disposing
the read scope. External-signal listeners detach on disposal, and already-aborted
signals reject new allocation. Cancellation does not dispose the caller's broader
scope or release independently retained data. Tests verify a query signal aborts
an HTTP request waiting for headers, along with scope/link lifecycle behavior.
The HTTP/scope/reference-store suites pass 39 tests, and the owned-cache/real-HTTP
suites pass 11; HTTP/Parquet/core compilation and targeted lint pass. Query execution
still needs allocation-budget creation and decoded/operator ownership propagation;
this closes the cancellation link for the existing optional owned-read capability.

Owned prefetch admission now uses the smaller of cache capacity and current
shared-pool headroom. Ranges that cannot fit are left for demand without starting
I/O. If competing work consumes that headroom after admission, only allocation
budget failures are treated as declined speculative work; cancellation, transport
errors, and physical-read budget failures still propagate after all launched work
settles. Tests cover zero-I/O admission decline, a successful smaller demand,
pressure arriving after admission, and propagation of non-allocation failures.
Ten owned-range unit tests and four real HTTP fixture cases pass; Parquet
compilation and targeted lint pass. This is measured speculative admission, not
a byte-reserving global scheduler: required concurrent reads can still fail when
their combined mandatory allocations exceed available capacity.

### Public R2 evidence for owned reads

`bench/cloud-range-validation.mjs` now measures the production owned HTTP path,
including the ownership-aware store-buffer and range cache. Its Fetch wrapper
combines the caller cancellation signal with its timeout instead of overwriting
caller cancellation. A read-only run on November 12, 2026 passed at both 64 MiB and
128 MiB allocation-pool limits:

- Owned prefetch plus overlapping demand made four data-range requests totaling
  4,096 bytes, with peak concurrency four. Including the two-byte metadata probe,
  there were five HTTP range requests and 4,098 response bytes. Charged allocation
  peaked at 4,098 bytes; warm prefetch made no new request.
- Both cold and warm owned decode stages produced all 144 expected rows from the
  independent fixture generation contract. Cold stages each made eight data reads
  totaling 9,507 bytes. Warm stages made six/8,344 and five/8,065 reads/bytes at the
  respective pool limits. With a 4 KiB cache, completion order affects eviction.
- Cumulative charged read-allocation peaks through warm decode were 13,337 and
  13,198 bytes. Consumer disposal plus cache eviction returned each pool to zero.
- The run also revalidated the legacy path's request/concurrency assertions.

The owned decode harness retains input ownership for each decode stage and uses
an object-decoding route, while the legacy lake query can choose other physical
paths. Their timing and warm request counts are therefore not a controlled
performance comparison. This small public-cloud fixture proves transport,
ownership cleanup, and row correctness; it does not prove peak whole-query memory
near either cap, decoded-object accounting, or whole-Worker RAM usage.

Decoded cache admission now measures retained backing payload rather than logical
view lengths. The shared core `retainedBatchBytes`/`retainedVectorBytes` helpers
count full ArrayBuffer backing sizes and deduplicate buffers across columns,
validity masks, nested vectors, binary values, and dictionaries. A one-byte view
of a 64 MiB buffer therefore reports 64 MiB retained. UTF-16 string payload is
estimated conservatively; JS object/array headers and allocator overhead remain
outside this payload metric. The logical work-size estimator used by physical
planning remains separate.

The decoded vector cache now returns actual admission success, including shared
pool rejection, rather than reporting success after a rejected insertion. Tests
cover oversized backing views, shared nested backing, exact cache charging, and
failed pool admission. The estimator, decoded-cache, and real generated-Parquet
vector suites pass 27 tests; Parquet/core compilation and targeted lint pass.
Existing decoded-cache borrowers still need explicit output ownership propagation;
correcting retained size does not by itself make their lifetimes allocation-safe.

DecodedColumnCache now supports owned batch, vector, and generic work-product
entries (including dictionaries). Inserts retain existing backing ownership
without a second pool charge; acquired owners remain charged across cache eviction
and producer disposal. Vector/dictionary admission preserves existing cache policy,
and ordinary borrowed entries cannot masquerade as owned hits. Tests exercise a
composite dictionary vector at an exact 12-byte pool limit, producer disposal,
consumer pins across eviction, batch ownership, policy rejection, and mixed
owned/unowned entries. The decoded-cache and generated-Parquet vector suites pass
26 tests, with Parquet compilation and targeted lint passing. Decoder producers
still need to reserve and attach all allocations before these APIs can replace
their current borrowed-cache paths.

Parquet decoding now accepts a caller-controlled `decompressionScope` and uses
scoped Snappy/ZSTD codecs in full-column, windowed-vector, column fallback, and
object decoding. Codec output buffers are reserved before allocation and retained
for the decode lifetime; factory failures release reservations. Uncompressed
pages continue to alias their input backing and require input ownership rather
than a fictitious decompression charge. Generated Snappy fixtures exercise both
full-column and windowed vector paths, verify all decoded rows, and prove a zero
output budget rejects before decompression allocation. Codec tests also cover
exact admission, malformed input, and failure cleanup. All 21 codec/vector tests
pass, with Parquet compilation and targeted lint passing.

This scope tracks decompression outputs only. Codec-internal workspace, decoded
arrays/objects, normalization allocations, and transfer of backing ownership into
batches/caches remain to be integrated. Callers currently retain this scope for
the whole decode lifetime; it is not yet a streaming whole-query memory cap.

Late-materialized reference collection now shares the stable bounded heap used by
row top-k, replacing the per-candidate O(k) worst-reference scan with O(log k)
selection. Its serialized-reference accounting updates only accepted/replaced
references and skips serialization without a byte limit. The unordered
late-materialized LIMIT path also accounts each appended reference once instead
of reserializing the growing reference array. Existing comparator ordering and
final materialization behavior remain intact.

Route-specific tests verify exact serialized-byte admission and one-byte-under
rejection for ordered/unordered late materialization, plus top-k tie/offset parity
with a full-sort oracle and predicate-column-only initial reads. The query, heap,
and incremental accounting suites pass 80 tests; core compilation and targeted
lint pass. Row aggregate-state accounting and window GROUPS/peer work remain open,
as does allocation ownership for references and materialized outputs.

Row aggregate budget accounting now updates the changed group's charge instead of
sorting/snapshotting every group after every input row. It initializes restored
groups once, keeps aggregate byte and buffered-value totals, includes JSON array
separator overhead exactly, and avoids accounting snapshots during updates when
neither budget is configured. Final bookmark serialization remains unchanged.
Tests compare incremental admission against complete serialized snapshots after
each prefix of Unicode/escaped-key input, cover exact/one-byte-under thresholds,
restored state, and buffered median/distinct-value limits. All 76 query tests pass;
core compilation and targeted lint pass.

This removes work proportional to all groups per update. A large variable-size
state within the changed group still creates a snapshot for accounting; per-state
mutation deltas and allocation ownership remain required to eliminate that cost
and prove physical memory bounds.


Window frame evaluation now uses explicit evaluation-local peer cursors in both
row and segment-tree backends. Three peer positions (current row, GROUPS start,
and GROUPS end) plus a lazily counted group total replace repeated peer-array
construction on sequential evaluation. Auxiliary storage is constant; there is
no partition-sized index to allocate. Equality is still the existing contiguous
`samePeer` rule, with no assumption that representation equality forms a binary-
searchable sort key. The direct frame helpers retain the uncached path as an
independent oracle and for callers without an evaluation context.

Tests compare spans and excluded rows with that oracle under arbitrary access
order, changing offsets, repeated noncontiguous equality classes, all exclusion
modes, and empty/single-peer partitions. A 10,000-row sequential GROUPS test checks
all spans and bounds key visits below 100,000. All 29 peer/frame/backend tests
pass; core compilation and targeted lint pass. Nonmonotone endpoints remain
correct but can traverse large distances repeatedly; adaptive budget-admitted
indexing remains an opportunity for those workloads. Frame materialization and
aggregate computation have separate costs from peer discovery.

The reproducible CPU harness now benchmarks GROUPS cursor lookup against
`b54d4ed`, checking complete span parity first. On Node v24.18.1, with one warmup
and the median of three samples, groups of five equal keys measured:

| Partition rows | Baseline GROUPS lookup | Cursor lookup |
| --- | ---: | ---: |
| 2,000 | 978.756 ms | 2.003 ms |
| 5,000 | 6,234.226 ms | 4.426 ms |

This measures fixed-offset frame discovery only, not complete window aggregation
or cloud latency. The cursor holds constant auxiliary state. The same run passed
all existing kernel parity checks; the independent reference suite passed all
81 tests after the cursor integration. Reproduce with a current core build and
`node bench/performance-memory.mjs`.

Allocation reservations now support `retainIn` within one budget tree. Each budget
holding the allocation is charged its full backing size; common ancestors are
charged once regardless of reader count. Parent/cache retention can outlive the
producing query without keeping that query's local charge alive. A new reader must
pass its own cap before obtaining a handle. Admission failure is atomic, unrelated
root pools are rejected, and release drops each local/ancestor charge only after
its final applicable handle is gone. This replaces the prior reservation behavior
that permanently attributed all retained handles to the original query.

Tests cover sibling readers, duplicate pins, promotion to parent/cache ownership,
tighter reader limits, nested shared ancestors, existing reader allocations,
released handles, and unrelated roots. The reservation/scope/cache suites pass
37 tests, and an additional atomic-admission test passes in the nine-test budget
suite. Core compilation and targeted lint pass. OwnedMemory wrappers and cache
admission still need to expose/use this cross-budget reservation contract before
cross-job cache reuse is integrated end to end.

OwnedMemory now exposes cross-budget retention through scalar, composite, HTTP-view,
and Parquet-buffer ownership wrappers. SharedMemoryCache promotes inserted owned
payloads into its budget and can acquire a hit into a reader's budget. Parent
storage is charged once while each reader passes its own cap; unrelated pools
remain incompatible. Composite admission rolls back partial pins on failure.
Decoded-cache owned acquisition accepts the same reader budget.

Owned range-cache hits now participate in that contract. A cached coalesced window
that exceeds a reader's cap is declined, allowing measured exact-range fallback.
Tests verify producer-local charges disappear while reusable cache data survives,
cache eviction does not uncharge live consumers, tighter readers reject oversized
pins, composite rollback, warm cross-job reuse without I/O, and smaller exact reads
under tight reader limits. The scope/cache suites pass 31 tests; owned-range,
decoded-cache, and HTTP-response suites pass 38 tests. Parquet/HTTP/core compilation
and targeted lint pass. In-flight deduplication is still per-budget, and query
creation/decoder/operator integration remains outstanding.

Near-cap tests now fill real typed buffers at both 64 MiB and 128 MiB: one shared
input occupies half the pool and two independently capped reader jobs each allocate
a quarter-pool output. The root reaches exactly its limit while each reader is
charged for its input pin plus output. An additional byte is rejected before its
factory runs. Cache eviction cannot uncharge either active reader, and staged
reader disposal releases the pool to zero. The budget/scope/cache suites pass
42 tests including these cases; they test tracked buffers, not process heap size.

The public R2 validator now uses separate child budgets and verifies a later job
reuses cached data after both original scopes close. Its first run exposed five
requests instead of four because pending reads were keyed by child budget.
Pending read maps now use shared allocation-pool identity within each range-buffer
instance; each waiter retains returned backing in its own child budget. Unit tests
verify one in-flight read across two child budgets. The cache/owned-range suites
pass 35 tests. The R2 rerun passes at both pool limits with four data reads,
4,096 data bytes, concurrency four, no new request for the later job's cached
read, all 144 decoded rows correct, and zero charges after final cleanup. Sharing
pending reads across separate range-buffer instances remains outside this change.

Cross-budget admission now pins its source throughout synchronous reclamation.
A reclaimer can evict/release the original source handle while the target budget
is admitting a new reader; without a guard, live backing could temporarily become
uncharged. Composite outputs pin their complete backing set before admitting any
component, so a callback disposing the producer cannot drop later components.
Deterministic tests invoke exactly those reentrant releases and verify charges
remain live throughout admission, ownership transfers correctly, and final cleanup
returns to zero. All 44 reservation/scope/cache tests pass, including the near-cap
real-buffer cases; core compilation and targeted lint pass.

### Growing row aggregate states: incremental serialized accounting

Row aggregate budget checks now measure each buffered value once and adjust only
changed entries. Median and quantile append charges; distinct aggregates charge
new keys without sorting their sets; mode subtracts the prior entry and adds its
updated count, including decimal digit transitions. Restored states are measured
once on first byte-budget use. Row-only budgets read collection sizes without
materializing snapshots. Final bookmark serialization and aggregate results retain
their existing formats and ordering.

Exact byte-boundary tests compare against independently serialized complete
operator states at every prefix, including escaped Unicode, lone surrogates,
null inputs, duplicate values, mode counts crossing ten, and restored state plus
new rows. A 2,000-row test bounds total JSON serialization volume relative to final
state size to catch quadratic rescanning. The full suite passes 1,027 tests with
97 skipped (92 files passed, seven skipped); repository lint and typecheck pass.
These checks preserve the existing serialized operator-size contract; they do not
turn those estimates into physical allocation bounds or account for final sorting
workspace. Query-wide allocation propagation remains outstanding.

### Shared stream admission and native R2 ranges

The bounded HTTP response reader now delegates to core `readOwnedStream`, which
provides the same reservation-before-allocation, overlapping-buffer growth charges,
full-backing ownership, and cancellation cleanup for other streaming adapters.
HTTP retains its decoded-response limit and error metric. The core reader also
rejects invalid byte limits and cancels the supplied stream before reading it.

`R2ObjectStore.getRangeOwned` now consumes the binding's streaming body through
that reader. It bounds output by requested range length, handles whole-object
`size` correctly when reserving a partial tail, validates safe integer ranges,
and cancels bodies delivered after scope disposal. Custom R2 implementations
without a stream fail explicitly rather than bypass admission via `arrayBuffer`.
The binding's pending `get` cannot itself be aborted; body consumption can.

A real local workerd R2 binding stores a 4,096-byte fixture and verifies exact
128-byte reads, six-byte tails, one-byte-under budget rejection, and zero charges
after cleanup. All nine workerd tests across seven files pass. This exercises the
native binding, not public-cloud latency or remote service behavior. Unit suites
for R2, shared streams, and HTTP responses pass 28 tests, including oversized-body
cancellation, invalid ranges, and scope closure during response arrival. Lint
passes. Normal query creation/ownership propagation and S3 owned reads remain
outstanding; this is not an end-to-end query or Worker heap cap.

### S3 owned range transport

S3 now exposes `getRangeOwned` through the shared owned stream reader. The signed
request carries the scope's abort signal; scope closure during credential lookup
or signing prevents fetch, and transport aborts normalize to the query abort
error. Successful owned reads require status 206, a matching safe-integer
Content-Range (including object-tail truncation), identity encoding, and an exact
body length. Invalid responses are canceled; truncated outputs release their
reservation before failing. Zero-length requests avoid I/O, and invalid range
integers are rejected. The legacy byte-returning read path is unchanged.

The S3 suite passes all 18 tests. A real loopback HTTP server serves a deterministic
4,096-byte fixture, observes signed Range requests, and proves exact 128-byte
admission, a six-byte tail, one-byte-under rejection before engine allocation,
and cleanup. Injected responses cover ignored ranges, wrong offsets, short/long
bodies, and cancellation. This is actual HTTP transport evidence, not AWS signature
validation or public-cloud performance evidence. S3 compilation, repository
typecheck, and lint pass. Query-wide creation and propagation of allocation
ownership remain unfinished.

### Query allocation-pool creation and propagation

Lake now creates a shared `memoryPool`, and each QueryResult creates a distinct
child `memoryBudget`. Callers may supply a pool shared across Lakes and caches.
`budget.maxAllocatedMemoryBytes` sets the query allocation cap independently of
serialized operator-size accounting. Without an explicit allocation limit, the
existing `maxMemoryBytes` is used when supplied; otherwise an internally created
pool defaults to 64 MiB. A supplied pool supplies the default child ceiling.
Admission uses actual shared headroom, rather than dividing capacity by an assumed
concurrency count. This is a tracked-allocation contract, not a process-heap cap.

The query child is passed to scan options, object/task planning, and window
execution options, including late materialization paths. Adapters can allocate
scratch in MemoryScopes and release it in generator cleanup; backing that escapes
must retain separate ownership. No query-wide scope is automatically disposed
while consumers could still retain output.

New query-level tests exercise two simultaneously suspended iterators, shared
pool exhaustion by a third query, tighter child limits with spare parent capacity,
early iterator return, reuse after cleanup, and default/explicit limits. These
use an allocation-aware fixture scanner whose scratch does not escape, and do
not claim that existing Parquet decoded outputs are accounted. Query, query-memory,
and window-runner suites pass 85 tests; core compilation and lint pass. Parquet
must still consume this propagated budget for complete decode/output lifetimes;
metadata, operators, and returned outputs also require their remaining ownership
integration before an end-to-end cap can be claimed.

### Decode-stage backing handoff

MemoryScope can now retain its completed live allocations as an independent
composite owner. This captures possible aliases without copying payloads or
releasing their charges when the producer closes. Released, pending, and future
allocations are excluded. A regression test retains a subarray, settles a separate
pending allocation, disposes the producer, and verifies only the subarray's full
backing remains charged until its consumer releases it.

The internal ParquetReadScope connects a decoder's borrowed AsyncBuffer interface
to owned range reads and optional owned prefetch. Its allocation scope is also
usable by the scoped compressors. It never falls back to borrowed prefetch or
untracked reads. After decode settles, retainInputBacking preserves existing
input/decompression backing across stage disposal. This conservative handoff does
not account for newly allocated decoded arrays or objects; those require separate
admission. It is not yet selected by the normal scan adapter.

A real generated Parquet fixture with integer and Unicode string columns decodes
through this bridge; retained batches remain correct after stage disposal and
charges return to zero after consumer release. Failure tests cover unfunded reads,
malformed metadata cleanup, and adapters missing owned reads. All 15 scope/bridge
tests pass; core/Parquet compilation and repository lint pass. Normal scan
integration and decoded-storage admission remain required before claiming a query
memory bound.

### Admission for engine-created typed Parquet vectors

ReadParquetBatchOptions now accepts `allocationScope`, used for engine-created
typed vector buffers as well as decompression outputs. The existing decompression
scope remains supported. Nullable numeric outputs, validity masks, dictionary
indices, and typed conversions reserve their exact element-width payload before
constructing the typed array. Reinterpretation and subarray views do not acquire
a second allocation charge. Object/column fallback compression also observes the
unified scope. The stage's retained-allocation handoff includes these admitted
buffers when they share its scope.

Real uncompressed Parquet fixtures exercise INT32 through full-chunk and early-stop
page paths, plus FLOAT, DOUBLE, INT64, and BOOLEAN nullable outputs. Four INT32
rows require exactly 20 bytes (16 value bytes plus four validity bytes); a 19-byte
budget fails before creating the value buffer, with peak charge four. All cases
verify decoded values and cleanup. Vector, compressor, and stage suites pass 30
tests; Parquet/core compilation and lint pass.

This covers typed output allocations made by the engine conversion functions.
Hyparquet's decoded arrays, definition levels, strings/JavaScript arrays, fallback
vector construction, cache ownership, and codec workspace still require admission
or ownership integration. Existing non-null typed arrays may alias upstream
backing; they are deliberately not misreported as newly admitted allocations.
Normal scan activation remains incomplete.

### Numeric PLAIN alignment-copy admission

Inspection of the installed hyparquet decoder confirms there is no allocation
callback in its page-reader interface. Its numeric PLAIN reader returns views for
aligned input but creates hidden ArrayBuffer copies for misaligned input. A local
numeric reader now admits those copies for INT32, INT64, FLOAT, and DOUBLE when an
allocation scope is supplied. Aligned reads remain zero-copy aliases. It checks
count, byte size, and DataView-relative bounds before allocation or cursor advance.
The dictionary-page reader uses this path; other physical representations still
use the upstream PLAIN reader.

Tests cover all four widths, combined DataView/reader offsets, exact admission,
one-byte-under rejection without cursor advancement, aligned aliases with no new
charge, malformed lengths, and full INT64 precision. A real numeric dictionary
Parquet fixture verifies both the alignment allocation and dictionary-index
allocation occur through the scope and that cleanup releases all charges. All
28 numeric-reader/vector tests pass; Parquet compilation and lint pass.

This does not intercept upstream data-page PLAIN reads, definition/repetition
levels, encoding-specific arrays, string conversion, or decoder workspace. Those
need an allocation-aware page decoder interface or equivalent controlled decoding;
a retained-output estimate after allocation would not establish admission bounds.

### Broad regression and cloud checkpoint after decoder admission changes

The default suite passes 1,059 tests with 97 skipped across 96 passing files and
seven skipped files. All nine workerd tests across seven files pass, including the
native R2 fixture; all 81 independent reference tests pass. The large-file gate
passes. The workerd cancellation case emits its runtime's `pump canceled`
diagnostic while the cancellation assertions and test suite pass.

The public R2 validator passes again at 64 MiB and 128 MiB pool configurations.
Owned coalesced prefetch uses four data reads / 4,096 data bytes, concurrency four,
and peak tracked backing of 4,098 bytes (including the HTTP probe). Later-job warm
reuse issues no extra range read. Both cold and warm decode phases reproduce all
144 expected rows; cold phases use eight reads / 9,507 bytes. Warm owned phases
use five or six reads depending on completion order in the small cache. Peak
tracked backing in those phases is approximately 13 KiB, and final cleanup is
asserted to return to zero. These are small-fixture transport/backing checks, not
near-cap full-decoder or process-memory evidence. The original serialized-budget
query path and explicit owned-read harness remain distinct routes.

### Admitted flat numeric PLAIN data pages

The scoped vector reader now selects a local allocation-aware page path for flat
INT32, INT64, FLOAT, and DOUBLE PLAIN pages. Both V1 and V2 use admitted typed
definition-level buffers and admitted numeric alignment copies; aligned numeric
values remain aliases of existing page backing. V2 verifies flat row/null counts
and level lengths. A bounded hybrid-level decoder writes into the admitted output,
rejecting zero runs, oversized RLE runs, truncated bit packing, invalid levels,
and overflowing headers. It does not expand output based on encoded run lengths.
Unsupported encodings/representations retain their existing decoder route and are
not claimed as allocation-bounded.

Exact-budget fixture expectations now include the formerly hidden page allocations:
four nullable INT32 rows require 32 bytes in the tested layout (4 definition,
8 alignment, 4 validity, 16 output), rather than the earlier output-only 20 bytes.
V1/V2 synthetic encoded pages verify 12-byte level/alignment admission and
one-byte-under failure; real writer fixtures cover nullable numeric widths and
Snappy full-chunk/early-stop execution. All 34 numeric-page/vector/compressor tests
pass, with Parquet compilation, repository typecheck, and lint passing.

Remaining decoder gaps include non-PLAIN encodings, binary/string and nested
representations, conversion allocations, fallback readers, and codec workspace.
The normal scan adapter still needs the complete ownership lifecycle before it
can activate these scoped paths without prematurely releasing escaped outputs.

### Admitted dictionary indices and zero-copy vector reuse

The admitted flat-page path also handles PLAIN_DICTIONARY/RLE_DICTIONARY indices
using bounded hybrid decoding into a pre-admitted Uint32Array. It validates bit
width and dictionary bounds, including zero-width index streams and empty
dictionaries. Non-null dictionary vectors reuse the admitted indices as views;
nullable scatter still creates its separately admitted output. Dictionary payload
representations have their own remaining allocation coverage requirements.

Tests enumerate all 16 four-row null patterns across V1/V2 and RLE/bit-packed
levels, including all-null/all-present pages and padding. Dictionary tests cover
exact 20-byte level/index admission, 19-byte rejection, invalid dictionary indices,
and zero-width indices. The real numeric-dictionary fixture confirms the redundant
vector-index allocation is absent while decoded INT64 values remain exact. All
31 page/vector tests pass, as do Parquet compilation, repository typecheck, and
lint. Other encodings and normal scanner ownership integration remain outstanding.

### Numeric byte-stream-split admission

The admitted flat-page decoder now transposes BYTE_STREAM_SPLIT values for INT32,
INT64, FLOAT, and DOUBLE into one pre-admitted backing allocation. It validates
count, cursor, and source byte bounds before allocating or advancing the cursor.
The typed interpretation aliases that backing rather than creating another copy.
This works through the same V1/V2 level/decompression path as numeric PLAIN pages.

Tests cover all four numeric widths, nonzero DataView/reader offsets, exact caps,
one-byte-under rejection without cursor movement, source truncation, and cleanup.
A real nullable FLOAT fixture with Snappy compression runs through full-chunk and
early-stop paths at exactly 40 bytes of tracked page allocations; 39 bytes rejects
the final output buffer with a 24-byte peak. The 40 bytes comprise four level,
eight decompression, eight transpose, four validity, and sixteen output bytes.
All 42 plain/page/vector tests pass; Parquet compilation and lint pass. Delta
encodings, other physical representations, conversion workspaces, and normal
scanner lifecycle integration remain unfinished.

### Admitted integer delta decoding

The scoped flat-page reader now decodes DELTA_BINARY_PACKED INT32/INT64 values
into pre-admitted typed output. Bit-width metadata is read directly from the input,
removing the upstream per-block Uint8Array workspace. The decoder validates block
and miniblock geometry, encoded counts, varint bounds, widths, and complete padded
miniblock spans. Cursor updates commit only after success. Zero-count headers and
zero-width miniblocks are handled explicitly; INT64 arithmetic remains bigint.

Real 300-row Snappy fixtures cover nullable INT32 and INT64 values spanning multiple
blocks, including INT64 values above Number's safe integer range, in full-chunk and
early-stop modes. Exact measured allocation caps reproduce all rows; one byte less
rejects admission and disposal releases charges. Unit tests cover both signed INT64
extremes, singleton/empty streams, zero-width blocks, malformed geometry/counts,
overflow/truncation, and cursor rollback. All 40 delta/vector tests pass; Parquet
compilation, repository typecheck, and lint pass. String/binary delta encodings,
other representations, conversion workspaces, and full scanner lifecycle
integration remain outstanding.

### Measured delta throughput and INT32 correction

`bench/decoder-memory.mjs` compares the installed upstream delta kernel with the
admitted implementation, checks every decoded value, and verifies exact peak
output admission and cleanup. It reports median-of-seven timings after three
warmups for 5,000 and 50,000 values. This is a CPU kernel comparison, not cloud
latency or whole-query memory evidence.

The first run found a constant-delta INT32 regression (50,000 values: 0.49 ms
upstream versus 1.44 ms admitted). INT32 reconstruction now uses exact number
arithmetic with explicit 32-bit wrapping; bigint remains for header interpretation
and INT64 values. The sum of signed current/minimum and unsigned packed delta
fits exactly in a JavaScript number. Tests cover positive/negative and large
minimum deltas plus full-width unsigned packed deltas across signed boundaries.

On Node 24.18.1, the corrected 50,000-value run measured 0.55 versus 0.43 ms for
linear INT32, 3.99 versus 1.65 ms for varying INT32, and 3.70 versus 3.95 ms for
varying INT64 (upstream versus admitted). Small timings vary with JIT/scheduling;
the INT64 result includes admission/cleanup and is not claimed as a speedup.
All 42 delta/vector tests pass, with compilation, typecheck, and lint passing.


### Measured adaptive GROUPS indexing

WindowPeers now starts with constant-space cursors and measures peer comparisons.
After work exceeds four partition passes, it attempts to admit one Uint32 boundary
index (four bytes per group plus a sentinel). Monotone traversals avoid that
allocation. Failed admission retains the cursor fallback without exceeding the
query cap. Group lookups become constant-time and row-to-peer lookup uses binary
search after index creation. Row, run, and vector window execution receive the
query child budget; partition evaluation releases the optional index in finally,
including when a later dynamic frame expression throws. Standalone callers with
no allocation budget retain constant-space behavior.

Tests verify no allocation for monotone access, exact index caps, one-byte-under
fallback, bounded peer-key visits for repeated distant seeks, results against
explicit expected sums, and cleanup on success/error. All 29 targeted peer/backend/
runner tests pass; core compilation, repository typecheck, and lint pass.

`bench/window-peer-memory.mjs` verifies results and reservation cleanup and measures
median-of-five timings after warmup. Node 24.18.1, 1,000 alternating distant seeks,
five rows per peer group: 5,000 rows improve from 1,009.37 ms with cursors to 7.20 ms
adaptively, using 4,004 index bytes; 20,000 rows improve from 4,095.80 to 28.30 ms,
using 16,004 bytes. Construction and admission are included. These are adversarial
CPU traversal timings, not general window-query or cloud latency claims. Other
window working storage still needs complete allocation coverage.

### Window task allocation-budget propagation

Parquet task options now carry the caller's allocation budget into scan options
and final window work-unit evaluation. The scan adapter forwards the query child
through its task-execution route, and planning read options preserve it when
constructed. Allocation-only task options no longer disappear merely because
serialized QueryBudget/statistics were omitted.

A real 800-row Parquet fixture split across two row groups runs two tasks and fans
in a dynamic GROUPS sum. With 3,204 bytes available the adaptive boundary index is
admitted; with 3,203 bytes it falls back to cursors. Both reproduce independently
computed results and return tracked charges to zero. All six task-execution tests
pass; Parquet compilation and repository lint pass. This proves index-budget
propagation through task fan-in, not allocation coverage of task rows, partials,
metadata, or decoded pages.

### Admitted boolean page decoding

Scoped flat PLAIN and RLE boolean pages now decode into admitted Uint8 storage
instead of upstream JavaScript boolean arrays. The vector conversion preserves an
explicit boolean marker and produces bool vectors, so true/false/null semantics
remain unchanged. Definition-level, compact-value, validity, and nullable-scatter
buffers are each admitted before allocation. PLAIN payload length and RLE boolean
values are checked; the bounded hybrid decoder also applies to boolean RLE.

Real six-row nullable boolean fixtures cover both encodings in full-chunk and
early-stop modes. Exactly 22 bytes admits the tested layout, while 21 bytes rejects
and disposal cleans up. Synthetic V1/V2 pages verify bit ordering and both encodings
at an exact eight-byte level/value cap. All 43 page/vector tests pass; Parquet
compilation and lint pass. Strings, binary/nested representations, conversion
workspaces, and activation through the complete normal scanner lifecycle remain
unfinished.

### Packed binary layout component

The upstream binary PLAIN reader allocates a JavaScript array and one view object
per value; UTF8 conversion adds another array. An internal packed binary reader
now validates the complete length stream before allocating a Uint32 start/length
table (eight admitted bytes per value). Payload bytes alias the original input,
and individual views are produced only on access. Fixed-width input uses implicit
offsets and no table. Variable-length offsets have an explicit 32-bit relative
range bound; source DataView offsets are handled separately. This avoids presenting
JavaScript object-size estimates as exact allocation admission.

Six tests cover exact/one-byte-under table caps, empty values, binary and Unicode
payload bytes, nonzero source offsets, fixed-width aliases, malformed lengths,
cursor preservation on failure, and cleanup. Parquet compilation and lint pass.
This component is not yet consumed by the normal vector reader. Packed binary/
string vector integration, decoding string outputs, and output ownership remain
required; its tests do not prove full Parquet binary-query memory bounds.

### Generic packed vector shapes

The core vector contract now includes explicit `binary-view` and `utf8-view`
shapes: one payload, either interleaved start/length ranges or an implicit fixed
width, a row offset, length, and optional validity. Generic scalar access decodes
UTF8 on demand or returns a binary view. Gathering retains the payload through a
dictionary vector rather than constructing one view/string per output row.
UTF8 grouping uses the generic scalar key contract. Parquet vector slicing
preserves payload storage and adjusts row offsets and validity independently.
Retained-byte accounting counts complete payload, range, and validity backing
buffers, deduplicated across columns and nested dictionary references.

The full TypeScript build and 96 targeted core/Parquet tests pass. New cases cover
multibyte Unicode, empty and null values, nonzero row offsets, repeated gathered
rows, dictionary grouping, predicates, fixed-width binary aliases, concatenation,
and multiple views sharing one backing buffer.

This is representation infrastructure; the packed Parquet reader is not yet wired
to produce it. Concatenation can still materialize scalar values, UTF8 scalar
access allocates strings, and gathered indices are not yet admitted through a
query scope. Normal scanner ownership, real binary Parquet fixtures, and cloud
query peak-memory validation remain required before claiming bounded execution.

### Scoped PLAIN binary and string page integration

Scoped flat PLAIN BYTE_ARRAY decoding now produces packed vectors for raw bytes
and UTF8/STRING logical values. Required and all-present pages directly expose
the packed payload/range storage. Nullable pages map logical rows to compact
present values with admitted Uint32 indices and byte validity, retaining an
empty dictionary safely for all-null pages. Other logical conversions continue
through their existing path; this does not reinterpret JSON, decimals or UUIDs.

Real writer-generated fixtures cover raw binary and multibyte text, empty values,
mixed nulls, required/all-present/all-null columns, one-row output batches and
nonzero requested row ranges. Both full-chunk and early-stop readers pass. The
six-row mixed-null fixture needs exactly 68 admitted bytes (six definition bytes,
four eight-byte ranges, six four-byte indices and six validity bytes); 67 rejects,
and disposal returns usage to zero. Synthetic V1/V2 pages additionally verify
empty/binary values and malformed payload rejection before range-table admission.
All 58 targeted decoder/vector/layout tests and the full TypeScript build pass.

These bounds cover decoder-created typed storage, not total query memory. The
payload aliases its input, whose ownership must be retained independently.
Dictionary-encoded binary dictionaries, other binary encodings, scalar string
allocations, codec workspace, generic operator allocations, and activation through
the normal scanner ownership lifecycle remain unfinished. These local fixtures
do not substitute for cloud transport and near-cap concurrent-query validation.

### Explicit decoded-batch ownership handoff

An internal owned vector-batch iterator now separates decoder-stage ownership
from receiver ownership. Each physical row group has a stage; yielded owners
conservatively retain all completed admitted backing at handoff. The stage is
disposed on normal completion, failure, cancellation, or early iterator return.
Receivers explicitly release their owners, which remain valid and charged after
stage disposal. Borrowed decoded-cache options are excluded from this interface
because they cannot provide a safe ownership transfer.

Real Parquet fixtures verify equal tracked peaks for one versus twenty row groups
when outputs are consumed and released, retained output after early return,
requested row offsets across groups, cancellation with retained output, admission
failure cleanup, and successful five-group execution at the measured exact cap
with rejection one byte below it. Seven ownership/read-scope tests pass; repository
type checking and lint pass.

This iterator is not yet selected by the normal scanner. Its physical lifetime
boundary is a row group, not an output batch; page and row-group storage must fit
the admitted budget. Existing vector output may also exceed the requested batch
row count at page boundaries. Cross-row-group prefetch, owned decoded-cache reuse,
metadata admission, operator handoff and bounded output slicing must be integrated
before replacing the current cloud scan path. This checkpoint makes no claim of
full query, JavaScript heap, or Worker isolate bounds.

### Bounded direct-vector output batches

The aligned Parquet vector reader now splits page intersections into at most
`batchSize` rows (default 4096). It rejects zero, negative, fractional, non-finite,
and unsafe integer sizes before reading data. Numeric, dictionary and packed
payload slices retain their original backing; output row offsets remain absolute
and the final partial batch is preserved. Source iterators are closed on normal
completion, early return and errors. Previously, direct pages could ignore the
requested output row count; existing shape tests now assert bounded slices and
verify complete results across all slices.

Real fixtures cover full-chunk and early-stop reads, a nonzero start, a partial
final batch and shared numeric backing across all emitted slices. The owned-batch
early-return test now receives exactly its requested one-row output. All 52
targeted vector/ownership tests pass, as do repository lint and type checking.
The full suite passes with loopback networking enabled: 1,130 tests passed and
97 skipped across 101 passing files. The sandboxed attempt could not run local
HTTP servers and CLI recipes; the approved rerun resolved those failures.

This is an output row-count guarantee, not a page-decode allocation guarantee.
Physical page/row-group storage still needs admission. Legacy string-array slicing
copies references and nested slicing still materializes values; generic view-based
nested slicing and admission of those remaining allocations are outstanding.

### Shared vector slicing without nested materialization

Core `sliceVector` and `sliceBatch` now provide validated contiguous slicing for
all vector shapes. Parquet page alignment uses this shared implementation.
Lists and maps retain their children and slice offset views without rebasing;
structs recursively slice their fields. Validity is sliced independently, including
repeated packed-vector slices. Entire retained backing remains charged by the
existing retained-byte estimator. Parquet grouped-task offset/limit handling also
uses batch slicing, eliminating its contiguous gather-index array and payload copy.

Tests cover repeated nullable list slices, empty intervals, nested structs, map
keys/values, packed row offsets, invalid bounds, empty projections, shared buffers,
and retained-byte accounting. The 69 targeted core/Parquet/owned/task tests pass,
along with the TypeScript build, repository type checking and lint. Existing real
nested Parquet fixtures remain correct under bounded batch output.

`node bench/vector-slice.mjs` checks every output row against the former nested
materialization path, asserts shared child/offset storage, then measures seven
samples after three warmups. For 2,000 rows with 8 or 128 children each, this local
run measured materialization medians of 1.01 ms and 11.02 ms; view slicing was below
0.002 ms in both cases. Those tiny view timings are timer/JIT-sensitive, not an
end-to-end speedup claim. The structural guarantee is no nested child traversal
or payload allocation during list/map slicing. Legacy UTF8/binary JS arrays still
copy reference arrays; view objects and struct field maps still allocate ordinary
JavaScript objects. Full scanner/operator ownership remains outstanding.

### Owned decoding over real HTTP ranges

The HTTP fixture suite now runs the owned vector decoder against the actual
HTTP adapter and range cache, with separate concurrent child budgets under both
64 MiB and 128 MiB roots. Earlier range/metadata owners are disposed and the cache
is cleared before the concurrent jobs, so this exercises cold in-flight range
deduplication. Every observed cold physical range is unique across the jobs;
both produce the same projected sales rows as the local reference. Seven-row
batch limits hold. A subsequent job reuses the cache without additional requests.

An early-return job retains one output owner after closing its iterator, verifies
the row remains readable and its child budget remains charged, then releases it
and verifies zero child usage. Completed jobs similarly release all child pins;
the shared cache remains charged to the root until explicitly cleared. The test
also verifies no full GETs, at most two active HTTP reads, and zero root usage
after final cleanup. All four HTTP fixture cases pass with loopback access.

These are real HTTP transport semantics with a local fixture server, not a remote
provider latency measurement or a near-cap Worker load test. The fixture is small;
parsed metadata, materialized output objects and uninstrumented decoder workspaces
are still outside the asserted allocation bound. Normal query-path activation and
cross-group prefetch scheduling remain required.

### Memory-aware prefetch for owned row-group decoding

The owned iterator now reuses the direct-reader range planner to prefetch across
selected row groups. Windows contain at most the requested concurrent-read count
(default four), and their compressed ranges must fit within half the currently
available shared-budget headroom. This is a conservative headroom policy, not a
decoded-size prediction. The owned range cache still performs final admission
using actual backing sizes and can decline speculative work. Demand decoding
retains its separate row-group scope; prefetch scopes are closed after each window.
The ObjectStore's physical read semaphore remains the concurrency authority.

Tests cover two-group windows across five groups, closing prefetch scopes, skipping
speculation when another job leaves only measured demand headroom, rejecting
invalid/unbounded window counts, and stopping after the first window on early
consumer exit. The four real HTTP fixtures pass with this scheduling enabled,
including concurrent cold deduplication, warm reuse and the two-read physical
concurrency cap. Normal scanner activation, owned decoded-cache handoff, metadata
and operator accounting, and near-cap Worker validation remain outstanding.

### Lazy prefetch planning and failure cleanup

Owned prefetch no longer materializes range plans for every selected row group.
The shared planner exposes a lazy iterator, and the owned reader retains only
its bounded lookahead window. A fixture makes the third group's column metadata
throw on access; a two-group window successfully yields and exits without touching
it. Existing window-size and shared-headroom assertions remain green.

Additional tests verify that a prefetch transport error propagates unchanged after
its admitted response storage is released, and a job cancelled before iteration
starts performs no speculative calls or allocations. All 15 ownership tests and
four real HTTP fixture cases pass, alongside the build, type checks and lint.
The borrowed reader still uses the materialized planner wrapper; parsed metadata
itself is not allocation-accounted. These remain distinct from the now-bounded
owned prefetch plan storage.

### Lazy planning in the normal direct reader

The normal direct-vector reader now consumes the same lazy range planner one
prefetch window at a time. The full selected-group array and membership set have
been removed. A reader without a prefetch capability does not construct a range
plan at all. This brings bounded plan storage to the active scan path independently
of the unfinished allocation-ownership integration.

Real fixtures verify early exit after one row without inspecting column metadata
beyond a two-group lookahead, and correct two/one-group windows across interleaved
predicate-pruned groups. All 65 vector/ownership tests and four HTTP range fixtures
pass, along with build, type checking and lint. Physical decode buffers, parsed
metadata, owned decoded-cache integration and full query/Worker bounds remain
separate outstanding requirements.

### Scoped decoded-cache handoff

The vector decoder now passes its allocation scope into decoded batch, page and
dictionary cache operations. Scoped reads acquire an owned cache entry in the
reader's budget and adopt that pin into its stage. Scoped writes retain completed
stage allocations independently for the cache, releasing the temporary handoff
owner afterwards. An adoption failure releases the acquired pin, including when
the receiving scope was already closed.

The owned iterator now accepts decoded-cache configuration. Its keys include an
owned-path namespace and absolute row-group start, preventing local row offsets
in the per-group metadata view from colliding across groups. Existing cache
admission policy remains in force. A real three-group fixture verifies cold page
admission, later-job reuse without new decoded page writes, zero producer-child
usage after completion, and readable retained output after both iterator return
and cache eviction. The final receiver release returns root usage to zero.

All 79 decoder/cache/ownership tests pass, together with build, type checking and
lint. The four real HTTP fixtures also pass with owned decoded-cache reuse enabled
for concurrent jobs under 64/128 MiB roots. Retention conservatively captures stage
backing and can keep more memory than an individual cached page requires. It does
not account uninstrumented JS arrays/strings or codec workspace. Selective backing
ownership, normal query activation and full Worker memory validation remain open.

### Oversized decoded-cache pins fall back to decoding

Scoped decoded-cache lookups now decline a cache hit when transferring its backing
would exceed the reading job's allocation budget. Only the allocated-memory budget
error is treated as a miss; cancellation, ownership mismatch and other failures
still propagate. A conservative cached stage can retain much more than the working
set needed to decode the same page, so optional reuse must not prevent a fitting
decode. Mandatory decoder allocations remain subject to admission.

Cache-level tests cover vector, dictionary and batch hits with 64-byte backing
under a four-byte reader budget, verify no leaked child pin, and admit a four-byte
fresh output. A real Parquet fixture caches a page with an additional 2 KiB stage
workspace, then correctly re-decodes it in a 1 KiB child budget and replaces the
entry. Producer/reader/cache cleanup returns root usage to zero. All 31 cache and
ownership tests pass, as do build, type checking and lint. This improves dynamic
reuse decisions without claiming complete query or JavaScript heap accounting.


### Native Worker/R2 owned-decoder checkpoint

The workerd suite now writes and reads a real Parquet object through its native
R2 binding, then runs two concurrent owned decoders and a later cache-reusing job
under separate child budgets within 64 MiB and 128 MiB roots. Fixtures include
multiple row groups, numeric and packed PLAIN string columns, Unicode, null and
empty values, and one-row batch output. An early-return consumer retains readable
output after cache eviction; releasing it returns both child and root usage to
zero. These checks exercise native R2 body streams rather than local-file reads
or a mocked fetch response.

The full default suite passes 1,158 tests with 97 skipped. All 11 workerd tests
pass across seven files; the existing intentional stream-cancellation case still
prints its pump-cancelled diagnostic. Repository type checking and lint pass.
The new fixtures validate allocation ownership in this runtime but are small:
they do not prove near-cap whole-query or isolate-heap bounds, account all codec/
metadata/JS output storage, or activate ownership in every normal query path.

### Scanner ownership capability

`ScanAdapter` now has an optional `scanOwnedVectorBatches` capability requiring a
query memory budget and returning explicit owners. Receivers release each output
owner; an output can outlive iterator return. The Parquet adapter exposes its owned
direct-vector pipeline through this capability, including projection, row bounds,
predicate pruning, cache configuration, cancellation and missing-column filling.
The general column fallback retains admitted read/decompression backing rather
than silently reverting to borrowed reads. Scoped column-cache hits/writes now
use ownership handoff as well.

Real adapter fixtures cover explicit projected direct vectors and an all-column
fallback, Unicode/null/empty values, early return with readable retained output,
full consumption and zero budget usage after release. All 20 adapter/owned-reader
tests pass with build, type checking and lint.

Query consumers do not yet select this capability: their operator/output retention
must be made explicit first. Metadata is still read through the existing metadata
path. The fallback currently retains its admitted backing for the scan lifetime,
and upstream object allocations are not instrumented; tighter fallback lifetimes
and full allocation coverage remain required. This interface is an ownership
contract, not a claim that every scanner allocation is bounded yet.

### Respect explicit query batch sizes in columnar paths

Query columnar scheduling previously applied a 262,144-row minimum even when the
caller explicitly requested a smaller batch. It now uses that value only when no
batch size was supplied. Explicit limits propagate through columnar aggregation
and late materialization rather than being enlarged for throughput.

Tests assert scanner requests at sizes 1, 3 and 17 for aggregation, and at most
seven rows for late-materialized top-k with ties and offsets. Results remain
correct across batch boundaries. All 84 query/adapter tests pass, along with build,
type checking and lint. The automatic default still needs byte-aware adaptation;
this change preserves user input but does not make the default an allocation cap.
The query-consumer audit also confirms that retained references/operator outputs
need explicit lifetime handling before owned scanning can replace borrowed loops.

### Row-group lifetimes for general owned decoding

The owned reader now chooses direct-vector or general-column decoding within the
same per-row-group stage. The scanner delegates both paths to that lifecycle,
removing its scan-long fallback backing retention. Requested row bounds are
translated into each metadata view and returned offsets translated back to their
absolute positions. Explicit output owners still survive stage/iterator disposal.
Batch sizes are validated before either path to prevent invalid fallback loops.

Real fixtures verify equal tracked peaks for one versus twenty row groups in both
direct and general decoding, and correct bounded outputs/absolute offsets for a
fallback request crossing row groups. All 22 owned-reader/adapter tests pass with
build, type checking and lint. This bounds admitted fallback backing by live group
and retained-output lifetimes; it does not account the upstream decoder's ordinary
JavaScript arrays/objects or complete the normal query-consumer integration.

### Pending-prefetch cancellation and independent cache pools

Owned-reader integration tests now hold a prefetch allocation pending, cancel its
job, and verify that its reservation remains charged until the work settles.
Completion then rejects the iteration, releases all usage, and leaves the iterator
terminal. This checks the full prefetch-stage lifecycle rather than only the
underlying allocation primitive.

A real multi-group Parquet fixture also warms a decoded cache in one allocation
pool and reads it from a completely separate pool. Incompatible owned entries are
misses; the second job decodes correctly, releases its own usage, and does not
change the first pool's cache charge. Final eviction returns that pool to zero.
All 21 owned-reader tests pass, along with type checking and lint. These results
validate accounting isolation and cancellation cleanup, not total heap bounds.

### Near-cap decoded-job admission with real cache buffers

New 64 MiB and 128 MiB pool cases measure a real Parquet fixture's decode peak,
then allocate and fill a cache payload consuming all remaining headroom. A live
cache reader pins that payload while the first decoded output fills the pool
exactly. A second job fails admission and leaves zero child usage; the first
output and cache bytes remain readable. Releasing the first output allows the
second job to decode successfully. Releasing the cache pin and evicting remaining
entries returns root usage to zero. Both root peaks equal their configured caps.

All 23 owned-reader tests pass with repository type checking and lint. This is
real allocated-buffer pressure plus decoded-output retention in Node, rather than
reservation-only arithmetic. The source is an in-memory Parquet fixture; native
Worker/R2 lifecycle tests and HTTP range tests are separate evidence. Near-cap
native Worker cloud queries and uninstrumented heap allocations remain unproven.

### Speculation versus decoded-cache priority

Integration tests now combine the owned range cache and latency-policy decoded
cache within the same small shared pool/cache. When the decoded entry fills the
pool, speculative prefetch performs no source read. When allocation headroom exists
but cache capacity is full, the newly fetched lower-priority range is discarded
and the decoded entry remains reusable by the reading job. All pins and cache
entries release cleanly. The existing policy already behaved correctly; no policy
change was needed. All 28 range/decoded-cache tests, type checking and lint pass.

This verifies priority preservation, not that every speculative read is useful:
the second case still fetches a range that cache capacity cannot retain. Avoiding
such wasted I/O needs an admission forecast that also handles concurrent eviction
and demand deduplication; it must not compromise mandatory read correctness.

### Priority-aware speculative admission forecast

The shared cache now exposes advisory capacity available to a priority class,
excluding live higher-priority entries and removing expired entries. Owned range
prefetch computes this once per window and combines it with allocation headroom
and its configured byte limit. It skips reads whose results would immediately be
discarded behind higher-priority decoded entries. The forecast is not a reservation;
actual allocation/cache admission still handles concurrent changes.

Tests verify the forecast does not evict live entries or allocate payloads, expired
backing is released, and speculation performs zero reads when decoded entries
consume cache capacity. A subsequent demand read still succeeds and preserves
the decoded entry. All 39 shared/owned-cache tests pass; the owned/HTTP range rerun
also passes, including real concurrent requests and cache reuse. Build, repository
type checking and lint pass. This removes the wasted-read case documented above
without changing mandatory demand behavior or claiming full memory coverage.

### Expiry-aware prefetch headroom

Prefetch now samples shared allocation headroom after its cache admission forecast
has removed expired entries. Sampling before expiry could unnecessarily decline
work even though expiry had just freed enough backing. Tests distinguish an expired
entry with no remaining owner, which permits the read, from an expired entry with
a live reader pin, which stays charged and prevents speculation. Both respect the
exact eight-byte root cap and release to zero. All 16 owned-range tests, type
checking and lint pass. This is an admission-order correction; it does not weaken
live-pin accounting or change demand-read limits.

### Reference and packaging verification checkpoint

The accumulated changes pass all 81 reference tests, generated compatibility
metadata verification, the tracked-file size gate, and fixture reproducibility
(generation produces identical file hashes). The full build succeeds, including
bundled Node/fetch/Cloudflare/CLI entries and declarations. The isolated package
installation smoke test also passes after installing the produced tarballs in a
temporary consumer, verifying packaged entry points rather than workspace imports.

The size gate and package smoke test required execution outside the subprocess
sandbox; their initial failures were `spawnSync ... EPERM`, and approved reruns
passed. These gates complement the recorded runtime evidence. They do not close
query/operator/output ownership gaps, missing allocation instrumentation, near-cap
native cloud stress validation, changesets, or the final acceptance audit.

### Shared constructor allocation admission

Core `vectorFromValues` and `batchFromColumns` now accept an allocation scope.
Numeric, bigint, boolean and timestamp payload buffers, nullable validity buffers,
and list/map offsets are admitted before allocation. Nested child/struct-field
construction propagates the same scope. The general Parquet column fallback passes
its scope into batch construction, closing the gap between tracked transport/
decompression and newly created typed vector backing.

Tests prove an 18-byte nullable numeric boundary and a 37-byte nested list boundary,
including one-byte-under rejection before the failing buffer allocation and zero
usage after disposal. Bigint/boolean/timestamp construction, existing nested slices,
real owned Parquet fixtures and scanner fallback tests also pass: 60 targeted tests
total, with build, type checking and lint green. Temporary JavaScript arrays,
objects and strings remain uninstrumented; this is typed-buffer admission rather
than complete fallback heap accounting.

### Large-list construction boundaries

List construction no longer spreads child arrays into function arguments, which
can fail at runtime for otherwise valid large lists. Iterative append handles a
200,000-child fixture. Construction also rejects child counts beyond the physical
Int32 offset capacity before flattening, rather than allowing offset wraparound.
Tests verify exact typed-buffer admission, preserved first/last values, rejection
of an oversized sparse input without traversing it, and cleanup. All 28 targeted
constructor/batch/adapter tests pass, with build, type checking and lint green.
The flattened intermediate JavaScript array still exists and remains a separate
allocation-accounting/removal requirement.

### List and struct construction without reference-array intermediates

Core construction now reads through a shared indexed value-source abstraction.
Lists build admitted offsets, then expose child values through a resettable cursor
over the original parent lists; they no longer flatten all children into a new
JavaScript array. Struct fields similarly expose indexed views instead of allocating
one temporary array per field. Repeated inference/construction passes and nested
sources preserve empty/null rows and unsigned-bigint inference.

All 110 targeted constructor, batch, slice, real Parquet and ownership tests pass,
with build, type checking and lint green. `bench/vector-construction.mjs` verifies
every row against the former flattening approach, warms three times and reports
seven-sample medians. For 2,000 rows with 8/128 children, this local run measured
0.44/7.47 ms for flattening versus 0.40/6.04 ms for indexed construction, removing
16,000/256,000 intermediate reference slots. These are local microbenchmarks, not
cloud-query speedups. Map intermediates, output JS arrays/strings/objects, upstream
decoder allocations and complete query ownership remain outstanding.

### Map construction without retained entry arrays

Map construction now counts entries into admitted Int32 offsets and constructs
key/value vectors through independent restartable entry cursors. It no longer
retains flattened key and value arrays. Offset overflow is rejected explicitly.
Tests exercise empty/null maps, nested structs/lists, repeated unsigned inference,
an exact 56-byte typed-buffer boundary, one-byte-under rejection and cleanup.
All 89 targeted constructor/batch/slice/real-Parquet tests pass with build, type
checking and lint green.

The construction benchmark verifies every result row. For 2,000 maps with 8/128
entries, local seven-sample medians after warmup were 1.50/18.98 ms for flattened
arrays and 0.98/16.68 ms for cursors, removing 32,000/512,000 retained intermediate
reference slots. Repeated iterator passes still create transient entry tuples;
output string arrays, object bookkeeping and upstream decoder storage remain
outside complete allocation accounting. The repeated list benchmark also showed
small-input variability (0.36 ms flattened versus 0.49 ms indexed at width eight),
so the primary guarantee is removed intermediate storage, not universal speedup.


### Nested owned decoding in native Worker/R2

A new workerd fixture writes nested LIST/MAP columns to the native R2 binding and
reads them through the general owned fallback. It covers Unicode, empty strings,
null list elements, empty/null containers and nullable numeric map values across
row groups with one-row output batches. The measured admitted peak succeeds as
an exact limit; one byte below rejects and releases all usage.

All 12 workerd tests pass across seven files, and the full default suite passes
1,183 tests with 97 skipped across 103 passing files. Repository type checking and
lint pass. This validates the new shared nested constructors through native R2
streams. The bound still covers admitted transport/decompression/typed backing,
not uninstrumented upstream or JavaScript heap allocations; full normal-query
ownership and near-cap isolate validation remain incomplete.

### Packed binary construction in generic scoped fallback

Scoped core binary construction now produces `binary-view` storage with admitted
Uint32 start/length pairs and a single admitted byte payload, rather than a retained
JavaScript array of views. It validates the physical range capacity before buffer
allocation. The copy gives the scoped output independent payload storage; ordinary
unscoped construction keeps its existing representation.

Tests cover null/empty values, nonzero source subarrays, payload isolation, exact
39-byte admission and one-byte-under rejection. A real Parquet fixture verifies
that the general owned scanner fallback emits packed binary output and releases
its usage. This fixture exposed upstream UTF8 coercion of unannotated BYTE_ARRAY
values. All Parquet decoder entry points now disable that coercion while retaining
schema-declared string decoding. The fixture checks owned direct/fallback scans
and object scans with invalid UTF8 bytes, nulls, and empty payloads. The targeted
constructor/adapter/owned-reader suite passes 37 tests. Generic strings remain
unchanged: UTF8 conversion would not preserve unpaired UTF16 surrogates exactly,
so removing their reference arrays requires an exact-preservation representation.

### Normal aggregate queries consume owned scan input

Grouped and ungrouped physical aggregate snapshot paths now select the owned scan
capability when available. The internal column-input iterator has an explicit
input-lifetime contract: iteration consumers must detach state before requesting
the next batch; retained-vector consumers continue to require ownership transfer.
The iterator releases each owner in `finally`, including physical execution
failure, budget failure, and early iterator return. Owned-only scanners can now
execute these aggregate paths without an unrelated borrowed-column capability.
Aggregate snapshots contain independent scalar values and timestamp/bigint
encodings; they do not retain input typed-array storage. Top-k still retains
vector aliases and does not opt into the iteration lifetime.

Tests exercise an owned-only scanner with a 16-byte pool across three batches,
asserting zero usage before every subsequent allocation. A group-limit failure
verifies owner release and upstream iterator closure. Real three-row-group
Parquet fixtures run through ordinary Lake grouped and ungrouped aggregation with
one-row batches, verify results, succeed at their measured admitted peak, reject
one byte below, and release all usage on both paths.

This activates transport/decode backing admission for these normal aggregate
paths. Aggregate snapshot/state JavaScript storage, other query paths, retained
operator outputs, metadata, and upstream decoder workspace still require coverage;
these tests do not establish a whole-query heap cap.


The full default suite now passes 1,191 tests with 97 skipped. All 13 workerd tests
pass, including a new normal grouped query over native R2 streams at the measured
exact admitted peak and one byte below. The adapter advertises owned scans only
when its store supports owned range reads; borrowed-only wrappers retain their
existing query route. A dedicated capability regression and the existing SQL
Iceberg binding test cover that distinction. Raw BYTE_ARRAY expectations now use
bytes; the schema-unification string fixture uses the writer's supported STRING
type instead of an ignored column-level converted_type hint.

### Gather preserves nested backing and admits output buffers

Generic row gathering now uses dictionary views for list, struct, map, and packed
byte/string columns. All such columns in one output share a single Uint32 row
selection. It no longer materializes nested JavaScript rows and rebuilds their
child vectors merely to reorder/select rows. Numeric and existing dictionary
payloads retain their copying behavior. An optional MemoryScope admits typed
payload, validity, and selection allocations before construction, using the same
allocation helper as generic vector construction.

The output aliases input backing: ownership must cover both the selection and its
source for the entire output lifetime. This does not yet enable retained top-k
ownership, nor does it guarantee that holding a whole source is cheaper than
compacting a sparse retained result. Measured compaction and retained-backing
transfer remain required before completing that path. JavaScript string/reference
arrays are also outside this typed allocation coverage.

Tests cover null/empty/repeated nested rows, maps/structs/lists, repeated gathering,
shared selection identity, exact/one-byte-under limits, and a 200,000-element list
selected using four admitted bytes. The full suite passes 1,194 tests with 97
skipped; type checking, compilation, and lint pass. `bench/vector-gather.mjs`
compares every result row before measuring three warmups and nine runs (median).
For 2,000 source rows and 1,000 selections, rebuilding lists of width 8/128 took
0.28/4.12 ms; shared-index gathering took approximately 0.004/0.002 ms and 4,000
index bytes. These tiny gather timings are noisy kernel measurements, not claimed
whole-query speedups or whole-heap bounds.

### Gather boundary and validity admission checks

Gather now validates integer row indices against the batch before allocating any
output. Dictionary selections additionally reject indices above Uint32 capacity
instead of silently wrapping. Tests cover negative, fractional, out-of-range,
non-finite, unsafe integer, and exact Uint32 boundaries without giant allocation.
All-present selections of nullable source columns no longer allocate a validity
mask that would be discarded; a two-row Float64 gather therefore succeeds in an
exact 16-byte scope. Empty selections require no mask. The targeted gather,
sorting, and query suite passes 105 tests; compilation, type checking, and lint
pass. The last full-suite checkpoint above predates these additional cases.

### Owned gather handoff across execution stages

`gatherOwnedBatch` now composes admitted gather buffers with independently retained
input ownership. It charges source/output overlap during construction and releases
all temporary pins on failure. Numeric output is detached and does not retain its
source; nested, dictionary, string, and byte output conservatively retain source
ownership. A sibling query may receive the output when both budgets share a root:
its child accounts the transferred backing while the root charges physical storage
once. Input ownership remains the caller's responsibility.

Tests verify sibling transfer, source release, exact 40-byte numeric overlap versus
39-byte rejection, invalid indices, cross-pool rejection, released-input rejection,
and empty dictionaries that still reference source backing. A real Parquet raw-byte
fixture gathers null/empty/repeated values, closes its scanner and producer stage,
and proves that only the consumer remains charged until final release. This is an
operator handoff primitive; normal retained top-k integration, source compaction,
and complete JavaScript allocation coverage remain open.

### Measured packed gather compaction

Owned gather now measures the exact typed backing required to detach packed
UTF8/binary output, including ranges, validity, selected payload, and other numeric
columns. It compacts only when the output is smaller than the retained input owner
and current shared/child headroom covers the complete construction overlap. With
insufficient headroom it retains the dictionary view; no user tuning is required.
The byte copy preserves encoding exactly and uses constant scalar planning
workspace. Shapes that still share nested, dictionary, or JS value storage prevent
this detachment decision, since copying just one column would not release the
conservatively retained source owner.

A 1,024-byte source selected down to one byte retains nine output bytes when
1,033 bytes of peak capacity are available. At 1,032 the same operation selects
the four-byte dictionary path; at 1,027 neither representation fits and it rejects
without leaking a pin. Tests cover Unicode, fixed/variable byte ranges, row offsets,
nulls, empty payloads, repeats, and independence from subsequent source mutation.
Real Parquet fixtures verify compaction and ownership release both in memory and
through native R2 streams; all 14 workerd tests pass. This does not yet compact
nested/dictionary-wrapped sources or activate normal retained top-k ownership.

### Dictionary-aware compaction and real encoded fixtures

Measured gather compaction now follows dictionary chains down to packed strings,
packed bytes, or raw byte-array leaves without materializing decoded strings or
intermediate row arrays. Null validity is respected at every dictionary level and
at the leaf. The same overlap/headroom decision applies, and detached output
releases source ownership. Nested lists/maps/structs and JS string dictionaries
still need an exact, allocation-admitted compaction representation.

Tests exercise two dictionary levels with independent outer/inner/leaf nulls,
empty values, repeats, and exact retained output accounting. Real Parquet fixtures
now explicitly assert a dictionary-page offset and use repeated raw-byte values
to ensure RLE_DICTIONARY encoding. Direct and fallback reads both compact after
handoff; the native R2 versions pass as part of all 15 workerd tests.

Fixture investigation also exposed an upstream writer edge case: requesting
RLE_DICTIONARY for high-cardinality values can cause its dictionary heuristic to
return no dictionary while the data-page encoding remains RLE_DICTIONARY. The
result decodes incorrectly. This is a separate correctness follow-up, still open;
the valid repeated-value fixtures do not constitute a fix for that writer case.

Dictionary compaction checkpoint: 1,214 default tests pass (97 skipped), all 15
workerd tests pass, and compilation, type checking, and lint pass.

### Writer dictionary fallback correctness resolved at the package boundary

LakeQL now normalizes explicit RLE_DICTIONARY requests to the upstream writer's
adaptive mode. Both modes use the same dictionary heuristic, but automatic mode
correctly emits PLAIN when cardinality or dictionary size rules decline a
dictionary. Legacy PLAIN_DICTIONARY requests use PLAIN, matching the writer's lack
of legacy dictionary emission without retaining an invalid dictionary marker.
Other encodings and source columns are unchanged; caller options are not mutated.
The fix is in published LakeQL code, with no dependency patch or lockfile change.

Five round-trip regressions cover unique raw bytes (including null and empty),
both dictionary encoding names, repeated Unicode strings that still encode a real
dictionary, and repeated large values whose dictionary exceeds the page budget.
Metadata assertions distinguish real dictionary pages from valid PLAIN fallbacks.
A native R2 fixture verifies the raw-byte fallback; all 16 workerd tests pass.
The earlier dictionary writer follow-up is resolved by this normalization contract.

### Fixed-width dictionary compaction

The measured gather decision now includes dictionary-backed fixed-width numeric,
boolean, timestamp, and null leaves. Compaction resolves dictionary chains directly
into admitted output arrays without a materialized intermediate index array.
It preserves the physical typed-array constructor, timestamp unit/UTC metadata,
and validity at every dictionary layer. Mixed packed-byte/numeric dictionary
batches can now detach their entire input owner.

Regressions cover float32, signed/unsigned 64-bit extrema, booleans, timestamps,
nulls, repeats, and exact retained byte counts. A 24-byte input plus an eight-byte
numeric output compacts at a 32-byte limit; at 31 bytes it chooses a four-byte view
and retains 28 bytes. Both preserve UINT64_MAX exactly and release all usage.
The real dictionary-page fixtures now include raw bytes and INT32 columns, with
matching direct/fallback results after scanner shutdown in memory and native R2.
All 16 workerd tests pass, along with the targeted ownership suite, compilation,
type checking, and lint. Nested/JS string compaction and normal retained operator
integration remain open; this is not a completed whole-query allocation bound.

### Admitted concat backing for retained operators

Concat now accepts an allocation scope and admits typed payload, validity, and
same-dictionary index buffers before construction. Nested/differing-dictionary
fallback constructors receive the scope for their typed allocations. Numeric,
signed-bigint, unsigned-bigint, and timestamp concatenation share one typed-copy
helper rather than separate implementations.

`concatOwnedBatches` retains input ownership during source/output overlap, releases
source pins for detached output, and retains them for shared dictionaries or JS
value storage. Failure during any later input admission releases earlier pins;
empty concatenation requires an explicit budget. Tests cover nullable bigint
extrema at exact/one-byte-under limits, numeric overlap, shared dictionary
survival after both input handles are released, and cross-pool failure cleanup.
Source pins remain conservative: same-dictionary output also retains obsolete
source index allocations until selective backing ownership is implemented.

This closes the unadmitted typed-concat allocation path needed by retained top-k,
but normal top-k has not yet adopted the owned operator chain. Nested fallback
materialization and JS arrays remain outside full pre-allocation coverage; no
whole-query cap is claimed by these tests.

### Current public R2 query validation under shared-pool pressure

`bench/cloud-range-validation.mjs` now runs ordinary aggregate queries against the
public R2 window-events fixture with one/four concurrent jobs and 64/128 MiB pools.
A filled, pinned reusable-cache allocation leaves 128 KiB of shared headroom.
Results are checked against independently generated expected values; every GET
must be ranged and return HTTP 206. Global physical-read concurrency stays at
four, and cache/pin cleanup returns admitted pool usage to zero.

Raw observations are saved in `benchmarks/cloud-range-2026-11-12.json`. Across the
four configurations, admitted peak above the pinned cache was 24,800–24,815 bytes.
Cold single-job aggregates took 822–947 ms with four data reads (6,296 bytes);
four-job cold runs took 1,442–1,487 ms with sixteen reads (25,184 bytes). Warm runs
needed no data reads, but still issued two two-byte size probes per job: their
wall time was 324–526 ms. These are single observations dominated by remote
latency, not statistical speedup claims. The small 144-row fixture and Node runtime
do not prove realistic near-cap Worker heap behavior.

**New integration finding:** concurrent normal query wrappers do not share pending
range reads. Pending maps in `withOwnedRangeCache` are scoped to each buffer wrapper,
so four cold jobs repeat data reads even though they share the completed cache.
Moving pending state to the shared-cache/root-pool identity also requires fixing
cancellation ownership: store-buffer read metrics and nested read-control wrappers
currently retain the initiating query's abort signal. Sharing those requests as-is
would let cancellation of one query fail another. The fix must share physical read
lifetime across active waiters while preserving per-query budget/read metrics and
bounded concurrency, with cancellation/failure tests across separate wrappers.

### Shared owned reads across scanner wrappers

Owned pending reads are now shared by cache identity and root allocation pool,
with existing object/version/range cache keys. Independent normal query wrappers
can join one physical read. Owned store-buffer I/O follows the supplied stage's
signal rather than the initiating query's signal; scanner elapsed-time limits are
attached to each query's waiter scopes. The existing waiter accounting cancels
physical work only after the last active waiter leaves. Borrowed reads retain
their previous per-query cancellation behavior.

New tests use separate store-buffer wrappers, each with its own query abort signal
and child budget. They prove first-query cancellation does not kill a survivor,
all-waiter cancellation closes the physical stage, a failed shared read can be
retried, distinct ETags remain isolated, and all charges release on cleanup.
The targeted suite passes 33 tests.

A real public R2 rerun is saved in
`benchmarks/cloud-range-shared-pending-2026-11-12.json`. Four cold normal aggregate
jobs now make seven data reads (20,858 bytes), compared with sixteen (25,184 bytes)
before this change, at both 64/128 MiB pool sizes. Peak admitted overhead above the
pinned cache is 27,827 bytes versus the 128 KiB available headroom; physical
concurrency remains four. Observed cold four-job wall time was 973–1,064 ms versus
1,442–1,487 ms previously, but these are single remote-network observations, not
statistically established speedups. Warm data reads remain zero. Remaining cold
metadata duplication and per-job two-byte size probes are still present.
Physical read counters are charged to the initiating request; joined consumers
receive their own allocation pins. Per-consumer logical read accounting and full
metadata ownership remain separate outstanding work.

Shared-wrapper verification: 1,231 default tests pass (97 skipped), all 16 workerd
tests pass, and compilation, type checking, and lint pass.

### Owned metadata footer lifetime and cold-read deduplication

Owned scanner metadata loading now uses admitted range reads and retains footer
backing until scanner completion. The bounded shared metadata cache offers optional
owned acquire/store operations; consumers pin cached backing in their own budgets,
and eviction does not invalidate an active consumer. Ordinary metadata cache
adapters without this ownership capability remain on borrowed paths; owned scans
parse via admitted reads instead of assuming borrowed cached objects have owners.
Separate owned keys avoid mixing the two lifetimes.

A real fixture verifies cold admission, warm no-read acquisition, per-child pins,
cache eviction while a consumer remains live, final cleanup, and tiny-budget
rejection. Native Worker and normal query fixtures retain their exact-peak tests.
Parsed metadata objects themselves still allocate inside the upstream decoder;
this change covers their referenced footer backing, not those JS allocations.
The owned cache's charge represents backing bytes rather than the borrowed cache's
serialized-object size estimate. Consequently lower retained-cache counters are
**not** evidence of reduced heap use or completed metadata allocation coverage.

Public R2 results are saved in
`benchmarks/cloud-range-owned-metadata-2026-11-12.json`. Four concurrent cold normal
aggregates now use four data reads and 6,296 bytes, matching one job, compared with
16 reads/25,184 bytes initially and seven reads/20,858 bytes after only sharing data
requests. The four-job admitted peak is 29,748 bytes above the pinned cache,
within 128 KiB headroom at both 64/128 MiB pool settings. Cold wall observations
were 845–901 ms, still single network trials rather than statistical speedups.
Per-job size probes remain, and metadata parsing itself may still duplicate work
on simultaneous misses even though physical bytes are shared.

Owned metadata checkpoint: 1,232 default tests pass (97 skipped), all 16 workerd
tests pass, and compilation, type checking, and lint pass.

### Metadata cache capacity includes parsed-object cost

Owned cache entries can now supply an eviction cost greater than their admitted
backing bytes. This cost affects cache capacity, priority protection, and eviction;
it does not masquerade as physical allocation admission. Metadata entries include
both footer backing and the existing serialized-metadata size estimate in this
cost, restoring the parsed-object cache-capacity constraint that the first owned
metadata path omitted. Consumers still pin the actual admitted backing only.

Tests distinguish a 24-byte cache cost from an eight-byte physical allocation,
verify priority headroom and pin survival, reject invalid/oversized costs, and
exercise a real metadata fixture where the footer fits but combined cache cost
does not. That fixture remains readable, declines optional caching, and releases
all admitted usage on every attempt. This controls retained cache growth; upstream
metadata parsing and its temporary JS allocations still need pre-allocation
coverage before a whole-query heap bound can be claimed.

### Metadata eviction-cost measurement without full serialization

Metadata cache admission now measures the exact tagged JSON UTF8 cost incrementally
instead of constructing the entire JSON string and encoded Uint8Array solely to
obtain its length. Byte-array tags contribute their known hexadecimal length
without constructing hex strings. Object traversal retains only the active ancestor
set and per-object keys, rather than a serialized copy of the whole metadata tree.
Persistent encoded metadata caches continue using the existing encoder.

Parity tests compare with that actual encoder for a real multigroup fixture,
bigint tags, byte subarrays/ArrayBuffers, repeated objects, omitted fields, sparse
arrays, dates/toJSON, non-finite numbers, Unicode boundaries, unpaired surrogates,
control escapes, and cyclic-input rejection. Type checking and lint pass.

`bench/metadata-size.mjs` scales a real parsed row-group shape to 100/1,000 groups,
checks exact byte parity, and measures seven-run medians after three warmups. The
160,533/1,602,333-byte encodings took 1.50/11.90 ms to construct; direct measurement
took 0.65/3.25 ms. These are metadata-sizing kernel observations, not whole-query
speedups or proof of parsed-object heap bounds. Cache eviction costs are unchanged.

### Shared exact JSON string sizing for aggregate accounting

Metadata eviction sizing and buffered aggregate accounting now share
`jsonStringUtf8Bytes`. It counts JSON escaping and UTF8 bytes directly from UTF16
code units, avoiding both serialized-string and UTF8-buffer construction.
`jsonItemBytes` preserves numeric JSON behavior, including non-finite values and
negative zero. Tests compare every individual UTF16 code unit with the native
JSON/TextEncoder result, plus paired/unpaired surrogates, controls, and long mixed
strings; metadata encoder-parity checks remain green.

`bench/json-string-size.mjs` verifies parity then measures 1,000 strings per case
with three warmups and seven-run medians. Old/new observations were 1.36/1.29 ms
for ASCII, 2.85/0.96 ms for Unicode, and 6.94/1.18 ms for escaped text. ASCII CPU
cost was approximately unchanged; removal of temporary serialization applies to
all cases. These are accounting-kernel measurements, not end-to-end query claims.

### Fallback decoder allocations end at each requested batch

The owned fallback reader previously retained input, decompression, and typed-vector
allocations from every batch until the physical row group ended. It now creates a
separate allocation stage for each requested batch. The borrowed and owned readers
share the same batch planning and decode/cache implementation; row-group pruning
and metrics remain outside the per-batch decode. Independently retained outputs
remain charged after advancement or early reader closure.

A real encoded 128-row single-group fixture scans one row per batch: scanning all
128 rows fits the measured exact allocation cap for scanning only the first row.
One byte less rejects cleanly. Separate tests retain an earlier output across
advancement and closure, and exercise both direct and fallback cancellation and
failed admission. All 40 owned-reader/decoded-cache tests and all 16 native Worker
tests pass. This validates admitted backing lifetime, not the upstream decoder's
untracked JS arrays or a whole-process heap bound. Per-batch fallback rereads and
upstream repeated decoding remain performance work; this change does not claim
to eliminate them or establish new remote-network timing results.

Fallback lifetime checkpoint: 1,241 default tests pass (97 skipped), all 16 native
Worker tests pass, and TypeScript compilation, type checking, and lint pass.

### Owned top-k selection with admitted index workspace

`vectorTopKOwnedBatch` introduces the ownership-preserving top-k selection needed
by retained query operators. It pins its input, admits a Uint32 candidate heap
before allocating it, retains at most min(rowCount, offset + limit) indices, and
sorts that heap in place. Gathering accepts typed indices directly and uses the
existing owned gather contract for detached copies versus aliased backing. Heap
storage is released on success and failure; outputs remain independently owned.
Zero-limit selection requires no candidate heap.

Reference comparisons cover ascending/descending order, null placement, ties,
selection masks, offsets beyond the input, and limits from zero through larger
than the input. Exact measured overlap admission and one-byte-under rejection
verify cleanup. All 32 targeted sort/gather/concat tests pass. Compilation passes.
This is an operator primitive, not activation in normal query top-k: physical
backend output ownership and retained-query integration remain outstanding. JS
sort-key wrappers and uninstrumented gather representations are not newly claimed
as admitted allocations; no whole-query speedup is claimed from these tests.

### Measured top-k input order and sparse-selection sizing

The owned top-k primitive now counts selected rows before admitting its heap,
so a large requested limit cannot force index allocation for masked-out rows.
A sparse 128-row fixture with one selected row succeeds at exactly input backing
plus four index bytes and eight output bytes. Selection ordering uses the same
scalar/null/timestamp comparison and row-index tie breaker as ordinary top-k.

`bench/vector-owned-top-k.mjs` exposed a reverse-ordered-input regression in the
initial heap implementation: 100,000 rows / k=1,000 took about 39 ms versus 5.6 ms
for borrowed periodic trimming. The owned implementation now detects monotonic
selected input (stopping immediately once mixed ordering is established) and
fills its bounded indices directly for that case. Mixed input still uses the
heap; no sort-key cache or extra batch-sized workspace is introduced. No user
flag is needed. Monotonic, equal-key, sparse/empty masks, both directions, and
out-of-range offsets are checked against the existing reference.

The benchmark validates output parity before timing, uses three warmups and
seven-run medians, and checks release accounting. Results are saved in
`benchmarks/vector-owned-top-k-2026-11-12.jsonl`. For 100,000 rows / k=1,000,
borrowed/owned times were 5.63/2.10 ms ascending, 5.73/2.15 ms descending, and
25.23/4.07 ms mixed. Incremental admitted heap-plus-output backing was 20,000
bytes at both 10,000 and 100,000 input rows (200 bytes for k=10). These are CPU
operator measurements, not cloud-query or whole-heap bounds. Normal physical
execution still exposes borrowed batch outputs; introducing a complete ownership
contract there remains necessary before activating owned top-k in query execution.

### Physical execution allocation-scope contract

Physical execution now accepts a caller-owned `allocationScope`. Callers pin input
backing into that scope, execute, and retain output backing before disposing it.
Backends advertise `allocationScopeOperators`; planning can require this contract,
and dispatch rechecks it, including CPU replay compatibility. Unsupported backends
cannot silently ignore a requested scope. This capability denotes propagation of
instrumented allocations and backing lifetimes, not coverage of all JS heap use.

CPU top-k advertises the contract and uses owned selection, admitting index/output
backing and retaining aliases in the supplied scope. A physical-execution test
checks exact peak admission, one-byte-under rejection and cleanup, and output
survival after scope disposal. Another verifies rejection during planning and
before execution for a backend without the capability. All 17 targeted physical
and owned-top-k tests pass; compilation passes.

Only top-k currently advertises scoped execution. This is an explicit operator
capability boundary: selection masks, computed projection intermediates, ordering,
and reductions need their own allocation propagation before claiming support.
Filtered normal-query top-k therefore remains unactivated; the next integration
step is scoped selection masks and retained query-output handoff. Existing borrowed
execution remains unchanged. Conservative fragment scopes may retain input and
intermediate backing through fragment completion even when a final output is
fully detached; shortening that lifetime is still possible work.

Full regression checkpoint after the physical scope contract: 1,249 default tests
pass (97 skipped). The example type check exposed mixed source/dist identities for
`MemoryScope`; the WebGPU example now resolves both `lakeql` and `lakeql-core` to
the same core source entry, keeping its backend and query context types consistent.

### Scoped predicate masks and filtered physical top-k

Predicate selection and expression evaluation now accept an optional allocation
scope. It propagates through numeric/bigint/timestamp comparisons, dictionary
masks, null checks, IN/BETWEEN, logical masks, CASE conditions, scalar calls, and
vectorized spatial/H3 predicate helpers. Every mask buffer in these paths is
admitted before construction. Existing no-scope call sites preserve their API.
The caller's scope conservatively retains intermediate masks until disposal;
shorter expression-stage lifetimes remain an optimization opportunity.

CPU physical selection now propagates that scope, including selection intersection
and compact batch-output indices/gather. Its advertised scope operators are now
`select` and `top-k`, allowing generic fused filtered top-k execution. Scope support
still does not cover computed projection, general ordering, or reduction state.
Normal query ownership handoff remains to be integrated.

Tests compare scoped and borrowed masks for SQL null semantics, nested boolean
expressions, CASE, H3 membership, and nullable dictionaries. Exact measured caps
succeed and one-byte-under caps reject with cleanup. A dictionary fixture proves
both its three-byte dictionary mask and four-byte expanded mask are charged.
The physical test additionally runs fused filtering/top-k at its exact cap and
checks retained output after scope disposal. Geometry backend workspaces, parsed
H3 lists/sets, scalar temporary JS values, and returned row ownership remain
outside these mask-buffer claims.

Scoped predicate checkpoint: 1,259 default tests pass (97 skipped), all 16 native
Worker tests pass, and compilation, type checking, and lint pass.

### Normal retained top-k uses owned scan and physical scopes

The normal columnar top-k path now opts into owned scanning when the adapter
supports it. Each physical fragment pins its input, requires scope-capable backend
placement, and retains its candidate output before closing the fragment scope.
Cross-batch merges use owned concatenation and owned top-k; replaced candidates,
merge intermediates, final selection, and failure paths release their handles.
Borrowed-only adapters continue using the existing path. Explicit accelerator-only
placement cannot silently bypass the required ownership contract: a backend must
advertise scoped selection/top-k support to execute an owned fragment.

A query-level owned scanner test compares three versus thirty input batches at the
same measured peak cap and rejects one byte below it with iterator closure and
zero residual charged usage. A real three-row-group Parquet file tests filtered
ordering with offset/limit at its exact cap. Native R2 stream coverage exercises
the same normal query route separately from the in-memory fixture. These tests
cover query integration rather than just isolated vector primitives.

Remaining boundaries: late-materialized top-k takes its separate row-reference
path; borrowed-only scanners do not gain admission by this change. Returned
`toArray()` rows retain the existing caller-owned JS API (including possible
binary/nested aliases); their allocation/lifetime is not a post-query pool-bound
contract. JS object/string allocations, nested concat intermediates, and metadata
parsing also remain uninstrumented. Conservative fragment ownership can retain
unneeded input/masks until candidate replacement; therefore this activation does
not establish a whole-query or Worker heap bound.

Normal owned top-k checkpoint: 1,261 default tests pass (97 skipped), all 17 native
Worker tests pass, and compilation, type checking, and lint pass.

### Predicate workspace ends before retained candidate lifetime

`MemoryScope` can now retain or release one exact allocated value; it rejects
unowned views instead of guessing their backing. Predicate selection uses an
isolated evaluation scope, transfers only the final mask to its caller, and
releases other masks on success or failure. Physical selection releases masks
replaced by intersection. Physical top-k pins its active mask separately from its
batch backing, then releases that mask after selection so retained candidates do
not capture it.

Tests assert that nested/dictionary predicates retain only rowCount mask bytes,
while their evaluation peak still includes admitted intermediate allocations.
The nullable dictionary case peaks at seven bytes and retains four. Failed
admission and invalid predicate values release evaluation workspace while leaving
the caller scope open and reusable. Filtered and unfiltered physical top-k both
retain exactly 48 bytes in the fixture (32 input plus 16 output), with no mask
surviving. Exact-value retention/release tests verify independent handles survive
and unowned views are rejected. This shortens stage lifetimes; reducing the peak
inside deep predicate evaluation and untracked scalar/geometry workspace remain
separate work.

Predicate lifetime checkpoint: 1,263 default tests pass (97 skipped), all 17 native
Worker tests pass, and compilation, type checking, and lint pass.

### Indexed projection construction and scoped physical projection

Computed projections now feed indexed expression values directly into vector
construction instead of materializing a batch-sized scalar array first. The shared
`VectorSource` contract requires stable indexed values and a nonnegative safe
integer length. Numeric output/validity buffers are admitted through an optional
scope. CASE evaluation uses a separate scope so condition masks release when the
projected vector is complete. CPU physical execution now advertises and propagates
scope support for `project` as well as `select` and `top-k`.

The first experiment repeated string expression evaluation during inference and
filling and regressed string projection. The shared string constructor now fills
its final JS output array during inference, evaluating each source row once and
preserving nulls, Unicode, and unpaired surrogates. It creates no extra string-array
copy. Numeric expressions can still be evaluated during both inference and filling;
this is a memory/CPU tradeoff for expensive expressions and needs broader workloads.

Tests cover nullable arithmetic, CASE output and mask overlap (54-byte exact peak,
51 bytes retained), insufficient admission, invalid source lengths before value
access, single-evaluation string construction, and fused physical top-k/projection.
`bench/vector-project-source.mjs` verifies output parity and takes seven-run medians
after three warmups. The array-materializing versus indexed approach at 100,000
rows measured 6.13/4.25 ms numeric and 9.06/5.58 ms string, saved in
`benchmarks/vector-project-source-2026-11-12.jsonl`. This benchmark isolates projection
construction using the current shared constructor; it is not an end-to-end cloud
query comparison. JS string storage and temporary expression/geometry allocations
remain outside admitted typed-buffer accounting. Normal projected-query scan
ownership still needs integration beyond this physical-operator capability.

Projection checkpoint: 1,271 default tests pass (97 skipped), all 17 native Worker
tests pass, and compilation, type checking, and lint pass.

### Owned projected queries, scoped index outputs, and packed spatial predicates

Normal projected-query iteration now prefers owned vector scans, pins each batch
through selection/projection, and releases the execution stage and scan owner on
advancement, early limit, cancellation, or failure. Requested scan bounds and
batch sizes still propagate. Generic physical output materialization admits
selection/index buffers and constructs Uint32 indices directly instead of a JS
index array followed by a typed copy. Scope-compatible outputs now include batch,
selection, and indices. Tests cover filtered/unfiltered early limits with owned-only
scanners, exact admission, one-byte-under failures, and native R2 projected output.

Activating this path exposed missing packed-vector support in spatial/H3 predicates.
The kernels now accept utf8-view/binary-view geometry and packed dictionary values
using generic vector access and admitted masks, retaining SQL null behavior.
Committed GeoParquet WKB and WKT/H3 recipe fixtures verify the integration. R2
recipe mocks now expose streaming bodies, matching the native service interface;
these mocks remain distinct from the separate native R2 Worker tests.

Legacy metadata cache adapters are an explicit ownership-capability boundary.
Adapters exposing only borrowed get/set values retain the borrowed scan route and
its warm-cache behavior; the scanner advertises owned iteration only with no
metadata cache or a cache supporting owned acquire/store. This replaces the earlier
behavior of reparsing on every owned scan with a legacy cache. The default shared
metadata cache supports ownership and remains on the owned path. The public
`OwnedMetadataCache` contract and `sharedParquetMetadataCache` adapter allow custom
integrations to preserve ownership and cache reuse. Legacy borrowed caches still
prevent end-to-end admission; this compatibility route is not a memory-bound claim.
Returned JS rows and late-materialized paths retain their previously documented
ownership/accounting gaps.

Projected-query checkpoint: 1,276 default tests pass (97 skipped), all 18 native
Worker tests pass, and compilation, type checking, and lint pass.

### Broader release-gate verification and external conformance findings

The current built packages pass the isolated tarball-install runtime/TypeScript
smoke test. Build (including bundled declarations), compatibility freshness,
tracked-file-size, and deterministic fixture-generation checks pass. Coverage
passes: 90.93% statements, 84.59% branches, 95.10% functions, 92.91% lines; its
regression run has 1,276 passing tests and 97 skips. A changeset records the new
ownership APIs, observable binary behavior, and incomplete whole-heap coverage;
no versioning or release was performed. The changeset check examines committed
HEAD changes and reports no package changes in this uncommitted worktree, so it is
not evidence that the final release diff has been checked.

The refreshed DuckDB reference suite has all 81 tests passing. Two reference
adapters had expected binary columns as strings; they now use the existing strict
Uint8Array-to-hex comparator, matching the raw binary contract. A DuckDB-authored
FF8000 blob adds invalid-UTF8 evidence, rather than accepting either representation.

The initial conformance run passed seven Iceberg cases but skipped Apache Parquet
because its external fixture directory was missing. Fetching the configured Apache
corpus (243 Parquet files) exposes 167 failures, 83 passes total including Iceberg,
and one skipped missing-fixture guard. The failure inventory is saved in
`validation/external-conformance-2026-11-12.json`. It includes deliberately malformed
files, explicit struct/decimal unsupported-schema rejections, and decoder errors
in codec/encoding fixtures. The harness currently expects every file to decode;
expected rejection policy and baseline-versus-regression classification require
separate evidence. No failures have been converted to skips or blanket rejection
acceptance. **The complete conformance gate is failing, and release readiness is
not established.** The fetched corpus remains locally available for that work.

### External conformance baseline comparison

`scripts/audit-parquet-conformance.mjs` reconstructs core/Parquet source from HEAD
in an isolated temporary directory, transpiles it, and runs all 243 Apache fixtures
against both that source and the current built reader. Both use the same installed
dependencies; the report records the baseline commit, lockfile SHA256, and each
fixture SHA256. It neither changes the checkout nor relaxes the conformance tests.
The reproducible result is `validation/parquet-conformance-baseline.json`.

Baseline b54d4ed decodes 69 files; the current reader decodes 76, with **zero new
rejections**. Seven files newly decode, including ZSTD, delta-length byte arrays,
geospatial fixtures, and a zero-bit-width dictionary regression fixture. This is
operation evidence, not independent value validation for those seven files.
Twenty-three decoded-output digests change; all match under a separate diagnostic
that converts bytes back to UTF8 text, identifying the existing binary
representation correction. That normalization is deliberately lossy and is not a
correctness assertion. Strict byte-level DuckDB and invalid-UTF8 tests remain the
stronger evidence for binary handling.

Current rejection causes are 144 explicit struct-schema guards, one decimal
precision guard, ten unsupported compression-codec errors (five GZIP, three LZ4,
two LZ4_RAW), one BROTLI error, one BSON error, and ten other malformed-schema,
encoding, range, or decoder failures. The failure inventory includes malformed
variant fixtures that *decode* as opaque fields in both versions; success is not
proof that their logical variant contents were validated. Apache's fixture README
explicitly identifies malformed variants and dictionary/level/schema regressions.

The next conformance work must distinguish raw physical-field support from logical
format validation, test supported structures through generic vector capabilities,
and establish expected rejection contracts for malformed input. All 167 existing
rejections still fail the current decode-every-file harness. No baseline failure
has been silently accepted as correctness, and the complete gate remains failing.

### Independent values for all seven newly decoded Apache fixtures

`external-values.reference.test.ts` compares all seven newly decoded files with
DuckDB: the zero-bit-width RLE dictionary regression, BYTE_STREAM_SPLIT/ZSTD,
DELTA_LENGTH_BYTE_ARRAY, an empty compressed V2 page, and geography points/lines/
polygons. It checks complete ordered rows through both the public object reader
and the normal owned-query path, using 17-row batches. Geospatial comparisons use
strict Uint8Array bytes against DuckDB's ST_AsWKB hex output, not text decoding.
Each owned query succeeds at its measured exact cap and fails at one byte below,
with zero residual tracked usage. The external suite runs only when the corpus
is installed and reference testing is enabled; this checkpoint executed all seven.

All 88 reference tests now pass. This establishes values and tracked admission for
the new successful decodes, including the zero-bit-width fixture that DuckDB also
accepts; it does not establish validation of the separately malformed variant
fixtures. These checks use in-memory object stores containing real files and are
not remote-cloud or Worker heap measurements. The full Apache decode-every-file
conformance gate remains at its previously recorded failures.

### Remote value checking caught implicit output-column loss

Extending the public R2 benchmark to normal filtered top-k found correct ranked
values but missing unrelated output fields. The shared read-column planner treated
filter/order/projection dependencies as an implicit select list when `select` was
omitted. It now requests all source columns for omitted/empty/wildcard selection;
explicit selections still collect their expression dependencies for pruning.
Row-based computed projection now follows the vector path's same implicit-all
contract. Task planning preserves that contract too.

A real Parquet regression includes a nonpredicate label column and verifies its
presence after filtered ordering/limit and computed projection. Existing tests that
had encoded dropped fields now assert complete rows; dependency-collection tests
use explicit selection so they still verify pruning. The remote benchmark retains
its complete independent expected rows rather than narrowing the expected output
to accommodate the bug. Reading all requested output fields may transfer more data
than the erroneous dependency-only plan; it is required for correct results.

### Complete-row remote query matrix

The expanded public R2 run now passes all 24 ordinary query cases: aggregate,
filtered top-k with offset, and computed projection with early limit, each cold
and warm at one/four jobs under 64/128 MiB shared pools. Expected top-k and
projection rows independently reconstruct all seven fixture fields, including
millisecond UTC timestamps and categories, plus the computed projection. The
original explicitly selected five-column scan checks remain separate.
Results are saved in `benchmarks/cloud-range-projected-top-k-2026-11-12.json`.

Each pool holds an actual pinned allocation leaving 131,072 bytes of shared
headroom. Four concurrent jobs peak at 29,748 admitted incremental bytes for
aggregate, 127,995 for top-k, and 37,205 for projection, identical across the two
pool sizes. Top-k therefore passes with only 3,077 bytes of spare tracked capacity;
this fixture does not establish comfortable capacity for larger inputs. Cold data
requests/bytes are respectively 4/6,296, 8/9,507, and 3/6,017, unchanged between
one and four jobs. All warm cases issue zero data reads, but still perform two
size-probe range requests per job (four bytes per job). Peak concurrent data reads
are at most three. Clearing caches and releasing the pinned allocation leaves
zero tracked usage after every case.

These are real remote HTTP range operations against public R2, executed in Node,
not local-file timings or a Worker heap-cap proof. The peak counter is cumulative
across each cold/warm pair. Network timings are single observations and cannot
establish statistical speedups. Untracked JS objects, metadata parsing, returned
rows, and larger/adversarial workloads remain part of the unfinished memory audit.

### Shared selection-index allocation contract

Physical batch/index outputs and ordinary filtered projection now use the same
`selectedRowIndexBuffer` implementation. It counts selected rows, admits exactly
four bytes per selected index, and fills the final typed array directly without
an intermediate array or generator iteration. SQL unknown mask values remain
excluded. Row counts must be safe nonnegative integers within u32 index capacity,
and a supplied mask must match the row count; invalid shapes fail before allocation.
This consolidates the previously duplicated admission/bounds logic. It does not
reduce the required index backing size or claim a measured timing improvement.

Nine focused cases cover sparse/unknown masks, empty/unfiltered selections, exact
admission, one-byte-under rejection, invalid dimensions, and mismatched masks.
Together with the physical ownership and ordinary owned-query tests, all 20
focused tests pass; TypeScript project compilation passes. Selection masks and
indices still remain retained until the parent fragment scope closes; shortening
that overlap requires an explicit lifetime change and is not established here.

The full default regression suite after this consolidation passes 1,286 tests
with 347 skips (including separately enabled external/reference suites); lint
passes as well. This checkpoint does not rerun or resolve external conformance.

### End gather-index workspace before projection

`gatherSelectedBatch` now owns the temporary selection-index lifetime shared by
physical batch output and ordinary filtered projection. It releases that exact
allocation in `finally` immediately after gathering, including on output admission
failure. `gatherBatch` constructs independent dictionary indices for packed/nested
views, so returned batches do not alias the released workspace. Projection no
longer overlaps its expression buffers with those temporary gather indices.
Selection masks and the required gather output still live in the parent scope;
this is not a claim that all intermediate lifetimes are shortened.

A packed-string ownership test measures 16 bytes at the gather overlap and eight
bytes retained after return, then retains the output across scope disposal and
checks every value. A 15-byte cap rejects the output allocation and returns tracked
usage to zero immediately. These tests deliberately exclude borrowed input backing
from the workspace budget. TypeScript compilation and all 13 focused physical,
owned-query, and new gather-lifetime tests pass. No new CPU speedup is claimed.

The full default suite passes 1,288 tests (347 skipped), and all 18 workerd tests
pass, including native R2 fixture queries. Lint passes. These runtime checks do
not constitute a new remote public-R2 timing run or whole-Worker heap proof.

### Mixed concurrent public-R2 queries

The cloud benchmark now includes a mixed four-job workload: top-k, aggregate,
early-limit computed projection, and a second top-k. Each result is compared with
its own independently reconstructed expected rows. All 32 ordinary query cases
pass, including the original 24 cases and eight mixed-scenario cases. The
single-job mixed-scenario control is one top-k job, not a mixed workload.
Raw results are in `benchmarks/cloud-range-mixed-2026-11-12.json`.

With an actual pinned cache allocation leaving 131,072 bytes of shared headroom,
four mixed jobs peak at 68,771 admitted incremental bytes in the 64 MiB pool and
73,625 bytes in the 128 MiB pool. Scheduling changes overlap, so these are observed
peaks, not exact universal requirements. Both cold workloads perform eight data
reads totaling 9,507 bytes, the same as their single-top-k control; warm runs
perform zero data reads. Size probes remain eight requests/16 bytes for four jobs.
Data-read concurrency reaches four, demonstrating that the memory treatment has
not serialized all remote reads. All cases release tracked usage to zero after
cache cleanup. Complete values pass with the early-exit projection alongside
longer-running consumers.

These are readonly public R2 HTTP operations from Node using the small 144-row
fixture. They add mixed-lifetime and cross-query-shape evidence, but do not prove
whole-Worker heap bounds, adversarial large-file behavior, or byte-aware mandatory
scheduling. The audit's acceptance table now reflects current integration and
explicitly records the still-failing external conformance gate.

### Release completed direct data pages inside row groups

Direct decoding previously kept admitted data-page arrays and decompression output
in the row-group allocation scope. Owned scans now select an explicit iteration
lifetime: each data page gets an allocation scope, its owner is adopted by the
input scope during yield, and iterator advancement/closure releases that owner
and page scope. Consumers retaining an output before advancing keep its backing
charged independently. This applies to both full-column and page-window reads.
Dictionary allocations remain column-lived. Transport backing still belongs to
the input read scope; this change does not establish page-bounded transport.

The borrowed decoder defaults to caller-scope lifetime, preserving its existing
contract for vectors kept after iteration. The owned entry point selects iteration
lifetime automatically, so normal query users need no new tuning. Cached pages
explicitly retain both page and input scope backing, since packed vectors can
alias transport bytes or dictionary data. This remains conservative cache ownership;
selective backing retention is still unfinished.

A real generated 256-row, single-row-group Parquet fixture with 64-byte pages
checks both full-column and page-window decoding. Its admitted decode peak stays
at the first-page peak through every page, succeeds at that exact cap, and rejects
one byte below it with zero residual usage. A retained first output remains valid
and charged after every later page and parent-scope disposal. This test borrows
transport buffers deliberately to isolate decode allocation lifetime; it is not a
remote-cloud or whole-file memory bound. Existing cache pressure, early iterator
closure, nullable/type-width, codec admission, and value tests remain intact.

Page-lifetime checkpoint: 1,290 default tests pass (347 skipped), all 18 workerd
tests and all 88 reference tests pass. TypeScript compilation and lint pass.
The separate external conformance gate is still unresolved.

### Multi-column page ownership and scaling evidence

Two real-fixture regressions now exercise numeric and packed UTF-8 columns with
multiple, differently aligned pages, for both full-column and page-window reads.
They check every cold result, verify multiple page cache entries and zero warm
page writes, retain the first output across all later pages, evict the entire
cache, and verify that output remains valid and charged until its owner releases.
Both child and root usage then return to zero. The earlier isolated page test
also explicitly requires a peak smaller than a full column's numeric backing.
All 30 focused page/cache and owned-reader tests pass.

`bench/parquet-page-lifetime.mjs` generates real two-column Parquet files and
checks every value on every run. Three warmups and five measured repetitions
compare caller-scope and iteration-scoped decoding, with results recorded in
`benchmarks/parquet-page-lifetime-2026-11-12.jsonl`. With 1,024-byte pages, tracked
peaks at 10,000 rows were 60,596 versus 1,398 bytes; at 100,000 rows they were
600,872 versus 1,398 bytes. This demonstrates constant live admitted decode
backing across the measured row-group sizes, including column-page alignment.

Median times were 1.72/1.48 ms at 10,000 rows and 8.38/9.02 ms at 100,000 rows
(scope/iteration). The larger case shows roughly 8% timing overhead in this run;
page ownership is justified by the allocation reduction, not a CPU speedup claim.
Timings include per-value correctness checks and are not isolated decoder CPU.
Transport is a borrowed in-memory file, caches are absent, and these counters
exclude JS heap/metadata and input backing. Remote/cloud behavior is a separate
evidence track, not established by this benchmark.

### Page-window transport lifetime and automatic pressure response

`ParquetReadScope` now forwards explicit owned slices to a caller-supplied scope.
Iteration-lived page-window decoding uses a separate read scope for each header
and body, pins it into the active input scope during decoding/yield, and releases
it on advancement, early closure, or failure. Dictionary-page backing stays in
the parent scope because later pages may alias it. Decoded cache insertion keeps
both input and page ownership, preserving packed-byte aliases after eviction.
Full-column reads and borrowed-scope lifetime remain unchanged.

Owned decoding now automatically chooses page windows when a compressed column
chunk exceeds half the currently available shared budget. It reads the actual
chunk byte length and current headroom, leaving space for decoding and other
columns. This is an admission heuristic, not a complete decoder-size estimator
or a global byte scheduler. Large individual pages can still exceed the cap and
must reject; pressure-driven windows may increase cloud requests. Early-stop and
explicit row-range windows continue to work as before.

A generated real single-row-group fixture checks 128 and 2,048 rows with 64-byte
pages. Total admitted input-plus-decode peaks are equal across those sizes and
below 1,024 bytes. Both explicit page windows and automatic pressure selection
succeed at the measured cap; one byte below rejects with zero residual usage.
A retained first output remains valid after every subsequent page and iterator
completion and releases all tracked backing when its owner releases. These tests
use owned in-memory range reads, not remote cloud transport; metadata and caller
row arrays are outside the measured budget.

The pressure heuristic excludes chunks no larger than the initial 256-byte page
header probe: for those chunks, separate header/body requests can cost more than
reading the whole chunk once. This guard preserves exact-cap behavior on small
files. Automatic pressure selection is enabled by the owned scanner; merely
supplying a scope to the borrowed decoder does not silently change its transport
admission contract. Focused owned-reader/adapter/transport tests pass all 42 cases.

The transport-lifetime implementation also passed all 32 ordinary public-R2 query
cases, with raw evidence in `benchmarks/cloud-range-page-transport-2026-11-12.json`.
That remote run started before the subsequent adaptive-selection heuristic was
added, so it validates transport ownership and existing range behavior, not the
new automatic branch. The heuristic's tight-cap evidence is the separate owned
in-memory fixture test described above.

After the corrected automatic pressure policy, all 1,293 default tests (347 skips),
18 workerd tests, and 88 reference tests pass. Compilation and lint pass.

### Adaptive windows through the HTTP object-store adapter

`page-transport-http.test.ts` serves a generated 256-row, multi-page Parquet file
through an actual loopback HTTP server and the production HTTP object-store
adapter. With a roomy budget it observes a column-chunk request larger than 512
bytes. With a 512-byte job cap and no early-stop hint it observes only ranges of
at most 256 bytes, compares every result, and checks exact measured-cap success
and cap-minus-one rejection with zero residual tracked usage.

The test explicitly records the tradeoff: pressure-driven windows issue more
requests than the full-chunk path. Early iterator exit issues fewer requests than
a complete page scan. Two simultaneous 512-byte child jobs under a 1,024-byte
root complete correctly, reach two concurrent server requests under the global
read limit, stay within tracked capacity, and release to zero. No full-object
GET occurs. This verifies real HTTP streaming/range behavior and concurrent
progress; it is a loopback test, not public-cloud latency or a Worker heap proof.
The focused HTTP test passes alongside its assertion of bounded request sizes.

### Bounded page parsing must not silently truncate results

Page-window reading now rejects a declared page body extending past its column
chunk, or an invalid compressed body length, with `LAKEQL_PARQUET_READ_ERROR`.
Previously the beyond-chunk case returned end-of-input, allowing a malformed
chunk to appear to complete with missing rows. A generated four-row fixture with
its declared chunk shortened by one byte now rejects before yielding output and
releases all tracked backing.

Header-probe growth also now attempts the remaining chunk bytes when the next
geometric probe would exceed the chunk. Previously a header larger than 256 bytes
inside a chunk smaller than 1,024 bytes could fail without trying the available
remainder. A generated page header extended with a legal unknown 300-byte compact
protocol binary field verifies forward-compatible parsing, complete values, and
cleanup. That parser test supplies adjusted metadata for the extended chunk;
it does not claim to regenerate a new footer or validate unrelated metadata.

Both targeted regressions pass. They cover these specific bounded-reader defects,
not complete validation of the external malformed-file corpus, which remains
part of the unresolved conformance work.

Bounded-parser checkpoint: 1,296 default tests pass (347 skipped), all 18 workerd
tests and 88 reference tests pass. Compilation and lint pass.

### Mid-response cancellation with a surviving sibling

The adaptive-window HTTP test now holds one range response open after headers
and its first body byte, waits for the production adapter's client-side allocation
to be charged, and aborts that scan while another scan uses the same globally
limited store and shared pool. It observes the held server response closing,
checks the canceled child's charges return to zero, verifies every sibling result,
and confirms zero residual root usage within the 1,024-byte cap. Synchronization
uses observed allocation state and server response closure, not a guessed delay.
The targeted HTTP test passes. This adds actual streaming cancellation evidence
for page-window scope forwarding; it does not prove behavior of all remote
providers or fill the remaining untracked allocation gaps.

### Admit converted timestamp vector backing

The direct decoder's array-to-timestamp conversion omitted the active allocation
scope in both nullable and nonnullable cases. Both now propagate the scope into
shared vector construction. Nonnullable conversion uses an indexed source rather
than copying an intermediate array slice. Nullable conversion still builds its
JS reconstruction array; this change accounts the timestamp typed backing, not
that array or the upstream timestamp objects.

Generated real Parquet microsecond timestamp fixtures cover required/optional
columns, nulls, negative epoch values, zero, and submillisecond precision. Tests
check complete values and confirm the output buffer is one of the actual scoped
allocations, rather than inferring coverage from an unrelated positive counter.
Each fixture succeeds at its measured exact cap and rejects one byte below with
zero residual usage. The two focused tests pass. Correct admission can increase
reported peaks or reject a previously undercounted workload; the intended contract
is bounded allocation rather than preserving an understated counter.

Timestamp-admission checkpoint: 1,298 default tests pass (347 skipped), all 18
workerd tests and 88 reference tests pass. Compilation and lint pass.

### Remove nullable timestamp reconstruction storage

Nullable timestamp conversion now supplies an indexed source over dense decoded
values and definition levels to shared vector construction, eliminating its
row-sized JS reconstruction array. The source advances a row/value cursor and
rewinds when the constructor starts another pass, keeping each sequential pass
linear with constant cursor storage. The preallocated unused validity mask is
released before construction; the constructor's admitted mask is the returned
vector's validity. Upstream timestamp objects and decoder arrays still exist and
remain outside this improvement's allocation claim.

The timestamp fixture now includes leading, interior, and trailing nulls together
with negative/zero epochs and microsecond precision. Existing output-backing
identity assertions and exact-cap/cap-minus-one checks remain in place. No CPU
speedup claim is made without a separate benchmark.

Nullable-source checkpoint: all 1,298 default tests (347 skips), 18 workerd tests,
and 88 reference tests pass. Compilation and lint pass.

### Broader gates after page and timestamp integration

The current worktree passes `pnpm build` (including published JS/declarations and
fixture compilation), `pnpm typecheck`, `pnpm check:compatibility`,
`pnpm check:large-files`, `pnpm check:packages` (isolated installation/runtime/type
checks), and `node scripts/check-fixtures.mjs` (regenerated fixture hash checks).
`pnpm coverage` also passes: 91.01% statements, 84.79% branches, 95.13% functions,
and 92.99% lines. These are refreshed gates after the page ownership, adaptive
transport, bounded parser, and timestamp conversion changes, not inherited
results from an earlier implementation.

A fresh `pnpm test:conformance` still fails: 167 failures, 83 passes, and one skip.
Those totals match the previous recorded checkpoint; matching totals alone do
not establish per-file equivalence. No conformance assertions were relaxed.
The earlier baseline comparison remains historical evidence; this rerun does not
replace it with a new per-file baseline audit. Release readiness remains blocked
by unresolved conformance and the explicitly incomplete allocation/scheduling
contract, despite the passing package and runtime gates.

### Validate Snappy's declared output size

Both borrowed and scoped Parquet decompression now use the same Snappy wrapper.
It parses at most five bytes of the unsigned 32-bit length preamble, rejects an
unterminated/overflowed length, and requires it to match the page's declared
output length before constructing the output buffer. The upstream decompressor
previously skipped that preamble; its final payload-length check did not detect
a contradictory declaration. The scoped wrapper still performs budget admission
first, preserving budget rejection before touching malformed compressed input.

Four malformed-length tests cover mismatched declarations and oversized varints,
with zero residual admission after failure. A zero-budget empty-payload test
passes. Existing codec and real vector-page tests all pass (60 focused cases).
This validation uses constant scalar workspace; it does not establish bounds for
ZSTD's internal tables or other uninstrumented decoder workspace, which remains
an outstanding allocation-coverage requirement.

Snappy-validation checkpoint: 1,303 default tests pass (347 skipped), all 18
workerd tests and 88 reference tests pass. Compilation and lint pass.

### ZSTD codec boundary investigation

Inspection of the installed `fzstd` 0.1.1 implementation establishes that passing
our admitted destination avoids its full history/output window allocation, but
not all workspace. `rzfh` still creates a 12-byte repeat-offset backing; `rfse`
allocates combined frequency/state tables; `rhu` allocates weight/rank and Huffman
tables; sequence RLE modes create additional buffers. These allocations have no
allocator callback exposed by the installed API. Input-size or output-size
admission alone therefore cannot honestly be described as total codec admission.
A fixed guessed allowance would not establish exact allocation boundaries or
safe malformed-input behavior. The remaining integration needs allocator-aware
codec construction (including table replacement lifetimes) and fixture-backed
validation of those allocations, rather than another output-only wrapper.

A separate concrete correctness probe against the current scoped wrapper uses
the existing five-byte `hello` ZSTD test frame. Output length four throws a bounds
error; five returns `hello`; six returns `hello` followed by a zero byte, with six
bytes charged. The frame's declared content length is five, so the six-byte case
shows that the wrapper cannot rely on the decoder to enforce agreement with the
Parquet page size. This is an unresolved defect discovered by this investigation,
not a passing validation claim. A fix must handle unknown-size frames, concatenated
frames, skippable frames, and compressed blocks rather than comparing only the
first frame header or rejecting valid formats opportunistically.

### Allocator-aware ZSTD integration

The Parquet wrapper now uses an allocator-aware adaptation of fzstd 0.1.1 in
`packages/parquet/src/vendor/zstd.ts`. Provenance: upstream installed
`fzstd/esm/index.mjs`, SHA-256
`1a901fc5a58c349bbdb4f2aa1e688637ef56891ec58243a9f0cd863166b0d2a4`.
The MIT license is retained in a preserved license comment. The vendor body keeps
upstream JavaScript structure under TypeScript's no-check directive and is excluded
from project style rewriting; the integration entry point has an explicit typed
signature. This is a third-party source boundary, not an assertion that unchecked
vendor code satisfies the project's normal typechecked implementation contract.
The adaptation omits the unused streaming API and supplies a fixed-destination
entry point with allocation hooks rather than mutating global typed constructors.

Per-frame offset backing, FSE tables, Huffman workspace/tables, and sequence RLE
buffers now allocate through a separate workspace scope when a query scope exists.
Output admission still occurs first. Workspace charges coexist with output during
decoding and release on success/failure; the output remains owned by its caller.
Workspace is conservatively retained for the entire codec call, so repeated table
replacement and concatenated frames can accumulate charges until decoding returns.
Shorter table lifetimes remain work. Module-level constant tables and JavaScript
objects remain outside this admission contract; a whole-codec/whole-heap bound is
still not claimed.

The entry point tracks actual bytes produced and rejects destination underfill or
overflow, consumes concatenated frames into successive output regions, and checks
frame-content lengths when provided. Tests cover the previously silent five-byte
frame/six-byte destination mismatch, unknown-size frames, concatenated frames,
skippable frames, and exact workspace admission. The `hello` frame peaks at 17
charged bytes (five output plus 12 workspace), retains five after success, and
fails at 16 with zero residual charge. Existing scoped oversize-before-decode and
codec failure cleanup checks remain. All 62 focused codec/vector tests pass.
This is not a claim of checksum verification or complete malformed-ZSTD coverage.

Allocator-aware ZSTD checkpoint: 1,305 default tests pass (347 skipped), all 18
workerd tests and 88 reference tests pass. TypeScript compilation and lint pass.
The published-package build also passes, and the upstream copyright/license
comment is present in both the compiled decoder and bundled package output.

### Release ZSTD workspace at frame boundaries

The allocator-aware entry point now calls a frame-release hook in `finally` after
each frame, including malformed frames and skippable frames. The scoped wrapper
creates workspace lazily and disposes it at that boundary; output ownership spans
the full decode call. Independent frames no longer accumulate workspace charges.
This replaces the previous per-call lifetime for frame workspace. Entropy-table
replacement within a single frame still conservatively retains earlier tables
and remains separate work.

Tests decode 1 and 100 concatenated `hello` frames at exactly output bytes plus
12 workspace bytes; both retain only their output after success and reject one
byte below peak with zero residual usage. A truncated second frame also releases
the previously admitted output and all workspace. All 15 codec tests pass.
These synthetic frames test frame isolation; compressed-block entropy-table
coverage continues to rely on separate real-fixture/reference tests.

Frame-lifetime checkpoint: 1,308 default tests pass (347 skipped), all 18 workerd
tests and 88 reference tests pass. Compilation and lint pass.

### Release replaced ZSTD block workspace

After each decoded block the codec reports the backing buffers still referenced
by frame state: repeat offsets, the active Huffman table, and the active sequence
tables. The wrapper releases other admitted workspace allocations, retaining
repeat-table state for subsequent blocks. Frame completion still releases all
workspace. This removes accumulation from replaced tables and construction
scratch across blocks; allocations needed together while building a replacement
remain charged together. Module constants and JS objects remain outside coverage.

A deterministic 1 MiB payload with changing symbol distributions is compressed by
Node's native ZSTD implementation. The test verifies complete decoded bytes,
compares block-lifetime peak with the earlier frame-retention policy, succeeds at
the exact measured cap, and rejects one byte below with zero residual usage.
Full-buffer Node assertions avoid the generic assertion library's costly per-index
comparison; no values are sampled or omitted.

`bench/zstd-workspace.mjs` records three warmups and five measured decodes, checking
all output bytes after each timed region. Results in
`benchmarks/zstd-workspace-2026-11-12.jsonl` show 679,860 compressed bytes for the
1,048,576-byte input. Workspace peak falls from 20,631 to 13,656 bytes; total
admitted output-plus-workspace peak falls from 1,069,207 to 1,062,232 bytes.
Median decode times are 18.10/18.43 ms (frame/block lifetime), so no CPU speedup is
claimed. Input storage, compression, module constants, and JS heap are excluded
from those allocation counters; this is a codec benchmark, not a cloud-query cap.

Block-lifetime checkpoint: 1,309 default tests pass (347 skipped), all 18 workerd
tests and 88 reference tests pass. Compilation and lint pass.

### ZSTD frame-size arithmetic and header bounds

The adapted frame parser now reads content lengths without JavaScript 32-bit
bitwise truncation, rejects values outside safe integer range, and rejects a
known frame length larger than the remaining destination before workspace
construction. The window descriptor likewise uses arithmetic exponentiation
rather than wrapping shifts. A declared content length of zero is distinct from
an absent/unknown length, so a nonempty frame cannot evade the length check by
declaring zero. Required header, dictionary-id, content-size, and skippable-frame
length fields must fit inside the compressed input before being read.

Tests cover an eight-byte size with nonzero high bits, the corresponding valid
small size encoded in eight bytes, a false zero declaration, overflowing sizes,
and truncated ordinary/skippable frame headers. Failures leave zero tracked
usage. All 18 focused codec and native-compressed workspace tests pass. These
checks do not add checksum verification or claim complete malformed-block coverage.

Frame-parser checkpoint: 1,311 default tests pass (347 skipped), all 18 workerd
tests and 88 reference tests pass. Compilation and lint pass.

### Make the ZSTD adaptation fixed-output only

Removed inherited dynamic-output/history allocation branches and their unused
slice/concatenation helpers from the vendored synchronous decoder. Its only entry
point requires the caller's fixed destination, so those fallback paths were not
part of the supported integration contract. Frame state now always references
that destination and allocates only its 12-byte offset workspace through the hook.
Block decoding likewise always writes into destination views.

Review of remaining typed constructors distinguishes views over admitted buffers
from module-initialized constants; per-call backing construction goes through the
allocator hook. This improves auditability and removes latent bypass paths; it
does not charge module constants or JavaScript objects and does not itself claim
a measured speedup. All 18 focused codec/workspace tests and lint pass, and the
TypeScript projects compile.

### Conformance caught and resolved an empty Snappy section regression

The external gate rerun after the codec changes reported 168 failures rather than
167. Comparing failing paths identified one new rejection:
`data/datapage_v2_empty_datapage.snappy.parquet`. The earlier strict Snappy prefix
check rejected its empty Data Page V2 data section, whose compressed and expected
uncompressed sizes are both zero. The wrapper now accepts exactly that empty/empty
case, preserving rejection of empty input requesting nonzero output and all other
length checks. No conformance assertions were changed.

The actual Apache fixture was added to independent DuckDB value comparisons and
owned exact-cap/cap-minus-one admission checks. All 89 reference tests now pass;
all 18 focused codec/workspace tests pass after the correction. Compilation and
lint pass. The external gate returns to 167 failures, 83 passes, and one skip.
The full default/workerd/reference suites also passed for the fixed-output ZSTD
simplification before this empty-section correction; the subsequent targeted and
reference checks specifically cover the correction. Remaining external failures
are still unresolved, and the gate remains red.

### Native ZSTD parity matrix

`zstd-native-parity.test.ts` independently compresses 54 deterministic payloads
with Node's native ZSTD encoder: compression levels 1/3/9, lengths 0/1/31/1,024/
131,072/131,073, and repeated/structured/pseudorandom byte distributions. The
allocator-aware decoder must reproduce every byte, retain only its output after
decoding, and return tracked usage to zero after scope disposal. The matrix spans
empty frames, literals/RLE-friendly inputs, compressed sequences, incompressible
inputs, and the 128 KiB block boundary without assuming which encoding the native
encoder chooses.

For every compressed payload the test also truncates at three distinct positions
near the header and final block, requires rejection, and checks zero residual
admission. All three parameterized tests (54 payload cases and their truncations)
pass. These are deterministic native-encoder differential checks, not exhaustive
fuzzing, checksum verification, or a remote/cloud memory measurement.

### Share checked scalar decimal conversion

Integer-decimal row normalization previously called the array converter for every
cell, constructing a singleton input array and an output array and recomputing
the scale factor. Rows and columns now share a checked scalar converter; row
normalization prepares it once per decimal column and calls it directly. Existing
safe-integer rejection, null/undefined handling, column-specific error metadata,
and nonmutating row output are preserved. This removes per-cell temporary arrays;
column-array materialization and upstream decoder storage remain separate gaps.

All 93 decimal/public-reader tests pass, along with TypeScript compilation and
lint. This is allocation removal established by the changed construction path;
no measured CPU or whole-query heap improvement is claimed.

### Decimal row conversion measurement

`bench/decimal-row-conversion.mjs` compares the earlier singleton-array calling
pattern with shared scalar normalization using the current checked conversion
primitive. It includes nulls, negative unscaled integers, preserved nondecimal
fields, and frozen input rows; every output row is compared outside the timed
region. After three warmups, seven-run medians are 4.90/0.62 ms at 10,000 rows
and 43.76/3.66 ms at 100,000 rows (singleton arrays/shared scalar). Raw observations
are in `benchmarks/decimal-row-conversion-2026-11-12.jsonl`.

This isolates normalization and demonstrates the cost of per-cell temporary
arrays and converter setup. It does not measure Parquet I/O, decoding, concurrent
jobs, heap peaks, or cloud performance. The comparison reconstructs the old call
pattern using the current shared conversion primitive rather than building an
entire historical package revision.

### Shared Parquet batch-size validation

Direct, owned, and fallback column readers now use one `parquetBatchSize` contract:
a positive safe integer, defaulting to 4,096. The fallback planner previously
entered its row-stepping loop without that validation; zero/negative/nonfinite
values could cause stalled iteration or invalid ranges. Invalid values now fail
before schema planning, reads, or allocation, matching the existing owned/direct
contract. Valid caller row limits and the default are unchanged.

A generated real fixture checks all three reader entry points against zero,
negative, fractional, NaN, infinity, and unsafe integer sizes, asserting no
borrowed/owned range calls and zero peak allocation. Default and safe explicit
limits are covered separately. All 80 focused batch-size, owned-reader, and
direct-vector tests pass; compilation and lint pass. This closes input-shape
validation duplication, not the remaining measured byte-aware batch scheduler.

### Cancel owned reads while waiting for concurrency admission

Inspection of read scheduling found that queued owned reads observed the wrapper's
global signal but not their caller scope's signal until after acquiring a read
slot. A canceled job could therefore remain queued behind an unrelated slow read.
The wrapper now creates its combined read scope before semaphore acquisition and
uses that signal for queueing, execution, and post-read ownership handoff. The
scope is disposed in `finally`, while successful output retains caller ownership.
No range allocation or underlying I/O occurs merely from constructing this scope.

A regression holds one active read behind an explicit gate, queues another owned
read, disposes only the queued caller scope, and requires its rejection before
opening the active gate. Only one underlying read starts, peak allocation remains
zero until that read proceeds, and final usage returns to zero. All 19 focused
read-control/memory-store/semaphore tests pass. This fixes cancellation quality;
it does not introduce waiting byte reservations or claim to complete the mandatory
byte-aware scheduler, whose retained-input deadlock contract remains outstanding.

Owned-queue checkpoint: 1,317 default tests pass (348 skipped), all 18 workerd
tests and 89 reference tests pass. Compilation and lint pass.

### Current remote regression and cross-adapter codec gap

The current public-R2 matrix passes all 32 ordinary/mixed query cases after the
adaptive-window and queued-cancellation changes. Raw observations are saved in
`benchmarks/cloud-range-current-2026-11-12.json`. Maximum incremental tracked peak
is 127,995 bytes within the 131,072-byte headroom; all warm cases issue zero data
reads; observed data-read concurrency reaches four. Size probes remain. This
small-fixture Node/remote-R2 run does not establish large-file or whole-Worker heap
bounds or isolate ZSTD decoding performance.

The semaphore's queue was also reviewed: it already uses a doubly linked FIFO,
with constant-time enqueue, cancellation removal, and transfer. No replacement
queue structure is needed. This does not resolve byte-aware scheduling.

Cross-adapter search found `packages/lance/src/deletions.ts:decodeArrowBuffer`
still calls upstream fzstd directly. Its production deletion path reserves the
advertised output size through the Lance read context, but codec workspace and
actual decoded-length validation do not receive the Parquet adaptation. The
current adapter-local codec integration must become a shared capability before
claiming system-wide ZSTD coverage. The direct helper and other callers also need
explicit ownership boundaries; merely swapping imports would not establish those
bounds. This remains an identified implementation gap.

### Shared ZSTD boundary for Lance deletion reads (2026-11-12)

Moved the licensed fixed-output decoder from Parquet into
`packages/core/src/vendor/zstd.ts`, exported as `decompressZstdInto`.
Parquet and Lance now use the same actual-output-length validation and typed
workspace hooks. Existing upstream provenance and license remain unchanged.

Lance's range-read deletion path already leases decompressed output until
consumption finishes. It now additionally leases decoder workspace before each
allocation, releases replaced block tables and completed frames, and releases
workspace on decoding failure. Output is not charged twice. Borrowed
`decodeArrowBuffer` calls remain borrowed; the optional read context accounts
workspace only, with output lifetime managed by the caller.

Evidence: build, lint and TypeScript pass; 103 tests across the Lance suite and
Parquet compressor/workspace/native parity suites pass. Tests cover a 24-byte
output-plus-workspace exact cap, rejection at 23 bytes, truncated frames,
underfilled/overfilled Arrow envelope lengths, and reservation cleanup. The
official deletion fixture also passes a measured exact-cap range-read run and
rejects at one byte less; context-retained decoded metadata is explicitly
released before checking that the whole budget is reusable. The wider Lance
suite includes its loopback HTTP byte-range fixture test (run with network
permission). This is not a new public-cloud or Worker heap measurement.

Remaining: Lance still has its own context accounting rather than shared
root/per-query MemoryScope ownership. Decoder JavaScript objects and static
tables remain untracked. Sharing this decoder does not establish a whole-query
memory bound or resolve the outstanding byte-aware scheduler work.

### Lance pending range failure lifetime (2026-11-12)

The shared-budget ownership trace found that Lance used fail-fast Promise.all
for a group of admitted ranges. A failed or truncated response released the
entire group's reservation while sibling transport calls could still be
allocating. Changed this boundary to wait for every range to settle before
propagating failure and releasing the group reservation. Successful parallel
reads retain the same range planning and concurrency controls.

Two deterministic gated-transport tests prove that rejection and truncation
both leave the reservation occupied while a sibling remains pending: another
one-byte admission is rejected at a four-byte cap, then the full four bytes
are reusable after the pending read settles. This is a transport-lifetime
test, not a remote-cloud latency benchmark. All 84 Lance tests pass, including
the official fixture HTTP range test; TypeScript and repository lint pass.

Tradeoff: an error is reported only once outstanding borrowed transports settle.
A transport that ignores cancellation can therefore delay failure reporting;
releasing its allocation charge early would violate the budget contract.
Per-range owned transport and shared root ownership remain outstanding.

### Shared Lance operation admission (2026-11-12)

Added optional `OpenLanceDatasetOptions.memoryBudget`, using the same
MemoryBudget pool abstraction as the generic engine. Each dataset-open or
read operation creates a child with `budget.maxAllocatedMemoryBytes`; every
existing Lance range/decoded/workspace reservation now also reserves that
child before proceeding. Existing `maxMemoryBytes` checks remain in force.
Reservation objects release the exact corresponding shared charge; decoded
operation reservations are released together at the established cleanup boundary.
Dataset opening now uses finally cleanup for failures as well as success.

Evidence: a gated parallel test fills a six-byte root using a four-byte pending
read and a two-byte sibling lease, rejects excess admission, verifies the
four-byte child cap independently, and confirms zero residual charge and reuse.
The real takeRows fixture exercises the public pool option. All 85 Lance tests,
including HTTP range fixtures, pass; TypeScript and lint pass. Additional public
fixture assertions exercise simultaneous takeRows calls and zero-cap opening.

Boundary: these are existing instrumented Lance reservations, not full physical
ownership. Borrowed transport allocation behavior, manifest copies made before
accounting, retained parsed metadata, decoded index caches, and returned JS rows
still need ownership integration. Dataset-retained state is not kept charged
after open. This addition establishes shared admission for tracked operation
state and does not establish a total process/Worker heap cap. The default remains
unlimited unless a caller supplies a shared pool or allocation limit.

### Lance owned transport admission (2026-11-12)

Lance range groups now prefer ObjectStore.getRangeOwned when available, using
a MemoryScope over the operation's child budget and cancellation signal.
Owned adapters reserve their actual backing allocations before allocation;
the Lance logical-byte reservation still enforces its existing maxMemoryBytes
limit, but does not charge the shared allocation budget a second time.
RangeLease release disposes the transport scope. Failure still waits for all
sibling calls to settle before releasing the group. Borrowed adapters retain
the pre-reserved range-byte path.

Two gated owned-transport tests prove exact four-byte admission without double
charging, successful byte delivery, pending allocation retention after abort,
no borrowed fallback calls, and zero final usage. The official HTTP fixture now
passes a shared pool through dataset opening and projected reads and verifies
nonzero peak admission and zero final usage. All 87 Lance tests, TypeScript,
and lint pass. HTTP evidence uses a loopback server with the production adapter,
not a public-cloud benchmark.

Remaining boundaries: borrowed adapters cannot guarantee their physical
allocation sizes, and metadata copies, retained parsed dataset state, index
caches, and returned JavaScript rows remain outside complete ownership.
The owned transport improvement does not imply total Worker heap bounds.

### Admit and reduce Lance manifest copies (2026-11-12)

Dataset opening now reserves the retained tail copy before allocating it.
When the manifest fits in that tail, combined-message and protobuf-message
buffers are views over the admitted copy, removing two copies. Larger manifests
admit their combined prefix/tail buffer before allocation; the message is a
view over that buffer. Removed the later duplicate logical reservation.
Legacy metadata-cache writes admit their outgoing copy during construction/set,
then transfer lifetime responsibility to that legacy cache; cache hits retain
the existing operation-local accounting. Parsed dataset state remains a separate
unresolved lifetime boundary.

Evidence: 88 Lance tests passed including the HTTP fixture, with lint and
TypeScript passing. Added a subsequent focused parameterized boundary test:
both the original producer fixture and that fixture extended with a valid
unknown protobuf bytes field (forcing the >64 KiB prefix/tail path) succeed at
the measured peak and fail at peak minus one, with zero residual pool usage.
Both cases pass. Corrupt-manifest rejection also proves zero residual charge.
The extended fixture is a transformed producer fixture, not independently
generated producer output. No new end-to-end latency benchmark was run.


### Broader regression checkpoint after Lance ownership changes (2026-11-12)

Verified the current build rather than relying on the focused Lance suites:
1,324 default tests pass (348 skipped), 18 workerd tests pass, and 89 independent
reference tests pass. Build, repository lint, and package pack/install checks
pass. Package verification initially encountered sandbox subprocess EPERM and
passed when rerun with the required execution permission; no package assertion
was relaxed. Workerd emitted cancellation-related diagnostics while its tests
passed; this is runtime compatibility evidence, not a new heap-cap measurement.

Logs: /tmp/lakeql-shared-lance-build.log,
 /tmp/lakeql-shared-lance-full-tests.log,
 /tmp/lakeql-shared-lance-workerd.log,
 /tmp/lakeql-shared-lance-reference.log,
 /tmp/lakeql-shared-lance-packages.log,
 /tmp/lakeql-shared-lance-lint.log.
External conformance was not rerun at this checkpoint and its recorded failures
remain unresolved. Mandatory byte-aware scheduling and complete allocation
coverage remain acceptance blockers.

### Lance coalescing adapts to current headroom (2026-11-12)

Optional gap reads now fall back to exact merged logical ranges when the
coalesced group exceeds available shared/child allocation headroom, remaining
legacy memory allowance, or remaining physical-byte allowance. Coalescing also
declines when its gaps alone would turn partial data ranges into a forbidden
whole-file read. Exact requests remain subject to all existing byte, memory,
request-count, and whole-file constraints; no limit is relaxed.

Five added cases cover owned and borrowed transports with a pinned sibling
reservation, resumption of coalescing after that reservation releases, and
physical-byte, legacy-memory, and whole-file constraints. With a six-byte root,
two pinned bytes plus four requested bytes succeed as two reads; after release
the same logical request coalesces to one six-byte read. All 94 Lance tests,
TypeScript and repository lint pass, including the official HTTP fixture.

This is an adaptive optional-I/O choice, not the mandatory byte-aware scheduler.
A group whose exact retained output cannot fit still fails admission. Shared
headroom may change after planning; allocation-time admission remains mandatory.
No public-cloud latency claim is made for these local deterministic tests.

### Bound pending range work, not only active transport (2026-11-12)

Lance previously created a promise/callback and queued controlled read for every
physical range. The transport semaphore bounded active I/O but not this pending
work. Added generic core mapConcurrent: ordered results, at most the configured
number of active callbacks, stop admission on first observed failure, and await
all active callbacks before rejecting. Lance now uses that mapper with its
configured maxConcurrentReads (default four) instead of eager allSettled fan-out.
Result/range arrays remain proportional to the required retained result; pending
worker promises are proportional to concurrency. The mapper does not copy values.

Evidence: the 10,000-item test checks the initial callback window and ordered
results despite out-of-order completion. Tests cover active-sibling lifetime on
failure, synchronous undefined rejection, empty input, and invalid concurrency.
A Lance test verifies that a failed first read with concurrency one never starts
the remaining ranges and releases its shared charge. The 29 focused tests and
all 95 Lance tests pass, including real fixture HTTP reads. Build, TypeScript,
and repository lint pass.

This removes an unbounded pending-I/O queue; it does not implement waiting
byte reservations. Required retained results must still fit their budget.
Failure stats continue to record planned/admitted request counts as before,
which can exceed requests actually started after cancellation or failure.
No whole-heap or latency improvement is claimed from these tests alone.

### Drain fragment and file work before operation cleanup (2026-11-12)

Extended generic bounded mapping to Lance deletion checks across fragments,
fragment materialization, and files within a fragment. Each boundary stops
starting callbacks after its first observed failure and waits for active siblings
before returning to operation-level cleanup. Previously eager Promise.all could
reach releaseDecodedMemory while sibling decoding still used operation state.
Each mapper uses configured read concurrency, default four; the shared transport
semaphore continues to bound actual I/O across nested calls.

A real multi-fragment producer fixture test holds one fragment response pending
while failing its sibling. It verifies no early operation settlement, nonzero
pool usage while pending, no third fragment admission, original error propagation,
and zero usage after drain. All 96 Lance tests, TypeScript, and lint pass,
including the HTTP fixture. This complements generic mapper and range-level
lifetime tests rather than treating those narrow tests as whole-operation proof.

Nested mapper windows are bounded at each level, not a single global task-count
window. Parsed/output JS state and waiting byte admission remain outstanding.
Other eager hydration paths (notably Iceberg snapshots/manifests) were identified
for subsequent review; this checkpoint does not claim those are resolved.

### Bounded Iceberg snapshot/manifest hydration (2026-11-12)

Replaced eager Promise.all mapping for snapshot and manifest hydration with the
shared bounded mapper, using configured maxConcurrentReads or four by default.
Existing manifest-list/manifest promise caches remain shared across snapshots;
their deduplication and durable-cache behavior are unchanged. Active callbacks
drain before hydration failure is reported, and callbacks not yet admitted stop
after an observed failure. Abort checks remain at callback boundaries.

All 61 Iceberg unit/fixture tests pass, including real Avro/JSON metadata fixtures,
cross-snapshot deduplication, durable caches, cancellation/timeouts, and two new
gated failure tests for snapshot and manifest fan-out. Those tests verify only
two reads start, a third never starts after failure, and the metadata load waits
for its pending sibling. Errors preserve the existing LAKEQL_CATALOG_ERROR
wrapper and original cause. TypeScript and lint pass.

This bounds callback queues per hierarchy level. Retained hydrated metadata and
promise-cache entries still scale with the requested snapshot/manifest set and
lack complete allocation accounting. The tests use in-memory controlled stores;
no new cloud or Worker heap measurement is implied.

### Bounded scheduling measurements and Lance HTTP operation (2026-11-12)

Added bench/bounded-read-mapping.mjs comparing eager Promise.all plus the real
ReadSemaphore against mapConcurrent plus the same semaphore. Both execute the
same microtask transport simulation, verify every ordered result, and hold active
reads at four. Three warmups and five measured samples per case:

| Items | Eager pending peak | Bounded pending peak | Eager median | Bounded median |
| --- | --- | --- | --- | --- |
| 10,000 | 10,000 | 4 | 10.359 ms | 1.585 ms |
| 100,000 | 100,000 | 4 | 86.565 ms | 15.488 ms |

Pending counts include active callbacks. This measures scheduling overhead and
callback count, not heap bytes, network latency, or decoding performance.
Artifact: docs/benchmarks/bounded-read-mapping-2026-11-12.jsonl.

Extended the existing Lance random-read benchmark to supply a 64 MiB shared pool,
32 MiB per-operation allocation cap, record tracked peak/final allocations, and
reject residual charges. Ran five trials against the checked-in pylance 8.0.0
64-row producer fixture via its loopback HTTP range server, checking all five
requested projected rows against fixture expectations. Median open-plus-read wall
time was 16.910 ms; every trial reported 14 takeRows range requests, peak tracked
allocation 2,996 bytes, final allocation zero, and no whole-object GETs.
Artifact: docs/benchmarks/lance-http-current-2026-11-12.json.
This is current-operation evidence without a before/after baseline. It is a small
local HTTP fixture, not public-cloud latency or near-cap/whole-Worker heap proof.
Build and repository lint pass.

### Drain Parquet column cleanup before scope exit (2026-11-12)

Parquet aligned-column iteration used fail-fast Promise.all when calling column
iterator return methods. Replaced that cleanup boundary with shared
closeAsyncIterators: invoke every closer, settle all cleanup, then propagate the
first rejection in input order. Unlike bounded work scheduling, cleanup cannot
stop starting remaining closers when one fails; every column must release.

Tests include a synchronous throwing closer, a gated pending closer, a later
closer that must still execute, missing return methods, empty input, and undefined
rejection. The helper plus owned vector/page-cache fixture suites pass 32 tests;
TypeScript and repository lint pass. These fixture suites exercise normal
early-exit and ownership behavior; the adversarial closer timing is tested at
the shared helper boundary, not claimed as an injected corrupt Parquet decoder.
Also formatted the previously saved Lance HTTP benchmark JSON for repository
lint without changing measurement values.

### Bound Parquet read-ahead callbacks (2026-11-12)

Owned and borrowed range-cache prefetch now use the shared bounded mapper.
RangeCacheOptions.maxConcurrentPrefetchReads defaults to four and validates a
positive safe integer. The byte admission forecast still selects retainable
ranges; the callback window additionally prevents a large tiny-range set from
creating one pending promise per range. Owned prefetch continues to decline
allocated-memory pressure, while other errors stop new work and drain active
siblings. Legacy prefetch now also drains its active callbacks before rejection.

Two gated tests run 64 disjoint one-byte ranges through owned and borrowed
caches, prove a configured peak of two callbacks, verify cached bytes afterward,
and assert no additional demand read. Invalid concurrency cases are covered.
All 37 range-cache tests pass. The full default Parquet suite passes 357 tests
with 273 conditional skips, including local HTTP/real fixture coverage;
TypeScript and lint pass. External conformance was not enabled by this command.

The selection map still scales with selected ranges, within retained-byte
admission; this change bounds pending callbacks rather than claiming all JS
planning state is accounted. Per-invocation concurrency is not a global
byte-wait scheduler. Demand ownership, deduplication, cache admission and
allocation-time limits remain authoritative.


### Public R2 revalidation after bounded prefetch (2026-11-12)

Rebuilt and reran bench/cloud-range-validation.mjs against the public R2 fixture.
All 32 ordinary aggregate/top-k/projection/mixed scenarios pass across 64/128 MiB
shared pools, one/four concurrent jobs, and cold/warm caches. The root is pinned
to leave only 131,072 bytes for query admission. Maximum incremental charged
peak remains 127,995 bytes; active reads never exceed four; every warm ordinary
query case makes zero data reads. Lower-level range/decode scenarios in the same
script also pass. Artifact:
docs/benchmarks/cloud-range-bounded-prefetch-2026-11-12.json.

The current full default suite passes 1,350 tests with 348 skips; all 18 workerd
tests pass. Build passes. These remote assertions preserve the earlier recorded
transport/cache/allocation outcomes after scheduling changes. This is Node
reading a small public R2 fixture, not an in-Worker public-cloud stress run or
a measured total-heap guarantee. No remote latency speedup is inferred from
sequential runs with uncontrolled network conditions.

### Packed fixed-width binary pages (2026-11-12)

Unannotated FIXED_LEN_BYTE_ARRAY leaves now advertise direct vector support and
use the existing packed binary reader for PLAIN pages. The vector stores its
fixed width and aliases page backing; no per-value view array or offset table is
needed. Nullable pages use admitted definition/presence/validity buffers as the
existing variable-width path does. Missing fixed width is rejected. Annotated
UUID/decimal and other semantic conversion paths retain their existing rules;
dictionary/fallback allocations are not claimed resolved by this change.

Writer-generated required/nullable real Parquet fixtures cover non-UTF8 bytes,
nulls, split output batches, packed shape, exact-cap success, nullable cap-minus-one
rejection, and zero required typed workspace when input is borrowed. Owned input
tests retain the first batch after iterator closure, verify its values, and
release the owner back to zero charged usage. This proves backing lifetime rather
than interpreting the borrowed zero-workspace case as free transport memory.
All 359 default Parquet tests pass with 273 conditional skips, including HTTP
fixtures; TypeScript and lint pass. External conformance and independent reference
comparison for this newly enabled path still need refreshing.


### Fixed-width binary independent reference and conformance (2026-11-12)

Expanded the existing fixed-width binary DuckDB comparison to explicitly use
PLAIN encoding with UNCOMPRESSED and SNAPPY codecs. Both compare every byte via
hex, including nulls and invalid UTF8 values, and run normal owned scans in
two-row batches at the measured allocation peak and peak minus one. Successful
and rejected operations return their shared pool usage to zero. All 90 reference
tests pass; build, TypeScript, and lint pass.

Reran external conformance without changing expectations: 167 failures, 83 passes,
one skip. Compared all named failed tests with the previous detailed checkpoint:
no added or removed failures. This comparison does not check passing-output
digests and does not make the red conformance gate acceptable. Artifact:
docs/validation/fixed-binary-conformance-2026-11-12.json.
Logs: /tmp/lakeql-fixed-binary-reference.log and
/tmp/lakeql-fixed-binary-conformance.log.

### Packed binary and text dictionary backing (2026-11-12)

The scoped direct reader now represents eligible PLAIN dictionary pages as
PackedByteVector instead of constructing upstream per-value byte-view arrays
and then rebuilding a vector. Text, raw variable binary, and raw fixed binary
share this path. Variable-width ranges are admitted before allocation; fixed
width uses implicit offsets. Dictionary indices reference that packed vector
directly. Dictionary cache values can retain the packed representation and its
owned backing. Annotated semantic conversions keep their existing paths.

Three Snappy writer fixtures with repeated values and nulls assert actual
dictionary-page metadata, packed output shape, and all decoded rows. Each runs
without cache at measured peak and peak minus one, verifies zero residual charges,
then runs cold/warm with shared decoded cache and retains its first output past
iterator completion and cache eviction. Raw bytes include invalid UTF8; text
includes empty strings and multibyte characters. All 362 default Parquet tests
pass with 274 conditional skips; TypeScript and lint pass.

Independent reference/conformance and measured performance refreshes remain
needed for this change. This removes per-dictionary-entry arrays/views in the
eligible scoped path, not all dictionary/decoder JavaScript allocations.

### Packed dictionary validation and kernel tradeoff (2026-11-12)

Current build passes all 90 reference tests. External conformance remains 167
failures, 83 passes, one skip; comparing named failures to the fixed-binary
checkpoint finds none added or removed. No expectations were weakened.
Logs: /tmp/lakeql-packed-dictionary-reference.log and
/tmp/lakeql-packed-dictionary-conformance.log. Build and lint pass.

Added bench/packed-dictionary.mjs: identical raw binary dictionary bytes through
upstream readPlain plus the prior per-page array-copy/mapping shape versus packed
range construction and dictionary-vector reuse. Every value is checked outside
timing; three warmups and five samples per case. Results:

| Entries | Page uses | Previous shape median | Packed median |
| --- | --- | --- | --- |
| 1,000 | 1 | 0.034 ms | 0.054 ms |
| 1,000 | 32 | 0.226 ms | 0.018 ms |
| 10,000 | 1 | 0.379 ms | 0.151 ms |
| 10,000 | 32 | 1.220 ms | 0.054 ms |

The small single-use case regresses; repeated dictionary construction is where
the measured benefit is strongest. JIT/order effects and short durations limit
precision. Packed ranges explicitly admit eight bytes per dictionary entry.
The previous shape reports zero admitted typed workspace because its JS arrays
and byte-view objects were untracked, not because they used no memory. Neither
column is a total-heap measurement. This kernel excludes indices, decompression,
I/O and value access; deferred text decoding costs are not measured here.
Artifact: docs/benchmarks/packed-dictionary-2026-11-12.jsonl.

### Release dictionary predicate workspace after expansion (2026-11-12)

The comparison, geometry, and string/H3 dictionary predicate paths already
evaluate each dictionary entry once rather than decoding it for every row.
They duplicated the same row-mask expansion and retained the temporary dictionary
mask until the enclosing expression scope closed. Extracted expandDictionaryMask,
which preserves SQL unknown/null handling and releases that temporary allocation
in finally after constructing independent row-mask backing. Failed output
admission also releases the dictionary workspace immediately.

An eight-byte exact-cap test admits three dictionary truth bytes plus five
output bytes, then retains only the five-byte output. The output survives scope
closure through its owner and is independent of later dictionary-mask mutation.
At seven bytes, output admission fails and dictionary workspace returns to zero.
Borrowed use, empty dictionaries, null validity and out-of-range dictionary
indices preserve existing behavior. All 549 core tests pass, including the
comparison/spatial/H3 integration tests; TypeScript and lint pass.

This shortens temporary mask lifetime and removes duplicated expansion logic.
It does not account decoded text strings, eliminate row-mask storage, or claim
whole-query allocation completeness.

### Incremental compound predicate masks (2026-11-12)

Logical predicates no longer allocate all operand masks before reducing them.
They evaluate operands in original order, combine into the private left mask,
and release right-mask workspace immediately. AND/OR/BETWEEN reuse their left
backing; NOT transforms its private input mask in place. Source column buffers
are not mutated, and operands are still evaluated (no new short-circuit semantics).

Tests cover all nine SQL true/false/null pairs, two and 100 operands at the same
18-byte peak for nine rows, NOT at nine bytes, source immutability, retained output
ownership, one-byte-short admission, and later operand errors. The prior
four-row BETWEEN test expected failure at eight bytes; that cap now succeeds
because only two masks overlap. Added explicit eight-byte success and moved
failure/cleanup verification to seven bytes. All 554 core tests and 90 independent
reference tests pass. Build, TypeScript, and lint pass.

This bounds flat logical-mask workspace by the active expression frontier rather
than total operand count. Nested expression depth and other expression values
still require their own allocations; this is not a complete expression/heap cap.


### Compound predicate scaling evidence (2026-11-12)

Added bench/compound-predicate-memory.mjs against the built production predicate
kernel. Nullable boolean data covers true/false/unknown, with an independent
scalar truth-table result checked for every row outside timing. Three warmups
and five measured samples cover 10k/100k rows, AND/OR, and 2/32/128 operands.
Each execution uses an exact two-row-buffer cap, retains only its result, and
returns to zero after scope disposal.

Peak typed masks remain 20,000 bytes at 10k rows and 200,000 bytes at 100k rows
for every tested expression width. At 100k rows, AND medians are 1.99/36.69/146.62
ms for 2/32/128 operands; OR medians are 1.89/34.67/138.65 ms. At 10k rows with
128 operands, AND/OR take 14.58/13.77 ms. Runtime remains proportional to the
work evaluated while mask memory is independent of flat operand count.
Artifact: docs/benchmarks/compound-predicate-memory-2026-11-12.jsonl.

This is current-path scaling evidence, not a measured before/after speedup.
Input vectors are borrowed and their construction, JS heap, I/O and nested
expression allocations are excluded. Full current regression passes 1,363 tests
with 349 conditional skips; repository lint passes. Log:
 /tmp/lakeql-compound-full-tests.log.

### Compact CASE branch choices (2026-11-12)

CASE evaluation now records first matching branches in one admitted choice vector,
using Uint8/Uint16/Uint32 according to branch cardinality and a distinct ELSE
sentinel. Each WHEN predicate mask is consumed and released after recording
choices, instead of retaining one row mask per branch. Value lookup indexes the
chosen branch directly rather than scanning all branch masks for each requested
row. Predicates/branch expressions retain their existing evaluation order and
first-match/unknown semantics.

Tests cover 2, 100, 255 and 256 branches, exact typed-workspace caps, cap-minus-one,
first-match precedence, NULL WHEN conditions, absent ELSE, and failed projection
cleanup leaving the parent scope usable. Four-row simple CASE uses eight bytes
at up to 255 branches and twelve at 256, retaining only four/eight choice bytes
after predicates are consumed. All 560 core tests and 90 reference tests pass;
build, TypeScript, and lint pass.

Choice vectors do not account branch-expression JS objects or remove allocations
inside nested CASE/value expressions. No end-to-end performance improvement is
claimed without a measured benchmark; this checkpoint proves the flat CASE
workspace bound and semantics.

### Reuse compatible CASE truth backing (2026-11-12)

Refined compact CASE choices to reuse the first private predicate mask whenever
the branch-choice width is one byte. This is a vector-shape reuse rule: the
truth buffer is rewritten to first-branch/ELSE choices and becomes the retained
choice buffer. Wider choice representations allocate their required backing
and release the first truth mask normally. Empty branch lists allocate no
choice buffer. This avoids a two-buffer regression for one-branch CASE.

The expanded matrix proves four-row one-branch CASE at four bytes (three rejects),
ordinary byte-width cases at eight, the 256-branch Uint16 boundary at twelve,
and empty branches at zero. First-match and NULL/ELSE tests remain green.
All 562 core tests, TypeScript, and lint pass. The latest independent reference
checkpoint predates this shape-reuse refinement and remains 90 passing tests.

### Preserve direct projection vector aliases (2026-11-12)

Computed projections now reuse BatchExprValues.vector when expression evaluation
already provides a vector, instead of decoding every value through valueAt and
rebuilding storage. Current direct vector expressions are column references.
This preserves scalar, dictionary, packed byte/text, and nested vector shape
using the same borrowed-input contract as ordinary selected columns. The query
execution stage retains input ownership; the projection helper does not invent
a second allocation charge for an alias.

Five shape cases pass with zero output-allocation capacity. An owned-input case
retains aliases past stage closure at the original eight-byte charge, then
releases to zero. The fused top-k/project test verifies alias identity and now
retains 40 bytes rather than 56, reflecting the removed 16-byte output copy.
All 568 core tests and 90 reference tests pass; build, TypeScript and lint pass.

This is alias preservation, not general computed-expression zero-copy support.
Expressions without a direct vector still construct admitted output normally.
Returned JavaScript row materialization and input lifetime integration outside
the existing owned execution paths remain separate audit requirements.


### Release-gate refresh and direct decoder coverage (2026-11-12)

Package pack/install, compatibility generation check, tracked-file size, fixture
integrity/regeneration, dedicated typecheck, and lint pass. The tracked-file check
initially hit sandbox Git spawn EPERM and passed with required subprocess access.

The initial coverage run failed unchanged global thresholds: 89.26% statements
and 83.97% branches. The shared vendor ZSTD source reported zero because existing
integration tests exercised built package paths. Added direct core source tests
using native Node ZSTD frames: three compression levels, six sizes including
block boundaries/multiple blocks, constant/mixed/random data, concatenated frames,
truncation, overflow and underfill. Every complete decoded byte is compared.

Coverage now passes without exclusions or threshold changes: 90.96% statements,
84.68% branches, 95.16% functions, 92.94% lines. The vendor decoder itself reaches
83.73% statements and 64% branches; unsupported/malformed-format corners remain
untested and are not claimed proven. All 1,380 tests in the coverage run pass,
with 349 conditional skips. Logs: /tmp/lakeql-vector-coverage.log,
 /tmp/lakeql-vector-packages.log, /tmp/lakeql-vector-fixtures.log,
 /tmp/lakeql-vector-gates-typecheck.log.
External conformance remains red and full memory-contract acceptance remains open.

### Numeric IN lookup with admitted scratch (November 12, 2026)

`batch.ts` now reuses one candidate array across rows in the generic scalar IN
fallback. Numeric vectors with finite numeric literal lists can instead use a
sorted Float64 lookup, including NULL candidates and both direct NOT IN and a NOT
wrapper. The planner-free kernel estimates comparison savings from row/list size
and declines the lookup for small workloads. It also declines when the complete
lookup cannot fit the current allocation headroom. Scratch is admitted before
allocation and released before returning the output mask; insufficient optional
scratch does not reject an otherwise executable predicate. Output-mask admission
remains mandatory. No user tuning flag is required.

The lookup preserves current scalar NaN-target comparison behavior (it does not
change the project's floating-point semantics). Non-finite literals, mixed types,
nonliteral candidates and nonnumeric vector shapes retain scalar evaluation.
Fallback candidate expressions remain eager. Its reusable JS candidate array and
expression objects are not yet covered by the typed allocation budget, so this
change does not establish a whole-heap bound.

`in-mask-memory.test.ts` compares indexed and exact-output-budget fallback masks
with nulls, duplicate candidates, NaN/infinite targets, signed zero and negation;
it checks scratch-cap-minus-one fallback, output-cap-minus-one rejection, cleanup,
non-finite literal fallback and row-varying candidates. All 577 core tests pass
across 70 files; TypeScript project build and changed-file lint pass. The dedicated kernel benchmark is `bench/numeric-in-memory.mjs`, with
results in `docs/benchmarks/numeric-in-memory-2026-11-12.jsonl`: three warmups and
five measured samples, independent expected masks checked outside the timed
region, exact retained output and zero bytes after disposal.

At 100,000 rows, 128 literals measured 64.24 ms without scratch versus 3.62 ms
with 1,024 extra bytes; 1,024 literals measured 537.40 versus 5.61 ms with 8,192
extra bytes. Eight-literal lists declined the lookup with either budget and
measured approximately 4.9 ms. These compare current production fallback and
accelerated paths, not a historical release. Inputs are borrowed and measurements
exclude I/O, decoding, JS heap and whole-query scheduling. No cloud throughput or
Worker heap-cap claim follows from these kernel timings.

### Real encoded IN fixtures and mandatory-admission review (November 12, 2026)

The reference suite now writes 2,048-row nullable INT32 Parquet fixtures with
SNAPPY compression, separately using PLAIN and RLE_DICTIONARY value encoding.
Each fixture compares every selected row against DuckDB for IN/NOT IN, with and
without NULL in a 128-item duplicate-containing list. Batch sizes 3 and 2,048
exercise different execution shapes: the PLAIN numeric batch can use the sorted
lookup, while dictionary vectors retain the generic fallback. All 92 reference
tests pass. Every query's shared allocation pool returns to zero. These are real
encoded file/decoder checks through a memory object store, **not remote I/O**;
the earlier public-R2 evidence remains separate. TypeScript project build passes.

The mandatory scheduler remains unimplemented. A current-source review identifies
these constraints on the implementation, rather than treating a promise queue as
sufficient:

- `MemoryBudget.reserve` is synchronous and atomically checks the child and every
  ancestor after cache reclamation. It must remain usable by synchronous decoders
  and must reject impossible working sets without waiting.
- `ReadSemaphore` grants request-count slots, not memory. Waiting for allocation
  while holding such a slot can prevent the operation that would release memory
  from obtaining a slot. Memory admission must precede acquisition of scarce I/O
  slots, with cancellation releasing both reservations and queue membership.
- `readAlignedColumnVectorBatches` retains each column's current page while
  advancing other columns. A page waiting for space can therefore depend on an
  earlier page whose release requires completing that very aligned batch.
- Owned yielded batches can outlive their iterator. `query-memory.test.ts` already
  proves that independently retained query outputs can fill a shared pool and
  that another query rejects until those owners are explicitly released. An
  allocator cannot assume the consumer will release them while awaiting another
  `next()` call. Cancellation is necessary but cannot be the only way to escape
  an intrinsically impossible request.

The required scheduling unit must consequently include the retained input set,
mandatory transient decode/operator workspace, and output that must coexist until
handoff. Its admission must be atomic at the shared pool, with explicit ownership
transfer and release boundaries; per-allocation asynchronous waits alone are not
an acceptable implementation. Page/row batch splitting may reduce that working
set only where the physical decoder/operator supports it. Unsplittable pages or
retained outputs that cannot fit require a deterministic budget error. Optional
prefetch and lookup scratch remain independently declinable. Before integration,
regressions must exercise mismatched column page boundaries, multiple jobs with
retained outputs, cancellation during admission and during transport, and a
working set that fits each child separately but exceeds their shared parent.

Full default regression validation after the encoded IN comparisons: 1,386 tests
pass, 351 skipped, across 137 passing files. The initial restricted run failed
on loopback server permissions; rerunning with loopback access passes. Changed
reference-file lint also passes. Coverage and workerd were not rerun in this step.

### Definition-level workspace lifetime and validity reuse (November 12, 2026)

The scoped flat Parquet page decoder now releases normalized definition levels
when every row is present, before value decoding (and before V2 value-body
decompression). Those levels have no output consumer. Nullable packed binary/text
pages reuse byte-wide normalized levels as their final validity bitmap instead of
allocating and retaining a second identical byte buffer. Wider level buffers keep
the conversion path and are released after final validity construction. Page
ownership still pins the surviving validity buffer through output handoff.

Exact tests cover V1/V2 PLAIN numeric alignment, dictionary indices including
zero-width encoding, PLAIN/RLE booleans, nullable binary, truncated bodies and
budget failures. The nullable binary owner remains valid after decoder-scope
disposal and releases its final charge only when the owner is released. The
four-row all-present numeric/dictionary case drops from 20 to 16 admitted bytes;
the boolean case drops from 8 to 4. Four-row nullable binary drops from 40 to 36.
Real writer-produced PLAIN string/binary fixtures, including Unicode, empty
values, all-null and all-present optional pages, confirm full/windowed reads:
the six-row nullable case drops from 68 to 62 bytes and still rejects at 61.

All 364 default Parquet tests pass (276 skipped), including loopback HTTP range
fixtures with the required server permission. All 92 DuckDB reference tests pass.
TypeScript project build and changed-file lint pass. These are deterministic typed
allocation reductions; no runtime speedup or whole-heap bound is claimed. This
reduces a mandatory decode working set but does not implement the outstanding
shared byte-aware scheduler or account for upstream nested decoder allocations.

### Nullable page expansion consumes exact scratch (November 12, 2026)

`flatPageVector` now ends ownership of compact values/indices after nullable
expansion has copied them into final storage, provided those compact values are
an exact allocation of its scope. Full-page normalized byte definition levels
also become final validity for typed numeric, boolean and dictionary vectors.
Other definition buffers are released after construction when the output does
not use them. Array-based conversion retains its separate validity construction,
including timestamp inference, rather than prematurely releasing levels that are
still needed by a restartable source reader.

`MemoryScope.tryReleaseAllocation` makes the exact-allocation condition explicit:
it returns false for borrowed values and views instead of releasing their backing.
The strict `releaseAllocation` retains its error contract. Tests cover borrowed
views, independent retained handles, repeated release attempts and disposed scopes.
Byte-stream-split output is a view of an allocated transposition buffer, so its
backing remains conservatively retained; identifying and releasing that backing
requires explicit decoder ownership metadata, not an alias-based guess.

Writer-produced nullable numeric/boolean fixtures verify exact peaks and retained
owners after scope disposal. Four-row INT32/FLOAT now peaks at 28 rather than 32
bytes and retains 20 rather than 32; DOUBLE/INT64 peaks at 52 rather than 56 and
retains 36 rather than 56; BOOLEAN peaks at 10 rather than 14 and retains 8 rather
than 14. Full/windowed byte-stream-split and PLAIN/RLE boolean budget boundaries
also pass. Borrowed input bytes are excluded from these isolated typed-workspace
counts; real owned scan/cache regressions remain part of the full suite.

Validation: 1,389 default tests pass, 351 skipped, in 137 passing files; 92 DuckDB
reference tests pass. TypeScript project build and changed-file lint pass. Coverage,
workerd and external conformance have not been refreshed for this step. These
changes reduce live decoder workspace without claiming complete heap accounting
or completion of the mandatory byte-aware scheduler.


### Byte-stream-split exact allocation ownership (November 12, 2026)

The numeric byte-stream-split decoder now registers its returned typed numeric
array as the exact owned allocation during construction. Transposition still
writes through a byte view of the same backing; there is no added buffer or copy.
Previously the byte view was the registered value, leaving the returned numeric
view unidentifiable to exact scratch release. Nullable expansion can now release
the transposed allocation after copying its values. Nonnullable output retains
that allocation normally. This supersedes the preceding limitation about the
byte-stream-split returned view; other borrowed/aliased buffers remain protected.

Tests exercise INT32, INT64, FLOAT and DOUBLE exact owner retention/release,
misaligned encoded input, unchanged reader offset on failed admission, truncated
input, and cap-minus-one rejection. The real compressed nullable FLOAT fixture
still peaks at 36 admitted bytes, but retains 28 instead of 36 after expansion.
All 59 focused decoder/vector tests and 92 DuckDB reference tests pass; TypeScript
build and changed-file lint pass. Before this final adjustment, full coverage
passed 1,389 tests at 90.98% statements / 84.72% branches, and all 18 Worker tests
passed (including expected stream-cancellation diagnostics). Those runtime checks
are not a whole-Worker heap-cap proof.

### Remote validation after page ownership changes (November 12, 2026)

After rebuilding the packages, `bench/cloud-range-validation.mjs` passed all 44
checks against the actual public R2 `window-events.parquet` object. This includes
32 normal owned aggregate/top-k/projection/mixed query scenarios across 64/128 MiB
shared pools, one/four concurrent jobs and cold/warm caches. Pinned reusable cache
storage leaves only 128 KiB available to queries. Maximum observed incremental
charged memory was 127,995 bytes, active reads never exceeded four, warm query
scenarios issued zero additional data reads, and every final pool-cleanup
assertion passed. Range requests were verified as HTTP 206 with identity encoding;
full-object reads were prohibited. The captured result is
`docs/benchmarks/cloud-range-page-ownership-2026-11-12.json`.

This refresh validates real remote range behavior after the definition-level,
nullable expansion and byte-stream-split ownership changes. It uses the existing
144-row fixture, not a large-file throughput workload. Charged allocations and
artificial pinned-cache pressure are not process/Worker heap measurements. No
latency improvement or full 64/128 MiB runtime-cap guarantee is inferred.

External conformance was also rerun: 167 failures, 83 passes, one skip. Comparing
all 167 named failures against the packed-dictionary checkpoint found no added or
removed failures; the record is
`docs/validation/page-ownership-conformance-2026-11-12.json`. This compares failure
identities, not passing-value digests. The conformance gate remains red; the 92
passing DuckDB reference cases supply separate value-parity evidence.

### Exact allocation ownership lookup scaling (November 12, 2026)

`MemoryScope` previously found exact values by scanning every owner. Releasing
many allocations in reverse order therefore performed quadratic owner comparisons.
It now indexes each exact value and keeps duplicate handles in an insertion-order
linked ring. Expected lookup, insertion and removal are constant time, including
out-of-order handle releases. Distinct owners of the same value remain distinct;
retaining/releasing by value selects the earliest live owner as before. Removed
nodes unlink their neighboring owners immediately. Strict scalar equality behavior
(including unmatchable NaN) and borrowed-view rejection remain unchanged.

Tests cover duplicate owners, middle/first/last removal, replacement after complete
removal, retained handles, disposal, scalar keys and a 1,000-owner instrumented
regression showing exact lookup does not read unrelated owners' values.
`bench/memory-owner-lookup.mjs` records reverse-release kernel scaling in
`docs/benchmarks/memory-owner-lookup-2026-11-12.jsonl`: at 1,000 owners, emulated
linear search/current indexed lookup measured 8.05/0.23 ms; at 10,000 owners,
645.91/1.84 ms (three warmups, five measured samples). The linear comparator uses
the same current owner handles; this isolates search cost rather than comparing
whole historical implementations. Allocation/index construction and JS bookkeeping
memory are outside the timed region. The index adds O(live owners) bookkeeping,
which is removed on release but remains outside typed-allocation accounting.
This is a CPU scaling improvement, not a claim of lower total heap use.

All 1,392 default tests pass with 351 skipped; all 92 DuckDB reference tests pass.
TypeScript build and changed-file lint pass. Coverage/Worker/public-R2 evidence
above predates this index change. Shared byte-aware scheduling and complete
allocation/metadata bookkeeping bounds remain outstanding.

### Ownership bookkeeping heap measurement (November 12, 2026)

The exact-value index now reuses the returned ownership handle as its linked node,
rather than allocating an additional node object. Links are non-enumerable so the
public handle's enumerable fields/spread behavior remain unchanged. Duplicate
ownership, strict lookup and released-handle isolation tests still pass.

`bench/memory-owner-bookkeeping.mjs` adds a post-GC Node V8 heap measurement for
10,000/50,000 simultaneously live one-byte typed allocations, including values,
owner handles, reservations, indexes and the benchmark's handle array. It records
Node version and scope-module source SHA-256, and supports a module override for
same-runtime comparisons. The captured comparison against the separate-node
implementation is `docs/benchmarks/memory-owner-bookkeeping-2026-11-12.json`.
At 50,000 owners, median live heap delta decreased from 88,905,952 to 86,899,952
bytes (about 40 bytes per owner). Allocation time increased from 58.09 to 80.21 ms;
reverse release measured 18.55 versus 17.12 ms. At 10,000 owners heap decreased
from 17,964,304 to 17,585,952 bytes. This is a measured memory/CPU tradeoff, not an
unqualified performance win. The combined form is retained to reduce per-owner
storage while preserving the public enumerable handle shape.

More importantly, the experiment demonstrates that 50,000 bytes of typed payload
charges can coexist with roughly 87 MB of Node heap. It is a deliberately tiny-
allocation stress workload, not a representative query or Worker measurement,
but it makes the missing ownership/bookkeeping budget material and reproducible.
The remaining accounting contract must bound live allocation/handle cardinality
and reservation metadata, including retained/composite owners and child budget
chains, rather than treating payload bytes alone as a runtime heap guarantee.
GC measurements on Node cannot establish a portable per-object byte bound.

All 582 core tests pass, including a new public-handle shape regression; TypeScript
build and changed-file lint pass. The broader default/coverage/Worker checkpoints
above predate this combined-node adjustment.


### Share immutable reservation ancestry (November 12, 2026)

Every reservation/retention previously constructed and retained its own array of
budget ancestors. Budget parentage is immutable, so `MemoryBudget` now constructs
that array once per budget and shares it across all its reservations. Root lookup
also uses the cached ancestry. Atomic ancestor admission, reclaimers, shared
physical charging and independently retained handles retain their existing
contracts; public reservation methods are unchanged. A nested-branch test verifies
shared ancestor charging, transfer between sibling descendants, child and common
parent limit failures, and final cleanup.

The same one-byte-allocation heap workload now measures 77,717,768 bytes at 50,000
owners versus 86,899,952 for the preceding combined-handle implementation: roughly
9.18 MB less, or 184 bytes per owner. At 10,000 owners the median drops from
17,585,952 to 15,724,120 bytes. This removes redundant bookkeeping but still leaves
a large disparity between 50 KB of typed payload and total heap. The comparison
is recorded in `docs/benchmarks/memory-budget-ancestry-2026-11-12.json`. The benchmark
now supports a separate budget-module override and records both scope and budget
source hashes. Timing values are recorded but are not isolated throughput proof.
The cached array adds O(depth) storage once per budget; this experiment uses one
root budget and does not measure deep-hierarchy or retained composite workloads.

Validation: all 1,394 default tests pass with 351 skipped under coverage, 92 DuckDB
reference tests pass, and all 18 Worker tests pass. Coverage remains above unchanged
thresholds (90.99% statements, 84.73% branches). Build and changed-file lint pass.
This does not establish a complete bookkeeping cap, byte-aware mandatory scheduler,
or portable whole-runtime memory bound.


### Shared backing pins for composite retains (November 12, 2026)

Composite owned outputs previously cloned every backing reservation for each
retain, even within the same budget. Same-budget composite handles now share a
reference-counted backing set. Retain and non-final release are constant-time;
only the final release traverses backing allocations. Cross-budget retention
still admits individual backing allocations into the target child and counts
common ancestors once. A source composite guard keeps the entire backing set live
if admission triggers a reclaimer that disposes the original producer. Partial
admission failures roll back target pins. Initial same-budget backing capture also
uses ordinary retain, avoiding unnecessary cross-budget guard handles.

An instrumented test verifies that 100 retains of a 100-buffer composite do not
create additional backing handles. It disposes the producer, releases handles out
of order and checks that charges persist until the final handle, with idempotent
release and rejection of retain-after-release. Existing cross-job partial failure
and reentrant producer-eviction tests pass.

`bench/composite-ownership.mjs` measures 100 same-budget retains over 10/100/1,000
buffers. Each sample runs in a separate function so prior released handle arrays
cannot inflate the next GC baseline. The previous per-backing-retain scope module
and current scope module use the same current budget implementation; source
hashes and results are in `docs/benchmarks/composite-ownership-2026-11-12.json`.
At 1,000 buffers, median retention time is 37.97 versus 0.030 ms and incremental
post-GC heap is 66,664,592 versus 31,384 bytes. Current additional heap is roughly
31 KB across all three backing sizes. Preexisting backing construction is excluded;
this isolates repeated composite handle overhead, not entire queries, remote
throughput or Worker heap-cap behavior.

Validation: all 1,395 default tests pass (351 skipped), all 92 DuckDB reference
tests pass, and TypeScript build / changed-file lint pass. Cross-budget admission
remains proportional to the backing set and independent snapshots still construct
their own sets. Complete bookkeeping bounds, other untracked JS allocations and
mandatory byte-aware scheduling remain unfinished.


### Reuse unchanged scope snapshots (November 12, 2026)

`MemoryScope.retainAllocations` now captures one backing set for an unchanged
scope, then creates independent output handles sharing that set. Adding/adopting
an allocation or releasing a tracked handle invalidates the cached capture.
Disposal drops the cache's pin and the scope's original owners; output snapshots
still retain their own backing until explicit release. Pending asynchronous
allocations remain excluded until completion registers their owner. Each output
keeps its own value, even when its backing set is shared with another output.

The lifecycle regression captures two unchanged snapshots, adds an allocation,
releases the first allocation, releases the second, and captures after each change.
It verifies original/new backing sizes, one capture while unchanged, independent
output values, and charges persisting only until the last relevant output releases.
Existing cancellation, async-allocation, cache-transfer, reentrant reclamation and
real encoded fixture tests remain passing.

`bench/composite-ownership.mjs` now accepts `LAKEQL_RETAIN_MODE=snapshot` to measure
this path separately from retaining an existing composite. The comparison is
`docs/benchmarks/ownership-snapshots-2026-11-12.json`. For 100 unchanged snapshots
of 1,000 buffers, the previous fresh-capture implementation measured 27.99 ms and
66,690,880 bytes of additional post-GC Node heap; the current implementation
measured 0.049 ms and 34,632 bytes. Preexisting backing capture is excluded. The
optimization specifically benefits unchanged ownership sets; each mutation still
requires a fresh O(number of owners) capture on the next snapshot. This is a
kernel/heap experiment, not remote latency or a portable whole-runtime bound.

All 1,396 default tests pass (351 skipped), all 92 DuckDB reference tests pass,
and TypeScript build / changed-file lint pass. Coverage/Worker/public-R2 evidence
above predates snapshot reuse. Complete bookkeeping bounds and mandatory
byte-aware scheduling remain open.


### Ownership snapshot validation checkpoint (November 12, 2026)

The snapshot implementation now passes the refreshed broader gates: 1,396 default
tests under coverage (351 skipped), 18 Worker tests, dedicated typecheck,
package-install validation, tracked-file size, full lint, compatibility metadata,
and fixture integrity/regeneration. The unchanged implementation's 92 DuckDB
reference tests also pass. Coverage is 91.00% statements, 84.74% branches, 95.14%
functions and 92.96% lines; thresholds were not lowered. The checkpoint is recorded
in `docs/validation/ownership-snapshot-checkpoint-2026-11-12.json`.

The tracked-file size command first failed because the restricted runtime could
not spawn read-only Git, then passed with the required subprocess permission.
Full lint found formatting in the saved remote R2 JSON; formatting was corrected
without changing its benchmark data, then full lint passed. Worker stream-
cancellation diagnostics were emitted while all tests passed. External conformance
and public-cloud checkpoints are not refreshed here: conformance remains red at
167 failures, and the latest public-R2 run predates composite/snapshot sharing.
Whole-runtime allocation coverage and byte-aware mandatory scheduling remain
unproven; green regression gates do not close those requirements.

### Public R2 snapshot reuse with one-row batches (November 12, 2026)

The public R2 validation now includes an unfiltered computed projection with
`batchSize(1)`, verifies every emitted batch has exactly one row, and compares all
144 rows including nullable computed values. The four-job mixed scenario also
includes this streaming query alongside aggregate, top-k and limited projection.
This drives repeated scope snapshots through real remote transport, decoder,
projection and query handoff instead of relying solely on ownership microbenchmarks.

After a full package rebuild, all 52 checks pass. Forty normal owned query
scenarios cover 64/128 MiB pools, one/four jobs and cold/warm caches with pinned
cache storage leaving 128 KiB headroom. In total 3,456 one-row output batches were
verified. Maximum incremental tracked allocation is 127,995 bytes, at most four
reads are active, every warm normal-query case issues zero additional data reads,
and every final pool-cleanup assertion passes. Results are captured in
`docs/benchmarks/cloud-range-snapshots-2026-11-12.json`; each normal-query record
includes the number of one-row batches verified. Source benchmark and result
formatting pass lint.

This refresh covers the current composite/snapshot implementation on actual R2
HTTP 206 range reads. It still uses the 144-row public fixture and does not prove
large-file throughput, consumer-retained output heap accounting, the mandatory
byte-aware scheduler or a total Worker heap cap. Node timings include uncontrolled
remote latency and are not before/after performance evidence.

### Nested decoder boundary probe (November 12, 2026)

A current-source review confirms that the largest conformance failure group is
rejected by `schema.ts` before decoding: unannotated groups are treated as
unsupported struct columns. The installed upstream reader can assemble structs,
maps and logical variants, and the core vector builder has nested representations.
That does not by itself justify removing the schema guard: decoder success and
correct logical values/ownership are distinct requirements.

`bench/struct-decoder-probe.mjs` now provides a read-only diagnostic using the
existing internal column-batch decoder, explicitly bypassing the public schema
guard for five selected external fixtures. It compares materialized JSON with
DuckDB and records the first representation difference. Results are in
`docs/validation/nested-decoder-probe-2026-11-12.json`. The nested Rust struct
fixture decodes its single row but differs first in timestamp text format
(extended ISO versus DuckDB's timestamp text). Required/nullable Impala fixtures
decode one/seven rows but differ first in map representation (object versus
key/value entry arrays). Two sampled shredded-variant fixtures match JSON.

These results do **not** establish semantic mismatches in the timestamp/map cases,
comprehensive variant support, or safe public struct support. The next acceptance
work needs schema-aware canonical comparisons covering nested timestamps, maps,
null containers versus null members, and required/optional/repeated groups;
malformed variant inputs must still reject. Borrowed local buffers and upstream
nested JS assembly in this diagnostic are not allocation-bound evidence. Public
schema acceptance and conformance assertions were deliberately left unchanged.
The probe and captured JSON pass lint. This narrows the work from a blanket
"decoder cannot handle structs" assumption to explicit logical-conversion,
vector-shape and ownership contracts that must be proven before enabling them.


### Typed nested comparison isolates map corruption (November 12, 2026)

The nested probe now uses native DuckDB values through
`bench/nested-value-comparison.mjs`, comparing exact timestamp epochs, binary
bytes, bigint precision, list shape, struct members and typed map membership.
It unwraps non-null logical variant values but refuses ambiguous embedded variant
nulls; timestamp timezone annotations and physical variant tags remain separate
acceptance requirements. Seven standalone comparator regressions pass via
`node bench/nested-value-comparison.test.mjs`, including missing versus null map
keys and null containers versus containers with null members. Unknown reference
value types are not silently treated as equivalent plain objects.

The five-file probe was rerun, and also compared the upstream object decoder
using LakeQL's parsers/codecs before vector conversion. The Rust nested struct
matches DuckDB after typed normalization, confirming its earlier timestamp
formatting difference was not a value mismatch. Both sampled variant fixtures
also match logical values within the comparator's documented limits.

The required Impala fixture exposes a genuine semantic error: the first
`int_map_array` element is an empty map in DuckDB and upstream decoding, but
LakeQL's vector materialization produces a map-like object containing `k1: null`.
The nullable fixture similarly gains an absent `k3: null` key inside a list map.
`column-batches.ts` currently normalizes only top-level maps; nested map objects
reach generic struct inference, which unions object fields and fills missing
members with null. That behavior is valid for structs but changes map membership.
The nullable upstream comparison also reports undefined versus null in a nested
list, which is a separate representation-normalization issue, not proof of an
additional scalar value error.

Evidence is captured in `docs/validation/nested-decoder-typed-probe-2026-11-12.json`.
The next correction must preserve schema-declared map semantics recursively
through lists/structs and verify null/empty containers without relying on object
shape inference. Public struct acceptance is unchanged; removing the guard before
that correction would expose known incorrect materialization. These are borrowed
local decoder diagnostics, not remote transport or nested heap-bound proof.


### Recursive schema-directed map normalization (November 12, 2026)

`nested-values.ts` now compiles a value normalizer from the Parquet schema before
column values enter generic vector construction. It preserves MAP nodes as Maps
through lists, repeated structs, structs and nested map values, so struct field
union cannot invent absent map keys. Scalar-only schemas require no conversion;
unchanged struct objects and existing simple Maps retain identity where possible.
The list traversal mirrors the upstream reader's list assembly shape. Decoded
VARIANT values are not interpreted using their physical shredded-field schema.
This changes map representation, not public struct acceptance.

The original required/nullable Impala fixtures now match DuckDB in the typed
internal probe, including empty maps and missing keys. Two new reference tests
also project their list-of-map columns through the **public reader and owned
query path**, using one-row batches, exact measured tracked allocation caps,
cap-minus-one rejection, and zero final charges. They compare nested integer/map
values with DuckDB JSON (no timestamp or bigint coercion in these selected
columns). Unit regressions cover null/empty containers, absent versus null-valued
keys, maps inside repeated structs, nested map values, unchanged input objects,
and existing Map identity.

Validation: 1,400 default tests pass under coverage (353 skipped), all 94 DuckDB
reference tests pass, TypeScript project build, dedicated typecheck and full lint
pass. Coverage is 91.02% statements / 84.72% branches. External conformance remains
167 failures, 83 passes, one skip, with no added or removed named failures.
`docs/validation/nested-map-normalization-2026-11-12.json` records that comparison
and the post-fix typed probe. Public schema guards and conformance assertions were
not relaxed.

The normalizer's Map/list/object allocations and upstream nested JS assembly are
still outside complete heap admission. Exact-cap tests prove currently tracked
transport/decode/vector allocations only. Upstream object assembly can also erase
non-string map-key types before normalization; preserving those types requires
work at the typed assembly boundary, not guessing keys back from object strings.
No whole-Worker bound, general struct/variant acceptance, or remote nested-map
throughput claim follows from this correctness fix.

### Typed map-key boundary evidence (November 12, 2026)

`node bench/typed-map-key-probe.mjs` writes real SNAPPY Parquet fixtures with
INT64 and BOOLEAN map keys, reads them with both LakeQL and native DuckDB, and
asserts that the original input Map matches DuckDB without string coercion.
The INT64 case includes `9007199254740993n`, beyond exact JavaScript Number
representation. Results are saved in
`docs/validation/typed-map-keys-2026-11-12.jsonl`.

Both cases expose two independent unfinished contracts:

- The original core vectors retain `i64`/`bool` keys, but core row materialization
  converts those keys into object-property strings.
- Parquet decoding already produces `utf8` key vectors: upstream `assembleMaps`
  constructs plain objects before LakeQL's schema normalizer receives values.

The typed reference comparison reports a mismatch for both materialization
paths. Successful execution of this diagnostic means the fixtures were validated
and evidence collected; it does not mean typed map fidelity passes. Existing
string-key fixture parity cannot establish general map fidelity.

Completion requires preserving typed entries during nested assembly and defining
an end-to-end row representation for non-string keys, including operator consumers
and memory accounting. Parsing object-property strings back into inferred types
cannot establish that contract. A dependency-only local patch is insufficient:
the published bundle externalizes hyparquet, so consumers need the corrected
assembly through shipped code or an appropriate dependency release. These are
local correctness diagnostics, not cloud performance or heap-cap evidence.

### Literal object-key preservation in core output (November 12, 2026)

Core map/struct/row materialization and JSON-safe object conversion now store
`__proto__` as an enumerable own data property, rather than invoking the inherited
prototype setter. Ordinary keys retain the direct-assignment path. A shared
internal setter preserves the existing plain-object output contract and permits
replacement of the key without changing the object's prototype. This prevents
loss of a valid string map key at the core output and JSON boundaries.

Two regression tests cover map entries named `__proto__`, `constructor`, and
`toString`, null values, JSON-line round trips, and replacement with an object
then null while preserving the prototype. The three focused core test files pass
33 tests. This does not repair upstream Parquet object assembly, typed non-string
map output, or nested heap accounting; those remain required work. The consumer
review also confirms that returning JavaScript Maps alone is insufficient:
`jsonSafeValue` currently enumerates object properties and would emit an empty
object for a Map, so typed output needs a matching serialization contract.

### Exclusive admission partitioning (November 12, 2026)

`MemoryReservation.partition(parts)` provides an ownership primitive needed for
whole-working-set admission. It consumes an exclusively held reservation into
independently releasable extents whose sizes must sum exactly to the source.
Existing child/ancestor charges transfer without releasing capacity, attempting
new admission, increasing peaks, or invoking cache reclaimers. Thus a stage can
release its scratch extent while retaining an output extent; another job can use
the freed capacity without counting shared physical output twice.

The consumed source cannot retain or partition again, and releasing it is a
no-op. Shared reservations reject partitioning because another holder would
otherwise retain an obsolete extent. Invalid sizes or totals leave the source
live and unchanged. Parts can be retained across sibling budgets or partitioned
again after they become exclusive. Empty reservations and zero-byte extents are
supported. This operation divides admission ownership, not an existing buffer;
callers remain responsible for allocating matching extents and preserving views.

The 35 focused budget/scope/partition tests pass, including ancestor accounting,
sibling ownership, invalid requests, shared-owner rejection, recursive partitions,
zero sizes, unchanged peaks, and final cleanup. TypeScript project build and
changed-file lint pass. This is scheduler groundwork, not scheduler integration:
no decoder currently derives and reserves its entire mandatory working set using
this API, and no asynchronous admission queue or whole-Worker heap guarantee is
claimed. The next integration must connect admitted extents to allocation scopes
and explicit physical-stage working-set capabilities, preserving the retained
input and output constraints documented above.

Broader validation after this primitive: all 591 core tests pass across 72 files.

### Allocation-scope consumption of admitted extents (November 12, 2026)

`MemoryScope.allocateReserved` and `allocateReservedAsync` now consume an
exclusive reservation from the same budget before invoking an allocator. The
reservation exposes its budget identity so a sibling's child limit cannot be
bypassed. Validation failures (closed scope, mismatched budget, shared or consumed
reservation) leave caller ownership unchanged and do not invoke the allocator.
After transfer, allocator failure releases capacity; successful allocation enters
the existing scope ownership, retention, and disposal machinery. Original
admission handles cannot release transferred capacity or be reused.

Asynchronous consumption preserves the existing pending-I/O invariant: cancelling
or disposing the scope does not release bytes until the pending allocator settles,
including when it rejects. This is tested with controlled pending promises, not
claimed as new remote transport coverage. Normal `allocate`/`allocateAsync` calls
share the completion/error helpers but do not partition their fresh reservations,
avoiding extra partition bookkeeping on the established allocation path.

The broader core suite passed 596 tests across 73 files before adding one final
combined async-success/synchronous-disposal regression; the focused admission
suite then passed all six cases. TypeScript project build and changed-file lint
pass. These APIs connect admission ownership to actual scoped typed allocations,
but physical stages still need complete working-set estimation and integration;
no scheduler queue, cloud throughput gain, or total heap cap is established here.

### Packed-gather output admission integration (November 12, 2026)

Packed-byte gather now measures its payload, ranges and optional validity buffer
and reserves their combined typed backing before constructing any output buffer.
It partitions that admission and consumes each extent through the allocation
scope. An insufficient cap therefore rejects before partial output allocation;
if a later allocator fails, completed buffers and unused admitted extents are
released. Input ownership continues to be retained by the surrounding owned-gather
path. Scalar dictionary compaction and other gather shapes keep their existing
allocation paths and require their own complete-stage integration.

Four regressions verify an exact 31-byte output (repeated selected bytes plus a
null), rejection at 30 bytes with zero allocator calls and zero peak charges,
injected failure after the first buffer with complete cleanup, and empty output.
Together with existing owned-gather tests, all 17 focused cases pass. The broader
core suite passes 601 tests across 74 files; TypeScript build and changed-file lint
pass. This integrates pre-admission into a real operator but only for one packed
output shape. It does not yet reserve the complete retained-input/decode/operator
pipeline, schedule competing stages, or account for JS bookkeeping. No new remote
I/O or throughput result is claimed by these in-memory operator tests.

### Whole compact-gather admission (November 12, 2026)

Allocation scopes can now admit a complete new-output extent with `admit`.
Subsequent scoped allocations draw exclusively from that reservation, and reject
an underestimated extent even if the parent has spare capacity. They do not
silently add fresh reservations. Unconsumed admission is released on disposal;
completed or pending allocations keep their independent ownership. This extent
bounds total new allocation requests for the stage, not reusable scratch capacity:
released sub-extents do not replenish it. `reserve` transfers an extent to an
allocator that must consume or release it, allowing packed-gather's grouped
allocation to use the same admission.

`gatherOwnedBatch` uses its existing exact typed-backing estimate to admit all
columns together whenever it selects fully detachable compaction. Packed output,
scalar output, and compacted scalar dictionaries therefore share one admitted
output extent. Existing dynamic decisions about whether to compact are preserved;
shared/unsupported shapes retain their previous paths. Input remains charged
separately throughout construction. This does not assert that every generic
gather shape has a complete byte estimate.

New tests cover underestimated stage rejection without invoking the allocator,
invalid extents, duplicate admission, unused-capacity cleanup with retained output,
and a mixed packed/scalar gather with exactly 26 output bytes and independent
source release. That mixed fixture borrows its scalar input buffer; the assertions
cover new output and the explicitly owned payload, not total fixture heap usage.
All 604 core tests pass across 74 files; TypeScript build and changed-file lint
pass. Full pipeline admission, asynchronous scheduling, JS heap accounting and
cloud execution evidence for this new path remain outstanding.

### Admission integration validation checkpoint (November 12, 2026)

After scope admission and whole compact-gather integration, the full default
suite passes 1,419 tests (353 skipped), all 94 DuckDB reference tests pass, and
all 18 workerd tests pass. Coverage passes unchanged thresholds at 91.03%
statements, 84.73% branches, 95.17% functions, and 93% lines. Initial restricted
runs could not bind fixture HTTP servers or resolve the public endpoint; reruns
with the required access passed. External conformance was not rerun here.

The real public-R2 range validation passes all 52 checks, including 40 ordinary
owned-query cases and 3,456 verified one-row batches. Pinned caches in 64/128 MiB
allocation pools leave 131,072 bytes of query headroom; maximum incremental
tracked usage is 127,995 bytes, concurrent reads remain bounded, and final pool
charges return to zero. Warm ordinary-query runs use zero additional data reads;
small wire-level metadata probes still occur. Results are recorded in
`docs/benchmarks/cloud-range-admission-2026-11-12.json`. This fixture has only 144
rows, so this is remote correctness/ownership evidence, not large-file stress or
total Worker heap proof.

The current owned-top-k benchmark also passes its row equality and allocation
assertions across 10k/100k rows, ordered/reversed/mixed data, and limits 10/1,000.
The 100k mixed case measures 1.88 ms (limit 10) and 4.03 ms (limit 1,000), with
200 and 20,000 incremental typed bytes respectively. Its borrowed path measures
13.21 and 24.82 ms, but that compares different current algorithms, not the
incremental effect of admission versus the prior implementation. Results are in
`docs/benchmarks/vector-top-k-admission-2026-11-12.jsonl`; no cloud latency gain is
inferred. `docs/validation/admission-checkpoint-2026-11-12.json` records the gates,
source hashes, and limitations. Mandatory stage scheduling and remaining heap
accounting continue to be required work.

### Reentrant reclamation during admission (November 12, 2026)

Cache reclamation is synchronous user-extensible work and can cancel a scope
before a budget reservation returns. Review found that `admit` could then install
a reservation on an already disposed scope, leaking its charge; ordinary scoped
allocation could also invoke an allocator after that cancellation. Fresh scoped
reservation now rechecks scope liveness after reclamation and releases acquired
capacity before propagating failure. It also rejects if a reentrant callback
installed stage admission, preserving that inner admission instead of overwriting
or bypassing it.

Five regressions cover reclaimer-triggered cancellation during `admit`, direct
scoped reservation, synchronous allocation and asynchronous allocation, plus
reentrant admission installation. They assert no allocator invocation after
cancellation, no leaked charges, and preservation/cleanup of the inner stage.
All 13 focused admission tests and all 609 core tests pass; TypeScript project
build and changed-file lint pass. These are deterministic reclamation/cancellation
unit tests, not new remote transport measurements. The preceding cloud checkpoint
predates this correction.

### Partition ledger reuse and measured admission overhead (November 12, 2026)

Exclusive reservation partitioning now reuses the source allocation ledger for
the final extent instead of cloning every ledger. Earlier extents have independent
copies before the source extent is resized. The consumed handle remains unusable,
and its public byte count remains the original value. This removes one allocation
record and holder-map copy per nonempty partition, including the one-part transfer
used by `allocateReserved`, without changing charged bytes or release semantics.
A regression checks independent partition sizes and retained lifetimes after the
reused final extent is released. All 610 core tests pass; TypeScript build and
changed-file lint pass.

`bench/stage-admission.mjs` measures 10,000 stages each allocating three typed
buffers, with exact stage capacity and zero final charges. Three warmups and five
samples measured 29.74 to 26.24 ms for one-row stages and 44.70 to 45.77 ms for
128-row stages. These sequential same-runtime results show a small-input gain in
this run and an approximately unchanged larger-buffer result, not a stable
whole-query speedup. The structural bookkeeping reduction is independent of the
timing result. Budget source hashes, runtime and measurements are recorded in
`docs/benchmarks/stage-admission-2026-11-12.json`. This is a local allocation
microbenchmark, excluding decoding, remote I/O and total heap-cap validation.

### Compact-gather shape matrix (November 12, 2026)

A 39-case matrix now checks 13 detachable vector types at dictionary depths zero,
one and two. Each case exercises empty, single-row and repeated/reordered
selections, leaf and dictionary validity, exact admitted output bytes, source
release, and zero final charges. Types cover all numeric widths, boolean,
nanosecond timestamp, null, binary arrays, fixed packed binary with a row offset,
and multibyte packed UTF-8 with an empty entry. The 64-bit fixtures include values
beyond exact JavaScript Number precision. Every output is compared with source
vector materialization, and its owned bytes must equal `compactGatherBytes`.
These tests isolate output accounting: input vectors are borrowed fixtures with
an explicitly owned padding buffer to select compaction, not complete input-heap
or remote-file evidence.

All 39 matrix cases and 57 focused gather tests pass; TypeScript build and
changed-file lint pass. Review also removed a duplicate selected-row null scan
in the packed estimator by reusing the validity result already computed for the
column. This reduces one dictionary-resolution traversal per packed column;
no new wall-clock speedup is claimed. Fully generic nested/shared-shape admission
and physical decoder working-set contracts remain outstanding.

### Shared gather allocation capabilities (November 12, 2026)

`GatherAllocationPlan` now explicitly describes typed output bytes, whether the
output retains input backing, and whether it creates JavaScript arrays. The
non-compacting planner accounts for one shared selection-index buffer across
nested/packed columns, separate index buffers for ordinary dictionaries, scalar
values, and only necessary validity masks. `gatherOwnedBatch` admits the planned
typed output for either compact or shared execution before allocating columns.
The plan also drives source release, replacing a second independent list of
aliasing shapes. The JavaScript-array flag is descriptive: those arrays, object
headers and ownership bookkeeping are still not fully admitted.

A mixed scalar/text/packed/list/struct/map fixture verifies exact child-cap
success, value preservation, shared backing lifetimes, and cap-minus-one rejection
before any output bytes are charged. Dictionary cases verify independent indices,
all-present selection masks and empty output. All 651 core tests pass across 76
files; TypeScript build and changed-file lint pass. These tests use in-memory
vectors, not cloud reads. This extends atomic typed-output admission across
existing gather shapes; it does not establish full JavaScript heap accounting,
complete decoder/operator stage admission, or asynchronous mandatory scheduling.

### Shared text and binary gather indices (November 12, 2026)

The general gather path now represents UTF-8 string-array and binary-array
columns as dictionary views over a shared admitted selection-index buffer, like
nested and packed vectors. It no longer creates an `Array.from` value array for
each selected text/binary column. Nulls remain in the source vector, avoiding a
copied validity mask for these columns as well. The source remains independently
owned until the output releases it, and repeated gathers use the existing
dictionary-index mapping path. Fully detachable packed compaction remains the
existing dynamic alternative.

The vector-shape plan now reports no JavaScript value-array creation for shared
gather. This closes that specific output-array gap; it does not account for
source strings/arrays, vector objects, ownership bookkeeping, or all process heap.
Public gather output uses declared dictionary vector shapes for these values;
logical row results are unchanged. A new regression verifies shared index-buffer
identity, source-vector identity, inherited nulls, repeated selections, exact
12-byte output admission, independent input release and final cleanup. Its source
values are borrowed to isolate the output contract.

All 651 existing core tests pass, followed by all 15 owned-gather tests including
the new regression. All 94 DuckDB fixture reference tests pass; TypeScript build
and changed-file lint pass. These fixture checks are local/in-memory reads, not
new cloud throughput evidence. The allocation reduction is structural; no new
wall-clock performance result is claimed in this step.

### Shared text/binary gather scaling evidence (November 12, 2026)

`node --expose-gc bench/shared-text-gather.mjs` compares production gather with
an emulation of the former non-null text/binary `Array.from` branches, excluding
budget and ownership work from both paths. Each isolated trial retains 100 outputs
of 1,000 selected rows from 10,000 borrowed input rows, with 1/8/32 columns, one
warmup and five measured samples. Every selected value in one output is checked
outside the timed construction interval. Source hashes and results are recorded
in `docs/benchmarks/shared-text-gather-2026-11-12.jsonl`.

Measured construction medians (copied arrays to shared indices) are 2.62 to
0.77 ms for one column, 22.84 to 0.85 ms for eight, and 69.56 to 0.96 ms for 32.
Node post-GC retained JS heap falls from 1.18/9.32/37.23 MB respectively to
36.7/75.6/210.0 KB. The shared path additionally retains exactly 400,000 bytes of
unique typed backing across all 100 outputs at every width; this external storage
is explicitly reported and is not included in the heap figures. Thus the memory
benefit is not merely movement from JS heap to unreported ArrayBuffers.

These are local allocation/scaling observations against branch emulation, not a
historical full-release comparison. They exclude source storage, null handling,
decode/transport, complete owned-query overhead and Worker heap-cap proof. Nulls,
retained ownership and budget boundaries have separate regression coverage in the
preceding section. The benchmark and changed-file lint pass; no production code
changed in this measurement step.

### Preserve shared gather indices through concatenation (November 12, 2026)

Dictionary concatenation now reuses a concatenated index buffer across columns
whose complete ordered input-index sequences are identical by object identity.
Each column retains its own value dictionary and validity. The per-concat cache
is keyed by the first index buffer and verifies every later buffer before reuse;
a different sequence replaces the cached candidate rather than requiring a
quadratic scan of earlier columns. No value equality scan or cross-call cache is
introduced. This prevents a wide shared gather from immediately expanding back
to one copied index buffer per column at the next concat boundary.

A regression concatenates numeric and text dictionaries with shared indices and
a third column whose first index buffer matches but later buffer differs. It
verifies correct reuse/non-reuse, all materialized values, exactly 16 new typed
bytes and final cleanup. Source vectors are borrowed, so this is output allocation
evidence. All 653 core tests and 94 DuckDB reference tests pass; TypeScript build
and changed-file lint pass. The preceding full default run, before this concat
change, passed 1,467 tests with 353 skipped. Different value dictionaries across
batches can still trigger materialization fallback; this change does not solve
that remaining generic concatenation allocation gap or total heap admission.

### Direct fixed-width dictionary concatenation (November 12, 2026)

When dictionary identity differs across batches, concatenation now resolves
compatible fixed-width dictionary leaves directly into admitted typed output.
It follows dictionary levels and their validity, preserves the leaf's physical
type, and combines inherited nulls with outer validity. It does not build the
previous per-row JavaScript arrays or timestamp objects for this path. Timestamp
unit and UTC-adjustment metadata must match; mixed physical types, variable-width
and nested leaves retain the established materialization fallback. The identical-
dictionary shared-index path still takes precedence.

Regressions cover nested distinct bigint dictionaries with values beyond safe
Number precision, outer and leaf nulls, exact 36-byte output and cap-minus-one
failure with scope cleanup. Additional cases verify timestamp precision/metadata
and explicit rejection of incompatible units, UTC flags, mixed leaves and empty
inputs by the specialized path. The broad core run passed 655 tests, followed by
two additional timestamp/capability tests. All 94 DuckDB fixture reference tests,
TypeScript project build and changed-file lint pass. Tests are local and use
borrowed dictionary inputs; they establish output semantics and tracked typed
allocation boundaries, not complete input heap or remote throughput. Concat
allocation is still incremental rather than whole-stage admitted, and remaining
variable-width/nested fallback work is unchanged.

### Real encoded dictionary-concat reference fixtures (November 12, 2026)

Two new reference cases write actual nullable INT64 Parquet files with
RLE_DICTIONARY values and SNAPPY compression, decode their column vectors, assert
that decoding preserved dictionary shapes and that the two files have distinct
dictionaries, then concatenate those decoded batches. The 256 output values
include repeated integers above Number's exact range and nulls, and are compared
with DuckDB's native bigint/null rows without JSON coercion. Batch sizes 17 and
128 exercise both sliced dictionary batches and whole-file-sized batches.

Both cases pass. A 2,304-byte output cap succeeds (2,048 value bytes plus 256
validity bytes); 2,303 bytes rejects, and disposal clears all output charges.
The decoder input buffers are explicitly borrowed in these tests: this boundary
proves actual encoded operation and concat output admission, not complete decoder
memory admission. DuckDB reads temporary local files while LakeQL reads the same
bytes through an in-memory object store; neither is remote/cloud evidence.
Temporary files are removed and connections closed in finally blocks. TypeScript
project build and changed-file lint pass. The test source is
`packages/parquet/src/dictionary-concat.reference.test.ts`.

### Direct packed dictionary concatenation (November 12, 2026)

Distinct packed-text and binary dictionaries now concatenate by resolving selected
byte ranges, measuring their total payload, and copying directly into packed
output. Ranges, data and any new validity are admitted as a group. Existing concat
validity is reused and updated with inherited dictionary nulls. Text and binary
logical types cannot mix in this specialization; binary-array and packed-binary
leaves can combine without text conversion. Payloads exceeding Uint32 offset
capacity reject before packed output allocation. The identical-dictionary path
continues to preserve shared indices instead of copying payload.

Unit regressions cover nested dictionary validity, nonzero row offsets, multibyte
UTF-8, empty versus null values, exact 40-byte output and cap-minus-one rejection,
repeated binary values and mixed text/binary rejection. All 660 core tests pass.
The encoded reference fixture now additionally tests nullable UTF-8 dictionaries
containing multibyte and empty strings at batch sizes 17 and 128. It explicitly
uses scoped decoding: unscoped UTF-8 decoding produces string-array dictionaries
and would exercise the old fallback instead. Decoder storage has a separate
tracked scope; exact concat-output caps do not claim to include it. Both scopes
clean up. All 98 DuckDB reference tests pass, along with TypeScript build and
changed-file lint.

These are actual SNAPPY/RLE_DICTIONARY local fixture checks, not remote transport
measurements. Plain string-array dictionaries and general nested dictionaries
still have fallback materialization gaps; concat-wide stage admission and complete
JS heap accounting also remain outstanding. No measured throughput improvement
is claimed for this implementation step.

### Plain-string dictionary packing without per-value buffers (November 12, 2026)

Direct dictionary concat now accepts plain string-array leaves alongside packed
UTF-8 leaves. `losslessUtf8Bytes` measures selected strings without allocation;
`TextEncoder.encodeInto` writes directly into the already admitted destination.
No selected-row string array or per-value encoded payload buffer is created.
Output remains a declared packed UTF-8 vector, and source/outer nulls and empty
strings retain their semantics.

A deliberate capability boundary preserves arbitrary JavaScript string values:
lone UTF-16 surrogates cannot round-trip through UTF-8, so the packed path declines
such selected values before allocation and uses the existing lossless fallback.
Tests compare byte measurement with TextEncoder for ASCII, NUL, multibyte BMP,
valid surrogate pairs and replacement characters, and reject unpaired surrogates.
A mixed plain/packed concat verifies exact 39-byte output, supplementary Unicode,
empty values, and preservation of a lone surrogate through fallback.

Real SNAPPY UTF-8 dictionary fixtures now run with both scoped (packed dictionary)
and unscoped (plain string dictionary) decoding at batch sizes 17 and 128. Both
exercise direct packed concat and match DuckDB at exact output caps; one byte less
rejects. All 674 core tests and 100 DuckDB reference tests pass; TypeScript build
and changed-file lint pass. This is local encoded-fixture evidence, not a new
remote benchmark or whole-heap bound. General nested concat and complete stage
admission remain unfinished.

### Preserve leading U+FEFF in UTF-8 data values (November 12, 2026)

A Unicode round-trip regression exposed that the default TextDecoder stripped a
leading U+FEFF from each packed string, treating individual data values like
streams with a byte-order signature. Packed core materialization now decodes with
`ignoreBOM: true`, preserving the character as data. The unscoped Parquet parser
had the same behavior through its upstream default; LakeQL now supplies an
explicit string parser with the same value-preserving setting. Document/JSON
stream decoding is unchanged.

The initial regression failed for a standalone U+FEFF, U+FEFF-prefixed text, and
a double prefix, while an embedded character survived. Those cases now pass.
SNAPPY dictionary reference fixtures also include leading U+FEFF in multibyte
values and cover scoped and unscoped decode. Direct DuckDB JavaScript text results
lost the prefix too, hiding agreement between lossy paths: the reference now asks
DuckDB for `hex(value)` and reconstructs UTF-8 with Node Buffer, preserving the
actual SQL value bytes rather than relying on that text-result conversion.
Native bigint comparison remains unchanged.

All 1,490 default tests pass (359 skipped), all 100 DuckDB reference tests pass,
and TypeScript build and changed-file lint pass. These tests establish local
string fidelity and exact concat output sizes, not new cloud performance or
whole-process heap evidence. The broader scheduling and accounting goal remains
unfinished.

### Dictionary concat measurements and text tradeoff (November 12, 2026)

`bench/dictionary-concat.mjs` compares current direct concat with the prior
row-materialization branch emulated through `vectorValue`/`vectorFromValues`.
It uses two distinct 32-entry dictionaries, 1k/100k rows per input, three warmups
and five measured samples, validates every output value outside timing, and checks
zero final charges. Inputs are borrowed and non-null; bigint values exceed safe
Number precision and strings include leading U+FEFF and multibyte characters.
Results and source hash are in `docs/benchmarks/dictionary-concat-2026-11-12.jsonl`.

For 200k bigint rows, fallback measured 24.28 ms versus 1.29 ms direct, both with
1.6 MB tracked output. At 2k rows, results were 0.279 versus 0.121 ms. Text shows
a real tradeoff: at 200k rows, fallback measured 15.34 ms versus 17.90 ms direct;
at 2k rows, 0.237 versus 0.370 ms. Direct text tracks 4,137,500 / 41,364 bytes of
packed output. Fallback reports zero tracked bytes because its JS reference arrays
and strings are outside this counter, not because it allocates nothing. These
figures cannot establish a total-memory improvement for repeated text.

The measured text regression changes the next implementation priority: preserve
small distinct dictionaries through an admitted index/dictionary representation
when that costs less than expanded packed output, using measured cardinality and
byte sizes rather than a user-selected mode. Re-encoding repeated strings per
output row is not the desired final performance contract. Existing direct packing
remains useful when detaching expanded data is appropriate, and must not be
replaced with an untracked allocation fallback merely to improve timing. This
benchmark is local operator evidence, not whole-query/cloud throughput or a
Worker heap-cap proof. Benchmark assertions and changed-file lint pass.

### Adaptive packed dictionary union (November 12, 2026)

Distinct text/binary dictionary concat now compares measured expanded output
against a packed union of distinct source dictionary objects plus output indices.
The decision includes dictionary ranges/payload/validity, output indices/validity,
and admitted identity-index construction scratch. It chooses union only when that
complete typed construction cost is smaller and fits available scoped capacity;
otherwise it uses direct expanded packing. Leaf identities are deduplicated, not
value strings; no unbounded per-row string hash table is introduced. Nulls at
multiple dictionary levels resolve into the output mask. Large mostly-unused
dictionaries decline union. No user flag is required.

Two new regressions verify nullable repeated union output (1,038 retained bytes,
1,054 peak including scratch) and decline of a large unused dictionary in favor
of an 18-byte selected output. The preceding broad core run passes 675 tests;
these two additional tests pass. All 100 DuckDB reference tests pass with both
scoped/unscoped UTF-8 dictionaries. Their caps now derive the measured adaptive
construction peak and check exact-peak success and peak-minus-one failure, rather
than requiring the superseded expanded representation. TypeScript build and
changed-file lint pass.

The rerun `docs/benchmarks/dictionary-concat-adaptive-2026-11-12.jsonl` measures
200k text rows at 7.64 ms and 801,580 peak tracked bytes, versus the prior expanded
path's 17.90 ms and 4,137,500 bytes. The same-run fallback emulation measures
20.65 ms with untracked JS output; timing runs are observational rather than a
controlled historical release comparison. At 2k text rows the adaptive path is
0.244 ms versus 0.215 ms fallback, with 9,580 tracked bytes. This improves the
measured large repeated-text tradeoff without claiming every shape is faster.

Remaining limits: metadata Maps/arrays and object headers are not fully charged;
concat-wide admission is still incremental. `concatOwnedBatches` conservatively
retains source ownership for dictionary-shaped results, including this newly
constructed union, so precise alias-aware source release remains required. These
benchmarks borrow inputs and therefore do not demonstrate total owned-query peak
reduction or cloud/Worker heap bounds.

### Alias-aware owned dictionary-union release (November 12, 2026)

Owned concat now distinguishes dictionary identity shared with input columns from
newly constructed dictionary storage. Shared input dictionaries continue to retain
source ownership; a fresh packed union can release concat's source pins after
construction. Remaining plain text/binary and nested alias checks remain
conservative. This changes ownership decisions, not the caller's obligation to
release its own input handles.

A regression constructs two separately owned packed dictionaries in a producer
child budget, concatenates into a sibling consumer budget, and releases both
producer handles. The producer returns to zero while the consumer/root retain
exactly 818 bytes (800 output-index bytes, 16 dictionary-range bytes, 2 payload
bytes). All 200 rows remain correct, and final output release clears the root.
Existing shared-dictionary retention tests also pass, preventing premature release
of truly aliased source values. All 678 core tests, TypeScript build and changed-
file lint pass. This is scoped ownership evidence with real typed buffers, not
new remote-I/O or whole-process heap evidence. Per-source release for mixed outputs
that still share some columns remains conservative, and complete stage scheduling
and JS bookkeeping admission remain open.

### Bound dictionary-choice inspection for sparse selections (November 12, 2026)

The adaptive union planner previously measured every source dictionary entry
before learning that a tiny selection should use expanded packing. It now first
measures selected output and calculates the union's minimum typed cost from entry
counts, ranges, output indices and construction scratch. If that lower bound
already exceeds the expanded alternative or available capacity, it declines union
without reading unused payloads. A necessary payload scan also stops once the
measured union reaches expanded-output cost; null-mask costs participate in that
comparison. This preserves the measured choice without making sparse selection
pay for an obviously uneconomical dictionary scan.

A regression uses two 10,000-entry dictionaries selecting only entry zero and a
throwing getter on the first unused entry. It verifies that unused values are not
inspected, the selected rows remain correct, exactly 32 output bytes are charged,
and cleanup returns to zero. This is deterministic access-complexity evidence,
not a wall-clock benchmark. All 679 core tests and 100 DuckDB reference tests pass;
TypeScript project build and changed-file lint pass. This does not bound arbitrary
selected-string scan time or close remaining JS bookkeeping/scheduler gaps.

### Worker dictionary-concat regression and validation checkpoint (November 12, 2026)

A dedicated workerd regression now executes the adaptive plain-string dictionary
union with leading U+FEFF, accented text and supplementary Unicode. It owns the
input index buffers, constructs output at an exact 1,670-byte tracked peak, and
rejects at 1,669 bytes. After both producer handles release, exactly 854 output
bytes remain and all 200 rows are verified; releasing output clears the pool.
Source strings are borrowed, so these assertions cover typed storage and lifetime
behavior in the Worker runtime, not total isolate memory.

All 20 workerd tests pass across eight files. The full coverage run passes 1,494
default tests (359 skipped) at unchanged thresholds: 91.04% statements, 84.77%
branches, 95.09% functions and 93.01% lines. Dedicated typecheck passes after the
new Worker tests. Full lint passed before adding that test, and its changed-file
lint also passes. The preceding 100-test DuckDB reference run remains applicable
to unchanged production code. Source hashes and scope limits are recorded in
`docs/validation/dictionary-concat-checkpoint-2026-11-12.json`.

No public-cloud run was performed in this checkpoint. Worker runtime execution
is stronger than Node-only compatibility evidence, but it is still distinct from
64–128 MiB whole-isolate heap validation and large remote fixture stress. Those
requirements and mandatory scheduling remain open.

### Atomic typed working-set admission for dictionary union (November 12, 2026)

Adaptive dictionary union now creates a bounded construction stage and admits its
measured new typed output plus identity-index scratch with one budget reservation.
All construction buffers draw from that extent rather than independently seeking
capacity. Before handoff, scratch owners release and only completed output
backing transfers into the caller scope. Failure/cancellation disposes the stage;
failed parent adoption releases the independently retained output handle. Existing
input ownership and an already-created outer validity mask remain separately
charged, so this is an atomic construction stage rather than full-query admission.

The nullable-union regression verifies exactly one reservation for the 1,054-byte
working set and 1,038 bytes after scratch release. A new reentrant-cancellation
case closes the parent during reservation and verifies no leaked charge. All 680
core tests and 100 DuckDB reference tests pass. Both focused workerd exact-cap
regressions also pass, along with TypeScript build and changed-file lint. No new
remote benchmark was run. Concat-wide/multi-column planning, shared mandatory
scheduling and complete JavaScript heap accounting remain required work.

### Nested construction from parent admission (November 12, 2026)

`MemoryScope.childStage` transfers an exclusive extent from a parent's admission
(or reserves it when no parent admission exists), links cancellation, and makes
that capacity available to child allocations without a second global charge.
`availableAllocationBytes` reports unconsumed scoped capacity when present, rather
than only global free bytes. Adaptive dictionary planning now uses this capacity
and constructs through `childStage`, allowing future parent/multi-column plans to
fund it without the planner incorrectly declining union at zero global headroom.

A regression pre-admits 826 bytes in the parent, then constructs an 818-byte union
plus 8 bytes of scratch with zero additional budget reservations. Another divides
a 16-byte stage, starts an 8-byte pending allocation, cancels the parent, and
verifies that unused parent/child capacity releases while those 8 bytes remain
charged until work settles. Peak remains 16 and final usage is zero. All 682 core
tests and 100 DuckDB reference tests pass; TypeScript build and changed-file lint
pass. Child stages require disposal and explicit output transfer; released scratch
does not silently replenish the parent's total-new-allocation extent. This is
composable admission groundwork, not an asynchronous scheduler or full pipeline
working-set plan.

### Large public flights projection and mixed-shape cloud regression (November 12, 2026)

`bench/cloud-flights-validation.mjs` validates read-only HTTP 206 range execution
against the repository's public 25,238,218-byte, 5,819,079-row flights fixture. It
compares the first 2,048 rows of three numeric columns with DuckDB reading the
local fixture, using batch size 128, 64/128 MiB shared pools and one/two concurrent
jobs. All four scenarios pass, with zero final charges. Single-job tracked peak
is 16,724,760 bytes; two-job peak is 33,449,520 bytes. Requests were 17 versus 19,
and wire bytes 20,990,720 versus 20,990,724, consistent with cross-job read reuse.
The instrumented active count covers time to response headers, not complete body
lifetimes. Results are in `docs/benchmarks/cloud-flights-projection-2026-11-12.json`.

This is larger-object remote evidence but only a bounded projection, not a full
5.8M-row scan or total Worker heap test. It exposes an important unresolved read
amplification problem: a 2,048-row projection transfers about 21 MB of a 25 MB
object. Page/chunk read planning must be investigated next; successful allocation
caps do not make this I/O cost acceptable or establish throughput improvement.

Separately, rerunning the small public cloud suite found a real top-k integration
regression: concat received packed UTF-8 in one candidate batch and dictionary-
backed UTF-8 in another and rejected their physical type difference. Generic concat
now recognizes compatible logical shapes across dictionary and direct vectors,
normalizes direct vectors through admitted temporary identity indices, and uses
the existing dictionary concat operators. Temporary indices use a private cache
so they cannot escape via the batch-wide reuse cache, and release after output
construction. Incompatible logical shapes still reject. A regression verifies
mixed packed/dictionary text values, exact 24-byte construction peak and 16-byte
retained output. All 683 core tests pass. Remote rerun results are recorded below
when complete; the original cloud failure is not counted as a pass.

The fixed small-cloud rerun passes all 52 checks, including 40 ordinary-query
cases and 3,456 single-row batches. Maximum incremental tracked usage is 54,659
bytes within 131,072 bytes of headroom; warm ordinary-query data requests are zero
and final charges clear. Results are saved in
`docs/benchmarks/cloud-range-dictionaries-2026-11-12.json`. All 100 DuckDB reference
tests, TypeScript build and changed-file lint pass after the mixed-shape correction.
The remote test was necessary: prior unit/reference success had not covered this
particular top-k physical-shape transition. Large-fixture projection overfetch
remains open despite these correctness and bounded-allocation results.

### Early-stop cloud prefetch correction (November 12, 2026)

Both borrowed and owned Parquet vector readers now omit speculative whole-column
chunk prefetch when `canStopEarly` is true. The existing decoder already selects
page windows for this execution contract, so fetching entire chunks beforehand
undid the early-stop I/O benefit. Demand reads remain available through the
bounded range source; full-scan prefetch is unchanged. Borrowed concurrency
validation now runs independently of prefetch selection, rejecting non-positive,
fractional and non-finite limits even when early-stop disables prefetch.

The real public flights projection rerun passes all four 64/128 MiB and one/two-job
scenarios against the local DuckDB reference. Single-job wire bytes decrease from
20,990,720 to 5,380,784 (74.4%); requests decrease from 17 to 15. Two jobs transfer
5,380,788 bytes in 17 requests, preserving observed cross-job reuse. Tracked peaks
remain 16,724,760 and 33,449,520 bytes, respectively; final charges clear. Artifact:
`docs/benchmarks/cloud-flights-early-stop-2026-11-12.json`. Elapsed times are single
observations, not a controlled latency comparison. Remaining 5.38 MB transfer and
unchanged decoded peak require further page/dictionary investigation; this does
not establish full-scan performance or total Worker heap caps.

TypeScript build and changed-file lint pass. All 35 owned-reader suite tests pass,
including real writer fixtures asserting no whole-chunk prefetch for both borrowed
and owned early-stop iteration, correct first-row output, owned cleanup, and
invalid concurrency rejection. Full transport/reference gates remain to rerun for
this change.

Follow-up gates: all 375 enabled Parquet tests pass (284 conditional tests skipped),
and all 100 DuckDB reference tests pass. The first sandboxed Parquet invocation
could not bind loopback fixture servers (`EPERM`); rerunning with loopback access
passed all five affected HTTP cases and the rest of the suite.

The public cloud regression rerun also passes all 52 checks, preserving the
coalescing, deduplication, cancellation, cache and concurrency assertions in
`bench/cloud-range-validation.mjs`. Maximum reported incremental tracked charge
is 49,805 bytes within 131,072-byte query headroom. Results are saved in
`docs/benchmarks/cloud-range-early-stop-2026-11-12.json`.

Local physical page inspection of the flights fixture (separate from remote
measurement) finds first data pages with 787,456 / 790,528 / 699,392 values for
DEPARTURE_DELAY / ARRIVAL_DELAY / DISTANCE. Their compressed sizes are 990,821 /
995,160 / 963,109 bytes, respectively, plus small dictionary pages. Thus roughly
3 MB of compressed first-page data is required by this page-oriented decoder even
for 2,048 selected rows. This explains part, not all, of the observed 5.38 MB wire
transfer. The residual needs range-level tracing; the large decoded pages also
remain relevant to memory budgeting. Inspection evidence is recorded in
`docs/validation/flights-page-layout-2026-11-12.json`. It does not prove a lower
bound for every possible decoder implementation or account for the full heap.

### Cache-capability-aware owned read-ahead (November 12, 2026)

Range tracing in the flights benchmark exposed three identical 256 KiB requests
per selected column: dictionary header, dictionary body and first data-page
header. The default shared range cache has no allocation budget, so `setOwned`
cannot retain these completed buffers. Coalescing had therefore expanded each
small demand without enabling later reuse. Trace evidence is saved in
`docs/benchmarks/cloud-flights-range-trace-2026-11-12.json`; the benchmark now emits
requested and returned ranges and response lengths for every scenario.

`SharedMemoryCache.supportsOwnedRetention` now exposes whether a budget belongs
to a compatible allocation pool. Owned range reads use exact demand when that
capability is absent, and skip speculative prefetch which cannot survive until
consumption. Compatible budgeted caches preserve coalescing, in-flight joining,
retention and existing capacity admission. This does not silently add persistent
cache allocations to the user's pool or treat capability as guaranteed capacity.
Tests cover unbudgeted and unrelated-pool caches, exact returned bytes, skipped
unusable prefetch, and zero residual charge in both pools.

All four real-cloud flights scenarios pass again. Single-job wire bytes decrease
from 5,380,784 to 3,031,578 (43.7% further reduction; 85.6% versus the original
20,990,720). Two jobs transfer 3,031,582 bytes. Request counts remain 15/17, and
tracked peaks remain 16,724,760/33,449,520 bytes; final charges clear. Artifact:
`docs/benchmarks/cloud-flights-cache-capability-2026-11-12.json`. These are transport
byte improvements, not demonstrated latency or heap improvements. The first
compressed pages dominate the remaining bytes; full-page decoded allocations
remain a separate concern.

The core/Parquet run passes 1,060 tests with 284 conditional tests skipped.
After additionally suppressing unusable explicit prefetch, the 44 focused cache
tests and 100 DuckDB reference tests pass. TypeScript build and changed-file lint
pass. Public-cloud range regression verification is recorded separately when
complete. Shared mandatory scheduling, full heap accounting and general nested
execution coverage remain open requirements.

Final reruns for this change: all 1,060 enabled core/Parquet tests pass, and the
public cloud suite passes all 52 checks. Remote evidence is saved in
`docs/benchmarks/cloud-range-cache-capability-2026-11-12.json`. Compatible-cache
coalescing, reuse, cancellation and concurrency remain covered by that suite.

### Release numeric decompression scratch after independent output (November 12, 2026)

The numeric page reader now releases exact owned decompression output after
constructing dictionary indices, booleans, or numeric values that do not alias
that backing. It compares backing identity before release: aligned PLAIN views
retain their decoded page, packed binary/text paths retain their payload, and
caller input is never released by this rule. Untracked custom compressor outputs
are left untouched. This matters because `retainAllocations` conservatively pins
all current scope allocations; passing a vector to it does not select only that
vector's backing. Disposing a page scope after such a pin would not itself remove
unused workspace, so no such unsupported lifetime shortcut was introduced.

Unit tests explicitly distinguish copied and aligned output with a scoped
compressor. Existing real SNAPPY byte-stream-split fixtures now pass at an exact
28-byte cap and fail at 27 (previous exact cap 36). Peak falls by eight bytes and
retained usage falls from 28 to 20; real values and nulls remain unchanged in both
page-window and whole-chunk paths. The initial suite failed only on the old
36-byte peak expectations; those were updated together with the stricter cap and
cap-minus-one assertions. All 379 enabled Parquet tests pass, with 284 conditional
tests skipped, and all 100 DuckDB reference tests pass. TypeScript build and
changed-file lint pass.

All four real public flights scenarios pass. Single-job tracked peak falls from
16,724,760 to 14,712,683 bytes (12.0%); two-job peak falls from 33,449,520 to
28,462,305 bytes (14.9%, with overlap subject to scheduling). Wire bytes remain
3,031,578/3,031,582 and requests remain 15/17. Final charges clear. Results:
`docs/benchmarks/cloud-flights-decode-scratch-2026-11-12.json`. This is measured
tracked-allocation improvement on remote reads, not a whole Worker heap guarantee
or a controlled latency result. Whole-page live indices/values, conservative
ownership elsewhere, mandatory scheduling and general heap coverage remain open.

### Decoder lifetime runtime checkpoint (November 12, 2026)

Added ownership tests for compressed V1/V2 dictionary and boolean pages, checking
that independent output remains charged after exact decompression backing is
released. These unit tests use an instrumented identity compressor to isolate
ownership behavior; they do not validate compression correctness. A malformed
compressed dictionary keeps its 20 bytes charged until stage cleanup, then clears
them. The focused numeric-page suite passes all 21 cases.

Real compression is covered separately: expanded the workerd native R2 matrix to
UNCOMPRESSED PLAIN, SNAPPY PLAIN and SNAPPY dictionary numeric columns in both
64/128 MiB allocation pools. Each exercises concurrent jobs, warm reuse, cache
eviction and output surviving iterator close. All 24 workerd tests across eight
files pass. This uses the local workerd R2 binding, not a production cloud endpoint;
public HTTP results above remain distinct. The runtime emitted `pump canceled`
during the successful run; exit status and test assertions passed.

The full default coverage checkpoint passes 1,513 tests (359 skipped), with 91.02%
statements, 84.77% branches, 95.08% functions and 92.99% lines. It preceded the final
additional malformed-dictionary test, which passed in the focused run. Dedicated
typechecking passes. Full lint passes after formatting two benchmark JSON artifacts
without changing their measured values. Source hashes and checkpoint scope are in
`docs/validation/decoder-lifetime-checkpoint-2026-11-12.json`. These checks do not
close the outstanding scheduler, complete heap accounting or conformance gaps.

### Fixed-width concat working-set admission (November 12, 2026)

Owned concat now computes the complete new typed output extent for compatible
fixed-width physical vectors before allocating any column. The planner covers
boolean, u8, i32/u32, f32/f64, i64/u64, matching timestamp representations, null
vectors, and per-column validity buffers. It checks accumulated sizes for safe
integer overflow. Inputs remain independently charged during construction; the
single output admission is consumed by the existing scoped column allocators.
This establishes an atomic new-output stage boundary for these shapes without
changing synchronous allocation into per-buffer waits.

The planner explicitly declines dictionary, variable-width, nested, mixed-type,
and timestamp-conversion paths. Those need their own complete working-set plans;
the existing execution path remains available but is not described as atomically
planned or safe to queue. In particular, the shared mandatory scheduler is still
unimplemented. Read/decode scheduling must also include aligned-column retained
pages, codec workspace and handoff ownership, and cannot wait while holding scarce
I/O slots or assume externally retained outputs will release themselves.

Tests compare measured bytes with actual scoped concat for all ten supported
physical types, with and without validity, including bigint boundaries and
nanosecond timestamps. A four-column output requires exactly 88 bytes; a cap of
87 rejects with zero output allocation peak. Existing charged-input concat tests
continue to cover input/output overlap and source release. An unsafe accumulated
length rejects before allocation. All 696 core tests and 100 DuckDB reference
tests pass at the main checkpoint; the final overflow case passes in the complete
14-test planner suite. TypeScript build and changed-file lint pass. No remote or
latency improvement is claimed from this admission change.

### Shared-dictionary concat admission (November 12, 2026)

The concat planner now also handles columns whose dictionary object is identical
across all input batches. It measures new indices and per-column validity while
preserving the execution cache's complete ordered index-source identity rule.
The planner models candidate replacement as well as hits: after a different source
sequence replaces the candidate under the same first-index key, a later original
sequence requires another output allocation. Dictionary contents may themselves
be variable-width or nested because this execution shape retains them without
materializing new values. Distinct dictionaries requiring conversion or union
remain explicitly unplanned at the concat-wide boundary.

A four-column fixture with shared numeric/text indices, distinct validity and
candidate replacement measures exactly 26 bytes rather than a naive 34-byte sum;
cap 25 rejects with zero output peak. A mixed scalar/dictionary fixture charges
40 bytes of input plus 24 bytes of output at cap 64; cap 63 rejects before partial
output allocation, and completed output remains valid after caller input release.
These inputs deliberately expose conservative source ownership, rather than
claiming that only dictionary backing is retained.

All 700 core tests and 100 DuckDB reference tests pass at the main checkpoint.
After adding the final charged-input matrix, all 19 planner tests pass. TypeScript
build and changed-file lint pass. Public cloud integration results follow when
complete. This extends atomic typed-output planning; it does not implement shared
waiting/scheduling, eliminate all conservative ownership, or prove a full heap cap.

The public-cloud suite passes all 52 checks after concat admission integration,
including top-k physical-shape transitions and shared cache/concurrency cases.
Results are in `docs/benchmarks/cloud-range-concat-admission-2026-11-12.json`.

### Concat planner metadata and scaling (November 12, 2026)

Shared-dictionary planning now stores a candidate column name for each first-index
identity rather than retaining a new batch-sized array of source indices for each
candidate. The immutable input batches already identify that sequence. Planning
compares candidate indices through those batches and preserves replacement/hit
semantics tested above. Retained planner metadata is O(columns), rather than
O(columns × batches); iteration remains O(columns × batches), without inspecting
rows or dictionary values. This bounds the structure's growth but does not make
JavaScript Map/string/object overhead part of tracked allocation accounting.

A regression instruments column lookups across 512 batches and 16 columns,
checks a linear bound, and makes dictionary-value access throw. All 703 core tests
and 100 DuckDB reference tests pass, along with TypeScript build and changed-file
lint. Existing exact-cap, candidate-replacement and retained-input tests pass.

`bench/concat-planning.mjs` measures 24 planning-only scenarios across fixed/shared
shapes, 4/32 columns, 16/256/4,096 batches and 1/128 rows per batch. It verifies every
planned extent, excludes input construction from timings, and records five-sample
medians after three warmups with a source hash. At 32 columns/4,096 batches, fixed
planning took 2.45/3.04 ms for 1/128 rows, and shared-dictionary planning took
3.15/2.82 ms. The measurements are consistent with metadata-sized rather than
row-sized planning. Results: `docs/benchmarks/concat-planning-2026-11-12.jsonl`.
These are local planning timings with borrowed fixtures, not output-construction,
whole-query/cloud speedups or total Worker heap proof. Scheduler integration and
remaining physical-shape plans remain outstanding.

### Direct packed concat and payload admission (November 12, 2026)

Direct packed UTF-8/binary concat now uses the same measured packed-copy kernel as
dictionary concat, accepting direct vectors without constructing identity indices
or row-sized JavaScript arrays. Output remains packed. The kernel copies selected
payload bytes into independently owned backing, preserving row offsets, fixed
widths, validity, empty values, leading U+FEFF, multibyte text and arbitrary binary
bytes. Null payload bytes are omitted. The concat-wide planner now measures direct
packed payload plus ranges/validity and admits it together with other supported
columns; Uint32 payload-offset overflow rejects before allocation.

Tests cover variable/fixed-width sliced inputs, null versus empty, invalid UTF-8
binary, and source mutation after output construction. A charged fixture requires
four bytes of input plus twenty output bytes at cap 24; cap 23 rejects with only
input charged, and input release leaves exactly twenty bytes until output release.
All 706 core tests pass at the main checkpoint, and all five focused packed-concat
tests pass after the final overlap cases. Expanded real SNAPPY PLAIN text fixtures
at batch sizes 17/128 compare every concatenated value with DuckDB, alongside the
existing dictionary cases: all 102 reference tests pass. The plain fixtures are
all-present; nullable direct packed behavior is covered separately by the explicit
vector fixtures. These reference reads use local files/memory storage, not cloud.
TypeScript build and changed-file lint pass.

`bench/packed-concat.mjs` compares the production packed copy with an emulation of
the former row-materialization path, verifies every output value outside timing,
and checks final charges. At 200,000 rows, UTF-8 median changes from 40.83 to 7.83 ms,
and binary from 28.31 to 8.01 ms (five samples after three warmups). UTF-8 packed
output is 3.2 MB tracked; the old zero tracked bytes excluded JS strings/arrays
and did not mean zero memory. Binary tracked output is 2.2 MB in both paths. These
are local borrowed-input microbenchmarks, not whole-query or cloud latency proof.
Results are in `docs/benchmarks/packed-concat-2026-11-12.jsonl`.

The public-cloud regression suite passes all 52 checks after the physical output
shape change, including ordinary top-k, concurrency and cache reuse. Evidence:
`docs/benchmarks/cloud-range-packed-concat-2026-11-12.json`. Distinct dictionary
union planning, nested materialization, shared scheduling and full heap accounting
remain outstanding.

### Recursive struct concat and exposed map failure (November 12, 2026)

Matching struct field sets now concatenate recursively through typed field
operators, preserving parent validity, bigint/packed children and nested physical
representations. The concat planner recurses over the same structural capability
and admits parent masks plus planned field output. Different field sets retain
the prior normalization fallback. Dictionary ownership collection now traverses
nested fields/children so a nested shared dictionary remains charged after caller
input release. Concat field insertion uses safe own-property assignment for the
literal `__proto__` key.

Tests cover recursive structs at an exact 40-byte output cap and zero-output
rejection at 39; nested shared-dictionary input/output overlap at 32 bytes and
rejection at 31; parent nulls, bigint precision, packed binary, literal prototype
keys, and heterogeneous field normalization. All 713 core tests and 102 DuckDB
reference tests pass. TypeScript build and changed-file lint pass.

The new internal `bench/struct-concat-fixtures.mjs` probe bypasses the public schema
gate explicitly. It compares decoded three-row batches and their concatenation
with local DuckDB, duplicating the batch sequence to exercise multiple inputs even
for one-row fixtures. Two copies of `nested_structs.rust.parquet` match, with an
exact 3,456-byte tracked output cap and cap-minus-one rejection. This does not
relax the public struct guard or establish remote/whole-heap behavior.

The same probe FAILS for `nonnullable.impala.parquet`: decoded inputs match DuckDB,
but concat changes an empty map inside a list into `{k1: null}` at
`rows.0.int_map_array.0`. The remaining list/map row-materialization path loses
schema semantics. This is outstanding correctness work, not a passing validation;
the probe exits nonzero, so later files have not been validated by that run.
Evidence is recorded in `docs/validation/struct-concat-fixtures-2026-11-12.jsonl`
and `docs/validation/struct-concat-known-failure-2026-11-12.json`. The next required
step is schema-preserving typed list/map concat, including sliced offsets, null
parents, empty collections, heterogeneous children and retained ownership.

### Typed list/map concat resolves the empty-map failure (November 12, 2026)

List/map concat now validates monotone bounded offsets, slices the referenced
contiguous child windows, concatenates children through typed operators and
constructs rebased Int32 offsets. Empty maps remain maps, rather than being
re-inferred as structs with null-valued keys. Null-parent child spans remain hidden
but charged/copied; unused child prefixes and suffixes are excluded. Empty child
batches do not force a concrete populated child into a null inferred type. The
recursive allocation planner admits offsets, parent validity and supported child
output together, with Int32 offset overflow rejection.

Mixed null/concrete batches now explicitly normalize null shapes through admitted
masks/offsets/indices. Struct/list/map schemas survive this normalization, and
temporary normalization buffers release after construction. Input ownership
collection includes direct vectors because normalization may wrap them in a
shared dictionary. Mixed-shape normalization remains outside the complete atomic
planner; it is bounded by individual mandatory allocations, not a schedulable
whole-stage extent. Legacy string/reference-array slicing and unsupported child
fallbacks still have JavaScript allocation gaps. Map row materialization still
uses string keys; upstream typed-map-key loss remains separate outstanding work.

The previously failing internal probe now passes all three local real fixtures:
`nested_structs.rust.parquet`, `nonnullable.impala.parquet`, and
`nullable.impala.parquet`, including duplicated inputs, exact output caps and
cap-minus-one rejection. Decoded inputs and concatenated outputs both match
DuckDB. Peaks are 3,456 / 296 / 2,088 tracked output bytes, respectively. Evidence:
`docs/validation/collection-concat-fixtures-2026-11-12.jsonl`. The historical failure
artifact points to this resolution. The public struct gate remains unchanged;
these are internal local-fixture probes, not production-cloud struct acceptance.

Tests cover empty maps inside lists, sliced/rebased child offsets, null collection
batches, malformed offsets, direct scalar alias ownership after null normalization,
and exact 74-byte output admission with zero-output rejection at 73. All 720 core
tests, 102 reference tests and 24 workerd tests pass. The existing physical-shape
assertion expecting map-to-struct conversion was updated to require a map; logical
rows remain verified. TypeScript build and changed-file lint pass. All 52 public
cloud regression checks also pass; results are in
`docs/benchmarks/cloud-range-collection-concat-2026-11-12.json`.

`bench/collection-concat.mjs` verifies every output of a local uniform-key list/map
microbenchmark. At 20,000 rows, five-sample median changes from 7.69 ms for the old
row fallback emulation to 1.03 ms for typed children. Tracked output rises from
240,004 to 420,008 bytes because map keys/offsets are preserved instead of changing
maps into structs; old JavaScript object intermediates are excluded from those
counts. This is not evidence of a total-memory reduction. Results are in
`docs/benchmarks/collection-concat-2026-11-12.jsonl`. Scheduler integration, complete
shape planning and whole-heap coverage remain outstanding.

### Preserve shared dictionaries during null normalization (November 12, 2026)

A new regression exposed another empty-map corruption path: concatenating a null
batch with a dictionary-backed map batch introduced an additional dictionary
layer, sent the inputs through differing-dictionary row materialization and
changed `{}` into `{k: null}`. The regression failed in both null-first and
null-last order before the fix. Null normalization now points at the existing
concrete dictionary, allowing shared-dictionary concat to preserve its schema and
identity. Its zero-valued indices remain hidden by an admitted all-null mask.
Normalization uses a private index-copy cache so temporary input indices cannot
escape into the outer batch cache after their ownership releases.

The boundary fixture requires a 25-byte construction peak and retains 20 output
bytes; cap 24 rejects and cleans up completely. The matrix covers both batch
orders and zero/one/two nested dictionary layers, preserving empty maps, populated
maps, nulls, repeated indices and dictionary identity. All 724 core tests and 102
reference tests pass at the main checkpoint; the expanded final matrix passes in
the 19-test collection suite. TypeScript build and changed-file lint pass. The
three internal real-fixture comparisons still match DuckDB at exact caps and
cap-minus-one rejection; results are saved in
`docs/validation/null-dictionary-concat-fixtures-2026-11-12.jsonl`. No new remote
performance claim is made. Differing nested dictionaries still need a typed
selection/concat path; this correction does not claim to remove their fallback or
complete mixed-shape atomic planning.

### Typed selected-row expansion for differing nested dictionaries (November 12, 2026)

Differing dictionaries over matching struct/list/map shapes now expand selected
rows into typed metadata and use recursive concat, avoiding the row-materializing
fallback for those shapes. Struct fields share one resolved index vector;
collections build rebased offsets and child-selection indices, with map keys and
values sharing the selection. Nulls are resolved through every dictionary level,
invalid indices/offsets reject, and each selection's complete typed extent is
admitted through a child stage before allocation. Temporary stages remain charged
through recursive construction and release on success/failure. Unselected
collection payload is not expanded; an empty selected row can be represented in
eight admitted bytes despite a 100,000-value unused child span.

The expanded real-fixture probe initially exposed dictionaries whose leaf was
itself all-null. Mixed all-null/concrete dictionary inputs now normalize that
representation to a null vector before selecting the nested path. This prevents
nullable fixture maps from acquiring extra null-valued keys. All six combinations
(three real files, direct and distinct dictionary-wrapped decoded batches) now
match local DuckDB, including exact measured-cap and cap-minus-one checks.
Wrapped/direct output peaks differ because some wrapped outputs retain borrowed
source dictionaries: they are not comparable whole-query memory measurements.
Evidence: `docs/validation/nested-dictionary-concat-fixtures-2026-11-12.jsonl`.
The public struct guard is unchanged, and fixture inputs remain outside this
output-only accounting probe.

Tests cover map/list/struct dictionaries, repeated selections, multi-level nulls,
all-null dictionary leaves, exact-cap failures/cleanup, invalid indices and no
inspection of unused child payload. All 740 core tests, 102 reference tests and
24 workerd tests pass, along with TypeScript build and changed-file lint. The
public cloud suite passes all 52 checks; results are in
`docs/benchmarks/cloud-range-nested-dictionary-2026-11-12.json`.

A local uniform-key dictionary/list/map benchmark verifies every output value
outside timings. At 20,000 rows, five-sample median is 9.00 ms for the old row
fallback emulation versus 3.38 ms for typed selection. Tracked peak rises from
240,004 to 640,600 bytes: new typed selection/map metadata is admitted, while old
JS row objects were outside those counts. No total-memory reduction is claimed.
Results: `docs/benchmarks/nested-dictionary-concat-2026-11-12.jsonl`.

Complete concat-wide admission for differing nested dictionaries is still absent:
individual selection stages are atomic, but their overlap with recursive output
is not yet a single planned extent. Heterogeneous structural normalization,
legacy JS child-array slicing, codec/operator gaps, shared mandatory scheduling
and complete heap accounting remain open. This change does not imply otherwise.

### Nested stage lifetime and validation checkpoint (November 12, 2026)

Added integration tests proving nested selection consumes a pre-admitted 12-byte
parent extent without double charging, releases its child charge without silently
replenishing that extent, and handles parent cancellation triggered reentrantly by
cache reclamation. The cancellation test requires `LAKEQL_ABORTED`, disposed parent
state and zero final charge. A producer/consumer test keeps a nested shared payload
charged after producer cleanup: 48-byte peak, 40 retained consumer/root bytes after
producer release, and zero after output release. All 11 focused nested-dictionary
tests pass, including the strengthened cancellation assertion.

The complete default coverage run passes 1,574 tests (361 skipped) across 153
passing files: 91.08% statements, 84.87% branches, 95.18% functions and 93.07% lines.
Dedicated typechecking passes; full lint passes across 501 files. The coverage run
preceded only the stronger cancellation assertion, which passed in the focused
rerun. Evidence/hashes: `docs/validation/nested-lifetime-checkpoint-2026-11-12.json`.

External conformance remains red: 167 failed, 83 passed, one skipped. These counts
match the prior reported counts, but that alone does not prove identical failure
sets. Inspection of the current harness shows it expects every `.parquet` file to
decode, including `bad_data` corruption fixtures. The local upstream
`fixtures/external/parquet-testing/bad_data/README.md` documents negative dictionary
counts, malformed levels, inconsistent column sizes and malformed variant bounds;
its zero-bit-width regression is not interchangeable with those malformed cases.
Conformance acceptance needs explicit per-fixture expected behavior and validation
of failure reasons, not a blanket skip or accepting any exception. Valid-format
support gaps, including the public struct guard/codecs, remain distinct. No
conformance expectations were weakened or failures counted as passing here.

### Per-fixture conformance failure attribution (November 12, 2026)

Rebuilt the packages and reran all 167 failing public-reader fixtures with
`bench/conformance-diagnostics.mjs`. The persisted report is
`docs/validation/conformance-causes-2026-11-12.json`; it includes the input test
report SHA-256, each fixture SHA-256 and the complete error-message cause chain.
All 167 failures were classified, with no unmatched test titles and no fixtures
decoding successfully on this rerun. This is local memory-store evidence, not
cloud or Worker validation, and it does not rerun the passing fixture set.

The immediate rejection categories are 144 struct schema guards, 11 missing
compression codecs (five GZIP, three LZ4, two LZ4_RAW, one BROTLI), one decimal
precision guard, and 11 other schema/encoding/data errors. These are first
observable blockers, not exhaustive feature diagnoses: removing a guard may
expose another problem. Unsupported features on malformed files do not establish
correct corruption detection.

In particular, `data/rle_boolean_encoding.parquet` fails on missing GZIP support
in both `readParquetObjects` and `createParquetLake(...).path(...).toArray()`.
The `Invalid typed array length: 28` diagnostic belongs to
`data/nation.dict-malformed.parquet`, not the RLE-boolean fixture. No RLE decoder
change is justified by that error. The fixture's RLE behavior remains unverified
until its compression can be decoded under the required memory contract.

Another valid-format gap is `data/flba12_timestamp.parquet`: the parser attempts
`BigInt(Uint8Array)` for FIXED_LEN_BYTE_ARRAY timestamps. The fixture's own
`flba12_timestamp.md` specifies signed 12-byte values, including nanoseconds
outside INT64. Fixing only the public parser would be insufficient: typed vector
representation, comparison, normalization and allocation accounting must preserve
those values end to end without BigInt64Array truncation. This remains open;
no fixture expectations were weakened and no failures were reclassified as passes.

### Atomic concat planning for distinct scalar dictionaries (November 12, 2026)

The concat planner now measures compatible distinct fixed-width dictionaries,
including multi-level dictionary nulls and timestamp metadata. The typed kernel
and planner share `scalarDictionaryConcatPlan`, so their physical-shape and
validity decisions cannot silently diverge. The extent includes the output
backing and exactly one complete validity mask when required. This extends
whole-concat admission across these columns and recursively planned struct/list/map
children; it does not complete planning for distinct nested or packed dictionary
unions, mixed/null normalization, or heterogeneous fallback shapes.

Tests cover all nine concrete fixed-width shapes, nested dictionary selections,
inner nulls, an existing outer mask, exact caps and cap-minus-one failures before
even an earlier output column allocates. A shared-pool test charges 16 input bytes
and a sibling job's 12 bytes: construction requires 36 total bytes, rejects at 35
without increasing the existing 28-byte peak, and retains only eight output bytes
after input release. All charges clear after final release.

Non-nullable chains require no selected-row scan during planning. A proxy test
throws if the planner reads any numeric index, while nullable chains still inspect
selected validity as needed. The 36-scenario planning benchmark covers fixed,
shared-dictionary and distinct-scalar shapes over 4/32 columns, 16/256/4096 batches
and 1/128 rows per batch. At 32 columns and 4096 distinct-dictionary batches the
five-sample medians are 4.57 ms and 3.64 ms for 1 and 128 rows respectively. These
are planning-only measurements with borrowed inputs, not query latency or heap
limits. Evidence and planner/kernel hashes:
`docs/benchmarks/scalar-dictionary-concat-planning-2026-11-12.jsonl`.

Validation: full build and typecheck pass; changed-file lint passes; 755 core tests,
102 DuckDB reference tests and 24 local workerd tests pass. The final test-only
no-row-inspection assertion subsequently passes in the 33-test planner suite.
All six local nested-fixture variants still match DuckDB at exact measured caps
and cap-minus-one rejection; decoded inputs remain borrowed in this probe.
Evidence: `docs/validation/scalar-plan-nested-fixtures-2026-11-12.jsonl`.
The separate public R2 HTTP range suite passes all 52 checks, including concurrent
jobs and pinned cache headroom:
`docs/benchmarks/cloud-range-scalar-plan-2026-11-12.json`.
Shared asynchronous admission scheduling and whole-Worker heap coverage remain
open; these tests do not establish either contract.

### Whole-stage scheduler primitive and integration boundary (November 12, 2026)

Added internal `MemoryStageScheduler` for independent jobs sharing a root pool.
Each stage declares its complete new allocation extent; existing input owners
are pinned separately and stay charged through execution. Admission precedes
the callback, so transport/semaphore acquisition belongs inside that callback.
Compatible stages run concurrently according to available bytes. A FIFO queue
waits only while this scheduler has running stages; after they settle, requests
that cannot coexist with consumer-retained output reject deterministically.
There is no allocator-wide promise wait and no assumption that the consumer will
release an earlier result while asking for another one.

The scheduler bounds outstanding stages (running plus queued) to 64 by default,
with an explicit constructor limit. It permits one outstanding stage per job
budget. Stage callbacks must perform independent complete operations and must
not await another scheduled stage; nested operators consume existing scope
admission. Same-budget recursive submission rejects. This does not infer arbitrary
dependencies between callbacks submitted under different budgets. Input guards
remain charged until execution settles even after cancellation. Scope input
handles participate conservatively in output ownership unless a kernel proves
its output detached and releases those handles; the separate execution guards
still protect in-flight work. Queue cancellation removes listeners and pins;
active cancellation signals transport but does not prematurely free live buffers.

Twelve focused tests cover shared-parent contention, concurrent fitting stages,
retained outputs, cancellation while queued and in flight, completed scratch/input
lifetimes after abort, cache-reclamation reentrancy, queue capacity, recursive
submission, invalid extents/pools, execution failure and exact planned concat.
The concat integration uses 16 charged input bytes plus eight admitted output
bytes, peaks at 24, and retains only eight after detached input release.

`bench/scheduled-range-validation.mjs` separately exercises actual public R2 HTTP
206 reads through this primitive. Four 64-byte reads run with peak concurrency
one/two at pool capacities 64/128 **bytes**, respectively. Engine buffer charges
peak exactly at those capacities and finish at zero; all read payload hashes
match. This tiny exact-boundary probe is not a 64/128 MiB Worker heap test, a
decoder test, or automatic scan scheduling. Evidence:
`docs/validation/scheduled-http-ranges-2026-11-12.json`.

Automatic query integration remains open. A reproduced obstacle is
`withObjectStoreReadControls(...).getRangeOwned`: it constructs an independent
scope rather than consuming the caller's pre-admitted extent. With a four-byte
stage and pool, a direct four-byte memory-store read succeeds; the same read
wrapped with `maxConcurrentReads: 1` fails `LAKEQL_BUDGET_EXCEEDED` because it
tries to reserve again. Both clean up to zero. This needs a durable child-admission
contract before scheduling controlled reads. Complete aligned-column decode
working sets, mismatched page boundaries, shape-specific splitting, and remaining
operator/codec plans also remain prerequisites; the primitive is not exported
from the public index or substituted into incomplete reader paths.

Final validation for this primitive: 768 core tests pass across 86 files, the
TypeScript project build and dedicated typecheck pass, changed-file lint passes,
and the real HTTP probe passes again against the final build. Existing full scan,
workerd and conformance results above are earlier checkpoints; they are not
claimed as fresh scheduler integration evidence.

### Shared child admission through controlled reads (November 12, 2026)

Resolved the preceding controlled-read double-reservation reproduction.
`MemoryScope.childScope(signals)` now isolates ownership and cancellation while
drawing allocations from the parent's remaining extent. Nested children and
siblings consume the same admission; child disposal releases ownership without
replenishing consumed stage capacity or disposing the parent. Explicit child
stages can still partition capacity through `childStage`. A borrowing child cannot
install a fresh admission that bypasses its parent. Reentrant cancellation during
cache reclamation is checked again before invoking the allocator, and transferred
reservations are released if that check fails.

`withObjectStoreReadControls` uses this child contract rather than constructing an
independent allocation scope. Controlled range reads therefore consume previously
admitted bytes once, while ordinary callers without stage admission retain their
existing shared-budget behavior. Tests exercise nested/sibling consumption,
non-replenishment, child-only cancellation during reclamation, parent cancellation
with pending I/O, and two simultaneous four-byte reads at an exact eight-byte cap
with either one or two transport slots. Allocations after extent exhaustion reject
even if scratch release has freed physical budget capacity.

The public R2 probe now passes through the actual read-control wrapper. Four
64-byte HTTP 206 reads retain exact 64/128-byte peaks with one/two concurrent reads,
matching payload hashes and zero final charges. These are small engine-buffer
boundary checks, not whole-Worker heap limits. Evidence:
`docs/validation/scheduled-controlled-http-ranges-2026-11-12.json`.

Validation: full build and dedicated typecheck pass; changed-file lint passes;
797 core/HTTP tests pass; the full default suite passes 1,604 tests with 361 skipped
across 155 passing files; all 24 local workerd tests pass. The controlled-read
admission obstacle is resolved. Automatic scan scheduling still needs complete
decode/retained-input/output plans and an audit of other independent scopes in
range-cache and codec paths; this change does not assume those scopes already
consume stage admission or mark the overarching scheduling requirement complete.

### Decoder page and codec admission propagation (November 12, 2026)

ZSTD frame workspace now borrows its decoding scope's admission instead of making
independent reservations. Iteration-lifetime page scopes do the same in both the
full-chunk and windowed readers. `ParquetReadScope` accepts either a standalone
budget or a parent scope; windowed page transport supplies its parent so owned
header/body reads consume the declared extent. Ownership and cancellation
boundaries remain unchanged, and independently retained outputs survive disposal.

ZSTD tests exercise one, two and 100 concatenated frames with an admitted extent
covering output plus every new frame allocation, exact success and one-byte-short
rejection, output retention after scope disposal, and final zero charges. The
extent is total new allocation, not live peak: releasing one frame's scratch does
not replenish admission for the next. Reusable codec scratch and a complete
codec working-set planner remain separate opportunities; no memory reduction is
claimed from this ownership correction.

New real Parquet tests generate 100 nullable INT32 values with SNAPPY compression,
small pages and seven-row output batches. Both full-chunk and early-stop readers
match every expected row, replay successfully at the measured total allocation
extent, reject at extent minus one and clean up fully. These use a memory object
store; metadata and caller fixture storage are outside this decode-only probe.
They establish admission propagation, not a statically complete decoder planner
or a whole-query heap bound.

The shared in-flight range-cache stage intentionally remains independent of an
individual consumer's cancellation. Replacing that stage with `childScope` would
let the first departing consumer cancel another job's shared transport. Its next
integration step needs explicit transferable admission plus shared-owner lifetime
handling; ordinary child cancellation is not sufficient. Cache-vector handoff
scopes that only retain existing owners do not allocate new backing themselves.

Validation: full build, dedicated typecheck and changed-file lint pass; all 1,609
default tests pass (361 skipped), all 102 DuckDB reference tests pass, and all 24
local workerd tests pass. Public cloud rerun evidence is recorded separately below.

The final fixture assertion additionally wraps both reader modes in an owned
`ParquetReadScope`, so full-chunk transport as well as windowed header/body reads
consume parent admission. All 52 vector-batch tests pass after that test-only
strengthening. The public R2 range suite also passes all 52 checks; maximum
incremental charged bytes under pinned-cache scenarios remains 54,659 within the
131,072-byte headroom. Evidence:
`docs/benchmarks/cloud-range-page-admission-2026-11-12.json`.

### Shared range transport admission with independent lifetime (November 12, 2026)

Resolved the shared range-cache admission obstacle described above. A stage's
remaining reservation now lives in one reference-counted admission ledger.
`MemoryScope.detachedScope()` can share that ledger without inheriting the
initiating scope's cancellation. Every holder consumes the same remaining extent;
unused capacity remains charged until its last holder closes. Allocated buffers
retain their own existing ownership handles. Ordinary `childScope` still closes
with its parent, and fixed `childStage` still partitions a separate extent.
`admitReserved` explicitly consumes exclusive reservation ownership and preserves
caller ownership if validation fails.

The range cache uses a detached scope for shared in-flight transport, and forecasts
read-ahead from `availableAllocationBytes`, including previously admitted capacity.
This avoids both duplicate reservations and cancellation inherited from the first
consumer. It deliberately does not assume transport allocation equals range
length: adapters may allocate scratch, copy a partial backing buffer, or wrap
another cache. A regression starts delayed two-byte adapter scratch plus a
four-byte source range after the first consumer cancels, while the second consumer
still receives its two-byte slice. The exact shared peak is ten bytes and final
charges are zero. General working-set planning must still account for such adapter
capabilities; the ledger enforces a declared total rather than inventing that plan.

Tests cover exclusive transfer validation, detached lifetime, late allocation,
non-replenishment, coalesced read deduplication, warm-cache reuse, exact demand when
read-ahead cannot fit, extent exhaustion before the source allocator runs, and
first/all-consumer cancellation across separate scanner wrappers. A surviving
consumer preserves transport; the last consumer's departure still cancels it.

The new real public HTTP probe combines the internal scheduler, controlled owned
reads and the shared range cache. One 4,096-byte HTTP 206 data response serves two
concurrent 16-byte copies and a later 16-byte cache hit under an exact 4,144-byte
stage extent. After stage cleanup, 4,096 cached bytes remain; explicit cache
eviction brings final charges to zero. HTTP size discovery also performs one
two-byte range request before stage measurement; it is recorded separately rather
than omitted from the evidence. Artifact:
`docs/validation/scheduled-cache-http-ranges-2026-11-12.json`.

Validation: full build, dedicated typecheck and changed-file lint pass; 1,618
default tests pass (361 skipped); all 102 DuckDB reference tests and 24 local
workerd tests pass. The broader public cloud suite passes all 52 checks with a
maximum incremental charged peak of 54,659 bytes under 131,072-byte pinned-cache
headroom. Artifact: `docs/benchmarks/cloud-range-shared-admission-2026-11-12.json`.
Automatic query scheduling, complete decoder/operator working-set planning and
whole-Worker heap coverage remain open. These buffer-boundary probes establish
neither a complete 64/128 MiB heap guarantee nor an end-to-end latency improvement.

### Direct serialized-size accounting without batch JSON copies (November 12, 2026)

Inspection found another allocation-heavy budgeting path: every columnar top-k
merge materialized its entire retained batch, cloned it into JSON-safe/stably
ordered object trees, serialized that tree and allocated a UTF8 buffer merely to
measure its size. General operator-state accounting also repeated serialization.
`serializedValueBytes` now directly counts the existing serialized metric, and
`serializedBatchBytes` counts rows from columns without creating scalar row objects
or whole-batch JSON strings/buffers. Binary contributes its hex-encoded length
without constructing hex text. Object-key ordering does not affect byte length.

The counter preserves existing budget semantics: non-finite numeric strings,
safe/unsafe bigint conversion, timestamps, intervals, binary, omitted undefined
object fields, explicit undefined array values versus holes, and the existing
stable serializer's legacy `__proto__` omission. It does not change serialized
output or fingerprint behavior. Nested vector normalization still uses
`vectorValue` one cell at a time; complete nested heap accounting is not claimed.
This serialized-size metric remains distinct from allocated-memory accounting.

Differential tests compare the prior serialization pipeline against primitive and
structured edge cases, 500 nested combinations, dictionary batches, Unicode and
surrogate cases, empty batches and large binary input. A columnar top-k query
passes at the exact previous serialized limit and rejects one byte below it.
Two new real SNAPPY fixture comparisons (PLAIN and RLE_DICTIONARY text) run with
seven- and 128-row batches, compare top-k output to DuckDB, test both budget
boundaries and require zero final allocation charges. Those fixture tests use a
memory object store and local DuckDB, not cloud transport.

Local five-sample median accounting costs at 1,000/10,000/50,000 rows change from
1.98/14.79/72.88 ms to 0.112/0.864/3.15 ms. Every timed result matches the previous
byte count; the largest result is 3,688,891 serialized bytes. This is an accounting
microbenchmark, not an end-to-end query speedup or total heap measurement.
Artifact: `docs/benchmarks/serialized-size-2026-11-12.jsonl`.

Validation: full build and dedicated typecheck pass; changed-file lint passes;
1,623 default tests pass (361 skipped), all 104 DuckDB reference tests pass, and
all 24 local workerd tests pass. Automatic shared scheduling, complete decoder
working-set plans and whole-Worker heap coverage remain outstanding.

The public R2 rerun passes all 52 range/query checks after the accounting change;
maximum incremental charged backing remains 54,659 bytes under the 131,072-byte
pinned-cache headroom. Evidence:
`docs/benchmarks/cloud-range-serialized-size-2026-11-12.json`.

### Direct nested-vector serialized sizing (November 12, 2026)

The size counter now traverses list, struct and dictionary vectors directly,
avoiding decoded nested payload arrays/objects during ordinary budget checks.
Scalar leaves use their physical backing without a second vector dispatch.
Map sizing preserves null-key omission, stringified keys and last-key-wins
semantics by retaining only each distinct key's final entry index, then measuring
the surviving values. It does not serialize overwritten values: a regression
covers an overwritten timestamp outside JavaScript Date's representable range
followed by a valid timestamp under the same key. Empty/null collections do not
inspect unused child payload, and selected invalid dictionary indices still reject.

Struct/map field combinations that the existing JSON-safe conversion can recognize
as timestamp or interval records retain that normalization path. This is a logical
representation boundary, not a query-specific branch. Map key-to-index metadata
remains O(distinct keys) and outside the typed allocation ledger; semantic-record
fallbacks can still materialize a cell. No complete nested heap bound is claimed.

The initial visitor counted overwritten entries and showed a small-map CPU
regression. That measurement is retained in
`docs/benchmarks/nested-serialized-size-initial-2026-11-12.jsonl`. The final
five-sample benchmark at 20,000 rows/320,000 entries changes list accounting from
5.32 to 4.18 ms and duplicate-key map accounting from 12.45 to 10.44 ms relative to
the preceding cell-materialization counter. Every result has an identical byte
count. Final artifact: `docs/benchmarks/nested-serialized-size-2026-11-12.jsonl`.
These timings measure local accounting only, not remote scans or end-to-end queries.

The existing real nested Parquet probe now verifies direct size counts against the
previous serializer for decoded inputs and concat outputs. All six direct/wrapped
fixture variants still match DuckDB, including exact concat caps and cap-minus-one
rejection. Evidence: `docs/validation/nested-serialized-size-fixtures-2026-11-12.jsonl`.
The public struct guard is unchanged; these are internal operator/local fixture
checks with borrowed decoder inputs, not public struct-format support.

Validation: 1,627 default tests pass (363 skipped), all 104 DuckDB reference tests
and 24 local workerd tests pass, along with TypeScript project build, dedicated
typecheck and changed-file lint. The preceding cloud checkpoint remains separate;
no fresh cloud performance claim is made for this nested accounting change.

### Bounded map sizing workspace: validation and open CPU regression (November 12, 2026)

The in-progress map size counter replaces its uncharged native Map with a reusable
Uint32 key-index workspace. Key payload remains in source vectors. Recursively
nested maps hold separate workspaces, and successive rows reuse their storage.
The top-k size check supplies its query allocation budget. The workspace chooses
an index-only heap sort when hash capacity cannot fit; hash chains exceeding eight
probes also switch to heap sort, preventing adversarial quadratic probe growth.
Replacement buffers are charged while old buffers are still live. This covers
index backing, not transient normalized keys, closures or semantic-record fallback
objects, so it is not a whole-heap guarantee.

Targeted coverage now includes exact sorted capacity and one byte below it,
last-key-wins/null/prototype keys, nested visitors, exception cleanup, repeated
rows under one admitted extent, empty/malformed extents, and deliberately colliding
hashes with a key-inspection bound. The initial TypeScript project build and the
first 14 targeted tests passed; additional extent tests are recorded by the next
validation checkpoint.

**Open performance regression:** the new 20,000-row duplicate-key map benchmark
measures 24.10 ms with charged workspace versus 12.03 ms for cell materialization.
List accounting remains faster (4.16 versus 5.11 ms). Every measured byte count
matches, and each charged run returns its allocation ledger to zero under a
128-byte workspace cap. Artifact:
`docs/benchmarks/bounded-map-size-initial-2026-11-12.jsonl`.
This is a local accounting microbenchmark, not cloud transport. The bounded map
implementation is still under development; it must not be described as a CPU
performance win. Repeated source-key normalization/lookup and the per-cell hash
setup are the next profiling targets. Broad suites and real fixture reruns remain
to be completed for this workspace revision.

The follow-up removes repeated generic vector dispatch for string/dictionary map
keys, preserving each dictionary level's bounds/null checks. Hash slots use a
power-of-two mask and indexed iteration, and reuse the original buffer without a
new typed-array view for every row. The added shrinking-workspace regression
ensures stale slots from a larger previous row cannot leak into the next row.

Repeated five-sample measurements now show 20,000-row charged map accounting at
10.59 ms versus 12.00 ms for cell materialization; the initial approximately 2x
regression is resolved for this workload. At 2,000 rows the map path remains
slightly slower (1.31 versus 1.20 ms), so no universal speedup is claimed. Charged
list accounting is 4.27 versus 5.42 ms at 20,000 rows. Artifact:
`docs/benchmarks/bounded-map-size-2026-11-12.jsonl`.
All 17 targeted tests pass. The six real nested Parquet fixture variants pass
again, including DuckDB comparisons and serialized-byte equality:
`docs/validation/bounded-map-size-fixtures-2026-11-12.jsonl`. These remain internal
local-file/operator probes; they do not prove public nested schema support or
remote transport performance. Broader validation is in progress.

Completed workspace validation checkpoint: 1,635 default tests pass (363 skipped),
TypeScript project build and dedicated typecheck pass, and changed-file lint
passes. Reference/workerd/cloud suites have not yet been rerun for this revision;
their prior results are separate checkpoints. Broader working-set planning and
whole-Worker heap coverage remain open.

Fresh runtime checks for the bounded workspace revision: all 104 DuckDB reference
and 24 local workerd tests pass. An additional targeted test verifies two-level
dictionary key masks: a masked invalid selected index is ignored, but exposing
that same index throws. All ten serialized-size tests pass after this addition,
and the TypeScript project build remains green. The previous full-suite count
predates this one added test.

Next implementation boundary confirmed in current source: `concatAllocationBytes`
plans differing dictionaries only through `scalarDictionaryConcatPlan`; packed
and nested dictionary inputs still return an unknown extent. Their execution
paths already allocate selected expansion or adaptive union working sets in
`packed-gather.ts` and `nested-dictionary.ts`. A shared physical plan must expose
those complete extents (including outer validity, expansion scratch and union
scratch) before automatic scheduling can safely cover them. Merely scheduling
the existing partial output estimate would violate admission. This remains an
open requirement rather than a completed scheduler integration.

The fresh public R2 range/query suite also passes all 52 checks. Maximum
incremental charged backing remains 54,659 bytes under 131,072-byte pinned-cache
headroom. Artifact: `docs/benchmarks/cloud-range-bounded-map-2026-11-12.json`.
This verifies actual remote HTTP range behavior separately from the local nested
fixtures; it is not a whole-Worker heap or end-to-end latency guarantee.

### Shared packed expansion plan (November 12, 2026)

Extracted `packedVectorConcatPlan` from the packed construction kernel. It computes
selected payload bytes, range backing, and complete null-mask requirements across
dictionary levels without constructing output. `totalBytes` includes outer
validity; `allocationBytes` excludes a supplied outer mask already owned by the
caller. Expansion execution and adaptive dictionary union costing now consume
this same plan, removing the duplicated selected-payload sizing implementation.
Lossy UTF8 conversion (unpaired surrogate strings) and incompatible text/binary
shapes still report unsupported rather than changing values.

The first 48 planner/sort tests pass, including new exact-admission tests with
nested dictionary nulls, repeated Unicode keys, unused payload, borrowed versus
new validity, cap-minus-one rejection before allocation, empty input and mixed
physical kinds. TypeScript project build passes. This is a prerequisite, not
complete whole-stage admission: adaptive union construction scratch and nested
dictionary expansion still need shared plans and integration into
`concatAllocationBytes`. No new cloud performance claim applies to this refactor.
The additional 30 packed admission, packed/adaptive dictionary concat, direct
packed concat and nested dictionary tests also pass (78 targeted tests total).

### Complete packed dictionary concat admission (November 12, 2026)

`adaptivePackedDictionaryPlan` now exposes the chosen expansion or union working
set, including union index-construction scratch and caller-owned outer validity.
The executor and `concatAllocationBytes` share this decision. Distinct compatible
packed/text/binary dictionaries can therefore participate in atomic whole-concat
admission rather than falling back to incremental output allocation. The default
still selects union only when its complete construction extent is smaller than
selected expansion; large unused dictionaries retain the selected expansion path.

A new whole-concat boundary test includes a preceding output column and verifies
both supplied and generated outer validity. Its 1,254-byte extent succeeds,
1,253 bytes rejects before any output allocation, and the 16-byte construction
scratch is released so retained backing is 1,238 bytes. Borrowed inputs in this
probe are explicitly uncharged; it is an output/construction boundary test, not a
complete input heap measurement. All 44 focused concat/adaptive/packed tests pass.
The six real nested fixture variants also pass against DuckDB and serialized-byte
checks: `docs/validation/packed-plan-nested-fixtures-2026-11-12.jsonl`.
These are local fixture/operator checks. Nested dictionary expansion planning,
whole-Worker heap accounting and automatic query scheduling remain outstanding.
Completed validation checkpoint: 1,639 default tests pass (363 skipped),
TypeScript build, dedicated typecheck and changed-file lint pass. Prior
reference/workerd/cloud results predate this planner integration.

### Packed representation capacity boundary (November 12, 2026)

Review found that extracting expansion planning had moved the Uint32 payload
limit ahead of adaptive union selection. That could reject highly repeated data
whose expanded bytes exceed 4 GiB even though its compact dictionary fits. Cost
planning now retains safe-integer expansion sizes for comparison; execution
checks the chosen representation's actual payload capacity. Union payload itself
must still fit Uint32 offsets, and direct expansion rejects before reserving any
backing when its offsets cannot represent the result.

A regression uses two real 1 MiB binary buffers, each selected 4,096 times. The
expanded payload is 8 GiB; whole-concat planning instead selects a 2,129,944-byte
union construction extent. Execution succeeds at that exact extent, preserves
both byte patterns and dictionary selections, releases scratch and cleans up to
zero. Direct expansion rejects with zero peak allocation. This exercises local
physical vectors, not cloud reads or a whole-Worker heap cap. All 42 focused
planner/adaptive tests pass; broader prior checkpoints predate this correction.

### Nested dictionary selection metadata plan (November 12, 2026)

`nestedDictionaryExpansionPlan` now exposes the exact selection-index, offset and
validity backing required by struct/list/map expansion. Construction consumes the
same plan. Planning traverses selected row metadata without allocating selection
arrays or reading child payload values, and retains selected-index/offset checks
at every dictionary level. Safe-integer extent validation is explicit.

New tests cover each nested shape through two dictionary levels with null masks,
exact admission and cap-minus-one rejection before allocation, value equality,
cleanup, and unused malformed rows/payload getters. The four-row struct requires
20 bytes; list/map selections require 44 bytes. These extents describe temporary
selection metadata only. Complete nested concat still needs a selection-aware
recursive plan for child output, without materializing the expanded index arrays
during planning. It remains outside whole-concat admission until that plan exists.
Validation: all 15 nested planner/operator tests and TypeScript project build pass;
changed-file lint passes. This checkpoint introduces no cloud transport change.

### Packed admission planning cost measurement (November 12, 2026)

Added `bench/packed-concat-planning.mjs`, comparing planning-only, incremental
construction and atomic owned concat for 1,000/10,000/100,000 rows and dictionary
cardinalities 4/1,024. Every execution runs at its exact planned output/scratch
cap, checks values at batch boundaries, and returns tracked allocation to zero.
Inputs are borrowed; this is a local physical-operator benchmark, not remote I/O
or total heap measurement. The artifact includes the compiled packed kernel hash:
`docs/benchmarks/packed-concat-planning-2026-11-12.jsonl`.

At 100,000 rows, planning-only costs 1.58/1.95 ms, incremental construction
3.48/4.06 ms and atomic construction 4.11/5.21 ms for low/high cardinality. Atomic
construction therefore adds approximately 18%/28% in these five-sample medians.
Smaller cases are noisy (including one reversed ordering); no statistical latency
claim is made. Planned/observed charged peaks agree exactly at 400,152/442,836
bytes for the largest cases.

Open improvement: whole-concat admission currently returns only a byte count;
the executor recomputes the adaptive physical decision and selected payload scan.
A reusable physical plan must carry that choice through construction, including
nested children and scratch lifetimes. Reusing the plan should remove this extra
scan while preserving atomic admission. A global memoization cache would retain
input identities/payload and undermine lifecycle bounds; plan ownership belongs
to the individual operation. This measured cost is recorded as unfinished work.

### Operation-local concat plan reuse (November 12, 2026)

`planConcat` now returns the admitted extent plus a per-column physical plan tree.
Owned concat passes it synchronously into construction; scalar and packed
selection plans and nested struct/collection child plans are reused. The byte-only
API delegates to this planner for callers that need only a forecast. No global
cache retains input identities. Plans require immutable inputs for the duration
of the synchronous operation, matching the existing vector execution contract.
Unsupported normalization/nested-dictionary shapes continue without a complete
plan and remain outstanding admission work.

A mechanism test counts source key reads for 2,000 selected rows, both directly
and inside structs. Planning inspects the selected keys; execution inspects only
the two union entries (at most ten key accesses), proving the selected-key sizing
pass is not repeated. All 36 concat-plan tests pass, and the six real nested
fixture variants pass: `docs/validation/concat-plan-reuse-fixtures-2026-11-12.jsonl`.

Repeated local five-sample measurements at 100,000 rows show atomic concat at
2.55/3.27 ms for cardinalities 4/1,024, versus the preceding 4.11/5.21 ms checkpoint.
Exact planned/observed charged peaks remain 400,152/442,836 bytes, with zero final
charges. Artifact: `docs/benchmarks/packed-concat-plan-reuse-2026-11-12.jsonl`.
These are separate local runs and not statistical end-to-end or remote latency
claims. The source-access test establishes the removed work independently of
CPU timing. Plan objects/maps themselves remain JS bookkeeping outside the typed
backing ledger; whole-Worker heap coverage is still incomplete.
Completed validation: 1,646 default tests pass (363 skipped), TypeScript project
build, dedicated typecheck and changed-file lint pass. Reference/workerd/cloud
suites have not yet been rerun for this plan-reuse revision.

### Direct packed plan reuse (November 12, 2026)

Direct binary/text packed columns now retain their `packedVectorConcatPlan` in the
operation plan tree and pass it into construction, removing their second payload
sizing traversal too. Their former separate byte-count loop is removed, so direct
and dictionary expansion share validity/payload costing. Uint32 capacity is still
checked before whole-concat admission.

A 2,000-row fixed-width packed source records exactly 2,000 range-length accesses
in planning and 4,000 during copying (start plus length), without the extra 2,000
sizing accesses. The plan admits exactly 18,000 bytes and cleanup returns to zero.
All 37 concat-plan tests and the TypeScript project build pass. The immediately
preceding plan-tree revision passed 104 reference and 24 workerd tests; those runs
finished before this direct-packed extension and are recorded separately.
The latest full default suite passes 1,647 tests (363 skipped); changed-file lint
also passes. Nested dictionary child-output planning and whole-Worker heap bounds
remain open, and no fresh cloud timing claim is made for this revision.

### Replayable vector selections for nested planning (November 12, 2026)

Added an internal `VectorSelection` contract: immutable leaf backing, selected
length, and replayable index traversal with an explicit null sentinel. Identity
and dictionary selections, struct-field projections, and list/map child ranges
compose without retaining selected-index arrays. Descriptor storage follows the
physical nesting; collection traversal follows selected ranges, including
repetitions, rather than materializing child payload. Selected index/offset checks
remain explicit; parent null rows do not inspect invalid child references.

Nested expansion now shares this traversal in planning and construction instead
of maintaining a separate dictionary-index resolver. Tests cover repeated ranges,
unused malformed rows, payload getters that must not run during planning,
parent/child masks, null-typed children, missing struct fields and map bounds.
All 18 selection/nested tests and TypeScript project build pass. Six real local
nested fixture variants also pass:
`docs/validation/vector-selection-fixtures-2026-11-12.jsonl`.

This supplies selection-aware traversal for the recursive child-output planner;
that planner and atomic nested-dictionary concat admission remain unfinished.
Descriptors/callbacks and transient range pairs are JS bookkeeping outside the
typed ledger. This does not establish whole-Worker heap bounds or cloud behavior.

### Recursive detached selected output (November 12, 2026)

`planSelectedConcat` now plans and constructs complete detached output for
compatible fixed-width scalar leaves recursively inside structs, lists and maps.
It consumes replayable selections, propagates parent/child nulls, validates field
sets and timestamp semantics, and checks aggregate offset/byte capacity. The
plan contains output extents and child execution plans; it allocates no temporary
selection-index arrays. `planConcat` uses this capability for differing nested
dictionaries when scalar/packed dictionary plans do not apply. Execution bypasses
the old expansion/recursive-concat scratch path for these supported shapes.

Four new whole-operation tests cover structs, lists, numeric-key maps and nested
struct/list output with repeated dictionary selections, masked invalid indices,
child nulls, large bigint values and a null-typed input. Exact planned caps
succeed; cap-minus-one rejects before even a preceding prefix column allocates.
Peak equals retained output backing because this path has no typed scratch.
An existing cross-job ownership test now verifies detached output: source backing
can change after producer cleanup without changing results; retained charges fall
from 40 to 16 bytes while the 32-byte input plus 16-byte output peak remains 48.

All 52 targeted planner/nested tests pass and the TypeScript project build passes.
Six real nested fixture variants also pass against DuckDB:
`docs/validation/selected-concat-fixtures-2026-11-12.jsonl`. Those mixed fixtures
can still use existing fallback paths; they do not prove all nested shapes use
this new capability. Packed/text/binary selected leaves and incompatible shape
normalization remain outside this recursive plan, and automatic scheduling and
whole-Worker heap bounds remain open. No cloud-read performance claim is made.
Full validation checkpoint: 1,654 default tests pass (363 skipped), TypeScript
project build and dedicated typecheck pass. Reference/workerd/cloud checkpoints
above predate this new recursive execution path and are not claimed as fresh.

### Recursive selected packed leaves and measured traversal regression (November 12, 2026)

Recursive selected-output planning now supports text, binary, packed text/binary,
and compatible mixed packed/native leaves. It chooses selected expansion or a
smaller detached dictionary union using complete typed output costs; no typed
selection scratch is required. Raw packed bytes are copied without decoding,
including invalid UTF8 bytes, while native strings with unpaired surrogates
remain unsupported rather than changing values. Selected ranges are validated
against payload backing before admission. Union planning includes its dictionary
mask and output row mask and retains the 4 GiB representation boundary.

Tests cover nested repeated lists, mixed native/packed text, sliced range offsets,
empty values, child nulls, invalid ranges and lossless-string fallback. All 1,657
default tests pass (363 skipped), TypeScript build/typecheck pass, and all six
real nested fixture variants pass:
`docs/validation/selected-packed-fixtures-2026-11-12.jsonl`.

**Open performance regression:** the updated dictionary/list/map benchmark verifies
every output row and zero final charges for all three paths. At 20,000 rows,
existing typed selection takes 5.12 ms with 640,600 peak tracked bytes; recursive
planned selection takes 22.67 ms with 320,584 bytes. At 2,000 rows the same tradeoff
is 0.67 versus 2.58 ms and 64,600 versus 32,584 bytes. The old row-materialization
baseline reports fewer tracked bytes because its JS object payload is excluded;
it is not evidence of a smaller whole heap. Artifact:
`docs/benchmarks/recursive-selected-concat-2026-11-12.jsonl`.

The new path proves full output admission and removes typed scratch, but cannot
be called a CPU improvement. Repeated traversal of composed selections and
per-row range-pair/callback work need profiling and optimization before this
finding is closed. These are local borrowed-input operator measurements, not
cloud performance or whole-Worker heap guarantees.

### Reduced recursive selection traversal overhead (November 12, 2026)

Selection descriptors now compile immutable vector shape, length, masks and
dictionary links once instead of dispatching through `vectorLength` and shape
checks for each selected index. Collection range traversal passes scalar bounds
to visitors instead of allocating a pair per selected row. Bounds/null checks
remain in the compiled resolver and range visitor.

Selections also expose `mayHaveNull`. It conservatively follows dictionary masks
and struct-parent masks, but collection-parent null rows contribute no child rows
and therefore do not make an otherwise non-null child nullable. Recursive scalar
planning skips null-detection traversal when the shape rules rule nulls out.
Tests explicitly cover both propagation rules; all 21 focused tests pass.

The same 20,000-row local nested benchmark drops from 22.67 ms initially to
6.81 ms after resolver/range changes and 5.39 ms with nullability capabilities.
Tracked peak remains 320,584 bytes. The contemporaneous older typed-selection
path is still faster at 2.87 ms, with 640,600 peak tracked bytes. Thus most of the
regression is removed, but the roughly 1.9x CPU tradeoff remains open. Artifacts:
`docs/benchmarks/recursive-selected-traversal-2026-11-12.jsonl` and
`docs/benchmarks/recursive-selected-nullability-2026-11-12.jsonl`.
No whole-heap/cloud/end-to-end conclusion follows from these local kernel timings.
Further work should fuse repeated metadata traversal or select a fully admitted
scratch plan when measured available headroom makes that the better tradeoff.
Validation: 1,657 default tests pass (363 skipped), TypeScript build/typecheck and
six local nested fixture variants pass. Fixture evidence:
`docs/validation/selected-traversal-fixtures-2026-11-12.jsonl`.

### Shared selected collection lengths (November 12, 2026)

Each immutable operation-local selection now computes its child-entry count once.
Recursive offset planning and map key/value projections reuse that scalar count,
instead of independently traversing the same parent ranges. No selected indices
or payloads are cached. A pass-count test proves that planning both map children
uses one length traversal. Changed backing requires a fresh selection descriptor,
as required by the existing immutable-operation contract; malformed-offset tests
now construct that fresh descriptor after deliberately changing their source.

The repeated local benchmark measures 20,000-row planned selection at 4.01 ms
versus 3.21 ms for the older typed-selection path, with tracked peaks unchanged
at 320,584 versus 640,600 bytes. At 2,000 rows the timings are 0.526 versus
0.488 ms. This reduces the measured CPU gap further but leaves an approximately
25% tradeoff at the larger size. Artifact:
`docs/benchmarks/recursive-selected-length-reuse-2026-11-12.jsonl`.
Focused selection/planner tests (11 total), TypeScript project build, changed-file
lint and six real nested fixture variants pass. Fixture artifact:
`docs/validation/selected-length-reuse-fixtures-2026-11-12.jsonl`.
The earlier broad suite/runtime/cloud checkpoints are not fresh evidence for this
last optimization; the overall audit and whole-Worker heap requirements remain open.

### Fresh runtime checkpoint and direct null normalization (November 12, 2026)

The accumulated recursive planner/traversal revision passes 1,658 default tests
(363 skipped), all 104 reference tests, 24 workerd tests and 52 live public R2
checks. Cloud maximum incremental tracked backing is still 54,659 bytes under
131,072-byte pinned-cache headroom:
`docs/benchmarks/cloud-range-recursive-plans-2026-11-12.json`.
The document's opening checkpoint is updated to distinguish these results from
older release/coverage gates and unresolved external conformance failures.

A subsequent extension routes direct null-only batches mixed with compatible
typed batches through selected-output planning. Scalar, text and nested list
normalization can now admit the full result and avoid temporary masks/indices/
offsets created by the former normalization path. New tests cover null batches
first and last, exact caps, cap-minus-one rejection before output allocation and
cleanup. All 43 focused selected/concat-plan tests and TypeScript build pass.
An extra shape-detection pass initially violated the shared-dictionary lookup-count
regression; combining null detection and dictionary compatibility into one pass
restores that bound. This extension follows the broader runtime checkpoint above;
its full runtime rerun remains outstanding.

### Planned concat scheduler integration (November 12, 2026)

Added internal `concatScheduledBatches`, which computes a complete operation-local
physical plan and submits its extent plus owned inputs to `MemoryStageScheduler`.
Execution consumes the admitted scope and prepared plan directly, avoiding double
reservation. Unsupported shapes reject before queueing; pre-aborted work rejects
before planning. The synchronous and scheduled concat paths now share the same
alias-aware input-release helper: detached outputs release input pins while shared
dictionaries retain their backing through output handoff. Scheduler execution
guards still protect source backing until the stage settles.

Five new tests cover exact 16-byte input plus eight-byte output admission, queued
cancellation behind a live stage, two independent jobs fitting a shared 48-byte
pool, aliased dictionary lifetime, and unsupported-shape rejection before
admission. All 17 scheduler/operator tests and the TypeScript project build pass.
These are internal in-memory operator tests, not remote reads or automatic query
scheduling. Query integration still requires complete decoder/stage working-set
plans and independent-stage dependency rules; this helper is not exported from
the public package index.

The first broad rerun exposed six existing tight-cap failures in direct null
normalization: a shared nested dictionary previously fit a 25-byte construction
cap, while detached expansion required more output. The planner now compares
selected output against a complete shared-dictionary normalization extent: null
source index/mask scratch plus final concatenated index/mask backing. It preserves
the cheaper shared representation when all concrete dictionaries share identity.
The existing tests across null ordering and dictionary depth pass again, including
25-byte success, 24-byte rejection and 20-byte retained output. This corrects the
preceding direct-null extension; mandatory detachment is not the contract.
Thirty focused collection/selected/scheduled tests and TypeScript build pass after
this correction. The dedicated typecheck passed for the scheduler integration;
the final broad rerun is recorded below.
Final default-suite rerun: 1,665 tests pass (363 skipped). The earlier failed run
is superseded by this correction and rerun, not treated as an unrelated failure.

### Scheduled real nested fixtures and native leaf capability (November 12, 2026)

The real nested Parquet fixture probe now exercises scheduled concat in addition
to synchronous concat. It runs two jobs concurrently, gives each exactly its
planned output extent, compares every result to DuckDB, checks serialized-byte
equality, rejects cap-minus-one with zero peak output allocation, and requires
zero final charges. Initial evidence found two direct decoded variants without
a complete plan even though dictionary-wrapped variants were supported:
`docs/validation/scheduled-concat-fixtures-2026-11-12.jsonl`.

The gap was direct native text/binary leaves. They now use the existing selected
packed-output plan, including lossless-string checks and measured expansion/union
choice. Unpaired-surrogate native strings still report unsupported rather than
changing values. All six direct/wrapped variants now schedule successfully:
`docs/validation/scheduled-concat-native-fixtures-2026-11-12.jsonl`.
Per-job extents are 3,456, 359 and 2,600 bytes for the three fixtures; concurrent
peaks are exactly twice those values. Direct native output now has tracked packed
backing rather than uncharged native-array payload, so its ledger amount is not
directly comparable to the old partially tracked output.

These probes deliberately bypass the public struct schema guard and use local
Parquet files plus DuckDB. Decoder inputs remain borrowed, not allocation-owned;
the evidence proves scheduled output construction, not decoder working sets,
remote transport or a whole-Worker heap bound. Automatic query scheduling remains
open. The old test expecting native text planning to be unsupported is updated
to its measured nine-byte output; timestamp conversion rejection remains intact.
Validation after the native-leaf extension: 1,665 default tests pass (363 skipped),
TypeScript project build/typecheck and changed-file lint pass.

### Definition-level counting before allocation (November 12, 2026)

The flat numeric page decoder now separates hybrid RLE/bit-packed traversal from
its typed-array sink. Definition levels are validated and counted without a level
buffer. All-present pages allocate no level scratch; nullable pages replay the
encoded levels into their final normalized byte mask, even when the schema's
maximum definition level requires more than eight encoded bits. Dictionary and
boolean value decoding share the same run visitor with their existing typed sink.

Both V1 and V2 all-present tests now pre-admit exactly 16 bytes for four INT32
outputs; the former allocate/release-four-byte level pass would have consumed
part of that admission and failed. Malformed levels reject with zero level
allocation instead of a four-byte peak. Existing exhaustive four-row null-pattern
checks cover RLE and padded bit packing in both page versions, along with dictionary
bounds, boolean/binary paths and decompressed backing lifetime. All 21 page tests
and all 1,665 default tests pass (363 skipped); TypeScript project build passes.

Nullable pages perform a second encoded-level traversal, a CPU tradeoff still
requiring benchmark evidence and fresh real-fixture/runtime/cloud validation.
This removes one decoder scratch requirement; it is not a complete page working-set
plan. Codec workspace, retained dictionaries, converted output, aligned columns
and mismatched page boundaries still need complete stage planning before automatic
scan scheduling can use the shared scheduler safely.

### Definition-level benchmark and single-pass lazy masks (November 12, 2026)

Added `bench/definition-level-counting.mjs` with all-present RLE, all-null RLE and
mixed bit-packed levels at 1,024/65,536 rows. It verifies every decoded value and
mask and zero final charges. The baseline reconstructs the prior buffer/normalize
strategy using the current run parser; it is not an archived full old decoder.
Artifacts include the compiled decoder hash. Borrowed uncompressed V2 input and
alignment mean some numeric values alias input and have no new tracked backing;
these are not total-heap measurements.

The count/replay prototype regressed the 65,536-row mixed case from 0.413 to
1.946 ms. Evidence is retained in
`docs/benchmarks/definition-level-counting-2026-11-12.jsonl`. It is superseded by a
single-pass lazy-mask strategy: retain no mask while runs are present; on the
first null, allocate the final byte mask and fill the preceding present prefix,
then write normalized runs directly. This avoids replay and the full normalization
pass while retaining zero level scratch on all-present input.

The repeat measures mixed input at 0.444 ms versus 0.391 ms for buffer/normalize,
all-null at 0.028 versus 0.096 ms, and all-present at 0.0013 versus 0.129 ms.
The all-present aligned fixture eliminates its 65,536-byte level allocation;
nullable tracked peaks are unchanged. The remaining mixed-case overhead is about
14% in these local five-sample medians, not a statistical whole-query claim.
Artifact: `docs/benchmarks/definition-level-lazy-mask-2026-11-12.jsonl`.
All 21 focused page tests pass, including exhaustive null patterns and exact
all-present admission. Malformed streams discovered after the first null can
have an allocated final mask; caller scope cleanup remains required on failure.

The preceding count/replay revision passed the 104 reference/24 workerd checks
and all 52 live R2 checks, with 54,659 maximum incremental tracked bytes:
`docs/benchmarks/cloud-range-level-counting-2026-11-12.json`. These cloud results
predate the single-pass revision; they are not claimed as its fresh validation.
Fresh single-pass validation: 1,665 default tests pass (363 skipped), all 104
reference and 24 workerd tests pass, and TypeScript build/typecheck and
changed-file lint pass. Complete decoder working-set scheduling remains open.

### Flat fixed-width page allocation capability (November 12, 2026)

Added internal `planFlatNumericPage` for uncompressed flat INT32/INT64/FLOAT/DOUBLE
PLAIN and BYTE_STREAM_SPLIT pages. V2 pages explicitly marked uncompressed can
qualify even when their column codec is compressed. The plan validates/counts
encoded definition levels without output allocation, accounts for a final mask
only when needed, validates numeric payload bounds, and distinguishes aligned
PLAIN input aliases from admitted alignment copies and split transposition output.
It returns the complete new typed extent plus an execution callback and an
explicit input-borrowing flag. The page backing must be pinned separately.

Planning and normal decoding share level-layout validation. Tests cover V1/V2,
all-null/mixed/all-present rows at four byte alignments, exact admission, values,
backing identity, cleanup, codec capability boundaries and split output. The
aligned split case still allocates detached output, unlike aligned PLAIN.
All 24 focused page tests and TypeScript project build pass.

This capability covers the physical page decoder only. Logical conversion,
dictionary ownership, compressed codec workspaces, repeated/nested decoding and
multi-column alignment are separate required plans. It is not yet wired into
normal scan scheduling, and no whole-query memory or cloud-latency claim is made.
Execution replays definition levels after planning; its scheduling/latency tradeoff
needs measurement alongside real fixture integration.
Full default validation: 1,668 tests pass (363 skipped); dedicated typecheck passes.
The final alignment-specific test adjustment also passes in the 24-test page suite.

### Scheduled flat-page real-file operation (November 12, 2026)

Added `bench/scheduled-flat-page-fixtures.mjs`. It writes actual uncompressed
Parquet files, obtains reference rows with DuckDB, reads their encoded page
headers, copies the encoded page into charged backing and schedules two physical
decodes under the exact shared input-plus-two-output cap. Every decoded row is
compared to DuckDB; aligned output keeps its input pin, detached output releases
it after execution, and all final charges return to zero. Where new output is
nonzero, cap-minus-one rejects before any decoded output allocation.

All 20 generated cases pass: PLAIN INT32/FLOAT/DOUBLE and BYTE_STREAM_SPLIT
FLOAT/DOUBLE, nullable/non-nullable data, and two backing alignments. Five cases
alias input; 18 exercise cap-minus-one (two aligned non-null PLAIN cases allocate
no new output). Shared tracked peaks range from 4,004 to 24,004 bytes. The writer
emits V2 and does not support writing INT32 BYTE_STREAM_SPLIT; those boundaries
are reported rather than presenting synthetic pages as writer-generated coverage.
V1 and INT32 split retain separate unit coverage.

Artifact: `docs/validation/scheduled-flat-page-fixtures-2026-11-12.jsonl`.
This is stronger than borrowed-input operator evidence because the encoded page
backing is charged, but it is still local-file physical decoding. Metadata parsing,
fixture generation, logical conversion, decompression workspace and whole-runtime
heap are excluded. It does not demonstrate automatic scan scheduling, remote
range behavior or 64/128 MiB total Worker caps. The probe and lint pass without
production source changes in this checkpoint.

### V2 Snappy physical-page allocation plans (November 12, 2026)

Flat numeric planning now includes built-in Snappy on V2 pages. V2 level bytes
remain uncompressed, allowing mask size and present-value counts to be validated
before admission. The extent includes decompressed data plus the final mask and,
for BYTE_STREAM_SPLIT, detached transposition output. PLAIN values alias the
charged decompressed backing, not the compressed input. Execution installs the
scoped built-in codec; callers supplying a custom Snappy implementation are
explicitly unsupported by this plan rather than silently overridden. V1 compressed
levels and ZSTD codec workspace still require different planning capabilities.

The generated real-file probe now passes 40 local Parquet/DuckDB cases, including
20 Snappy cases. Eight split cases release decompression scratch before output
handoff. The artifact distinguishes total admitted pool capacity from observed
peak, since the first synchronous decode can release scratch before the second
stage reserves its extent. Input backing and output/scratch are charged; exact
single-job cap-minus-one rejection and zero final charges remain verified.
Artifact: `docs/validation/scheduled-snappy-page-fixtures-2026-11-12.jsonl`.

A focused literal-Snappy test also verifies output, exact admission, custom-codec
rejection and cleanup after a truncated compressed stream. All 25 page tests and
TypeScript project build pass. These remain local physical decode capabilities;
logical conversions, aligned-column working sets and automatic cloud scan
scheduling are not claimed complete.
Full default validation passes 1,669 tests (363 skipped); dedicated typecheck and
changed-file lint pass. Fresh remote/runtime validation remains separate work.

### ZSTD Huffman allocation validation (November 12, 2026)

Workspace review found that individually valid Huffman weights could sum to an
invalid table depth. The decoder previously derived and allocated the table
before later rank validation rejected it. It now rejects depths above the
decoder's 11-bit capability before allocating the table backing. This establishes
a 4,096-byte maximum for this individual allocation, including malformed input;
it does not establish a complete frame or stage working-set bound.

A crafted single-block frame with 128 weights of 11 records allocator calls and
requires rejection after only the 12-byte frame state and 292-byte weight
workspace. Native-compressor parity, concatenated frames, truncation checks and
existing multi-block workspace lifetime tests remain passing: 8 tests across
three focused suites. TypeScript project build passes. This is decoder allocation
validation, not remote-read or whole-Worker heap evidence.

Complete ZSTD admission remains open: replacement entropy tables allocate across
blocks, and releasing old backing does not replenish total-new stage admission.
A plan must account for that distinction or introduce safely reusable workspace;
a live-peak estimate alone is insufficient. The normal scan scheduling and
64/128 MiB runtime validation requirements remain unchanged.

### Reusable ZSTD workspace under stage admission (November 12, 2026)

Scoped ZSTD now detects inherited stage admission and retains dead workspace
backings for exact-size reuse. Live entropy tables remain unavailable for reuse;
each reused buffer is zeroed to preserve fresh-allocation semantics. Frame
completion makes all workspace reusable, and decode completion or failure disposes
the pool. Without admission, the existing eager release behavior remains intact,
avoiding increased live retention for incremental-budget decodes. This choice is
automatic and requires no caller flag.

Eight concatenated native-compressed frames now decode with output capacity plus
one frame's measured total workspace allocation extent. Previously each frame
consumed new admission despite releasing its tables. Tests also cover exhausted
admission, truncated input, and final zero charges. The initial exhaustion fixture
used only default entropy tables, so its 12-byte allowance legitimately succeeded;
the corrected fixture exercises allocated entropy tables. All 10 focused ZSTD
tests pass, including native parity and existing incremental exact-peak checks.
TypeScript build and changed-file lint pass.

This removes repeated identical-frame allocation growth, but does not yet supply
a complete static workspace plan for arbitrary block/table combinations. Pool
retention and zeroing cost need measurement with mixed real-file pages. No new
cloud, automatic scan scheduling, or whole-runtime heap claim follows from these
local codec tests.

### Measured admitted ZSTD reuse tradeoff (November 12, 2026)

Added `bench/zstd-admitted-workspace.mjs` and recorded
`docs/benchmarks/zstd-admitted-workspace-2026-11-12.jsonl`. Native compression
generates multi-block frames with changing entropy distributions; the eight-frame
case uses different seeds rather than repeating one identical frame. Every decode
is compared byte-for-byte, final budget charges must be zero, and measured total
new allocation extents are replayed at exact capacity and one byte below it.
Five timed samples follow three warmups; the artifact records decoder hashes.

For 1 MiB, incremental release took 6.35 ms versus admitted reuse at 6.16 ms;
workspace rose from 13,656 to 15,131 bytes. For eight frames totaling 8 MiB,
incremental release took 49.94 ms versus reuse at 45.90 ms; workspace rose from
13,656 to 19,611 bytes. These single-run medians indicate acceptable measured
overhead, not a guaranteed speedup. Extra retained workspace buys lower total-new
allocation growth under non-replenishing stage admission. Benchmark lint passes.

The measured extent is a validation oracle obtained by decoding, not a production
preflight plan. Arbitrary-input planning must derive its bound without a full
decode first. Inputs here are synthetic native-compressed payloads, not Parquet
pages or cloud reads; encoded input backing and JavaScript heap are excluded.
Real-page integration and remote/runtime validation remain required.

### Static reusable ZSTD workspace capability (November 12, 2026)

The scoped codec now exports an internal conservative total-new backing bound of
62,394 bytes for its reusable workspace pool. Its derivation follows allocator
sites in the vendored decoder: seven FSE tables per possible size class (three
old sequence tables, three replacement tables, one temporary Huffman table), two
Huffman tables per power-of-two size class, one 292-byte weight workspace, one
12-byte frame-offset allocation, and six 3-byte RLE tables. Summing size classes
accounts for free buffers retained across changing blocks and frames, rather than
mistaking live peak for total admission. Shared backing sizes across table types
make this sum conservative. The previous Huffman depth validation is required
for the bound. Output backing and JavaScript metadata are excluded.

Admitted allocation checks this capability before creating new workspace backing,
so later decoder changes cannot silently exceed the contract. Three native
compression levels over 2 MiB of changing multi-block entropy data decode under
output plus this static allowance, using inherited admission through a child
scope. Cleanup reaches zero. All 11 focused ZSTD tests, TypeScript build and
changed-file lint pass. These tests support operation; the allocation-site
derivation establishes the bound independently of those particular inputs.

This internal capability is not yet connected to flat-page planning. Its 61 KiB
conservative reservation also exceeds the roughly 15–20 KiB observed workspace
in recent probes; tighter header-derived plans can improve scheduling utilization.
Generated real Parquet pages, cloud execution and whole-runtime heap validation
remain pending, and the overall goal remains incomplete.

### V2 ZSTD flat-page admission and real files (November 12, 2026)

Flat numeric page plans now support built-in V2 ZSTD, including its 62,394-byte
reusable workspace allowance, decompressed data, null mask and detached split
output where needed. Execution explicitly selects reusable workspace even if the
plan is used with an incremental scope. Custom ZSTD codecs remain unsupported by
this allocation contract and are never silently replaced. Compressed V1 levels,
logical conversion, dictionary pages and multi-column alignment remain separate
capabilities.

The generated Parquet probe now passes 60 cases, including 20 ZSTD pages written
with native compression and read independently by DuckDB. Every row matches;
two scheduled decodes share charged encoded backing, insufficient admission
rejects before decode, and all charges return to zero after outputs release.
Artifact: `docs/validation/scheduled-zstd-page-fixtures-2026-11-12.jsonl`.
The artifact calls the removed allowance `nonRetainedExtentPerJob`: conservative
unused admission is not allocated scratch, and stays reserved until a stage
settles. Peak checks distinguish that reservation from backing released during
execution. Retained output checks subtract both unused allowance and freed scratch.

All 30 focused numeric-page/workspace tests pass, including custom-codec refusal
and truncated ZSTD cleanup. TypeScript build and changed-file lint pass after
fixing benchmark import ordering. These are real local files and scheduled
physical decoder operations, not automatic query scan scheduling or remote
object-store validation. Conservative workspace sizing remains an optimization
opportunity; whole-Worker heap and final release gates remain incomplete.

### Post-ZSTD cloud and runtime regression checkpoint (November 12, 2026)

Fresh public R2 validation passes all 52 reports, recorded in
`docs/benchmarks/cloud-range-zstd-plans-2026-11-12.json`. This exercises the
production HTTP range adapter, overlap/coalescing, bounded concurrency, cache
reuse, and ordinary owned queries with pinned cache and limited allocation
headroom. It is remote-object evidence, unlike the generated local page probe.
It does not establish that the new physical-page planner is integrated into
normal scans or that these remote objects exercise its ZSTD capability.

All 24 workerd tests pass across eight files. The runtime printed a canceled
stream-pump diagnostic but reported no test failures. These remain the existing
runtime tests, not near-cap total-heap proof or new ZSTD-specific Worker coverage.

The first full default run exposed two stale admission expectations: raw ZSTD
frames now reuse one 12-byte offset buffer instead of consuming 12 new bytes per
frame. The tests now require exact `5 * frameCount + 12` admission for 1, 2 and
100 hello frames, retaining cap-minus-one rejection and output lifetime checks.
The rerun passes all 1,674 default tests (363 skipped). Changed-test lint passes.
No broader completion or whole-runtime memory claim is made.

### Charged dense integer-decimal conversion (November 12, 2026)

Normal owned page decoding and dictionary conversion now pass their allocation
scope into integer-decimal conversion. Dense typed input converts directly into
an admitted Float64Array, removing the intermediate JavaScript number array and
its later copy into vector storage. Unscoped and nullable JavaScript-array callers
retain their existing representation and null semantics. Existing exact-unscaled-
integer validation remains unchanged; failed conversion releases its allocation.
This closes a real allocation gap in normal scans, but nullable expansion and
other logical conversions still need complete stage planning.

Tests verify negative/zero/safe-boundary values, unsafe integer rejection, exact
24-byte output admission, rejection at 23 bytes before allocation, and cleanup.
Writer-generated nullable Snappy PLAIN and DELTA_BINARY_PACKED decimal files pass
through the owned batch reader with small batches and zero final charges. Attempted
INT64 decimal dictionary generation fails inside the writer with mixed BigInt/
number index handling, so that encoding is not claimed as fixture coverage here.
All 42 focused decimal/owned-reader tests and all 1,677 default tests pass
(363 skipped); TypeScript build and changed-file lint pass.

`bench/decimal-owned-conversion.mjs` compares checked array-then-vector conversion
against the charged direct typed path with byte-for-byte output validation and
seven timed samples. At 10k values, medians are 0.536 versus 0.316 ms; at 100k,
5.220 versus 2.422 ms. Artifact:
`docs/benchmarks/decimal-owned-conversion-2026-11-12.jsonl`. Both produce the same
8-byte-per-value final backing; baseline JS intermediate memory is not measured.
These are local component timings, not end-to-end cloud or whole-heap evidence.

### Final numeric-vector allocation planning (November 12, 2026)

Added internal `planFlatNumericVector`, composing physical page planning with
checked integer-decimal conversion and final nullable vector expansion. Physical
plans expose row count, present-value count and byte width so the composed plan
does not infer allocation shape from allocated output. Supported logical shapes
are unannotated fixed-width numbers, compatible integer annotations and checked
integer decimals; object-producing conversions such as DATE decline explicitly.
Integer annotations must match their physical integer storage width.

The complete new-backing extent includes physical decode, dense decimal output
where applicable, and expanded nullable storage. Normalized definition bytes are
reused as validity. All-null input is decoded/validated and returns a null vector
without allocating expanded values. Detached decimal conversion releases exact
owned physical values; unsigned reinterpretation keeps its aliased backing.
Input ownership remains separately required, and only unconverted non-null
borrowed PLAIN output retains an input dependency.

V1/V2 tests cover all-null, mixed and all-present patterns with and without
decimal conversion, checking rows, exact admission consumption, cap-minus-one
pre-admission rejection and final zero charges. An initial all-null expectation
exposed upstream empty-array representation; the explicit null-vector result
removes that unnecessary expansion rather than reserving an unused values buffer.
All 28 numeric-page tests, TypeScript build and changed-file lint pass.

This capability is not yet integrated into the normal scan scheduler. Generated
real-file scheduled vector tests, unsigned backing-lifetime coverage, dictionary
and other logical shapes, aligned column working sets, cloud and whole-heap
validation remain outstanding. Existing physical-page evidence is not presented
as validation of this new composed operation.

### Scheduled final-vector real-file ownership (November 12, 2026)

Added `bench/scheduled-flat-vector-fixtures.mjs`, with 72 passing generated
Parquet/DuckDB cases recorded in
`docs/validation/scheduled-flat-vector-fixtures-2026-11-12.jsonl`. Coverage includes
INT32/FLOAT/DOUBLE and integer-backed DECIMAL, nullable/non-nullable pages, backing
alignment, PLAIN/supported BYTE_STREAM_SPLIT, and uncompressed/Snappy/ZSTD codecs.
Each case schedules two complete final-vector plans with charged encoded input,
compares every row to DuckDB, verifies exact retained output backing, checks
cap-minus-one refusal before decode and releases all charges. Decimal references
use DuckDB's explicit DOUBLE cast to match the existing public number contract.

The composed planner now uses a child physical-decode scope and transfers only
backing retained by final output. Compact decoded backing is released after
nullable expansion or decimal conversion even when values are typed views over
a separately owned decompression buffer. Validity ownership transfers directly;
non-null unsigned reinterpretation retains the original backing. This addresses
the lifetime distinction that exact-object release alone cannot resolve.

Additional unsigned tests cover high-bit INT32 values, nullable/non-nullable
output, all three codecs, and retained output after scope disposal. They verify
exact final backing charge and zero after owner release. All 31 numeric-page
tests and all 1,682 default tests pass (363 skipped); TypeScript build and
changed-file lint pass. This is local generated-file and scheduling evidence,
not automatic scan integration, remote-page behavior or whole-Worker heap proof.
Those requirements and complete aligned-column admission remain open.

### Complete numeric-vector admission in normal scans (November 12, 2026)

Chunk and window transports now share `decodeFlatPageVector`, removing duplicate
physical-to-vector construction. Supported numeric pages automatically use a
complete vector plan when its total-new extent fits current allocation authority.
The private scan path supplies built-in scoped codecs, which planning rebinds to
the admitted child stage. Surviving final output ownership transfers to the caller
before stage disposal, releasing unused conservative reservation and scratch.
Public planning still declines custom codec contracts.

When a conservative complete extent does not fit, the existing individually
charged decode path remains available. This matters for ZSTD, whose worst-case
pool reservation can exceed actual live workspace substantially. The fallback
preserves configured allocation limits and avoids rejecting a decode that can
operate within them, but does not establish complete pre-admission for every
page. Unsupported physical/logical shapes also retain their existing path.

All 1,682 default tests pass after integration. Two additional real-file tests
then confirm complete child-stage admission actually occurs in ordinary nullable
Snappy chunk/window scans; all 39 owned-reader tests pass. TypeScript build and
changed-file lint pass. All 24 workerd tests pass, and the freshly rebuilt public
R2 matrix passes all 52 reports in
`docs/benchmarks/cloud-range-vector-integration-2026-11-12.json`. Its maximum
incremental tracked peak remains 54,659 bytes under 131,072 bytes of headroom;
four concurrent reads and warm-cache reuse remain exercised.

This is automatic per-page admission within normal execution, not shared FIFO
scheduler integration or an aligned multi-column working-set contract. Queueing,
shape-aware batch sizing, complete fallback allocation coverage and realistic
whole-Worker near-cap validation remain open. Network timing in this regression
matrix does not isolate planner CPU cost; dedicated local measurements are still
needed to quantify planning overhead in normal scans.

### Normal-scan planning cost and alias-only scope reduction (November 12, 2026)

Added `bench/numeric-vector-scan-planning.mjs`. It generates actual local Parquet,
scans 65,536 rows across three codecs, nullable/non-nullable data and batch sizes
64/1,024, and checks every row outside the timed iterator calls. The baseline
disables complete planning in the same compiled scan module; it is not a historical
binary. Five samples follow three warmups and source hashes identify each revision.
Input buffers and JavaScript heap are excluded from tracked allocation evidence.

The first run exposed a regression: at batch size 1,024, non-null uncompressed
scans took 0.362 ms incrementally versus 1.199 ms planned; nullable Snappy took
1.548 versus 3.276 ms. ZSTD conservative admission raised tracked peak from
11,180 to 70,582 bytes for non-null pages and 23,543 to 85,937 for nullable pages.
Artifact: `docs/benchmarks/numeric-vector-scan-planning-2026-11-12.jsonl`.

Alias-only final vectors now execute physical decode directly in the existing
stage, avoiding a redundant scratch scope and ownership snapshot. Zero-new-byte
plans also skip an empty child admission stage. These are allocation-shape rules,
not codec/query-specific exceptions; nonzero backing still requires admission.
The rerun records non-null uncompressed 0.357 ms planned, Snappy 1.219 ms and
ZSTD 3.126 ms; corresponding incremental medians are 0.559, 0.685 and 2.788 ms.
Nullable Snappy remains 2.505 versus 1.411 ms, and nullable ZSTD 4.428 versus
2.745 ms. Artifact: `docs/benchmarks/numeric-vector-scan-scope-reuse-2026-11-12.jsonl`.
Run-to-run noise precludes treating these small component timings as universal
speedups, but the redundant-scope cost is reduced and the remaining regression
is explicit. ZSTD reservation peaks are unchanged.

All 70 numeric-page/owned-reader tests, TypeScript build and changed-file lint
pass. Remaining work includes nullable planning replay/ownership overhead and
tighter ZSTD reservation, alongside shared scheduling and complete heap coverage.
The default is not considered performance-validated to completion merely because
its correctness and cloud regression checks pass.

### V2 metadata-driven planning without level replay (November 12, 2026)

V2 physical plans now derive present-value count and level offset from validated
header fields instead of traversing definition levels during planning and again
during execution. V1 retains level traversal because its header lacks null counts.
Execution still parses levels and checks actual present/null counts before value
decoding. A false header cannot expand allocation authority: malformed null-count
metadata rejects in planning, while inconsistent encoded levels fail during
execution within the admitted extent. Plans remain input-immutable operation
descriptors, not certifications that compressed data is valid.

Tests cover negative, fractional, excessive and NaN null counts plus headers
claiming zero/all nulls for mixed data, asserting failure without exceeding
admission and zero final charges. All 32 page tests pass; the prior 70-test
page/owned-reader run also passes after the production change. TypeScript build
and changed-file lint pass. All 72 generated scheduled vector cases pass again in
`docs/validation/scheduled-flat-vector-v2-header-2026-11-12.jsonl`.

The local scan rerun is recorded in
`docs/benchmarks/numeric-vector-v2-header-plan-2026-11-12.jsonl`. At batch size
1,024, nullable planned medians are 1.744 ms uncompressed, 2.327 ms Snappy and
2.967 ms ZSTD, versus respective incremental medians 1.058, 1.194 and 2.587 ms.
The preceding planned measurements were 2.459, 2.505 and 4.428 ms. This reduces
repeated level work but does not eliminate planning overhead; ZSTD conservative
reservation peaks remain unchanged. Remote/runtime validation and the broader
shared scheduling/heap acceptance work remain separate requirements.
