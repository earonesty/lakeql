# Performance and memory audit: historical execution log 2

[Current requirements and evidence](performance-memory-audit.md).


### Header-derived ZSTD workspace admission (November 12, 2026)

Added `planZstdWorkspaceBytes`, a no-entropy-decode frame/block traversal used by
V2 page plans. Raw/RLE frames require only the reusable 12-byte offset backing;
each compressed block contributes at most 11,812 bytes of new entropy workspace
(weight workspace, temporary FSE weights, Huffman table and three sequence
tables). The sum saturates at the proven 62,394-byte reusable size-class bound.
Frame offsets are reused across concatenated frames. Skippable frames allocate
no workspace; dictionary-ID/content-size header fields and checksum trailers are
skipped with bounds checks. Truncation and reserved block types reject before
admission. Entropy contents and output correctness still require actual decoding.

Tests cover raw/RLE/skippable/concatenated frames, every truncated prefix of small
fixtures, reserved block types, unaligned inputs, native checksummed frames at
three compression levels and lengths from zero through 1 MiB. Actual scoped
decode succeeds within each derived allowance with zero final charges. All 40
focused page/workspace tests pass. The initial large-fixture test used argument
spread and hit the JS call-stack limit; replacing it with typed-buffer copying
allows the intended multi-block validation to run.

All 72 final-vector and 60 physical-page generated Parquet/DuckDB cases pass:
`docs/validation/scheduled-flat-vector-zstd-framing-2026-11-12.jsonl` and
`docs/validation/scheduled-physical-zstd-framing-2026-11-12.jsonl`. The scan benchmark
in `docs/benchmarks/numeric-vector-zstd-framing-2026-11-12.jsonl` reduces planned
ZSTD tracked peaks from 70,582 to 20,012 bytes non-null and from 85,937 to 35,367
bytes nullable. At batch size 1,024, planned times are 3.102/3.147 ms versus
incremental 2.723/2.549 ms. Reservation overhead remains, but small pages no longer
reserve the entire arbitrary-stream bound. These are local component results.

TypeScript build and changed-file lint pass. Full default regression validation
passes 1,689 tests (363 skipped). Fresh remote/runtime coverage, shared queued
scheduling and the whole-Worker memory contract remain outstanding.

### Unequal page-boundary alignment and shared pools (November 12, 2026)

Added generated Snappy INT32/nullable DOUBLE fixtures to owned-reader tests.
The test parses actual page headers and asserts that the columns have different
row-boundary sequences; differing boundaries are not merely assumed from schema.
Both chunk and window paths are exercised with batch sizes 1 and 13. All rows
and output offsets match the generated input, and each output respects the
requested batch size. A wide-budget measurement is replayed at its exact observed
peak. Two jobs also consume the same source under one pool capped at twice the
measured single-job peak, with no overshoot or residual charge. Early iterator
closure leaves the first aligned output valid and charged until receiver release.
All 41 owned-reader tests, TypeScript build and changed-test lint pass.

Code inspection confirms that alignment currently advances column iterators
sequentially and retains each column's current page until its iterator advances.
Complete per-page admission therefore does not describe the whole aligned
working set: already retained peer pages and future replacement pages coexist.
A shared queue must account for those dependencies rather than wait for memory
that an already yielded peer page cannot release until alignment advances. These
tests establish the current lifetime/budget baseline for that scheduler work;
they do not implement aligned-stage admission or claim minimal required memory.
Fixtures execute through the local memory-store adapter, not remote object storage
or a measured 64/128 MiB Worker heap.

### Release consumed alignment peers before replacement (November 12, 2026)

Column cursors now carry an internal idempotent consumed-page release callback
for iteration-owned pages. Once all output slices through an alignment boundary
have been yielded, alignment releases every exhausted peer before advancing any
iterator. Chunk decoding releases its decoded page scope/pin; window decoding
also releases the consumed transport scope/pin. Ordinary iterator finalizers still
perform the same cleanup for errors and early closure. Consumer-owned outputs
and cache owners retain their independent references. Scope-lifetime decoding
does not opt into early release.

`bench/aligned-page-release.mjs` compares the current implementation against the
same compiled module with peer release disabled. Generated Snappy fixtures check
all rows and zero final charges with equal/unequal column boundaries and chunk/
window transport. Artifact:
`docs/benchmarks/aligned-page-release-2026-11-12.jsonl`. Aligned window pages reduce
mean tracked charge immediately before reads from 491.20 to 308.39 bytes with
276 reads in either case. Peak remains 760 bytes. Unequal pages and chunk paths
show unchanged peaks and mean read-entry charges in these fixtures. This is a
lifetime improvement, not evidence of a peak-memory or latency improvement.

All 41 owned-reader tests, including retained early-exit output and shared-pool
unequal alignment, pass. Full default validation passes 1,691 tests (363 skipped);
TypeScript build and changed-file lint pass. The release phase establishes a
clearer boundary for future aligned admission, but does not itself implement
shared queueing, eliminate retained unconsumed peers, or prove whole-Worker bounds.

### Aligned replacement failure and cancellation (November 12, 2026)

Added controlled replacement-read failure/cancellation tests using actual generated
two-column Snappy pages through owned window reads. Each test retains the first
aligned output, advances until a later read begins, and holds a charged asynchronous
transport allocation pending. One case throws the original transport failure;
the other aborts the query while allocation work remains unsettled. Both verify
that pending backing remains charged before settlement, the retained output stays
readable before and after failure, and the final receiver release returns all
charges to zero. These tests exercise the newly separated consumed-page release
phase while the next read is in flight, rather than only clean iterator closure.

All 43 owned-reader tests, TypeScript build and changed-test lint pass. Fresh
workerd validation passes all 24 tests. The injected adapter is local and provides
deterministic failure timing; this is not a claim of new remote failure-injection
coverage or proof that arbitrary adapters honor cancellation immediately. Shared
aligned admission and the broader memory/heap acceptance requirements remain open.

### Skip page bodies outside requested rows (November 12, 2026)

Window scanning previously fetched each page body before checking whether its
rows preceded the requested interval. The header/body read boundary now accepts
the row-range decision before fetching body bytes. Dictionary pages still load
normally, selected data pages decode normally, and body extent validation remains
in force even for skipped pages. Header probes can overlap a small payload prefix;
the guarantee is omission of the separate full-body request, not zero overlap.

A generated 1,024-row Snappy fixture parses its actual page headers, records the
excluded body ranges, and traces owned reads for rows 500–519. No excluded body
range is requested, every selected row matches, batch limits hold, and final
charges are zero. All 44 owned-reader tests and all 1,694 default tests pass
(363 skipped). Fresh DuckDB reference validation passes all 104 tests. TypeScript
build and changed-file lint pass.

Fresh public R2 validation passes all 52 reports, with maximum incremental tracked
charge still 54,659 bytes under 131,072-byte headroom. Artifact:
`docs/benchmarks/cloud-range-page-body-pruning-2026-11-12.json`. This is remote
transport regression evidence; the targeted skipped-body request assertions come
from the deterministic local fixture. Batch-size review also confirms that smaller
output batches alone cannot bound whole compressed-page decode. Page splitting,
aligned-stage scheduling and complete allocation/heap coverage remain open.

### Release failed page-header probes (November 12, 2026)

Owned window reads now obtain an explicit owner for each header probe. If parsing
fails and probing grows, that probe's owner releases before the next read starts.
Successful probes remain in the transport scope because parsed binary header
fields may alias their backing. Borrowed readers retain their existing behavior;
no ownership is inferred for an unowned buffer.

A corruption test wraps a generated fixture's owned transport and records budget
charge at every progressively larger header read. Multiple retries all begin
with zero residual probe charge, peak equals the largest individual probe rather
than the cumulative sizes, and failure leaves zero final charges. All 45
owned-reader tests, TypeScript build and changed-file lint pass. This is local
deterministic malformed-input coverage; successful-header metadata allocations
and complete pre-admission of parser working memory remain separate open work.

### Input-bounded compact page-header parsing (November 12, 2026)

Page-header reads now use `readCompactHeader`. Lists validate their declared
length against remaining probe bytes before allocating an array; binary views
must remain inside the supplied DataView, not merely its underlying backing.
Every struct requires its terminating STOP byte. Integer varints are bounded to
32/64 bits and nested parsing has an explicit 128-level stack-use limit. Page
headers normally use only a few levels; this limit is a parser resource contract,
not a query-specific operator restriction. Existing unsupported compact types
remain unsupported. Successful binary fields still borrow probe backing.

Header mapping now validates numeric, boolean and nested field shapes rather
than relying on unchecked upstream object types. Tests compare valid scalar,
binary, list and nested decoding against the upstream parser, and reject huge
declared lengths, missing STOP bytes, overflowing varints, excessive depth and
binary data beyond a subview boundary. All 48 focused parser/owned-reader tests
and 1,698 default tests pass (363 skipped). TypeScript build and changed-file lint
pass. All 72 generated final-vector Parquet/DuckDB cases pass in
`docs/validation/scheduled-flat-vector-bounded-header-2026-11-12.jsonl`.

The local scan matrix was rerun in
`docs/benchmarks/numeric-vector-bounded-header-2026-11-12.jsonl`; its baseline and
planned variants both use the new parser, so it does not isolate parser overhead.
Separate parser cost comparison remains needed. These checks bound parser work
by encoded input and recursion depth; JS object/array footprint is not yet fully
charged, footer metadata still uses its existing parser, and whole-Worker memory
acceptance remains incomplete.

### Isolated header-parser cost and integer-loop optimization (November 12, 2026)

Added `bench/compact-header-parsing.mjs` to compare the bounded parser directly
with upstream parsing, checking exact decoded objects and consumed offsets before
timing. The first generated-header run measured 20,000 parses at 7.80/7.84 ms
upstream versus 11.11/10.29 ms bounded (PLAIN/dictionary fixtures). Artifact:
`docs/benchmarks/compact-header-parsing-2026-11-12.jsonl`.

The bounded parser now relies on DataView's own single-byte bounds check and
assembles bounded 32-bit varints with bit operations instead of exponentiation.
The final-byte overflow check remains, and unsigned normalization preserves both
signed 32-bit zigzag extremes. Tests cover those boundaries explicitly. All 48
parser/owned-reader tests passed after the production change; the added boundary
case then passes in the four-test parser suite. Build and changed-file lint pass.

The benchmark now covers all row groups in generated files plus external
`nullable.impala.parquet` and `alltypes_plain.parquet`: 311 headers, including 13
with page statistics. Exact upstream parity passes for every header. The bounded
parser's remaining incremental cost is 1.06–1.51 ms per 20,000 parses, roughly
0.05–0.08 microseconds per header in this run. Artifact:
`docs/benchmarks/compact-header-external-2026-11-12.jsonl`; the intermediate same-
fixture optimization run is `compact-header-integer-loop-2026-11-12.jsonl`.
These are parser-only local CPU measurements, not I/O or whole-query claims.
The bounds checks are retained despite their remaining cost; metadata heap
accounting and footer-parser integration still require further work.

### Project page-header fields and release successful probes (November 12, 2026)

Compact parsing now supports an explicit field projection. Page scans request
only fields needed for data/dictionary decoding; skipped nested structs, lists
and binary values are traversed and validated without constructing their output
objects, arrays or views. Unselected 64-bit integers also avoid BigInt assembly.
Malformed skipped lengths and excessive nesting remain errors. Full parsing stays
available for callers that need all fields.

The mapped page header contains only validated numbers, booleans, enum values and
nested scalar records. It therefore cannot alias successful probe backing, which
now releases before the page body read. This supersedes the earlier conservative
successful-probe retention described above; arbitrary full-parser results can
still contain borrowed binary views and do not get this lifetime guarantee.

Tests cover projected nested values, exact consumption, and rejection of malformed
skipped fields. All 1,700 default tests pass (363 skipped); TypeScript build and
changed-file lint pass. The parser benchmark verifies projected output against
the corresponding subset of upstream output for all 311 generated/external
headers. On `nullable.impala.parquet` (13 statistics-bearing headers), 20,000
parses take 7.164 ms projected versus 9.161 ms full bounded and 7.684 ms upstream.
Artifact: `docs/benchmarks/compact-header-projection-2026-11-12.jsonl`.

The local owned-read probe records aligned window peak falling from 760 to 380
bytes, and unequal window peak from 848 to 412 bytes, with unchanged read counts.
Artifact: `docs/benchmarks/aligned-page-header-release-2026-11-12.jsonl`.
This measures typed backing, not JS heap or remote latency. Metadata field-type
allocation contracts, footer parsing, shared scheduling and complete Worker heap
coverage remain unfinished.

### Validate projected compact field types before allocation (November 12, 2026)

Compact projections now support explicit `i32` and `bool` scalar requirements;
nested projection objects require STRUCT wire type. Page-header projections use
those requirements for every selected field. A malformed selected integer can
therefore no longer request a list or binary view and rely on the later page
mapping check to reject it. The generic `true` selector still supports unconstrained
values for callers intentionally requesting them. Unknown skipped fields remain
validated without output-container construction.

New tests supply incompatible wire tags without payloads and verify type rejection
at the tag boundary, plus retained unconstrained-list behavior. All 51 focused
parser/owned-reader tests and all 1,701 default tests pass (363 skipped).
TypeScript build and changed-file lint pass. Exact projected/full parity also
passes across all 311 generated/external headers in
`docs/benchmarks/compact-header-typed-projection-2026-11-12.jsonl`. The benchmark's
projection oracle was updated to interpret typed scalar selectors; its first
attempt treated those strings as nested descriptors and failed in the oracle.

The 72 scheduled vector fixture cases were rerun as decoder regression evidence,
but their manually parsed headers use upstream decoding; they are not independent
coverage of this parser change. Parser evidence is the compact-format tests,
real-header parity, and normal owned-reader fixtures. Footer parsing, complete
metadata heap charges and whole-query/Worker acceptance remain open.

### Retry only incomplete compact headers (November 12, 2026)

The parser now distinguishes incomplete input with `IncompleteCompactHeaderError`.
Single-byte DataView exhaustion and truncated length-delimited fields use that
classification. Window header reads grow only for this error; incompatible field
types, unsupported types, varint overflow, excessive nesting and invalid mapped
fields fail immediately after releasing the probe. Permanent malformed input no
longer drives repeated larger reads across the column chunk.

Owned-reader tests distinguish a genuinely truncated unknown binary field from
an invalid wire type: truncation retries with no retained prior probes, while the
invalid type performs exactly one read. A synthetic valid V2 header with a
300-byte unknown field grows from the existing 256-byte initial probe to 1,024
bytes, then reads its six-byte body and yields value 42. Peak is 1,024 bytes and
final charge is zero. The first test expectation incorrectly assumed a 128-byte
initial probe; it was corrected to the existing policy without changing production
probe sizing. This is synthetic header transport coverage, not a writer-generated
long-header or remote failure-injection claim.

All 47 owned-reader tests and all 1,703 default tests pass (363 skipped), as do
TypeScript build and changed-file lint. Real-header parity/timing was rerun in
`docs/benchmarks/compact-header-retry-classification-2026-11-12.jsonl`.
Complete metadata heap accounting, footer parsing and shared aligned scheduling
remain outstanding.

### Owned footer assembly and dynamic probes (November 12, 2026)

Owned metadata reads now avoid the upstream uncharged concatenation when a footer
exceeds the initial range. With sufficient available allocation headroom, they
fetch only the missing prefix and assemble the footer in charged storage, releasing
both source ranges. When that combined peak cannot fit, they release the initial
range and fetch the complete footer instead. This deliberately retransmits the
suffix to keep the live encoded footprint bounded. The initial probe is capped
by available headroom (with an eight-byte trailer minimum), so a small footer in a
large file no longer inherently requires 64 KiB of free memory.

Five owned-metadata tests pass, including writer-generated large metadata with
exact-footer and double-footer budgets, one-byte-too-small rejection, a 100,000-row
file read with 1,024 bytes of headroom, cache pinning/eviction, malformed trailer
magic/length, and injected failures in both prefix and full-refetch continuations.
Successful owners retain the final backing; failures and released consumers leave
zero charge. TypeScript build and changed-test lint pass. The completed default
run before the dynamic-probe test passed 1,704 tests (363 skipped); it is not a
full-suite result for the newest tests.

These are memory-store range traces and allocation assertions, not remote latency
measurements. A concurrent headroom change can still cause a safely bounded
allocation failure; this is not complete queued-stage admission. Parsed metadata
objects still use upstream parsing and are not fully accounted as JavaScript heap.
Fresh remote/workerd validation, whole-worker bounds, and shared aligned scan
scheduling remain required.

### Fresh remote footer and Worker validation (November 12, 2026)

The current built revision passes all 24 workerd tests and all 52 public-R2
validation scenarios in `cloud-range-owned-footer-2026-11-12.json`. These retain
real HTTP range-status checks, mixed concurrent jobs, pinned-cache pressure and
warm reuse. Workerd emitted a canceled-stream diagnostic during cancellation
coverage but exited successfully. These runtime checks are not whole-heap proofs.

New `bench/cloud-owned-footer.mjs` compares complete parsed metadata with the
existing unowned reader over actual public R2 HTTP 206 responses. The fixture's
footer is 1,535 bytes. Caps of 1,535 and 3,070 bytes succeed with respective tracked
peaks of 1,535 and 3,070 bytes; a 1,534-byte cap rejects and cleans up. All three
cases finish with zero owned charge. Discovery/oracle reads are outside the
measured cases. Evidence: `cloud-owned-footer-2026-11-12.json`.

The rejected case exposes an additional transport inefficiency: after learning
the footer length, the HTTP adapter issues the 1,535-byte range request before
body allocation rejects. There is no encoded allocation overrun, but known
impossible requests should be rejected before network dispatch. This requires
coordinating admission with transport ownership rather than adding an independent
reservation that double-charges the same response. The public fixture has a small
footer, so large-footer assembly/refetch remains covered by generated local
fixtures, not this cloud run. Complete metadata-object accounting remains open.

### HTTP response preflight admission (November 12, 2026)

Owned HTTP ranges now reserve initial response capacity before fetch dispatch and
transfer that reservation into the shared owned-stream reader's first buffer.
The reservation remains charged while fetch is pending and is released on fetch
failure; transferred storage is not charged twice. Initial admission is bounded
by the requested range and configured response-byte limit. The generic stream
option allows other transports to transfer preflight reservations as well.

All 26 HTTP tests pass, including new no-request budget rejection, exact-capacity
success, charged capacity observed inside fetch, fetch-failure cleanup, and an
ignored-range full response. TypeScript build and changed-file lint pass. Fresh
real-R2 evidence in `cloud-owned-footer-admission-2026-11-12.json` confirms that
the 1,534-byte cap now issues only its initial probe: the impossible 1,535-byte
continuation no longer reaches the server. Exact and double-footer cases preserve
metadata equality and previous peaks; all final charges are zero.

A server that ignores ranges can exceed the initial capacity. It uses the existing
bounded stream-growth contract, including coexistence of old and replacement
buffers during copying: the test's four-byte request/eight-byte full response has
a 12-byte tracked peak. This is an explicit fallback cost, not a claim that every
server response fits the requested range. Runtime Fetch chunks and parsed JS
objects remain outside encoded-buffer accounting. Full regression is running;
shared scan scheduling and complete Worker memory coverage are still outstanding.

The completed full default regression for HTTP preflight admission passes 1,708
tests, with 363 skipped (165 files passed, nine skipped).

### Native R2 admission and pending cancellation (November 12, 2026)

Native owned R2 reads now reserve the requested range before calling bucket.get
and transfer that capacity to the same shared stream allocator as HTTP. Bucket
I/O does not accept the scope's cancellation signal, so disposal retains the
reservation until the pending get settles; a late body is canceled before use.
A dedicated test holds get unresolved, disposes its scope, proves competing reads
cannot consume that capacity or dispatch, then settles the call and verifies zero
charge. Existing malformed/oversized body and late-body cancellation checks pass.

The shared stream reader partitions and releases excess reservation when response
metadata establishes a shorter positive decoded extent. This preserves six-byte
retention for a 128-byte request at the end of a 4,096-byte R2 object, while the
pre-dispatch reservation peak remains 128 bytes. Whole-object size is never used
as the requested range allocation. Admission may conservatively reject an
oversized requested range even if a short tail would fit; callers with known file
lengths already clamp ranges, and allocation bounds take precedence over guessing
an unknown response length before dispatch.

All 27 focused R2/HTTP response tests, TypeScript build, and all 24 workerd tests
pass. The workerd suite uses its runtime R2 binding for real stream/tail behavior;
it is not a deployed remote R2 latency measurement. Earlier public HTTP R2 runs
remain separate evidence. The cancellation unit test uses a controlled fake bucket
to force the unresolved-I/O boundary. Shared scheduling, unaccounted metadata/JS
heap, remaining operator capabilities and final release gates are still open.

### Scheduled-stage cancellation reaches scoped transports (November 12, 2026)

Reviewing normal-scan integration exposed a scheduler prerequisite: its callback
received a canceled signal, but the allocation scope remained open, so HTTP reads
using scope.signal would not abort. Running-stage cancellation now captures a
completed-allocation guard, closes the scope to notify attached transports, and
releases the guard only when execution settles. Pending asynchronous allocations
continue to hold their own settlement reservations.

MemoryScope now tracks attached child scopes/stages and can retain their completed
allocation tree. Guarding only the parent would incorrectly release child scratch
on cancellation. Disposed children detach; intentionally detached cache scopes
are excluded because their lifetime is independent. Tests retain child scratch
across cancellation and prove transport signals abort while the full charge remains
until settlement. A scheduled HTTP integration test uses the actual HTTP adapter
with controlled fetch to prove the same behavior at the transport boundary.

The 33 scope/scheduler tests and 18 HTTP response tests pass, as does TypeScript.
The first integration/full-suite attempt used a nonexistent public scheduler export;
its import was corrected to the internal module without expanding the public API.
That full attempt passed 1,710 other tests, and the corrected full run is pending.

Normal scans still cannot simply wrap each decode in this queue: alignment currently
pulls and decodes column iterators sequentially, and page plans require bodies that
are already fetched. Complete aligned admission requires separating page preparation
from execution and retaining unconsumed peers as explicit inputs. Wiring isolated
page decodes into the queue would not establish the requested complete working-set
contract and is not claimed as integration here.

The corrected full run completes successfully: 1,711 tests pass, 363 skipped,
165 test files pass and nine are skipped.


### Combined transport validation and scope-tree boundaries (November 12, 2026)

The current public-R2 matrix completes all 52 scenarios after HTTP preflight,
native-R2 preflight and scheduler cancellation changes. Maximum incremental
tracked peak remains 54,659 bytes within 131,072 bytes of headroom beside pinned
cache allocations. Evidence: `cloud-range-preflight-admission-2026-11-12.json`.
These are remote HTTP measurements, not a whole-worker heap cap or evidence that
ordinary scans already use the stage scheduler.

Two additional scope tests cover nested child-stage allocations, release of unused
admission, disposed-child detachment, detached cache exclusion and snapshots before
and after allocation replacement. Completed tree backing remains charged until its
last guard releases; detached allocations have independent lifetime. All 35 scope
and scheduler tests pass, with TypeScript build and changed-test lint. This closes
ownership boundary coverage needed by future aligned-stage admission, not the
remaining integration itself.

### Separate page inspection from body loading (November 12, 2026)

Normal window reads now compose `preparePageWindow` and `readPreparedPageBody`.
Preparation parses the header, validates the compressed-body extent against the
column chunk, and releases header probe backing before returning the descriptor.
Body loading accepts the execution scope selected after inspection and uses owned
reads when available. This provides the separation needed to inspect multiple
column pages before admitting an aligned execution group. It does not yet change
the sequential iterator alignment or wire the shared queue into scans.

A writer-generated 2,048-row INT32 Parquet test verifies one 256-byte inspection
probe, zero retained inspection charge, disposal of the inspection scope, and an
exact-size body read in a different scope. The loaded bytes equal the fixture's
actual page body. An injected short response is rejected at the transport/decode
boundary and its owned backing is released immediately. Existing normal owned
reader tests still pass: 48 tests across the preparation and owned-vector suites.
TypeScript build and changed-file lint pass. These are local fixture/controlled
transport checks; remote matrix results from the preceding revision are not
claimed as evidence for this new boundary. Full default regression is running.

The preparation descriptor contains only header scalars and validated offsets;
complete decode extents still depend on the decoder's supported shape/codec plan.
Aligned planning must account existing peer pages and dictionaries explicitly,
and must not treat this descriptor extraction as complete allocation coverage.

The completed preparation-boundary regression passes 1,714 default tests, with
363 skipped; 166 test files pass and nine are skipped.

### Deferred page extent and cancellation validation (November 12, 2026)

Prepared-page inspection rejects negative, reversed, overflowing and out-of-file
column extents before dispatch. Body execution validates its extent against file
length and the parsed compressed size, and both inspection and execution reject
closed scopes before I/O. They also check scope closure after awaited reads, so a
borrowed adapter that cannot cancel its underlying operation cannot hand a late
page to a canceled decoder.

New tests prove no requests for invalid descriptors/closed scopes and force a
borrowed body to settle after cancellation. The combined preparation/owned-reader
suite passes 50 tests; TypeScript build and changed-file lint pass. The last full
suite remains the preceding 1,714-test checkpoint. This validation is a prerequisite
for deferred execution, not proof of grouped scheduling or cloud latency behavior.

### Structural child-scope disposal reduces planning overhead (November 12, 2026)

Attached scopes already have an explicit parent/child relationship for cancellation
guards. Child scopes and admitted child stages now use that relationship for parent
disposal instead of allocating an AbortController and parent-signal listener at
every synchronous decode boundary. External signals remain registered normally;
requesting a scope signal still creates a controller lazily. Parent disposal closes
attached descendants directly, while detached cache scopes remain independent.
Child-stage attachment rechecks parent openness after admission transfer.

All 1,716 default tests pass (363 skipped), all 24 workerd tests pass, and TypeScript
and changed-file lint pass. The focused 100-test run includes scheduled HTTP abort,
nested scratch guards, scope ownership and real owned Parquet readers. Reference
validation passed 104 tests and compatibility generation is current on the preceding
page-boundary revision; those are not post-optimization release gates.

The local generated scan benchmark was rerun separately from tests. Its initial
pre-change attempt overlapped reference tests and was replaced by an isolated run.
At batch size 1,024, nullable uncompressed planned scans change from 1.952 to 1.247 ms
per 65,536 rows; nullable Snappy from 1.867 to 1.263 ms. Tracked peaks are unchanged.
These are separate-run directional observations rather than an isolated causal
microbenchmark. Current paired incremental/planned medians remain 1.032/1.247 ms
for nullable uncompressed, 1.118/1.263 ms for nullable Snappy, and 2.298/2.362 ms for
nullable ZSTD. Planning overhead and reusable ZSTD workspace peak tradeoffs remain.

Evidence: `numeric-vector-page-preparation-2026-11-12.jsonl` and
`numeric-vector-structural-scope-disposal-2026-11-12.jsonl`. The benchmark now records
the compiled MemoryScope hash as well as the vector reader hash, since scope changes
would otherwise be invisible in its provenance. Row verification and cleanup checks
pass for all 24 combinations. Encoded input and JS heap are excluded; these local
scan timings do not establish cloud throughput or complete memory accounting.

### Header-only physical numeric decode extents (November 12, 2026)

`planFlatNumericPageHeader` now derives a conservative allocation extent for
supported flat V2 numeric pages without reading their bodies. It shares capability
selection, count/level validation and extent arithmetic with the existing precise
page planner. Unknown input alignment reserves the possible value copy; ZSTD uses
the enforced reusable-workspace ceiling until frame bytes provide a tighter bound.
The encoded input allocation remains separate. V1, nested, unsupported encodings
and custom compressed-codec contracts decline header-only planning explicitly.

Three codec tests exercise 60 writer-generated cases across INT32/FLOAT/DOUBLE,
PLAIN/BYTE_STREAM_SPLIT where supported, nullable/required data and two input
alignments. Every precise extent fits the header estimate, execution succeeds
under that admitted bound, decoded compact values equal the fixture values and
cleanup returns to zero. This is a physical-page prerequisite: logical conversion,
final nullable vector expansion, dictionary retention and aligned peer inputs are
not included and must be composed before grouped query admission.

All 1,719 default tests pass (363 skipped), as do TypeScript and changed-file lint.
The local normal-scan benchmark was rerun after tests completed in
`numeric-vector-header-extent-2026-11-12.jsonl`; its provenance now additionally
hashes numeric-page.js. It continues checking all rows and cleanup, but does not
exercise header-only group admission because that integration remains outstanding.
No remote throughput or whole-worker allocation claim follows from these results.

### Header-planned final numeric vectors (November 12, 2026)

`planFlatNumericVectorHeader` composes the physical header estimate with integer
decimal conversion and nullable final-vector expansion. Logical capability selection
and extent arithmetic are shared with the existing precise vector planner. Execution
checks body length and recomputes the precise plan, rejecting a missing capability
or a requirement greater than the prepared bound before decoding. Encoded input,
retained peers and dictionaries remain separate enclosing-stage responsibilities.

The header-plan fixture matrix now covers 108 actual writer-generated combinations:
three codecs; INT32, FLOAT, DOUBLE and INT64-backed decimal; supported PLAIN/BSS
encodings; required, mixed-null and all-null pages; and two alignments. Physical
and final-vector execution fit their header bounds. Final owners survive execution
scope disposal, materialize exactly the expected rows, and release to zero charge.
All-null vectors also pass this lifecycle check. The three parameterized codec
tests contain these combinations; they are not 108 separately reported Vitest tests.
The 35 combined numeric tests, TypeScript build and changed-file lint pass.
Full default regression is running.

These remain local-fixture plans, not normal-scan shared-queue integration. Next,
aligned preparation must compose body allocations and these output/decode extents
with the lifetimes of retained peer pages before dispatching the group. Conservative
ZSTD/header bounds must not silently replace tighter body-measured planning where
only the latter fits. The broader allocation and release gates remain open.

The final-vector header-plan full run completes: 1,719 tests pass, 363 skipped;
167 test files pass and nine are skipped.

### Prepared numeric page-group admission primitive (November 12, 2026)

`planNumericPageGroup` composes each prepared body's encoded bytes with its complete
header-planned vector extent. It validates all body descriptors before dispatch,
admits the full sum, and partitions child stages before starting I/O. Independent
pages execute with bounded concurrency (default four, explicit override supported).
On first failure it closes the group to notify peers and awaits all workers before
returning; pending allocation guards remain live through settlement. Detached decoded
outputs release exact encoded backing where possible. Returned vectors remain
owned by the caller's scope, which can retain them independently.

A real two-column Snappy fixture (INT32 and nullable DOUBLE) passes exact-group-cap
execution at concurrency one and two, with exact row comparison after parent scope
closure. Cap-minus-one fails with zero body requests. An injected peer failure holds
another owned read unresolved, verifies a nonzero pending charge, then releases it
and verifies complete cleanup. All 52 group/preparation/owned-reader tests pass,
as do TypeScript and changed-file lint. This is memory-store/controlled transport
evidence, not cloud concurrency evidence. No new full-suite checkpoint is claimed.

The group primitive is not yet used by normal scan alignment. It owns group outputs
together; efficient unequal-page alignment needs independent peer retirement and
retained-input accounting before integrating this operation with the shared stage
queue. It also deliberately declines unsupported header/shape capabilities instead
of claiming a complete estimate for fallback decoders. Conservative estimates may
not fit where a body-measured plan does; the eventual smart selection must preserve
that useful bounded path. Complete Worker heap coverage remains outstanding.

### Independent group-page ownership and queued fixture jobs (November 12, 2026)

The group plan now offers `executeOwned`, returning independent owners per page;
the existing execute wrapper transfers those owners into its caller scope. Each
page uses a separate transport scope, retaining encoded backing only when the
precise vector plan aliases it. This replaces unreliable exact-value release when
a transport owns a different view of the same buffer. The two-column fixture now
retains exactly 256 bytes for 64 INT32 values and 576 bytes for 64 DOUBLE slots plus
validity. Releasing the first owner leaves only the second charged, and a new first
page can decode while that second owner remains valid.

A two-job integration test combines the real fixture group with the shared FIFO
scheduler. The second job queues while the first holds admission; it then runs
within the pool while the first output remains retained. Releasing each output
reduces the charge independently and final usage is zero. The first test attempt
mixed source scheduler imports with compiled core error classes, so budget errors
were not recognized as queueable. The cross-package test now uses the compiled
scheduler alongside its compiled core dependency; core source unit tests remain
separate. No production error classification was weakened.

All four group tests, TypeScript and changed-file lint pass. These validate the
composition and ownership contracts with local real Parquet data and controlled
I/O; normal scan alignment is still not wired to the grouped scheduler. Actual
unequal-page iterator progression, measured-versus-conservative selection and
remote grouped stress remain required before claiming that integration complete.

### Grouped alignment over unequal numeric pages (November 12, 2026)

`readAlignedNumericPageGroups` now coordinates prepared column sources, admits
replacement pages as a group, and slices their overlapping row intervals into
requested-size batches. It releases all exhausted peer owners before inspecting
or loading replacements; unconsumed pages remain explicitly charged. Yielded
backing owners are independent of iterator and scope lifetime. It validates source
continuity, equal overall extents, decoded row counts and overlapping intervals,
and closes all sources and page owners on error or early return.

A writer-generated 512-row Snappy fixture has demonstrably different INT32 and
nullable-DOUBLE page boundaries. It produces exact rows with batch sizes one and
13, then replays at the measured peak budget. An early-stop test retains the first
batch, closes iterator and scope, verifies its values, and releases to zero. A
malformed source with a gap is rejected before replacement body dispatch; both
sources close and all charges are released. All six group/alignment tests pass,
as do TypeScript build and changed-file lint. The full default suite is running.

This is an internal grouped iterator with real-fixture operation, not yet the
normal scan route. Source preparation, metadata capability routing, decoded-cache
reuse, row-range pruning and shared-queue integration must still be connected
without degrading existing unsupported-layout fallback and cloud range behavior.
The new operator does not claim whole-query or whole-runtime memory coverage.

The completed grouped-alignment default run passes 1,725 tests (363 skipped),
with 168 test files passing and nine skipped.

### Row-window preparation for grouped numeric alignment (November 12, 2026)

Grouped alignment now accepts an explicit row interval, allowing the first live
page in each column to begin at a different physical row. It starts at the requested
row, clips the final batch at the requested end, and closes retained peers without
fetching replacements after that boundary. Empty ranges perform no reads. Existing
source-continuity and missing-overlap checks remain enforced.

`prepareNumericColumnPages` walks V2 headers within one column chunk and yields only
pages overlapping the requested rows. It does not fetch page bodies; group admission
still owns that step. It rejects unsupported non-V2 page layouts explicitly, so this
is not a replacement for dictionary/V1/nested routing. Header probes can overlap body
prefixes, but excluded pages have no explicit full-body requests.

A real 512-row fixture with unequal column page boundaries verifies [123,187),
[0,1), [511,512) and [100,100), exact output offsets/values, seven-row batching,
absence of excluded body ranges in read traces, and zero final charge. All seven
group/source/alignment tests pass, as do TypeScript and changed-file lint. The most
recent full default result remains the preceding 1,725-test checkpoint. Normal
scan routing, fallback selection, cache reuse and shared-queue integration remain
outstanding; these are local fixture traces, not remote group measurements.

### Real-R2 grouped alignment and exact-cap replay (November 12, 2026)

`bench/cloud-numeric-groups.mjs` exercises the internal grouped iterator against
actual public R2 HTTP 206 responses for event_id and nullable amount. Other fixture
columns include dictionary encoding or timestamp logical conversion and are not
silently treated as supported numeric group capabilities. It compares complete and
partial row windows with the existing normal reader across all three row groups.
The oracle and metadata reads are outside measured ownership cases.

All four combinations of one/four jobs and shared read limits one/two pass.
One job uses 12 range reads and 2,442 transferred bytes; four jobs use 48 reads and
9,768 bytes. Observed active transport never exceeds the shared limit. Tracked
peaks are 1,328 and 5,312 bytes respectively, and every case finishes at zero.
A second remote run sets the per-job cap to exactly 1,328 bytes (shared caps 1,328
and 5,312); all four cases pass at those limits with identical read counts/bytes.
Evidence: `cloud-numeric-groups-2026-11-12.json` and
`cloud-numeric-groups-exact-cap-2026-11-12.json`.

These are real remote transport/correctness/boundary observations, not a throughput
benchmark with statistical confidence. They use the raw controlled HTTP store,
without shared range-cache deduplication, so the linear request count across jobs
is expected and is not evidence of cache quality. Whole-runtime memory, metadata
objects, cloud failure injection, normal query routing and scheduler integration
remain outside this proof. Local controlled failure/cancellation checks remain
separate evidence. Benchmark lint passes; no production code changed in this step.

### Cache-enabled group validation exposes transport extent undercount (November 12, 2026)

The remote group benchmark now optionally uses a root-budgeted shared 4 KiB range
cache, 512-byte coalescing, distinct wrappers per job, and cold/warm waves. It also
checks the ordinary-reader oracle against the fixture generator's independent
expected event_id/amount values. The first cache-enabled execution fails safely
with `Allocation exceeds admitted stage extent`: nullable output requires 384
bytes but only 198 remain. There is no successful cache-group artifact; the empty
redirected output file was removed rather than presented as evidence.

Inspection identifies the missing contract. `withOwnedRangeCache.sliceOwned`
reserves a copied output slice, then its detached flight consumes admission for
the fetched backing. The group currently counts one encoded body and lets
transport consume the same admission ledger as decode. Consequently cold cache
transport can consume capacity intended for the final vector, even though an
uncached exact read passes. Optional coalescing can also increase input size.

This is an actual remote failure, not a hypothetical limitation. Group admission
must incorporate adapter-declared transport workspace and isolate transport from
decode capacity. Existing-input cache pins also need deliberate treatment alongside
new-allocation admission. Simply increasing a query-specific constant or disabling
cache reuse would not satisfy the shared accounting/parallel-range contract. Normal
scan routing remains intentionally unconnected until this prerequisite is resolved.
The benchmark changes pass lint; no production fix or successful cached-group run
is claimed in this checkpoint.

### Declared transport extents isolate cache workspace from decode (November 12, 2026)

Owned buffers now optionally declare `ownedReadAllocationBytes(start,end)` for
exact-range encoded storage. Unknown adapters decline grouped planning. The store
adapter includes its returned range plus a possible contiguous-buffer copy; the
range-cache adapter composes copied output with its source's coalesced-read bound.
Read-scope wrappers preserve the capability. Group plans include this extent and
partition a transport child stage separately from decode before I/O, so optional
read-ahead cannot spend vector capacity. Execution revalidates the transport
capability/descriptor before dispatch.

The previous remote cache failure is resolved. Eight cold/warm public-R2 cases
(one/four jobs, read limits one/two) pass with exact rows, concurrency bounds and
zero per-job charge after cleanup. Shared caching reduces four-job cold reads from
48 uncached to nine, and warm reads to one. The remaining warm 512-byte read is not
claimed as full reuse; conservative admission/cache pin pressure needs further
measurement. Cache retains 2,838 bytes after each wave and releases to zero on
eviction. Largest reported pool peak is 16,342 bytes within 20,480. Peaks in the
warm report are cumulative pool high-water marks, not isolated warm-wave peaks.
Evidence: `cloud-numeric-groups-cache-2026-11-12.json`.

A local real-fixture regression verifies cold copy/workspace admission, distinct
wrappers sharing cache backing, zero additional warm reads with sufficient
headroom, unknown-capability refusal, and final eviction cleanup. All 1,727 default
tests pass (363 skipped), as do TypeScript and changed-file lint.

The store adapter's declaration concerns exact-range responses and contiguous
conversion. Unknown-length stream growth, oversized/full-response fallback or
custom transport workspace can still exceed that declared admitted extent and
fail safely; this is not a complete success guarantee for arbitrary ObjectStore
implementations. Conservative transport reservations also increase grouped peak
admission compared with the earlier single-buffer assumption, so the older raw
1,328-byte replay is not a current-revision guarantee. Measured transport plans,
cache-pin admission efficiency and normal scan routing remain outstanding.

### Existing-input credit preserves warm cache reuse (November 12, 2026)

A control run with 8 KiB per-job headroom eliminates the remaining warm read,
confirming admission pressure rather than cache-key mismatch; evidence is
`cloud-numeric-groups-cache-headroom-2026-11-12.json`. The benchmark now records
individual requested ranges as well as aggregate bytes/counts.

`MemoryScope.pinExisting` can exchange sufficient admitted input capacity for an
independent existing-backing pin. The original backing remains pinned throughout,
and no await separates releasing reserved capacity from acquiring the job pin.
This avoids charging both unused transport-input reservation and cached input.
Same-budget ownership is simply retained. When remaining admission is smaller than
the input, the pin uses separately available budget, preserving the existing
contract where stages admit only new output and inputs are charged separately.
Foreign pools are rejected before changing admission. Callers own and release the
returned pin independently, including through asynchronous cancellation.

Range-cache hits and shared pending-flight handoffs now use this operation. The
first implementation required remaining admission for every cross-job pin and
failed the existing delayed-scratch/canceled-initiator regression; the final
implementation preserves separately budgeted input pins, and that regression
passes. New scope tests verify exact charge transfer, retained input after scope
and cache closure, insufficient-credit behavior and foreign-pool rejection.

At the original 4 KiB job limit, all eight real-R2 cache scenarios now pass with
8 cold reads and zero warm reads, including four jobs sharing wrappers/cache.
The largest four-job cumulative pool peak falls from 16,342 to 13,270 bytes;
retained cache remains 2,838 bytes and eviction releases the pool completely.
Evidence: `cloud-numeric-groups-cache-pin-credit-2026-11-12.json`. These are range
and allocation observations, not a statistically controlled throughput claim.
All 1,729 default tests pass (363 skipped), all 24 workerd tests pass, and TypeScript
and changed-file lint pass. Normal scan routing and whole-runtime coverage remain
open; this fixes a shared ownership prerequisite rather than completing the goal.

### V1 numeric header bounds and grouped capability (November 12, 2026)

Header planning now supports flat numeric V1 PLAIN/BSS pages under uncompressed,
built-in Snappy and built-in ZSTD codecs. V1 null counts live inside the body, so
the estimate reserves possible validity, full-count value storage, decompression,
conversion and nullable expansion rather than assuming all rows are present.
Custom codec and unsupported shape contracts still decline. Grouped source
preparation accepts V1 row counts, and grouped execution can use this conservative
plan when a precise compressed-body plan is unavailable.

The new V1 execution path preserves non-null vectors that alias decompressed
scratch by retaining that backing before closing scratch scope. Nullable expansion
and decimal conversion retain their independent outputs. Eighteen synthetic
codec/null-mask/decimal combinations test all-present, mixed-null and all-null
operation under the admitted bound, retained output after scope closure and zero
charge on release. These are contained in three parameterized tests.

`bench/v1-numeric-header-fixture.mjs` also validates actual nullable/nonnullable
Impala V1 id pages against the ordinary reader, and validates locally recompressed
Snappy/ZSTD versions of those same encoded bodies. Evidence:
`v1-numeric-header-fixture-2026-11-12.jsonl` (six cases). Compressed cases are not
independently produced compressed Parquet files. The harness handles the corpus's
case-sensitive ID/id names and explicitly owns encoded input; its initial assertion
incorrectly expected an aliased uncompressed vector to own borrowed input by itself.
The corrected harness retains possible input aliases and releases all charges.

All 1,732 default tests pass (363 skipped), TypeScript and changed-file lint pass.
ZSTD header-only bounds remain conservative (about 62 KiB workspace even for tiny
pages); measured body planning and smart routing remain important. Dictionary,
nested and other logical/encoding capabilities, normal scan routing, cloud V1
coverage and whole-runtime memory bounds remain incomplete.

### Grouped numeric source completeness (November 12, 2026)

Integration review found that equally truncated column sources could finish together
and silently return a prefix of an explicit requested row range. Alignment now
rejects premature exhaustion when an explicit row end remains unsatisfied. The
header-only column source independently verifies that its physical chunk covers
the requested end, including exhaustion without another page header. Unspecified
alignment ends retain their existing stream-until-exhaustion contract, and empty
requested windows still perform no reads.

Two real-writer fixture regressions cover equal exhaustion after a decoded batch
and a chunk extent truncated at its first actual page. The first test retains the
output through the failure and scope disposal, verifies its rows remain readable
and charged, then verifies release returns accounting to zero. The second checks
that header traversal rejects the incomplete column and releases its probe memory.
All 10 grouped numeric tests pass; TypeScript build and changed-file lint pass.
These are local owned-store fixtures, not new cloud transport evidence. Normal
scan routing, scheduler integration, measured planning, and whole-worker bounds
remain open prerequisites to completion.

### Real-cloud grouped failure settlement (November 12, 2026)

`bench/cloud-numeric-group-failures.mjs` reads actual public R2 numeric page bodies
and asserts HTTP 206 responses and Range requests. Two deterministic cases delay
adapter settlement after both responses arrive: one peer throws, or the caller
cancels the scope. Both verify cancellation reaches both transport scopes, group
execution remains pending until delayed settlement, the first peer error is
preserved, and all accounting returns to zero afterwards.

Evidence: `cloud-numeric-group-failures-2026-11-12.json`. Each case performs two
real body requests (186 and 198 bytes) with a 1,712-byte admitted/peak bound. Peer
failure retains the remaining response until settlement (186 bytes in the saved
artifact, 198 in the preceding run, depending on response arrival order); caller cancellation
retains both bodies (384 bytes). The harness holds an independent settlement pin
as required for an adapter that deliberately keeps a response after caller-scope
cancellation. Its initial version omitted that pin and correctly failed its
retention assertion; adding the pin fixes the test adapter's lifetime contract,
not a production reader change. A watchdog bounds the two-response rendezvous.

The final harness run and changed-file lint pass. These are post-response adapter
failure/cancellation cases over real cloud bytes, not mid-stream network failures
or server outages. Metadata, oracle-independent planning objects, and JS heap are
outside the measured allocation scope. Normal scan routing still needs decoded
cache reuse and measured-vs-conservative admission decisions; this evidence does
not establish those integrations or whole-worker memory bounds.

### Grouped decoded-page cache ownership and cloud tradeoff (November 12, 2026)

The aligned numeric reader now accepts a decoded cache plus object path and row-group
origin, deriving the same identity keys as ordinary page reads (including size,
etag, column, absolute rows and body extent). It pins hits directly to the job budget
before admitting only missing pages. Independent per-page owners allow cache eviction,
early iterator closure and consumer output retention without uncharging live backing.
Failed missing-page execution releases already acquired cache pins. Decoded row counts
are validated before publishing any new cache entries. Closed scopes cannot consume
an all-cached replacement group. Cache admission remains governed by the existing
DecodedColumnCache policy; this change does not force decoded caching by default.

Three additional real-writer tests verify cold/warm reuse (warm job cap exactly 832
retained bytes with no body requests), output survival after eviction and closure,
cached-peer cleanup on an uncached transport failure, and rejection without cache
publication for inconsistent declared row counts. All 13 grouped tests pass. The
complete default suite passes 1,737 tests (363 skipped); TypeScript build and changed
file lint pass. The first sandboxed suite could not bind localhost HTTP fixture
servers; the successful rerun had the required local-network permission.

The public R2 benchmark now supports LAKEQL_GROUP_DECODED_CACHE=1. All eight cloud
cases pass with four-job peaks at 13,260 bytes, bounded by the 20,480-byte pool, and
zero remaining charge after eviction. Evidence:
`cloud-numeric-groups-decoded-cache-2026-11-12.json`. The explicit latency policy
allows decoded outputs into the same 4 KiB cache as encoded ranges: this produces
8 cold and 6 warm reads, compared with 8 cold and 0 warm reads for range-only caching.
Decoded products compete with useful encoded ranges in this deliberately small pool;
this result is a tradeoff, not a throughput improvement. The existing balanced policy
does not admit decoded work products at that cache size. Automatic routing must
preserve that policy and evaluate reuse rather than unconditionally caching decoding.
Normal scan integration, measured planning, scheduler integration and whole-worker
memory verification remain open.

### Cached grouped cancellation boundaries (November 12, 2026)

Added three real-writer fixture cases for cancellation before first iteration,
during prepared-source advancement, and between batches from an already decoded
page. Warm reads use an 832-byte job cap equal to cached backing, and a transport
that rejects any unexpected request. All cases abort without body reads, release
job pins and close started sources. In the between-batches case, cache eviction
and scope cancellation preserve the caller's first output and its 832-byte backing
charge until explicit release; no second output is yielded. The existing scope
checks and ownership transfer satisfy these cases without a production change.

All 16 grouped numeric tests pass, as do TypeScript and changed-file lint. This
adds cancellation evidence for decoded cache hits; it does not establish normal
scan routing, scheduled admission, remote stream interruption, or whole-runtime
memory bounds. The last complete default run remains 1,737 passing tests before
these three additional focused cases.

### Repository release-gate refresh (November 12, 2026)

Verified the current worktree with the following completed commands:

- `pnpm lint`: passes after formatting 13 benchmark JSON artifacts; values unchanged.
- `pnpm build` and `pnpm typecheck`: pass.
- `pnpm check:compatibility`: passes.
- `pnpm check:large-files`: passes with permission to invoke read-only `git ls-files`;
  the first restricted run failed to spawn git and was not a code failure.
- `node scripts/check-fixtures.mjs`: passes; this regenerates fixtures and compares
  before/after SHA-256 snapshots, establishing byte-for-byte reproducibility.
- `pnpm check:packages`: passes, including tarball installation in a temporary
  consumer and its runtime/type smoke checks.
- `pnpm test:workerd`: all 24 tests pass across eight files.
- `pnpm test:reference`: all 104 tests pass across four files.
- `pnpm test:conformance`: remains failing, 167 failures / 83 passes / one skipped.

The conformance runner recursively asks every external `.parquet` file to decode
successfully, including `bad_data` fixtures. Current failures also include valid
codec/logical-type fixtures and shredded variants. Thus this gate needs explicit
fixture correctness contracts plus implementation investigation; neither blanket
skipping failures nor treating malformed-input rejection as successful decoding is
an acceptable resolution. This refresh does not attribute all failures to the
performance changes or establish that they are harmless. Complete coverage and
conformance resolution remain outstanding, alongside the implementation gaps above.

The saved cloud failure artifact was also reconciled with prose: the peer retaining
186 versus 198 bytes depends on which actual response arrives first. Both runs
prove the same settlement invariant; the audit now identifies the saved value.

### Conformance root-cause census (November 12, 2026)

Added `bench/parquet-conformance-diagnostics.mjs` and saved its complete local
public-reader results in `parquet-conformance-diagnostics-2026-11-12.json`.
Every external Parquet file is attempted, with SHA-256 identity, byte size,
row count on successful decoding, and recursively unwrapped error details on
rejection. No input is skipped and no rejection is labelled expected. The census
contains 243 files: 76 decode and 167 reject (the separate conformance suite's
other tests account for its higher aggregate pass count).

144 rejections originate in the blanket struct-column guard in `schema.ts`.
Other root causes include GZIP (5), LZ4 (3), LZ4_RAW (2), BROTLI (1), decimal
precision (1), BSON (1), and individual malformed-schema/encoding/range failures.
The report records exact file membership rather than inferring validity from an
error category. Struct guard failures do not prove the underlying variant/nested
values can decode correctly; removing the guard alone would not establish support.
Durable resolution requires generic nested shape capability and accounted backing,
then value-level fixture assertions, rather than accepting arbitrary object arrays.
The diagnostic script and artifact pass formatting/lint. Successful decoding here
only establishes operation, not value correctness or bounded-memory operation.

### Shared scheduling inside grouped alignment (November 12, 2026)

Aligned numeric replacements now accept the core shared MemoryStageScheduler.
After decoded-cache hits are pinned to the job, only missing pages request FIFO
whole-group admission. Transport slots are acquired after admission. Existing stage
admission is consumed directly rather than recursively queuing the same job. Core
exports the scheduler contract for cross-package use; no user-facing query default
is changed by this internal integration.

Scheduled completion deliberately does not combine decoded pages into one owner.
Independent page owners survive stage closure and allow alignment to retire each
exhausted page separately. If cancellation wins after decoding but before scheduler
completion publication, the alignment adapter releases all independent results.
Previously retained peers and cached inputs remain charged to the job while waiting.

Three new real-writer fixture tests prove: competing jobs queue without dispatching
body reads; queued cancellation performs no body reads and cleans ownership; and
alignment inside an existing scheduled stage consumes its admission without nested
queueing. Retained first-job output survives both iterator closures and both job
scope closures until explicit release. All 19 grouped tests pass. Full default
validation passes 1,743 tests (363 skipped), with TypeScript and changed-file lint
also passing.

`LAKEQL_GROUP_SCHEDULED=1 LAKEQL_GROUP_CACHE=1` exercises the shared scheduler in the
real-R2 grouped benchmark. All eight scenarios pass: eight cold reads and zero warm
reads, with four-job cumulative tracked peak 13,270 bytes within 20,480 bytes.
Eviction returns the root to zero. Evidence:
`cloud-numeric-groups-scheduled-2026-11-12.json`. The local fixture explicitly forces
queue contention; the remote matrix demonstrates real transport/cache operation
but does not claim each remote run queued. Normal query routing, measured planning,
full allocation coverage and whole-worker memory limits remain incomplete.

### Dynamic measured admission for tight numeric groups (November 12, 2026)

Grouped alignment now measures owned page bodies when the conservative header
bound cannot fit the job's remaining capacity. Ordinary shared-pool contention
still goes through FIFO scheduling; it does not trigger speculative measurement
merely because other jobs are active. Existing admitted stages keep their original
contract. No new user configuration is needed for this internal policy.

Measurement reads inputs sequentially through individually admitted transport
extents, releases unused transport reservations, and retains encoded backing while
planning actual numeric decode work. The precise decode group is then admitted
(directly or through the scheduler) with inputs still charged separately. V1 pages
without a precise body planner retain their conservative decode estimate. Borrowed
input aliases transfer into independent output ownership; all measurement inputs
are released on success, decode rejection, cancellation, or later transport failure.
The measured plan has an explicit disposal lifetime and rejects execution after
its source scope closes or against a different job budget.

Real locally generated ZSTD fixtures establish both sides of the tradeoff: a
one-row pair whose header bound exceeds 60 KiB succeeds under 8 KiB with and
without scheduling; a 64-row compressed fixture still needs more workspace and
correctly rejects under the same cap without retaining inputs. Exact observed
peak replay succeeds, and one byte less rejects with zero remaining charge. A
second-input transport failure releases the first input. All 24 grouped tests
pass, including the five new cases; TypeScript and changed-file lint pass.
These are local owned-store cases, not evidence that a remote ZSTD fixture took
the measured branch. Sequential measurement trades parallel transport for a
smaller verified decode bound and remains bounded if the actual workspace does
not fit. Normal query routing and whole-worker allocation coverage remain open.

The complete default suite after this change passes 1,748 tests (363 skipped).

### Ordinary owned scans use grouped numeric admission (November 12, 2026)

Normal vector scans now route compatible multi-column projections through grouped
page preparation, shared FIFO admission, measured fallback and decoded-cache reuse.
Eligibility requires owned transport with a declared extent, iteration-scoped
allocation ownership, flat numeric physical/logical capabilities, built-in codecs,
and metadata declaring only supported encodings with no dictionary dependency.
Other projections keep their existing decoding contracts. Existing row-group
pruning and row-window offsets are preserved, as are decoded cache identity,
cache hit/miss metrics and synchronous decode timing. Yielded groups are adopted
into the caller scope until advance, allowing existing public owners to retain them.
A pool-keyed scheduler is shared across these Parquet scans.

Initial full-suite evidence showed that grouping a single column replaced efficient
whole-chunk reads with header/body requests and altered its established allocation
behavior. The final planner keeps adaptive single-column reads: no peer replacement
contention exists there, and existing precise per-page admission remains available.
Grouped coordination is selected when multiple compatible columns benefit from
joint admission and parallel demand reads. The synthetic adapter that adds 2 KiB
workspace now declares that workspace in its owned-read capacity contract.

Owned row-group prefetch and demand-page concurrency are separate internal settings;
forcing the inner prefetch window to one no longer forces grouped demand reads to
one. Caller read limits remain enforced by the ObjectStore. A new ordinary-reader
fixture proves concurrent body dispatch, partial row offsets, early closure and
retained output after scope disposal. All 1,749 default tests pass (363 skipped),
TypeScript and changed-file lint pass.

`LAKEQL_GROUP_NORMAL_SCAN=1 LAKEQL_GROUP_CACHE=1` now runs the benchmark through
`readOwnedParquetVectorBatches`, rather than invoking the internal alignment reader.
All eight real-R2 scenarios pass: one/four jobs, read limits one/two, eight cold
reads and zero warm reads. Peak active reads match the configured limits. The
largest cumulative tracked peak is 14,806 bytes within the 20,480-byte pool;
cache eviction releases the pool completely. Evidence:
`cloud-numeric-normal-scans-2026-11-12.json`. This establishes ordinary owned Parquet
scan integration, not complete query-operator scheduling or whole-worker heap
coverage. Mixed/nested/dictionary capabilities and the conformance gaps remain open.

Workerd validation after ordinary scan routing also passes all 24 tests.

### Read-concurrency validation independent of routing (November 12, 2026)

Centralized validation of prefetch and demand-page concurrency before reader
routing, cache lookup or transport. Both ordinary owned/borrowed vector readers
now reject invalid demand-page limits even when a single-column fallback bypasses
grouping. Grouped alignment also validates concurrency before empty-source or
cache-only short circuits. Previously those paths could silently accept a bad
limit because only group execution validated it.

Six invalid values (zero, negative, fractional, NaN, infinity and unsafe integer)
are exercised through owned and borrowed readers with and without early stop,
using transports that fail if any I/O occurs. A separate case covers empty grouped
sources. All 79 tests across owned-vector and grouped-reader suites pass; TypeScript
and changed-file lint pass. Valid routing and default concurrency are unchanged.
The full-goal implementation and release gaps documented above remain open.

### Measured chunk-versus-page routing tradeoff (November 13, 2026)

`bench/owned-numeric-scan-routing.mjs` compares ordinary owned scans with and
without the grouped transport capability using actual 65,536-row writer fixtures,
UNCOMPRESSED/SNAPPY, full/1,024-row scans, alternating execution order, two warmups
and eight measured samples. Every row is checked; timings include materialization
and assertions. This is an in-memory owned store, not disk or remote latency.

The initial unconditional multi-column page policy reduced full-scan tracked peaks
from 634,634 to 63,316 bytes (uncompressed) and 475,748 to 55,196 bytes (Snappy), but
increased range requests from 4 to 154 and local median elapsed time by about 27%
and 31%. Evidence: `owned-numeric-scan-routing-2026-11-13.json`. Early scans used the
same eight requests but parallel admission increased tracked peaks. These are real
tradeoffs, not a universal improvement.

Routing now retains whole chunks for full scans when an estimate covering selected
encoded/uncompressed bytes and full-width converted rows fits half the currently
available budget. Partial/early scans and memory pressure retain grouped pages.
This estimate chooses a path; it never exempts reads or decodes from allocation
admission. It considers all selected columns together, keeping headroom for other
work. A real-fixture regression verifies identical rows, fewer requests with ample
memory, and lower peak allocation under a tight cap.

Evidence after the policy change:
`owned-numeric-scan-routing-adaptive-2026-11-13.json`. Full scans return to four
requests and the former chunk peaks. Local paired medians remain mixed:
uncompressed previous/automatic 6.19/6.88 ms, Snappy 6.54/6.26 ms; early scans
0.325/0.363 and 0.329/0.376 ms. No general throughput improvement is claimed.
Transport latency-aware policy and further planner overhead reduction remain
worth investigating.

All 1,757 default tests pass (363 skipped), including 27 grouped-reader cases;
TypeScript and changed-file lint pass. Real-R2 ordinary scans also pass all eight
scenarios after the policy change, with eight cold/zero warm reads, concurrency
within limits, peak at most 14,806 bytes under 20,480, and zero after eviction.
Evidence: `cloud-numeric-normal-scans-adaptive-2026-11-13.json`. Whole-worker and
remaining operator/shape coverage are still incomplete.

### Real-cloud response consumption failures (November 13, 2026)

Extended `bench/cloud-numeric-group-failures.mjs` beyond post-response adapter
settlement delays. Two new cases wrap an actual public-R2 HTTP 206 response body,
deliver its first 99 of 198 expected bytes, then inject a stream error or premature
EOF. Metadata and prepared page headers are obtained independently; object discovery
requests are reported separately from the two grouped page requests.

Both cases preserve the complete 1,712-byte group admission bound and release all
tracked backing after execution settles. The injected stream error is preserved;
short input produces `LAKEQL_PARQUET_READ_ERROR: Incomplete Parquet page body`.
The upstream response reader is canceled in both cases. Existing delayed-adapter
peer-failure/caller-cancellation cases still pass in the same run. Evidence:
`cloud-numeric-group-stream-failures-2026-11-13.json`; benchmark/artifact lint passes.

These are real remote bytes with deterministic interruptions while consuming the
response, not observations of a live server outage or proof of every network
failure mode. They exercise the actual HTTP owned-stream and grouped cleanup path.
No production decoder change was needed. Whole-runtime accounting, complete query
operator scheduling and remaining shape/codec/conformance work are still open.

### Coverage refresh and measured-plan ownership checks (November 13, 2026)

`pnpm coverage` completes successfully over the 1,757-test default suite (363 skipped):
90.91% statements (22,552/24,805), 84.69% branches (16,906/19,962), 95.16% functions
(3,815/4,009), and 92.98% lines (19,968/21,475). Coverage highlights error guards in
measured planning that had not been exercised, rather than establishing complete
allocation coverage by itself.

Four subsequent real-writer cases validate uncompressed/Snappy output retention
after measurement-plan and source-scope disposal, rejection of a different job
budget without disturbing input charges, and rejection after explicit plan or
source-scope disposal. Inputs remain independently charged until the measured plan
is disposed; decoded output remains charged until its own owner is released.
All 31 grouped-reader tests, TypeScript and changed-file lint pass. No production
change was necessary for these lifecycle contracts. The document's opening status
has been reconciled with current evidence instead of leaving the old 1,711-test
checkpoint as its apparent current state. Whole-query/whole-runtime completion
remains explicitly unproven.

### Planned boolean backing and external fixture gap (November 13, 2026)

Extended the shared flat-value planner to BOOLEAN PLAIN and RLE. Plans distinguish
bit-packed encoded PLAIN bytes from one-byte-per-present-value decoded storage,
validity and nullable expansion. Boolean values always own expanded storage;
they never claim an input alias. V1/V2 paths retain the boolean vector tag through
normalization and scheduled/grouped output handoff. Mixed boolean/numeric ordinary
scans can now use the same explicit bounded capability and dynamic routing.

The real-writer header matrix now includes booleans for UNCOMPRESSED, SNAPPY and
ZSTD with all-present, mixed-null and all-null shapes and aligned/unaligned input.
Two additional parameterized tests cover V1/V2 × PLAIN/RLE × three codecs × three
null masks (36 combinations), including output retention after scope closure.
Three real-writer mixed-column scans verify partial row windows and unequal page
alignment under the three codecs. The complete default suite passes 1,766 tests
(363 skipped); TypeScript and changed-file lint pass.

Independent external validation is explicitly incomplete. The Apache fixture
`data/rle_boolean_encoding.parquet` has GZIP values and a two-byte repetition-level
section on its flat column. DuckDB reads 68 rows, while the owned reader rejects
its level layout before reaching the missing built-in GZIP capability. The new
`bench/boolean-owned-fixture.mjs` diagnostic records this failure for batch sizes
1, 7 and 1,024, preserving zero remaining allocation charge. Evidence:
`boolean-owned-fixture-2026-11-13.json`. Diagnostic rejection is not classified as
an expected pass, and no conformance assertion was weakened. This real-file gap
requires a checked flat repetition-level contract plus bounded codec support.
The generated-codec results above do not substitute for resolving that external
fixture or establish remote boolean operation.

### Checked explicit flat repetition sections (November 13, 2026)

Flat V2 value decoding and planning now include the repetition section when
computing level/value offsets and uncompressed payload extents. Zero-bit repetition
runs are validated against the exact row count and section consumption, without
allocating a level array. Constant bit-packed zero runs are visited once per run
rather than once per row. Nested repetition domains still require their separate
shape capability; this does not flatten nested data.

Tests cover explicit RLE and bit-packed zero runs, zero-length runs, overrun,
trailing encoded bytes and truncated varints. Invalid repetition data fails before
value/validity allocation. All 1,771 default tests pass (363 skipped), TypeScript
and changed-file lint pass. An initial malformed-input test incorrectly used a
Vitest tuple table as an array argument; the corrected object table exercises all
four malformed byte sequences.

The external Apache boolean diagnostic now progresses past level-layout validation
and fails specifically at unsupported GZIP decompression for all three batch sizes.
Evidence: `boolean-owned-fixture-levels-2026-11-13.json`; DuckDB still supplies the
68-row reference and all failed owned runs clean up to zero. This is progress in
the real fixture's decode path, not a successful complete fixture read. Bounded
GZIP and remaining end-to-end memory/shape contracts remain outstanding.

### Portable bounded GZIP and external fixture recovery (November 13, 2026)

Added a portable GZIP/DEFLATE decoder with caller-owned exact output and a fixed
2,152-byte scratch buffer. It uses canonical Huffman counts/symbol tables, bounded
code lengths and offsets, and a CRC32 table in that scratch. Stored/fixed/dynamic
blocks and concatenated members reuse the same backing; history references read
already written output and cannot reach before the current member. No growing
output or per-block backing allocation is used. Format references:
[RFC 1951](https://www.rfc-editor.org/rfc/rfc1951) and
[RFC 1952](https://www.rfc-editor.org/rfc/rfc1952).

Checks include optional header fields/header CRC, reserved flags/block/symbol
values, truncated bits/runs, incomplete or oversubscribed Huffman trees, output
capacity, member CRC/size, exact final size and overlapping caller buffers. Scoped
Parquet compression admits scratch plus output before allocation, releases scratch
on completion/failure, and preserves output ownership. Flat numeric/boolean header
and measured plans now include the GZIP workspace; compatible multi-column scans
can use existing automatic routing. Fixed JavaScript closure/view overhead remains
outside typed-backing accounting and is not a whole-heap claim.

Node-generated fixtures cover six sizes, two data patterns, four compression
strategies and three levels (144 combinations), plus concatenated members,
optional headers, every truncation of a sample, corrupt checksums/sizes, short
output sentinels, exact capacity/cap-minus-one, and invalid cross-member history.
A corruption test initially used Buffer.slice, unintentionally mutating its source;
independent Uint8Array copies correct that harness issue. The writer-derived page
planning matrix now includes GZIP. A static compressed fixture verifies exact
scoped admission in workerd without using a Node codec to decode.

All 1,779 default tests pass (363 skipped); all 25 workerd tests pass; TypeScript
and changed-file lint pass. The actual Apache RLE-boolean fixture now matches all
68 DuckDB reference rows at batch sizes 1, 7 and 1,024, with peak tracked backing
2,409 bytes and zero remaining charge. Evidence:
`boolean-owned-fixture-gzip-2026-11-13.json`. These are local external-fixture reads,
not cloud or whole-worker memory measurements.

The refreshed external corpus diagnostic decodes 81/243 files, up from 76, and
records the remaining 162 rejections in
`parquet-conformance-diagnostics-gzip-2026-11-13.json`. The actual conformance gate
remains failing at 162 failures / 88 passes / one skipped. Five former GZIP failures
now decode, including concatenated members and the boolean file; decoding alone
is not a value oracle for every file. Struct/variant, other codecs and the rest of
the complete allocation contract remain unfinished.

### GZIP scaling and broader external value checks (November 13, 2026)

`bench/gzip-bounded-scaling.mjs` measures five output sizes (1 KiB through 4 MiB)
with repetitive and deterministic random bytes, two warmups and five recorded
samples. Encoded input is independently owned and charged; decode reserves exact
output plus the fixed 2,152-byte scratch. Every run asserts peak equals input +
output + scratch, completed scratch is released, output survives source/scope
closure, and final ownership release returns to zero. Source generation and
reference storage are outside the measurement. Decoder source SHA-256 is recorded.
Evidence: `gzip-bounded-scaling-2026-11-13.json`.

Measured medians at 256 KiB / 1 MiB / 4 MiB are 1.11 / 4.36 / 17.10 ms for runs
and 1.14 / 4.49 / 18.03 ms for random input. This is approximately linear over
those sizes; scratch remains fixed. The largest tracked peak, random 4 MiB output,
is 8,392,063 bytes including encoded input. These local Node measurements exclude
JS/runtime heap and make no cloud-latency or whole-worker-cap claim.

`bench/gzip-owned-fixtures.mjs` compares five actual external files through the
owned reader at batch sizes 1 and 127. Concatenated GZIP members, indexed/bloom
metadata, the unusual map-schema fixture and RLE booleans match DuckDB values in
all eight corresponding runs, with zero remaining tracked charge. The extended
BYTE_STREAM_SPLIT fixture decodes in LakeQL, but the installed DuckDB rejects its
non-FLOAT/DOUBLE BYTE_STREAM_SPLIT types, so both runs report `reference-error`
rather than a false successful comparison. Evidence:
`gzip-owned-fixtures-2026-11-13.json`. The first harness version attempted the
comparison after reference failure; the corrected harness skips only that absent
comparison and preserves the explicit reference-error status. Benchmark and
artifact lint pass. Remaining whole-query and external-conformance requirements
are unchanged by these bounded codec measurements.

### Decimal oracle discrepancy and checked integer scaling (November 13, 2026)

The extended BYTE_STREAM_SPLIT file documents identical PLAIN twins for each
encoded column. The fixture harness now projects only those PLAIN twins through
DuckDB (casting its decimal to DOUBLE), then compares both LakeQL representations
against that independent reference. This avoids DuckDB's unsupported encoding
without substituting LakeQL's own decoder as the oracle. Evidence:
`gzip-owned-fixtures-plain-reference-2026-11-13.json`. Both batch sizes expose a
value mismatch: `1003.8580000000001` versus `1003.858`. The comparison retains
exact numeric equality; no tolerance conceals the discrepancy.

Inspection also found the same double-rounding mechanism in checked integer
DECIMAL conversion: multiplication by an already rounded `10^-scale`. Integer
conversion now divides by exact powers through `10^22`; larger scales use bounded
scientific decimal strings, avoiding an inexact or overflowing divisor. Unscaled
integers still require exact safe-integer representation. Invalid scales fail
before output allocation. Tests compare positive/negative values, zero, the safe
integer boundary, scales through subnormal/underflow ranges and the maximum safe
integer scale against decimal parsing; malformed scales leave peak charge zero.
Seven decimal tests and 46 numeric page/planning tests pass; Parquet TypeScript
build and changed-file lint pass. The requested decimal-fixtures test filename
does not exist; the latter command ran the two actual numeric test files only.

This corrects the checked integer conversion contract but does not yet correct
the byte-backed DECIMAL path used by the external fixture. Unifying that path,
its output allocation accounting and metadata/statistics conversion remains
required; the fixture mismatch is still open. These tests establish conversion
correctness and allocation rejection, not a new cloud performance result.

### Shared checked conversion for byte-backed decimals (November 13, 2026)

Decimal conversion now shares the checked scaling contract across INT32, INT64,
BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY leaves. Byte values use signed big-endian
safe-integer accumulation, rejecting overflow without constructing an input-sized
BigInt or decimal string. Top-level object/column adapters strip decimal annotations
before upstream decoding and normalize through the same converter; vector decode
also uses it. Dictionary lookup preserves already converted dictionary values.
Integer-only numeric planner eligibility remains separate from decimal semantics.
Dense scoped byte-decimal output is charged before Float64Array allocation;
nullable fallback arrays and upstream byte-view arrays remain part of the broader
unaccounted JavaScript allocation work, not a complete memory-bound claim.

Nine decimal tests cover sign extension, both byte physical types, positive and
negative safe boundaries, empty/overflow bytes, null preservation, exact output
capacity and cap-minus-one rejection. Parquet TypeScript compilation passes.
The refreshed local external fixture comparison now matches all ten runs,
including all 200 extended BYTE_STREAM_SPLIT rows at batch sizes 1 and 127:
`gzip-owned-fixtures-decimal-2026-11-13.json`. All tracked charges release to zero.
This closes the recorded fixture rounding mismatch without numeric tolerance;
it does not establish remote/cloud behavior or complete nested decimal support.

The initial full-suite run encountered sandbox-denied local HTTP listeners and
CLI subprocess failures (9 failed / 1,773 passed); it is not a clean validation
result. A rerun with local listener/subprocess access is being checked separately.

The unrestricted rerun passes all 1,783 default tests (363 skipped), including
local HTTP and CLI tests. Changed-file/artifact lint also passes. Workerd,
reference, external conformance and complete release gates still need refreshing
for this change; the whole-query allocation objective remains incomplete.

### Decimal statistics capability and runtime/reference refresh (November 13, 2026)

The decimal changes pass all 104 independent reference tests and all 25 workerd
tests. Inspection found a separate pruning correctness boundary: upstream
metadata converts FIXED_LEN_BYTE_ARRAY decimal bounds with the old rounded
factor, and BYTE_ARRAY bounds through a string decoder, before returning them.
The raw unscaled bytes are no longer available to our metadata normalizer.
Those byte-decimal min/max bounds are now excluded from pruning; null/distinct
counts remain available. Integer-backed decimal statistics continue through the
checked value converter. This is a representation-capability rule: a lossy bound
cannot establish row exclusion under a different conversion contract.

A real extended BYTE_STREAM_SPLIT fixture test verifies that both decimal columns
have original bounds, that all four min/max fields are removed and null counts
are preserved. The test runs when the fetched external fixture is present and
explicitly skips when absent. Both metadata tests pass; Parquet TypeScript builds.
Restoring efficient byte-decimal pruning requires retaining raw statistic bytes
through footer decoding and converting them with the shared checked converter;
it remains an audit item, alongside parsed-metadata allocation accounting. This
change makes pruning conservative rather than claiming that work is complete.
The runtime/reference passes precede this metadata-bound exclusion change.

### Projected raw footer statistics prerequisite (November 13, 2026)

The compact-protocol parser now expresses struct-list projections and typed binary
fields. It checks the list/element wire shape before allocating container output,
projects selected fields within each struct, and borrows binary views from the
existing footer backing. The new FOOTER_STATISTICS_PROJECTION traverses file row
groups, column chunks, column metadata and the four binary min/max fields, skipping
unrelated fields. This avoids a second complete decoded metadata object when
recovering exact decimal bounds, although selected JS containers still require
allocation accounting.

Parser tests verify projected values, skipped fields, borrowed buffer identity,
and rejection of wrong list/element/binary wire types. The real extended
BYTE_STREAM_SPLIT fixture verifies one row group with 14 projected column entries
and footer-backed binary bounds. This is a parser capability prerequisite; the
ordinary metadata reader still conservatively excludes byte-decimal bounds until
raw-bound normalization is integrated. No restored-pruning or whole-heap claim is
made by this step.

### Owned footer decimal-bound recovery (November 13, 2026)

Owned footer reads now recover raw binary decimal bounds from the already-read
footer and apply the same checked converter as values. The projection runs only
when schema includes a non-integer decimal leaf; integer/nondecimal files avoid
the additional traversal. The projection is restricted to the declared footer
metadata extent and must consume it exactly. Missing or unsafe bounds remain
absent rather than reverting to lossy upstream conversion. No binary payload copy
or additional range request is introduced; selected JS containers remain outside
complete allocation accounting.

The external fixture test compares recovered min/max against all decoded values
for both decimal columns, asserts one owned range read, and verifies release to
zero. An initial test wrapper stripped the store's owned-read capability; the
corrected wrapper counts the source's actual sliceOwned calls. Fourteen parser,
metadata cache and owned-footer tests pass, including existing cache eviction,
consumer pinning and tight-budget cases. Parquet TypeScript compilation and
changed-file formatting pass. These are local memory-store tests using a real
file. Borrowed/unowned metadata reads still conservatively omit byte-decimal
bounds; integrating raw footer access there and proving cloud pruning operation
remain outstanding. This step does not complete whole-query accounting.

### Unified footer acquisition and decimal statistics (November 13, 2026)

Owned and unowned metadata acquisition now share the footer reader and raw-bound
normalization. Both retain the 64 KiB initial probe, fetch only the missing prefix
when concatenation is permitted, and use the existing budget-dependent footer
reread when owned headroom cannot admit concatenation. The unowned path retains
its untracked allocation contract; sharing code does not turn it into a budgeted
API. Byte-decimal min/max values now agree across both routes and with all decoded
values in the real external fixture. The owned route still needs one request for
that fixture and releases tracked backing to zero. Invalid file lengths reject
before I/O; bad magic and oversized footer declarations reject after one read.

All 1,786 default tests pass (363 skipped), including local HTTP range tests and
CLI subprocess recipes. All 104 independent reference tests pass. Parquet
TypeScript compilation and changed-file formatting pass. This supersedes the
previous unowned-byte-decimal-statistics omission; dedicated real-cloud decimal
pruning, complete metadata allocation accounting and the remaining audit scope
are still open. No whole-worker memory bound follows from this validation.

### Footer corpus regression and decimal pruning operation (November 13, 2026)

The refreshed external diagnostic remains 81 decoded / 162 rejected over 243
files. Every per-file result, including SHA-256 and failure details, is identical
to the post-GZIP diagnostic: `parquet-conformance-diagnostics-footer-2026-11-13.json`.
The conformance gate still fails with 162 failures / 88 passes / one skipped;
this is unchanged, not an accepted-failure baseline or a completed gate.

The real decimal fixture test now exercises public task planning for both byte
columns: equality at each decoded minimum and maximum retains row-group range
[0,1), while a threshold above maximum produces an empty row-group range list.
The initial assertion expected no file task; the API preserves a file task with
empty rowGroupRanges, so assertions now inspect the actual execution ranges for
both inclusion and exclusion. All three metadata-cache tests pass. These are
local real-fixture pruning checks, not remote object-store evidence. Struct shape
support remains the largest external rejection category (144 files); complete
memory accounting and the remaining audit requirements remain open.

### Incremental footer-statistics projection (November 13, 2026)

Compact struct-list projections now accept an index-dependent selection and an
optional synchronous visitor. A visitor consumes each projected struct without
allocating or retaining the result list. Unselected structs are validated and
skipped without constructing their object trees. Metadata normalization uses
nested visitors for row groups and columns, selects only byte-decimal statistics,
and converts each column's borrowed binary bounds immediately. The projection no
longer retains arrays proportional to all row groups/columns or bound views for
unrelated columns. Input traversal remains linear; existing parsed metadata and
fixed-depth temporary objects still require broader accounting.

Sixteen compact-parser, metadata-cache and owned-footer tests pass. New tests
verify selection by index, visitor ordering, absence of a retained result list,
and immediate propagation of visitor failure before the next entry. Existing
real-fixture decimal min/max, pruning boundaries, one-request reads and ownership
release tests remain green. Parquet TypeScript builds and changed-file formatting
pass. This establishes a structural allocation reduction, not a measured process
heap reduction or cloud-latency speedup; those claims need separate evidence.

### Footer projection scaling measurement (November 13, 2026)

`bench/footer-projection-scaling.mjs` compares retained-tree and visitor projection
on synthetic compact footer structures with 16 columns and 16/128/1,024/4,096 row
groups. Both paths inspect all binary bounds, validate identical counts/checksums,
assert borrowed input-buffer identity and consume the full input. Two warmups and
five recorded samples run under Node with GC between trials; source generation is
excluded. Artifact `footer-projection-scaling-2026-11-13.json` includes parser hash.

Tree/visitor median milliseconds are 0.26/0.31, 2.35/1.90, 10.97/8.66 and
36.64/26.44 respectively. The smallest case has visitor overhead; larger cases
benefit in this local run. Retained projected column objects, counted from the
returned structure, fall from 256/2,048/16,384/65,536 to zero. Visitors still create
short-lived selected objects and views; zero retained results is not zero
allocation. An initial heap-delta experiment was discarded because GC/liveness
made it unsuitable as retained-memory evidence. No process heap/peak or cloud
latency measurement is claimed. Parsed metadata storage, complete query allocation
coverage and remote decimal operation remain outstanding.

### Remove repeated schema-tree construction during statistics normalization (November 13, 2026)

Inspection of hyparquet's getSchemaPath confirmed that every lookup reconstructs
the entire schema tree. Decimal statistics normalization called it for each
column/row group and again during projection, making wide-schema processing
multiply full-tree allocation and traversal. Metadata without decimals now exits
normalization immediately. Decimal metadata builds one physical-leaf index and
uses component-preserving JSON path keys for subsequent lookups. This replaces
repeated full-schema reconstruction with one schema traversal plus path-key work.
Path depth still contributes to key construction; no constant-depth claim is made.

The index uses an explicit stack, preserves dotted names as separate components,
and rejects missing/excess children, duplicate leaf paths and invalid child
counts. Tests cover nested siblings, literal dotted names, malformed trees,
10,000-column width and 10,000-level depth without recursive stack growth. Eleven
schema/metadata/owned-footer tests pass, including real decimal fixture bounds
and pruning. Parquet TypeScript builds and changed-file formatting pass. The
index itself is proportional to schema/path size and still needs inclusion in
complete parsed-metadata accounting. No new benchmark speedup or whole-heap bound
is inferred from these structural tests.

### Measured schema lookup scaling and regression checkpoint (November 13, 2026)

`bench/schema-leaf-scaling.mjs` measures the upstream tree-rebuilding lookup
against the physical-leaf index, including index construction, at widths
64/256/1,024 and one/eight row groups. It alternates execution order, uses two
warmups and five samples, and checks exact schema-element identity on every
lookup (including names containing literal dots). Artifact:
`schema-leaf-scaling-2026-11-13.json`, including Node version and index source hash.

At one row group, tree/index medians are 0.523/0.037, 1.898/0.062 and
31.076/0.237 ms. At eight groups they are 0.976/0.078, 17.198/0.233 and
245.048/0.862 ms. These local lookup measurements expose the repeated-tree
complexity and include one index build per trial; they do not measure complete
footer parsing, cloud latency or heap usage. All 1,790 default tests pass (363
skipped), including local HTTP and CLI coverage.

A follow-up source inventory finds six remaining getSchemaPath call sites in
column-batches/vector-batches. Several require complete SchemaTree paths for
repetition/definition levels and nested decoding, not just physical leaves.
A shared indexed full-path capability is therefore still needed to remove the
analogous scan-setup cost without losing shape information. Complete index and
parsed-metadata allocation accounting remains open.

### Reusable full schema paths for column normalization (November 13, 2026)

Added a full schema-path index that constructs a validated SchemaTree once,
indexes children by component name, and returns only a newly allocated ancestor
array per lookup. Nodes preserve element identity, child ordering, path names
and subtree counts used by nested decoding. Construction uses an explicit stack;
full node paths still cost the sum of node depths, matching the required tree
representation rather than claiming constant space. Duplicate sibling names,
invalid child counts, missing children and excess entries reject explicitly.

Column-batch nested-value normalization now builds this index once per call,
replacing full-tree reconstruction for every selected column. Tests compare
complete trees and ancestor paths with the upstream helper for nested/repeated
and dotted-name schemas, confirm shared node identity, and check invalid trees.
Five targeted schema/metadata tests, all 104 independent reference tests and
Parquet TypeScript compilation pass; changed-file formatting passes. The five
remaining vector-batches lookup sites still need consumer-lifetime integration.
Index allocation accounting and end-to-end scaling remain open; no whole-query
or cloud performance claim follows from the leaf microbenchmark.

### Consumer-scoped vector schema lookup (November 13, 2026)

Vector capability checks, range enumeration and ordinary scans now create a
private shallow metadata context with a lazy full-path lookup. Routing, prefetch,
whole-chunk decoding and page-window decoding share that context within the
consumer. The original/cache metadata is never mutated and no global index cache
extends its lifetime. Separate public consumers may each build an index; the
owned reader's row-group consumer boundaries still limit reuse across those
calls. Full-path index storage remains part of unresolved JS allocation accounting.

All 1,792 default tests pass (363 skipped) before adding the explicit context
isolation regression. That additional test uses frozen caller metadata, checks
capability/range enumeration, scans twice, compares all rows, and verifies zero
remaining charge and unchanged metadata keys. Its first harness invocation used
the wrong owned-reader argument shape; corrected to the actual budget/options
signature. All 54 owned-reader tests then pass. Parquet TypeScript compilation
and changed-file formatting pass. This replaces all five upstream full-tree
lookup sites in vector-batches; it is not proof of complete allocation bounds.


### Cloud and Worker regression after schema indexing (November 13, 2026)

The ordinary-owned-scan benchmark was rerun against the existing public R2
window-events fixture over verified HTTP 206 range responses. All eight scenarios
pass: one/four jobs, read concurrency one/two, cold/full and warm/partial scans.
Cold runs issue eight reads, warm runs zero; observed concurrency equals each cold
limit. Maximum tracked peak is 14,806 bytes within a 20,480-byte shared pool, and
cache eviction returns charge to zero. Artifact:
`cloud-numeric-schema-index-2026-11-13.json`. Values match independently generated
fixture rows. Metadata/oracle allocations and JS heap remain excluded; this is
real cloud transport evidence, not proof of whole-query or Worker heap bounds.
All 25 workerd tests and repository typechecking also pass after schema-index
integration and the caller-metadata isolation test. Opening audit checkpoints
have been refreshed to distinguish these results from older coverage evidence.

### Structural preflight for metadata allocation planning (November 13, 2026)

Added inspectCompactStructure to validate/count compact-protocol structs, fields,
lists/list items, binary values/bytes and maximum depth without retaining the
decoded metadata tree. It reuses the same bounds-checked parser with an empty
projection, so skipped values are still validated and lists/binary payloads cannot
request allocations during preflight. Nine compact-parser tests pass, including
exact nested counts, truncated payload rejection and existing projection tests;
Parquet TypeScript compilation passes.

`bench/footer-structure-census.mjs` ran the preflight over all 243 local external
fixture footers with exact declared-extent consumption. All 243 validate; maximum
observed depth is 9, fields 5,892 and list items 2,074. Per-file hashes/counts are
recorded in `footer-structure-census-2026-11-13.json`. This validates footer
structure only and does not change the failing full-data conformance gate.

The preflight is an allocation-planning prerequisite, not yet wired as a claimed
heap reservation. A defensible retained/transient object-allocation model for
upstream metadata conversion, schema indexes and runtime overhead is still needed
before its structural counts can establish byte admission. Arbitrary multipliers
would not prove the requested whole-Worker bound. Existing JSON cache cost remains
a serialization/eviction cost, not a heap-size oracle.

### Preflight before object allocation and measured overhead (November 13, 2026)

Both metadata acquisition paths now validate compact footer structure before
calling the upstream object parser. The validator sees only the declared metadata
extent and requires exact consumption. Oversized lists/binary payloads, excessive
nesting and trailing metadata bytes fail before upstream decoded-tree allocation.
Owned failures release all tracked backing. Eighteen parser/metadata tests and all
104 independent reference tests pass; Parquet TypeScript builds.

The refreshed corpus diagnostic remains 81 decoded / 162 rejected. One previously
rejected malformed file (ARROW-GH-41317) now reports the struct capability guard
instead of the former schema-path error after the intervening schema changes;
no decoded file became rejected. Artifact:
`parquet-conformance-diagnostics-preflight-2026-11-13.json`. This is still a failing
conformance state, not a complete allocation contract.

`bench/footer-preflight-overhead.mjs` measures local upstream parsing with/without
preflight on three real files, alternating order, two warmups/five samples of 100
parses, with output equality. Baseline/preflight median milliseconds are
0.03269/0.03833 (extended BYTE_STREAM_SPLIT), 0.00404/0.00552 (concatenated GZIP),
and 0.00258/0.00344 (RLE boolean). Evidence:
`footer-preflight-overhead-2026-11-13.json`. The validation has measurable CPU cost;
these tests exclude I/O and LakeQL normalization. Preflight is always enabled
because allocation-driving lengths must be validated before use. Structural
validation still does not reserve a justified byte cost for valid metadata's
JavaScript objects, so that central accounting requirement remains open.

### Single-pass decimal selection and copy avoidance (November 13, 2026)

Decimal column/row normalization previously rebuilt the complete top-level scalar
schema list for every selected column. It now selects decimal leaves once per
call, deduplicates selected names, and creates converters only for those leaves.
Metadata annotation removal copies the schema only on the first required edit;
column normalization copies the values record only when an available decimal
column needs conversion. No-op metadata, columns and rows preserve identity.
Subtree skipping is iterative, eliminating its recursive call-stack growth.

Tests cover duplicate selected names (one scaling operation), nested/top-level
name collisions, no-op identity and source immutability. Thirteen decimal/metadata
tests and all 104 independent reference tests pass; Parquet TypeScript builds and
changed-file formatting pass. The improvement removes repeated schema traversals
and unnecessary copies; selected-entry arrays, upstream parsed objects and full
schema indexes still require the larger allocation-accounting work. This step
does not claim measured whole-query latency or a complete heap bound.

### Eliminate retained quadratic schema path prefixes (November 13, 2026)

The full-path index initially copied each node's complete name path during
construction. An explicit stack prevented stack overflow but did not prevent
quadratic retained path slots on a deeply nested chain. Nodes now retain parent
links and depth; their SchemaTree.path getter materializes an independent array
only on demand, without memoizing every prefix. Node/child lookup and subtree
count semantics remain unchanged. Calling every path getter can still perform
sum-of-depth work; normal decoding need not pay it merely to build the index.

A 10,000-level test checks construction, root subtree count, complete ancestor
lookup and independent path values after caller mutation. All 57 schema/owned
reader tests and 104 independent reference tests pass; Parquet TypeScript builds.
`bench/schema-depth-scaling.mjs` measures construction plus one full-depth lookup
and path materialization: medians 0.098/1.240/15.115 ms at depth 100/1,000/10,000.
Artifact `schema-depth-scaling-2026-11-13.json` records two warmups/five samples.
At depth 10,000 the eager representation would retain 50,005,000 path slots;
the requested leaf path uses 10,000 slots plus linear index nodes/links. These
are structural counts, not JS heap-byte measurements. Runtime object/map/getter
cost and budget admission for indexes remain unfinished.

### Decimal benchmark reproducibility refresh (November 13, 2026)

A benchmark inventory found two scripts still importing renamed integer-only
helpers. Updated decimal-owned-conversion and decimal-row-conversion to the
shared checked converters. Their expected values now use decimal scientific
notation parsing rather than the old rounded-factor multiplication. The first
row rerun caught its remaining stale oracle expression; correcting that oracle
restored exact value checks without tolerance. No production arithmetic was
changed to satisfy a benchmark.

Refreshed artifacts are `decimal-owned-conversion-checked-2026-11-13.jsonl` and
`decimal-row-conversion-checked-2026-11-13.jsonl`. At 100,000 values, array-then-vector
versus owned typed conversion measured 5.060/2.821 ms; at 10,000 they were nearly
equal (0.512/0.514 ms). Row normalization measured 44.028/4.974 ms at 100,000 rows
for singleton arrays/shared scalar conversion, and 4.451/0.610 ms at 10,000.
Seven samples follow three warmups. Owned outputs fit the exact output-byte cap
and release to zero; borrowed inputs and intermediate JS arrays are excluded from
that accounting. These local microbenchmarks neither establish cloud decimal
operation nor complete heap bounds. No published decimal file was identified in
the existing R2 benchmark scripts; remote decimal evidence remains outstanding.


### Footer/schema integration validation checkpoint (November 13, 2026)

After the preflight, incremental decimal selection and lazy schema paths, all
1,797 default tests pass (363 skipped), all 25 workerd tests pass, repository
typechecking passes, full lint checks 611 files without changes, and the complete
TypeScript/package/fixture build passes. The prior 104-reference-test checkpoint
covers these production changes. These checks validate integration; they do not
complete external conformance, current coverage acceptance, package-install and
remaining release checks, or the central whole-query allocation requirement.

The next metadata accounting boundary remains a single decoded object graph:
upstream parsing first retains a complete Thrift tree while constructing the
public metadata graph. Structural preflight prevents malformed allocation-driving
lengths but does not eliminate this overlap for valid input. Streaming conversion
into a bounded representation, with explicit lifetime/admission for metadata and
schema indexes, is still required for a stronger contract. Existing serialization
size estimates cannot be substituted for runtime allocation bounds.

### Validated row-group byte ranges for incremental footer conversion (November 13, 2026)

Compact struct-list projection now supports a range visitor. With list:false it
validates each entry without constructing its fields and emits start/end offsets
relative to the supplied DataView only after the whole entry succeeds. It retains
no list result. Tests verify exact ranges, independent slice decoding, and that a
truncated later entry emits no partial range while earlier complete entries remain
observable. Ten parser tests pass; Parquet TypeScript compilation passes.

The external footer census now also locates row groups and compares each isolated
slice against the upstream compact parser, including exact slice consumption.
All 243 footers and 443 row-group slices pass; evidence is
`footer-rowgroup-ranges-2026-11-13.json`. This is local metadata-structure evidence,
not whole-file value conformance or a cloud performance measurement.

Production still uses the full upstream metadata conversion. The range visitor
establishes a tested boundary for replacing its complete intermediate Thrift tree
with incremental row-group/column conversion. That conversion, its retained
representation and allocation admission remain required before claiming this
metadata memory gap is closed.

### Isolated row-group metadata conversion parity (November 13, 2026)

Added a per-row-group converter preserving column extents, codecs, encodings,
statistics, sorting, bloom/index locations, size/geospatial metadata and crypto
payload representations. It reuses upstream scalar-statistic conversion with
LakeQL parsers so this integration step does not redefine logical-type semantics.
Unchanged histogram/crypto payloads may retain their existing representation;
this is not yet an all-owned metadata format. Statistics with a missing schema
leaf reject explicitly rather than silently discarding a required conversion.

The footer census compares each converted isolated group with the upstream full
metadata result: 442 groups across 242 files match. The remaining malformed file,
bad_data/PARQUET-1481.parquet, fails in the upstream full-metadata oracle because
its statistics column has no schema entry; its comparison is recorded as rejected,
not a successful match. Artifact `rowgroup-metadata-conversion-2026-11-13.json`.
The focused converter test and Parquet TypeScript build pass.

The converter is not yet selected by production metadata reads. Schema/envelope
conversion, incremental column retention, raw decimal-bound normalization and
allocation admission still require integration. Current parity evidence is local
metadata comparison, not whole-file value or remote-operation proof.

### Production incremental row-group metadata conversion (November 13, 2026)

Added file-envelope/schema conversion preserving logical types, field metadata,
key/value metadata and GeoParquet marking. Complete metadata matches the upstream
parser on all 242 externally supplied files where that oracle succeeds (442 row
groups); the malformed PARQUET-1481 oracle failure remains explicit. Evidence:
`incremental-metadata-parity-2026-11-13.json`. Production footer reads now use the
incremental converter: validate structure, decode schema/envelope, then locate,
decode and convert one row-group slice at a time. Converted envelope containers
are released from the raw record before row-group decoding. Raw decimal-bound
normalization and owned footer lifetime remain on the normal path.

This eliminates simultaneous retention of the complete raw row-group Thrift tree
and complete converted row-group tree. It still retains the converted metadata
and temporarily one whole raw row group; histogram/crypto payloads keep their
existing representation. Incremental column conversion, valid-object admission,
schema/index ownership and whole-worker heap proof remain unfinished.

All 1,802 default tests pass (363 skipped) and all 104 independent reference tests
pass after the production switch. Generated one/twenty-row-group parity and
malformed-footer tests pass, as do owned footer and decimal pruning tests.
Parquet TypeScript compilation and changed-file formatting pass. The final
raw-envelope reference release was added after the broad runs and receives a
focused metadata rerun. Additional parser passes trade CPU for lower retained
intermediate structure; end-to-end overhead and cloud/Worker refresh are still
required before claiming an overall performance improvement.

### Incremental column conversion within row groups (November 13, 2026)

The production incremental reader now visits and converts each column chunk as
its compact struct completes. It retains only row-group scalar/sorting metadata
and converted columns, rather than first retaining all raw column structs in a
row group. The shared column converter also serves the isolated-row-group parity
harness, preserving the same field/statistics semantics. Struct-list projection
accepts full-entry selection with a visitor while suppressing its retained list.

Complete metadata parity remains successful on 242 external files (442 groups),
with the same explicit malformed upstream oracle failure. Evidence:
`incremental-column-metadata-parity-2026-11-13.json`. All 104 reference tests pass.
Four incremental-metadata tests include a generated 1,024-column row group with
full upstream equality; ten compact-parser tests also pass. Parquet TypeScript
compilation and changed-file formatting pass.

Raw intermediary retention is now limited to one column plus the row-group
scalar/sorting envelope, rather than the full group/file column tree. This is a
structural improvement, not an allocation-byte bound: individual column metadata,
converted output, schema/indexes, and intentionally retained payload arrays still
need explicit admission. Complete-parser performance and cloud/Worker validation
remain required for this new production path.

### Incremental parser CPU tradeoff and cloud footer validation (November 13, 2026)

All 25 workerd tests pass after incremental column metadata integration. Real
public R2 footer reads also pass exact encoded-capacity and cap-minus-one checks:
footer extent 1,535 bytes, failure at 1,534, success at 1,535 and 3,070, one HTTP
206 request per case and zero remaining charge. Evidence:
`cloud-incremental-footer-2026-11-13.json`. Parsed objects and whole-worker heap
remain excluded; this does not turn the encoded cap into a whole-metadata cap.

The complete local parsing benchmark now includes preflight plus incremental
conversion. Initial medians were roughly twice upstream parsing on three small
external fixtures (`incremental-metadata-overhead-2026-11-13.json`). Inspection
identified redundant row-group scanning: locate/skip followed by slice decoding.
Production now uses nested visitors to decode/convert columns and groups directly
in the second footer pass, avoiding that redundant group traversal while retaining
only one raw column and the small group envelope.

Updated baseline/incremental medians are 0.03201/0.05659 ms (extended BSS),
0.00392/0.00768 ms (concatenated GZIP), and 0.00262/0.00521 ms (RLE boolean),
recorded in `incremental-metadata-visitor-overhead-2026-11-13.json`. Small-file CPU
overhead remains; no blanket speedup is claimed. The retained intermediate-tree
reduction is structural, not yet a measured heap-byte bound. Complete metadata
parity remains 242 successful files with the same malformed oracle failure in
`incremental-metadata-visitor-parity-2026-11-13.json`. Ten focused tests and
Parquet TypeScript compilation pass after the visitor refinement. Cloud/Worker
results above precede this last traversal-only refinement and require final refresh.

### Incremental schema and key/value envelope conversion (November 13, 2026)

The first footer pass now converts schema and key/value entries through visitors,
without retaining their complete raw lists alongside converted outputs. Missing
schema rejects explicitly. Empty versus absent key/value lists remain distinct,
and field ordering does not affect schema availability in the second pass.
Individual logical-type/unknown payload representations remain compatible with
the existing parser.

Complete external metadata parity remains 242 successful files with the same
malformed upstream oracle failure; evidence:
`incremental-envelope-metadata-parity-2026-11-13.json`. Six incremental metadata
tests pass, including one/twenty row groups, 1,024 columns, invalid footers and
hand-authored compact envelopes with row groups before schema and backward field
IDs. Both absent and present-empty key/value metadata match upstream output.
Parquet TypeScript builds and changed-file formatting pass.

This removes the remaining full raw envelope lists from the incremental path.
Final schema/key/value/row-group objects and temporary individual entries remain;
the change is not a runtime heap-byte model or completed allocation admission.

### Required-field validation for incremental metadata (November 13, 2026)

Review found that an omitted row-group list could become an empty converted list
and missing count fields could survive casts. The incremental reader now requires
a present row-group list, positive integer file version, nonnegative i64 file row
count, and present columns/nonnegative i64 size and row count for each row group.
Compact projections validate i64 wire types before reading their payloads.
Present-empty lists remain valid and distinct from omitted required fields.

Tests explicitly omit lists/version/counts, use an i32 in an i64 field, and supply
a group without required counts. All 23 incremental/owned-footer/parser tests
pass. External metadata parity remains 242 successes with the same malformed
upstream oracle failure: `incremental-required-fields-parity-2026-11-13.json`.
Parquet TypeScript compilation and changed-file formatting pass. These checks
close a correctness regression risk introduced by streaming defaults; they do
not establish valid-object byte admission or complete semantic validation of all
Parquet metadata fields.

### Isolated retained-metadata heap experiment (November 13, 2026)

`bench/metadata-retained-heap.mjs` generates actual Parquet files and runs each
parser/case in three fresh Node subprocesses, with two warmups, an event-loop
boundary and double GC. Input generation is outside the measured interval and
the parsed result remains explicitly reachable through the post-GC measurement.
The first sandboxed subprocess run returned empty output; the authorized local
subprocess rerun completed. Evidence: `metadata-retained-heap-2026-11-13.json`.

For 256 columns × 128 row groups, median retained-heap deltas are 24,141,432 bytes
(upstream) and 24,276,384 (incremental); parse medians are 65.80/82.57 ms. At
128 × 64, retained deltas are 6,102,360/6,073,504 bytes and times 12.11/22.69 ms.
The smallest case has divergent/noisy retained deltas (818,520/55,824), so it is
not used to infer an allocation ratio. Post-parse heap readings vary with GC and
are not peak measurements; they do not establish a consistent memory reduction.

This experiment contradicts any inference that eliminating the full raw tree
alone solves metadata capacity. Final converted metadata dominates retained
storage in larger cases. A compact budgeted retained representation is the next
major requirement; structural intermediate reductions are insufficient. These
Node observations are not portable object-size constants, a Worker heap proof,
or justification for an arbitrary reservation multiplier. CPU overhead remains
an explicit tradeoff to address alongside that representation change.

### Budget-admitted footer row-group directory (November 13, 2026)

Added a fixed-width footer directory with two Uint32 offsets per row group,
relative to the existing compact metadata view. A validation/counting pass retains
no row-group trees; the second pass fills one exactly admitted typed allocation
of eight bytes per group. Footer backing ownership remains the caller's separate
responsibility. Range views borrow that backing and validate indices/extents.
This is an internal primitive for on-demand metadata, not yet a replacement for
the public retained metadata graph.

Generated actual Parquet files with one/twenty row groups verify each isolated
view's row count and exact byte consumption, buffer identity, exact capacity,
cap-minus-one rejection before allocation, and zero release charge. Malformed
footer and invalid directory extents reject. Three focused tests and Parquet
TypeScript compilation pass; changed-file formatting passes.

The typed directory cost is exact, unlike inferred JS object size. Integrating it
with planning/scanning, bounded decoded-group lifetimes, cache pins, metadata
serialization and shared-job admission remains required. Current production still
retains the converted object graph; this primitive alone does not reduce that
retained footprint or complete the whole-query budget contract.

### Directory-selected row-group decoding (November 13, 2026)

Added an isolated row-group decoder using the same incremental column converter
and required-field checks as production full-metadata conversion. A directory
slice can now be decoded independently, with exact slice consumption and no
materialization of peer groups. Required group-field validation is centralized
in the shared converter so indexed and full readers cannot drift on that check.

Twelve directory/converter/incremental-metadata tests pass. A generated real
20-group Parquet file is accessed in last/first/middle/repeated order and compared
with the upstream full metadata oracle. Corrupting an unrelated group's first
byte leaves selected-peer decoding unchanged while selecting the corrupt group
fails exact-consumption validation. Directory charge is 160 bytes and releases
to zero. Parquet TypeScript compilation and changed-file formatting pass.

This is an internal random-access metadata primitive, not yet a public scan/cache
path. Decoded group JS allocations are not charged by the directory's typed-byte
reservation. Production planning needs explicit decoded-group lifetimes and
admission before it can replace the retained full object graph with this path.

### One-pass row-group source for planning (November 13, 2026)

Extracted the generic planner to consume an Iterable of decoded row groups while
retaining only planned row-group entries and merged ranges. The existing public
metadata-array planner delegates to it, preserving schema capability checks,
row offsets, optional byte-range inference, skipped groups and adjacent-range
merging. Schema validation accepts the schema-only envelope it actually needs.
The new entry point remains internal until footer acquisition/lifetime integration
can supply the compact source safely.

A real generated 20-group file is indexed under a 160-byte directory budget and
decoded lazily into this planner. Full-selection, partial-pruning and all-pruned
plans match the existing metadata-array route exactly; the generator is consumed
once, closes normally, and directory charge releases to zero. Ninety planner and
public-adapter tests pass; Parquet TypeScript compilation and formatting pass.

This connects the compact directory to generic planning logic, but public file
planning still acquires full metadata. Decoded-group allocation, envelope/footer
ownership, metadata cache compatibility and retained plan-output admission must
be integrated before changing that public acquisition path. No end-to-end memory
cap is claimed by the iterable abstraction.

### Indexed footer envelope and lifetime contract (November 13, 2026)

Separated schema/file-envelope decoding from full row-group materialization. The
new internal indexed-footer source combines that envelope with the admitted
8-byte/group directory and exposes explicit random-access group decoding, without
a row_groups array on the retained envelope. GeoParquet annotation marking keeps
separate physical-leaf conversion inputs so deferred statistics use the same
pre-marking semantics as full conversion. Group reads and opening a source reject
a disposed directory scope; footer backing must independently outlive reads.

An actual generated 20-group file verifies complete envelope parity, absence of
materialized groups, first/middle/last decode parity, exact 160-byte directory
charge, cap-minus-one rejection, zero cleanup and closed-scope errors. Eight
indexed/incremental metadata tests pass; Parquet TypeScript builds. This remains
an internal raw-statistics source: normalized decimal pruning, public acquisition
and cache integration must be completed before routing ordinary planning through
it. Envelope/leaf-copy/decoded-group JS storage is still outside typed directory
accounting, so it is not a whole-metadata memory guarantee.

### Checked decimal statistics in indexed group decoding (November 13, 2026)

Indexed footer reads now request checked decimal-statistic conversion while raw
column bytes are available. Integer physical bounds are decoded to their exact
unscaled integer, byte-backed bounds go directly to the checked converter, and
unsafe/invalid bounds are omitted while preserving valid peers and null counts.
This avoids an additional full-footer statistics projection for indexed reads.
The default isolated converter retains upstream parity mode for the existing
full-reader normalization pipeline; the indexed source explicitly selects checked
semantics.

The real extended BYTE_STREAM_SPLIT fixture verifies complete indexed group
parity with the ordinary normalized metadata reader and identical plans for
max-bound equality and above-max exclusion on both decimal columns, under the
exact eight-byte directory cap. A focused converter test proves the corrected
1003.858 bound and unsafe-bound removal. Four indexed/converter tests pass, plus
the preceding six indexed/converter/metadata-cache tests; Parquet TypeScript
builds and changed-file formatting pass. External tests explicitly skip when the
fetched fixture is absent.

This closes the indexed decimal-pruning semantic gap. Public footer acquisition,
cache reuse, decoded-group/envelope allocation admission and whole-query memory
proof remain required; the eight-byte reservation covers only the directory.

### Owned indexed file planning (November 13, 2026)

Extracted shared footer acquisition/preflight from full metadata conversion and
connected it to the internal indexed planner. A trailer-sized probe lets this
route retain the declared footer without a data-page prefix. Existing acquisition
chooses prefix concatenation when capacity permits, or releases the probe and
fetches the exact footer under pressure. The planner owns footer and directory
through planning and disposes both on success or failure.

A generated actual twenty-group Parquet file verifies full, partial and fully
pruned plan parity with the metadata-array planner. At exactly footer bytes plus
160 directory bytes, planning succeeds with two owned reads; one byte less rejects
and releases all tracked memory. Injected continuation failure releases backing;
a prior abort performs no reads. Five indexed-acquisition/metadata-cache tests
pass, and Parquet TypeScript compilation passes.

These are memory-store range tests, not cloud latency or transport proof. This
route remains internal: decoded envelope/group objects and retained plan output
need admission, public cache integration remains outstanding, and an eight-byte
probe adds a request relative to a sufficiently large first read. Dynamic probe
selection needs cloud measurements before adoption as the public default. This
checkpoint establishes ownership and cleanup, not a whole-query memory cap.

### Real R2 indexed-planning acquisition comparison (November 13, 2026)

Added bench/cloud-indexed-planning.mjs and recorded eighteen measured cases in
benchmarks/cloud-indexed-planning-2026-11-13.json. The read-only public R2
window-events fixture has a 1,535-byte footer and a 24-byte directory. Every data
request requires Range and HTTP 206; discovery and oracle acquisition are outside
measured cases. Successful plans equal the ordinary metadata-array oracle, all
tracked allocations release to zero, and peaks respect each configured budget.

At the exact 1,559-byte indexed capacity, three-sample median indexed latency is
396.73 ms with two requests / 1,543 transferred bytes, versus 219.99 ms with one
request / 1,559 bytes for full metadata planning. At 65,536-byte capacity, indexed
latency is 438.21 ms, two requests / 1,535 bytes and 3,070-byte tracked peak; full
planning is 191.66 ms, one request / 4,854 bytes and 4,854-byte peak. Indexed
capacity-minus-one rejects after acquisition and releases to zero. Sequential
three-sample network timings are diagnostic, not stable throughput estimates.

The unconditional trailer probe is a demonstrated remote request regression,
not a suitable public default. An adaptive initial range must balance saved
round trips against retained prefixes and directory headroom; public adoption
must wait for that policy and decoded-JS/output admission. The tiny three-group
fixture does not demonstrate the intended large-footer object-retention benefit.
This benchmark measures actual remote transport and tracked encoded/directory
bytes, excluding JS object graphs and total Worker memory. Initial sandbox DNS
failed; the authorized network-enabled run completed successfully.

### Adaptive indexed-footer acquisition (November 13, 2026)

Indexed planning now starts with the existing bounded larger footer read. A
validation-only group count determines directory capacity without decoded group
objects. When the directory fits alongside the retained read, that backing stays
live. If a retained data-page prefix prevents directory admission, the acquisition
child scope is disposed and its local buffer reference dropped before an exact
footer refetch. Directory construction shares the same counting/validation helper.
This policy avoids unconditional trailer requests while preserving exact-capacity
operation. It introduces a counting pass, whose large-footer CPU cost remains to
be measured alongside the broader metadata design.

Nine focused acquisition/directory/cache tests pass, including one-request ample
capacity, two-request exact capacity, cap-minus-one rejection, plan parity and
cleanup. Parquet TypeScript and changed-file formatting pass. Actual R2 rerun:
benchmarks/cloud-indexed-planning-adaptive-2026-11-13.json contains eighteen cases.
At 65,536 capacity, indexed planning now uses one request, with 4,878 tracked peak
bytes (4,854 backing + 24 directory), versus full planning's one request and 4,854
peak. Three-sample median latencies were 142.81/194.75 ms respectively; these
sequential network samples do not establish a speedup. At exact 1,559 capacity,
indexed planning still needs two requests (376.06 ms median) versus full planning's
one (205.18 ms). All successful plans match, all tracked charges release to zero,
and all cases respect capacity. The previous unconditional-probe artifact remains
as evidence of the regression this policy addresses.

This internal path still needs public cache integration and admission for envelope,
decoded-group and plan-output JS allocations. Encoded-byte accounting and a tiny
remote fixture cannot establish a 64–128 MiB whole-Worker bound.

### Indexed planning width/group scaling (November 13, 2026)

Added bench/indexed-planning-scaling.mjs and recorded local memory-store results
in benchmarks/indexed-planning-scaling-2026-11-13.json. Generated real Parquet
files cover 32 columns × 32 groups, 128 × 64 and 256 × 128. Both routes include
owned acquisition and planning; two warmups precede five timed samples per mode.
Every output equals the full planner oracle and every tracked budget returns to
zero. Full mode runs first, so timing is diagnostic rather than order-balanced.

Median full/indexed milliseconds are 3.536/5.174, 28.878/31.367 and
98.925/122.932 respectively. Tracked peaks are 65,536/65,792 bytes for the small
case and identical 1,006,220 and 4,049,426 bytes for the larger cases. Acquisition
concatenation dominates those larger encoded peaks. Indexed planning currently
adds validation/indexing work and does not improve encoded-buffer peak. These
measurements do not include decoded-object heap or runtime allocation peaks.

The claimed architectural benefit remains reducing simultaneously live decoded
row-group objects, not a demonstrated speedup or complete memory bound. Measure
that lifetime/heap benefit and reduce redundant metadata passes before choosing
public routing. Cloud request evidence from the preceding checkpoint cannot
substitute for this local CPU/scaling evidence, or vice versa.

### Shared envelope/count pass for indexed planning (November 13, 2026)

Envelope parsing now validates/counts the row-group list while visiting schema
and key/value entries. Indexed planning reuses that envelope for directory
admission and opening the source; directory construction accepts its validated
count and still checks every written index and final count. This removes the
separate admission-count walk and directory-count walk on the retained-buffer
path. If pressure requires refetching exact backing, the old envelope/view and
buffer references are dropped, and the replacement envelope is parsed anew.
Closed-scope opening still rejects before decoding. Full metadata parsing now
rejects missing required row groups during envelope validation.

Fifteen indexed/footer/incremental tests pass, as do Parquet TypeScript compilation
and formatting. An initial test failure was an error-message mismatch for earlier
missing-list rejection; the error now identifies the required row-group list.
The scaling rerun is recorded in
benchmarks/indexed-planning-shared-envelope-2026-11-13.json. Median full/indexed
milliseconds are 4.021/4.431 (32×32), 31.292/25.562 (128×64), and 97.080/95.610
(256×128). The prior indexed largest-case median was 122.932 ms. This supports
eliminating repeated walks, not a stable cross-environment speedup claim; sample
order and local runtime variance still apply. Decoded-object heap bounds, public
integration and whole-query budget proof remain outstanding.

### Indexed source retained-heap evidence (November 13, 2026)

Added bench/indexed-metadata-retained-heap.mjs and recorded results in
benchmarks/indexed-metadata-retained-heap-2026-11-13.json. Each mode/shape uses
three fresh Node processes, two warmups, an event-loop boundary and double GC
before/after the retained operation. Full mode retains all converted metadata;
indexed mode decodes every group in sequence and retains the source, directory
and last decoded group. Both verify expected groups/columns, indexed verifies
summed row counts and exact directory charge, and directory cleanup returns zero.

Median retained V8 heap deltas (full/indexed) are 854,992/60,456 bytes for 32×32,
6,133,728/165,056 for 128×64, and 24,298,424/280,912 for 256×128. Indexed typed
array deltas are exactly 256, 512 and 1,024 bytes; full adds zero typed bytes in
this measurement. Largest-case parse/visit medians are 87.22/84.64 ms; these
are not planning or acquisition timings. Input backing and fixture generation
are excluded from both baselines. Initial sandbox subprocess execution produced
empty stdout; the authorized subprocess run completed all eighteen samples.

This is direct evidence that keeping an indexed source plus one decoded group
reduces retained object graph growth compared with full metadata. Post-GC heap
is runtime-specific, not transient allocation peak, not a portable admission
formula, and not a whole-Worker bound. It does not establish that GC will reclaim
prior groups before subsequent allocations. Public integration still requires
bounded decoded lifetimes/admission and accounted envelope/plan output, with
actual concurrency and Worker-memory validation.

### Shared-envelope regression checkpoint (November 13, 2026)

Added a generated-file regression for supplied directory counts: negative,
noninteger, NaN, impossible extent, too few and too many counts all reject;
failed directory construction leaves zero charged bytes, while the correct count
uses exactly sixteen bytes for two groups and releases to zero.

The complete default suite now passes 1,817 tests with 363 explicit skips across
177 passing / nine skipped files. All 104 reference tests and repository-wide
typecheck also pass. Logs: /tmp/lakeql-indexed-envelope-tests-authorized.log,
/tmp/lakeql-indexed-envelope-reference.log and
/tmp/lakeql-indexed-envelope-typecheck.log. The first sandbox run had nine failures
and two unhandled errors from blocked localhost listeners and empty CLI subprocess
output; rerunning with authorized listener/subprocess capabilities passed without
source changes. This refresh covers the production envelope changes and internal
indexed acquisition, not the still-outstanding public integration, Worker refresh,
conformance limitations or whole-query heap admission contract.

### Indexed planning shared-pool overlap and integration boundary (November 13, 2026)

Added a concurrent generated-file test with two child budgets under one parent
pool. A read barrier holds both initial acquired buffers simultaneously, proving
actual overlapping charges instead of merely launching sequentially settling
promises. Both planners return full-oracle-equivalent plans; each child peaks at
its exact footer-plus-directory capacity, the parent peaks at their sum, and all
three budgets release to zero. Three indexed-planning tests pass; formatting
passes. This is memory-store ownership concurrency, not remote request scheduling.

Inspection confirms standalone public planRowGroups currently has only predicate
options and no allocation-budget contract; the owned scan path acquires cached
full metadata and pins its owner across vector consumption. Switching either to
an indexed source without changing its ownership/consumer contract would leave
cache and decoded-object accounting unresolved. The next integration needs an
owned metadata-source abstraction with explicit envelope and decoded-group
lifetimes, preserving existing cache pins and row-group access semantics. Typed
directory charges alone are insufficient to advertise that API as heap-bounded.

### Reusable acquired indexed source lifetime (November 13, 2026)

Extracted adaptive acquisition into internal acquireIndexedParquetSource. It
accepts a budget or parent allocation scope, retains footer/directory backing,
exposes checked envelope/group access, and has idempotent disposal. Acquisition
failure disposes its stage; successful acquisition transfers disposal responsibility
to the consumer. Indexed planning now consumes this source and closes it in
finally. Access after disposal, parent-scope closure or cancellation rejects;
explicit disposal also drops the wrapper's source reference.

A real generated twenty-group file verifies last-group oracle parity, exact live
charge before closure and zero charge afterward for direct disposal, parent
closure and post-acquisition abort. All accessors reject once closed; repeated
disposal is harmless. Six source/planner/indexed-footer tests passed before the
additional abort variant, and all four planner/source tests pass with that variant.
Parquet TypeScript compilation and changed-file formatting pass.

This establishes reusable acquisition ownership for subsequent scan/cache work.
Decoded metadata can contain borrowed buffers, so consumers must keep the source
alive while using them; decoded JS objects are still not charged here. It is not
a public whole-metadata budget contract or a substitute for cache-pin integration.

### Indexed source reuse through encoded range cache (November 13, 2026)

Added a generated-file test using two distinct cachedRangeBuffer wrappers sharing
one SharedMemoryCache and parent allocation pool. The first acquired source makes
one underlying owned read; after its disposal, only the reusable encoded entry
remains charged. The second source reuses that entry without another underlying
read. Evicting all cache entries while the second source remains live preserves
last-group oracle parity, and final source disposal releases the pool to zero.
Five planner/source tests pass.

The range cache returns an admitted output copy, so this is reuse plus independent
output lifetime, not a zero-copy pin lasting through group decoding. Initial test
capacity of one file plus directory was insufficient because input and output
coexist during acquisition; explicit two-file-plus-directory child capacity
passes. This exposes an outstanding acquisition policy gap: footer probe sizing
uses available bytes but not ownedReadAllocationBytes, whereas cached adapters
report copy/input costs through that capability. Admission remains safe (rejects
before excess allocation), but dynamic probe selection should account for adapter
workspace to avoid rejecting an otherwise feasible smaller read. Public routing
and decoded-object accounting remain unresolved.

### Adapter-cost-aware footer probing (November 13, 2026)

Footer acquisition now consults ownedReadAllocationBytes when selecting its
initial range and deciding whether prefix acquisition plus concatenation fits.
Oversized probes shrink geometrically down to the eight-byte trailer, avoiding
an assumption that arbitrary adapter cost functions are monotonic. Invalid
negative/nonintegral reported costs reject. Unknown adapters retain the prior
output-length forecast; actual scoped allocations remain the enforcement point.

A real generated Parquet file through a copying adapter verifies successful
planning within the configured budget, a reduced initial probe, oracle parity
and zero cleanup. Nine focused planner/source/cache tests pass, and Parquet
TypeScript compilation passes. Existing ample-capacity request assertions were
updated to honor the adapter's declared cost: store-buffer conservatively reports
twice the range length, and range-cache adds another output copy. The memory
store's actual aligned transfer can cost less than that forecast; correctness
still permits exact-budget fallback, but conservative forecasts can add reads.

This closes the previously observed unforecast copy-workspace probe failure.
Further work should refine underlying adapter capability precision rather than
assuming all owned store reads transfer aligned buffers. Remote timings need
refresh after this policy change; decoded JS accounting and public indexed scan
integration remain outstanding.

### R2 refresh after adapter-cost probing (November 13, 2026)

Read-only real R2 results are recorded in
benchmarks/cloud-indexed-planning-adapter-cost-2026-11-13.json (eighteen cases).
At 65,536 capacity both routes retain one-request operation: full/indexed median
193.72/192.70 ms and tracked peaks 4,854/4,878 bytes. At exact indexed capacity
1,559, both routes now use two requests: full/indexed medians 387.24/370.17 ms,
with peaks 1,535/1,559. Indexed capacity-minus-one still rejects; all successful
plans match and every measured budget releases to zero. Three sequential network
samples per case do not establish fine latency differences.

This is a measured tight-budget request regression for the full metadata route
relative to its previous one-request behavior. The store adapter forecasts a
possible output copy even when actual HTTP 206 ownership can transfer the buffer.
That forecast avoids underestimating noncontiguous results, including cases where
Range is ignored and the store returns a subview. A precise capability distinguishing
owned contiguous output from borrowed/subview output is required to recover the
request without assuming local-file/memory-store behavior for remote reads.
The regression is documented and unresolved, not counted as a completed perf win.

### Explicit owned-store allocation/output capability (November 13, 2026)

Added optional ObjectStore.getRangeOwnedAllocation, reporting new allocation
capacity including adapter workspace and whether output occupies its entire
ArrayBuffer or may be a view. URI mapping and read-control wrappers preserve the
capability. The Parquet bridge uses declared cost and adds a contiguous-copy
allowance only for view outputs; absent declarations retain the conservative
existing forecast. Invalid declared byte counts reject.

MemoryObjectStore declares its exact range-length allocation and exact-buffer
output, matching its admitted new Uint8Array implementation. Indexed-source tests
again pass one-file-plus-directory ample capacity without an extra probe; shared
range cache needs two-file-plus-directory acquisition capacity. Twenty-two focused
memory-store/planner tests pass, including wrapper propagation, copying adapter
behavior, cache reuse, concurrency and cleanup. Parquet TypeScript compilation
passed before the final test-only edits; formatting passes.

HTTP does not yet declare this capability: its ignored-Range fallback can return
a subview of a larger response. The R2 tight-budget regression therefore remains
open. Extending this capability must follow actual transport allocation/output
semantics, not extrapolate memory-store guarantees to remote reads.

### Exact owned HTTP windows and restored tight-budget request count (November 13, 2026)

Added generic readOwnedStreamWindow: it reserves exactly the requested output,
copies overlapping chunk segments, discards other decoded bytes, and drains the
stream while enforcing its decoded-byte limit. Incoming chunks remain runtime
owned. It rejects incomplete windows, excess decoded bytes and mismatched
transferred reservations; cleanup retains in-flight ownership until settling.
HTTP owned range reads now use this primitive for both 206 and ignored-Range
responses (including the existing 416 retry). Successful output always fills its
ArrayBuffer, so HTTP can truthfully declare range-length allocation with exact
output and the bridge needs no contiguous-copy allowance. Unowned HTTP behavior
is unchanged. Owned requests beyond response EOF now reject instead of returning
a shorter view; 206 excess bytes reject instead of expanding an owned range.

Thirty focused core/HTTP tests pass, including cross-chunk windows, empty chunks,
truncation, excess suffix, reservation mismatch, cancellation, fetch failure,
preflight admission and ignored ranges. Ignored-Range retained/peak bytes improve
from 4 to 1 and 12 to 4 in the existing fixtures. Core/HTTP/Parquet TypeScript builds
and formatting pass.

Actual R2 artifact: benchmarks/cloud-indexed-planning-exact-http-2026-11-13.json.
Tight full metadata planning at 1,559 bytes returns to one request (214.71 ms
three-sample median); indexed exact-capacity remains two (335.30 ms). At 65,536
capacity both use one request (196.01/191.95 ms), with unchanged tracked peaks
4,854/4,878 bytes. Plan parity, cap enforcement and zero cleanup pass all eighteen
cases. Fine timing differences are not stable speedup evidence. Ignored-Range
responses are still transferred/drained up to the configured decoded limit, so
this reduces retained engine memory, not network bytes or runtime chunk buffers.

The subsequent complete default suite passes 1,825 tests with 363 explicit skips
(178 passing files / nine skipped); log /tmp/lakeql-exact-http-tests.log. Worker
runtime refresh, public indexed integration and whole-query heap accounting still
remain required.

### Exact HTTP windows in workerd and adaptive R2 boundaries (November 13, 2026)

Added native-workerd HTTP adapter tests with injected chunked Responses for 200
ignored-Range and 206 paths. They verify exact four-byte output/peak, cap-minus-one
rejection before fetch, cross-chunk content, complete cleanup and truncated-window
rejection. These use workerd Fetch/ReadableStream primitives with deterministic
responses, not actual remote transport; preceding R2 HTTP benchmarks cover that.

All 28 workerd tests pass across ten files. The initial run exposed three native
R2 query tests assuming an ample-budget measured peak was an immutable minimum.
Adaptive probing legitimately reduced their peaks: aggregate 355→258, filtered
top-k 455→358, projected early-limit 389→292 bytes on the first tighter run. Tests
now repeatedly measure at the preceding peak until stable, assert non-increasing
usage, and require rejection one byte below the stable boundary. Output correctness
and zero cleanup remain asserted for every run. The rerun passes unchanged query
implementation; log /tmp/lakeql-exact-http-workerd.log. This validates runtime
compatibility and these small-fixture tracked boundaries, not whole-Worker heap
caps for arbitrary workloads.

### S3 exact-buffer forecast and pre-request admission (November 13, 2026)

S3 already validates Content-Range/identity encoding, allocates a known-length
buffer and rejects truncated output. It now declares range-length owned capacity
with exact-buffer output, avoiding an unnecessary bridge-copy forecast. Admission
moves before the signed HTTP request; a cap below the requested range rejects
without network I/O. Valid shorter end-of-object responses partition/release
unused reservation after header validation, preserving six-byte output ownership
for the existing 128-byte tail request.

All eighteen S3 tests pass against existing mocked cases and the actual signed
localhost HTTP fixture. The latter verifies exact capacity, whole backing layout
for a short tail, zero cleanup and only two requests (the insufficient-capacity
third attempt no longer reaches the server). Initial testing exposed that the
stream reader checks reservation size against the validated response bound before
shrinking; explicit reservation partitioning fixes the tail case. S3 TypeScript
and formatting pass. This is real HTTP/signing validation, not a live S3-provider
benchmark. Native R2 retains its current stream semantics and has not yet declared
a precise layout capability.

### Native R2 exact-buffer output contract (November 13, 2026)

R2 now validates whole-object size, computes the returned tail extent, partitions
unused preflight reservation and uses the shared exact-window collector. It declares
range-length allocation capacity with exact-buffer output. Incomplete bodies and
invalid sizes reject; the existing oversized-body budget metric is preserved via
a generic optional stream-window metric. Streaming ownership and pre-request
admission remain intact; arrayBuffer materialization is not used.

Eleven R2 unit tests pass, including NaN/negative/fractional object sizes, truncated
bodies, excess-body cancellation, pending-read ownership and cleanup. Thirteen
R2/stream tests passed before the additional malformed-size test. All 28 workerd
tests pass, including native R2 binding reads and six-byte tail backing assertions.
R2 TypeScript and changed-file formatting pass. Log:
/tmp/lakeql-exact-r2-workerd.log.

The first workerd run exposed two example mocks reporting returned range length
as object.size. Corrected them to preserve whole-object size, matching the real
R2 binding tested in the same suite. The stricter extent validation now makes that
previously masked mismatch visible. This establishes adapter capability precision;
it does not account for decoded query objects or establish whole-Worker heap caps.

### Allocation forecasts through object-store caching (November 13, 2026)

cachedObjectStore now preserves a known owned allocation/output contract while
adding one range-length allowance for its cold-read cache copy. It does not
blindly forward a one-buffer source cost through a wrapper that can retain two
buffers. Unknown source capabilities remain unknown. A shared-pool test verifies
cold and warm exact-buffer outputs, an eight-byte forecast/peak for four-byte
payloads, four reusable bytes after each job and zero after eviction. All 27
object-store cache tests pass; core TypeScript and formatting pass.

The repository-wide typecheck passes. The first complete suite after R2 changes
found one additional recipe mock reporting range size instead of whole-object
size; corrected it consistently with native R2 and the Worker fixtures. That run
had 1,825 passes, one failure and 363 skips. A full rerun follows the fixture fix
and cache-capability change; final outcome is recorded below.

Final rerun: 1,827 tests pass, 363 explicitly skipped, across 178 passing and nine
skipped files. Log /tmp/lakeql-store-capability-tests-final.log. The complete goal
remains open, particularly public indexed source integration, decoded allocation
admission and whole-Worker memory evidence.

### Demand-driven row-group plan entries (November 13, 2026)

Extracted generic iteratePlannedRowGroups so row offsets, pruning and byte-range
inference produce one entry on demand. Existing collected planning delegates to
this iterator and retains its prior output/range-merging behavior. Added internal
iterateOwnedIndexedRowGroups, whose acquisition lives through iteration and is
disposed in finally on exhaustion, early return or consumer failure. Both indexed
planning routes share one decoded-group generator.

Generated-file tests prove that a filtered consumer closing after its first
selected group visits only the first two groups and closes the upstream iterator.
Owned streaming tests verify the exact footer/directory charge while yielded and
zero after early break or consumer exception. Ninety-eight planner/indexed/public
adapter tests pass; Parquet TypeScript compilation and formatting pass.

Streaming consumers no longer need to retain a complete plan array merely to
visit entries. The array-returning API intentionally still collects results;
consumers can also retain yielded entries themselves. Individual decoded groups,
envelope objects and output entries still require allocation admission, so this
is a lifetime/retention improvement, not a whole-query heap bound. Public task
planning/scan routing has not yet switched to this internal iterator.

### Production task planning consumes streaming summaries (November 13, 2026)

ParquetScanAdapter.planTask now summarizes streamed planned groups directly into
merged group ranges and selected-group count. It no longer materializes a complete
PlannedParquetRowGroup array only to discard it when returning ScanTaskPlan.
Collected public planning and the summary path share range-appending logic;
pruning, offsets and range inference remain in the generic iterator. Existing
metadata acquisition/cache behavior and total/planned statistics are preserved.

A generated four-group fixture verifies summary parity for contiguous selection,
sparse selection and all-pruned output, with no retained rowGroups property.
Ninety-five planner/public-adapter/metadata-cache tests pass; three planner tests
pass after the final shared range-helper extraction. Parquet TypeScript builds
and formatting pass. This reduces production task-plan retention to merged ranges
rather than per-selected-group objects. Decoded full metadata is still acquired
and individual plan entries are transient JS allocations, so indexed-source
routing and allocation admission remain required.

### External normalized indexed metadata parity (November 13, 2026)

Added bench/indexed-metadata-corpus.mjs and artifact
benchmarks/indexed-metadata-corpus-2026-11-13.json. The local external Parquet corpus
compares indexed envelopes and every row group with the ordinary normalized reader,
not raw upstream decimal conversion. Each indexed directory uses exact eight-byte
per-group capacity and releases to zero. Planner outcomes are compared for full
selection and finite numeric max-bound predicates on top-level columns.

242 files / 442 row groups match; 148 successful planning outcomes and 306 matching
unsupported-plan outcomes are recorded separately. bad_data/PARQUET-1481.parquet
is rejected by the ordinary reader with "Statistics column has no schema leaf"
before indexed comparison, so is not a parity success. Each artifact entry includes
its file hash. Indexed metadata or planner mismatches make the script exit nonzero;
oracle rejections remain explicit diagnostic entries without an expected-failure
allowlist.

This materially broadens real-fixture metadata/pruning evidence beyond generated
INT32 files and the single decimal fixture. It is local metadata validation, not
remote transport, full data decoding or a claim that unsupported schemas work.
Public indexed routing and allocation admission remain outstanding.

### Indexed source integrated into uncached budgeted task planning (November 13, 2026)

ParquetScanAdapter.planTask now acquires an indexed source when a memory budget
and owned reads are available and no decoded-metadata cache is configured. It
uses the existing range-cache wrapper, shared decoded-group iterator and streaming
range summary, updates total/selected counts and disposes source ownership on all
exits. Configured decoded-metadata caches retain their existing get/set contract;
this boundary preserves explicit cache semantics rather than silently bypassing
them. Unbudgeted/borrowed reads retain their compatible acquisition path.

A real generated three-group fixture verifies filtered task-plan parity, statistics,
exact footer-plus-directory admission, cap-minus-one rejection and zero cleanup.
Twenty-four adapter/cache/indexed-planning tests pass; Parquet TypeScript compilation
and formatting pass. A complete regression run is recorded below when finished.

This is production routing for the uncached budgeted case, not completion of all
metadata routing. The default lake's configured shared metadata cache, vector scan
consumers and decoded JS allocation admission still need integration. The passed
byte budget covers acquired encoded backing and directory, not the whole JS graph
or merged output-range array.

Complete regression result: 1,831 passed / 363 explicitly skipped across 178
passing and nine skipped files. Log /tmp/lakeql-indexed-task-routing-tests.log.

### Task-plan shape omits unused physical byte ranges (November 13, 2026)

The generic planned-group iterator now accepts an explicit includeByteRanges
output capability (default true). Both task-summary acquisition routes request
false because ScanTaskPlan returns row-group ranges/counts, not physical ranges.
They therefore skip physical offset inference and avoid temporary byte-range
objects; ordinary file-range planning preserves its existing output. Pruning and
row offsets remain shared, without a predicate-specific branch.

A real generated fixture with an observed file-offset getter proves task-shaped
iteration performs no physical range inference while the default still produces
a byte range. Eighteen planner/adapter tests pass; Parquet TypeScript and formatting
pass. No standalone speedup is claimed without timing evidence.

Default-lake cache inspection confirms it installs both shared encoded ranges and
a parsed metadata cache. Extending indexed routing to that default requires an
explicit cache/source policy preserving warm semantics; this change does not
claim to resolve that outstanding boundary.

### Default indexed task planning with explicit cache policy (November 13, 2026)

Added taskMetadataMode: auto/indexed/cached to Parquet lake/scanner configuration.
Default lake auto selects indexed task planning when no custom metadata cache was
supplied, while scans keep their decoded metadata cache. Explicit custom metadata
caches preserve cached task planning under auto. Callers can choose either mode
for the CPU/retention tradeoff; owned reads and an allocation budget remain required
for indexed routing. Encoded range caching supplies warm reuse without keeping
all decoded row groups. Performance documentation describes policy and accounting
limits. Borrowed/unbudgeted routes retain compatible behavior.

A default-lake generated-file test proves one cold owned read, no additional warm
read, identical filtered task output and complete parent-pool release after encoded
cache eviction. Fifteen adapter tests and the preceding 106 public/cache/adapter
tests pass. The complete default suite passes 1,833 tests with 363 explicit skips;
all 28 workerd tests pass. Parquet TypeScript and formatting pass. Logs:
/tmp/lakeql-default-indexed-tests.log and /tmp/lakeql-default-indexed-workerd.log.

This closes default task-planning indexed routing, not vector-scan integration or
whole-query memory accounting. Individual decoded group/envelope objects and
merged task output still need admission; cached mode retains its existing decoded
object graph. Further remote/public-task benchmarks must quantify cold/warm CPU
and request tradeoffs under the new default.

### Public task-planning R2 cold/warm concurrency matrix (November 13, 2026)

Added bench/cloud-task-planning.mjs and artifact
benchmarks/cloud-task-planning-2026-11-13.json. Eight public createParquetLake
scenarios compare auto/indexed versus cached task metadata at 64/128 MiB shared
allocation pools and one/four concurrent jobs, using shared max-two read control.
Every actual data response must be HTTP 206 with Range; every task's row-group
shape matches the common oracle. Each scenario includes cold and warm rounds,
checks tracked capacity and requires zero pool charge after final cache eviction.

Four-job cold indexed planning coalesces to one footer request; cached mode makes
four. Both have zero warm footer requests. Each job still makes one two-byte
HTTP discovery probe: httpStore.head uses ranged GET, so zero footer reads must
not be described as zero network reads. Initial benchmark assertions incorrectly
counted discovery as a footer miss; inspected the request and adapter, then
classified both explicitly in the artifact rather than discarding probe traffic.

Indexed retained tracked cache charge is 4,854 bytes in every case, versus 17,495
for cached metadata. Indexed cumulative tracked peaks are 9,732 bytes (one job)
and 24,366 (four); cached mode records 17,495. These accounting categories differ:
parsed-cache sizing is an estimate and uncached parsing objects/runtime transport
are not fully charged. Single cold/warm timing samples are diagnostic, including
an 808 ms cached warm outlier; no latency speedup is inferred. Configured 64/128
MiB pools are not observed whole-Worker heap limits. This validates real public
remote planning, concurrency reuse and cleanup while retaining the memory-proof
limitations.

### Owned vector reader accepts indexed group sources (November 13, 2026)

Added a generic ParquetRowGroupSource contract (envelope, group count, random-access
group read). The owned vector reader accepts either existing full metadata or this
source and consumes groups lazily. Direct-range prefetch now accepts a group
iterable, preserving bounded prefetch windows and shared schema context without
requiring a retained row_groups array. Per-group decoder metadata and global row
offset adjustment remain unchanged. Demand and prefetch traversal stop after the
requested row end, avoiding needless indexed decoding of later groups.

A real generated twenty-group fixture through the shared range cache compares
indexed/full decoding for rows 4–12, batch size two and prefetch concurrency two.
Rows and global offsets match; observed group reads never exceed group four,
and source/cache cleanup releases the shared pool to zero. Twenty-five source,
malformed-page and adapter tests passed before the final end-boundary observation;
all eight indexed-source tests pass afterward. Parquet TypeScript and formatting
pass.

This connects indexed metadata to the generic owned vector engine; public scan
adapter acquisition still uses full metadata and needs routing/lifetime integration.
Prefetch may decode groups separately from demand traversal, trading CPU for a
bounded metadata lifetime. Decoded JS allocation admission remains outstanding.

### Indexed acquisition in uncached owned scans (November 13, 2026)

ParquetScanAdapter owned scans without a configured metadata cache now acquire an
indexed source and pass it to the generic vector reader. Schema/projection checks
use its envelope; missing-only column output consumes the same lazy group source,
so schema evolution does not depend on an empty placeholder row_groups array.
Source ownership releases in finally, while retained output owners continue to
outlive iterator closure. Configured decoded caches retain their existing path.

The preceding 104 adapter/public tests pass, including direct/fallback outputs and
retained byte backing after scanner closure. A new real two-file schema-evolution
test verifies missing-only null batches over a partial row interval with correct
global offsets and zero cleanup. All sixteen adapter tests and all 28 workerd
tests pass; Parquet TypeScript and formatting pass. Runtime log:
/tmp/lakeql-indexed-scan-workerd.log.

This routes uncached owned scans through compact metadata. Default cached scan
policy remains unchanged; source/group JS admission, schema-unification planning
and missing-column JS vector allocation are still not fully accounted. The goal
remains incomplete despite the covered buffer lifetimes and runtime results.

### Real R2 indexed vector scan matrix (November 13, 2026)

Extended bench/cloud-numeric-groups.mjs with explicit indexed-metadata mode and
configurable encoded-cache capacity. This mode acquires indexed source backing
inside each job's allocation scope before invoking the ordinary owned vector
reader, then disposes it on every exit. Metadata/oracle discovery outside the
measurement remains identified separately.

Artifact: benchmarks/cloud-indexed-vector-scans-2026-11-13.json. Eight cold/warm
scenarios cover one/four jobs, read limits one/two, full/partial row windows,
nullable numeric columns, 16 KiB child budgets and an 8 KiB shared encoded cache.
All rows and global offsets match the independent expected fixture values, read
concurrency remains within its configured limit, job cleanup leaves only cache
charges and final eviction releases the pool to zero. Cold reads are seven for
one job and eight for four; all warm rounds have zero measured reads. Largest
tracked peak is 35,366 bytes inside the 73,728-byte four-job pool; one-job peak is
13,572 inside 24,576 bytes. Retained cache charges are 7,320/7,506 bytes.

This benchmark now includes acquired footer/directory bytes in child admission,
unlike prior full-metadata-oracle runs. It still excludes decoded JS graphs and
oracle storage, uses a small numeric fixture, and does not prove whole-query or
whole-Worker heap bounds. Failure/cancellation refresh for the new integrated
metadata path remains additional work; prior transport failure tests alone are
not claimed as that proof.

### Indexed data-read cancellation and disconnect isolation (November 13, 2026)

Added indexed-http-failures.test.ts using a generated real two-group Parquet file
and actual localhost HTTP 206 streams. Discovery and indexed footer acquisition
complete before the server stalls the first data-range body after one byte. Two
cases then either abort the caller or disconnect the socket while a sibling job
reads the same file successfully. The gate confirms live charged storage before
failure; server response closure is awaited, survivor rows match all forty fixture
values, canceled-job and shared-pool charges return to zero, and the parent peak
stays within capacity. Both tests pass; formatting passes.

This exercises failure after metadata acquisition through the indexed source,
owned vector engine and HTTP body collector together, rather than substituting
local-file errors or footer-only cancellation. It is deterministic local HTTP
failure injection, not an intentionally broken public cloud endpoint. Shared
range-cache waiter cancellation remains covered separately; this test does not
claim cancellation of a shared in-flight cache request. Whole-query JS accounting
and default cached scan-source policy remain outstanding.

### Default owned scan metadata policy and warm-hit reporting (November 13, 2026)

Added scanMetadataMode auto/indexed/cached alongside task policy. Default lake auto
selects indexed owned scans unless a custom metadata cache was supplied; custom
cache behavior is retained, with explicit overrides available for the CPU/retention
tradeoff. Direct scanner auto preserves its configured cache semantics. Explicit
indexed mode can use owned scanning even with a borrowed-only metadata cache,
because that scan route does not consume the decoded cache.

Indexed acquisition observes calls reaching the underlying owned file beneath the
range cache. If no underlying read is needed, it reports a metadata cache hit;
otherwise a miss. This avoids inferring reuse from shared query counters or treating
all encoded reuse as misses. Existing decoded-cache reporting remains unchanged.
An initial instrumentation placement error was caught by TypeScript and focused
tests, then corrected before verification.

Nineteen adapter tests pass, including cold/warm parity for auto/indexed/cached,
one warm metadata hit, zero warm range requests and zero after shared-cache eviction.
The preceding 108 public/adapter/cache tests pass. Full suite: 1,840 passed / 363
explicit skips, 179 passing files / nine skipped; log
/tmp/lakeql-default-indexed-scans-tests.log. Parquet TypeScript and formatting pass.
Performance documentation now describes both policies and the decoded-JS accounting
gap. No new throughput claim is made until public scan benchmarks compare policies.

All 28 workerd tests also pass after the default scan switch; log
/tmp/lakeql-default-indexed-scans-workerd.log. Whole-query decoded allocation
admission and Worker-heap proof remain required.

### Public scan metadata-policy R2 comparison (November 13, 2026)

Added bench/cloud-scan-metadata-policy.mjs and artifact
benchmarks/cloud-scan-metadata-policy-2026-11-13.json. Eight scenarios compare
public auto/indexed and cached scans across 64/128 MiB configured pools and one/four
jobs. Two selected numeric columns, null values and batch size seven produce all
144 independently generated expected rows. All cases assert zero scan-range reads
on warm rounds, tracked capacity and zero after final shared-cache eviction.

Both policies use seven cold scan-range reads and zero warm scan-range reads.
Each job additionally performs two two-byte discovery probes on both rounds;
these are explicitly retained in request reports. Indexed retained tracked cache
charge is 6,144 bytes versus cached 10,998. Indexed/cached cumulative tracked peaks
are 12,076/12,052 (one job) and 29,872/29,776 (four). The small directory raises
indexed peak while avoiding retained decoded metadata cache charge. Warm jobs
report a metadata reuse hit; shared in-flight indexed footer reads also count as
reuse for waiting cold jobs. Single timing samples vary and do not justify a
blanket latency speedup claim. Accounting still excludes decoded JS/runtime heap.

The trace exposes additional repeated discovery work: public scans perform a
planning probe and another scan probe even when the planned object is available.
Review object-info reuse before optimizing this; preserve path/version semantics
and distinguish immutable planned information from an unsolicited global cache.

### Reuse planned object information when opening scans (November 13, 2026)

Row, vector and owned-vector scans now share scanFile, which checks cancellation
and uses supplied ObjectInfo only when its path matches the requested scan path.
This reuses the planned size/ETag without another head/probe. Missing or mismatched
object information still performs discovery. This is per-operation planned-object
reuse, not a new global freshness cache or a guarantee of atomic object snapshots.

Real generated fixtures verify identical rows and zero extra heads for matching
information across all three scan APIs; a mismatched path requires a head. All
111 adapter/public tests pass; Parquet TypeScript and formatting pass.

R2 rerun: benchmarks/cloud-scan-planned-object-reuse-2026-11-13.json. All eight
public scan cases preserve row/cache/budget/cleanup checks. Discovery probes fall
from two per job to one, on both cold and warm rounds and under both metadata
policies. Cold scan-range requests remain seven; warm remain zero. Warm elapsed
samples are 152–316 ms versus 379–446 ms in the preceding artifact, consistent
with removing a round trip but not a stable speedup estimate from single samples.
This closes the observed duplicate-discovery issue without assuming local-file
head costs match cloud requests. Decoded allocation admission and whole-Worker
memory proof remain unfinished.

### Budgeted multi-file schema discovery (November 13, 2026)

Multi-file planObjects previously ignored its supplied MemoryBudget and loaded
full decoded metadata for every file to compare schema descriptors. With indexed
task metadata policy, owned reads and a supplied budget, it now acquires an indexed
source through the shared encoded range cache, compares only envelope schema, and
releases source backing in finally before proceeding. No row groups are decoded
for schema comparison. Explicit cached policy and callers without owned reads or
a budget retain their existing compatibility path.

Generated twenty-row-group fixtures verify identical schema plans against the
existing path, positive tracked allocation with zero retained backing, rejection
under a seven-byte budget with cleanup, and cleanup when two files have conflicting
column types. All 112 adapter/public tests pass; Parquet TypeScript and formatting
pass. Log: /tmp/lakeql-schema-discovery-tests.log.

This removes full row-group graph construction from budgeted default schema
planning; it does not account for the decoded schema envelope, descriptor maps or
returned per-file schema plans. Those JS allocations and whole-Worker heap
admission remain open. Remote request-count benchmarks must be refreshed for
multi-file workloads; this checkpoint makes no new cloud latency claim.

### Integrated schema-planning verification refresh (November 13, 2026)

After planned ObjectInfo reuse and budgeted multi-file schema discovery, the full
suite passes: 1,844 tests, 363 explicit skips, 179 passing files and nine skipped.
All 28 workerd tests and all 104 reference tests also pass. Logs:
/tmp/lakeql-schema-planning-full-tests-authorized.log,
/tmp/lakeql-schema-planning-workerd.log, and
/tmp/lakeql-schema-planning-reference.log.

The initial sandboxed full run failed on denied localhost listeners and subprocess
recipes; the authorized rerun above passed without source changes. This is not a
conformance or whole-heap completion claim. Inspection confirms envelope decoding
still creates schema/key-value JS objects before any decoded-object admission;
returned schema descriptor maps also remain outside typed allocation accounting.
A durable follow-up must admit decoded structures before construction and preserve
ownership through consumers, rather than relabeling encoded byte counts as heap.

Repository-wide type checking also passes after these integrations; log
/tmp/lakeql-schema-planning-typecheck.log.

### Compact parser pre-construction admission seam (November 13, 2026)

readCompactHeader now accepts an optional admission callback for selected struct
construction, selected field insertion, retained list capacity and borrowed binary
view creation. Admission runs before each corresponding construction; exceptions
propagate unchanged. Skipped projections do not admit discarded structures, and
incremental list visitors do not admit a list-sized retained array. Binary events
report borrowed payload length explicitly so future policies do not double-charge
footer backing as a copy.

Thirteen parser tests pass, including output parity with the reference decoder,
exact allocation-event sequences, projection omission, visitor list omission, and
rejected list admission before its first entry is read. Parquet TypeScript and
formatting pass. Log: /tmp/lakeql-compact-admission-tests.log.

This is a generic integration seam, not active decoded-memory accounting. It does
not model transient bigint arithmetic, property-name strings, converter-created
objects/strings, parser stack or runtime heap. No memory-byte coefficient or whole
Worker cap is asserted. Next integration must establish explicit cost/lifetime
contracts for raw parsing and converted envelopes/groups, reserve before decoding,
and transfer/release charges with actual consumer ownership. Existing callers
retain their behavior until such a policy is wired in.

### Project envelope fields before allocating raw metadata (November 13, 2026)

Envelope parsing now explicitly projects SchemaElement fields 1–10 and key/value
fields 1–2, with wire-type validation for consumed scalar/binary fields. Unknown
fields are structurally validated but skipped without constructing their lists,
views or nested objects. Logical-type payloads retain compatibility decoding.
readParquetFooterEnvelope forwards an optional raw admission callback; converted
schema objects and text decoding still require separate admission integration.

A synthetic valid footer with 1,024 unknown schema values produces the same schema
and exactly the same admission-event sequence as one without that field; truncated
unknown lists still reject. The envelope and adapter tests pass (31 tests).
Parquet TypeScript and formatting pass. The external corpus rerun validates 242
files / 442 row groups with the same one ordinary-reader rejection as before;
artifact: benchmarks/indexed-metadata-envelope-projection-2026-11-13.json. This is
local metadata parity, not cloud throughput or complete conformance proof.

### Owned decoded row-group lifetime contract (November 13, 2026)

ParquetRowGroupSource now optionally exposes acquireRowGroup with an independently
releasable OwnedMemory handle. The shared indexed-group iterator prefers ownership
when available and releases in finally after each group, including consumer exit.
Owned vector demand and speculative range planning both use this iterator. The
outer reader now explicitly returns the speculative iterator in finally; previously
an early exit could leave that iterator suspended at its last group.

Generated twenty-group fixture tests exercise full consumption, early break and
consumer failure with independently charged metadata lifetime tokens. They assert
two simultaneous demand/prefetch holders, zero live holders and budget cleanup on
all exits, and exact sixty-row output on completion. These tokens deliberately test
lifetime, not estimated JS object size. All 65 indexed-plan/owned-vector tests pass;
Parquet TypeScript and formatting pass. Log:
/tmp/lakeql-owned-metadata-lifetime-tests.log.

Indexed footer sources still expose the existing borrowed decode implementation;
wiring actual decoded admission into their owned acquisition remains open. This
contract provides the required independent lifetime for that integration without
retaining every decoded row group until source disposal. Prefetch range arrays and
output aliases require their own allocation analysis.

### Indexed-source group owners retain real backing (November 13, 2026)

The indexed source now implements acquireRowGroup. Each owner independently pins
its acquisition scope's completed footer backing, preserving borrowed binary
statistics after source disposal. It does not retain the lookup directory, which
is no longer needed once the group is decoded. Shared owners use existing physical
reservation identity rather than charging duplicate footer copies.

Real generated fixture tests verify retained metadata equality after source
disposal, two-owner sharing, directory release, invalid-index cleanup, rejection
of acquisition after closure, idempotent release and zero after the final release.
All 89 indexed-plan/owned-vector/adapter tests pass; Parquet TypeScript and
formatting pass. Log: /tmp/lakeql-indexed-group-owners-tests.log.

This connects the ownership contract to real backing in default indexed planning
and scans. Decoded JS group allocation itself remains uncharged; the owner is an
extensible lifetime carrier, not proof of a complete decoded-byte policy.

Actual R2 public scan rerun passes all eight scenarios (64/128 MiB configured
pools, one/four concurrent jobs, auto/cached modes), including independent row
parity, warm reuse and final eviction cleanup. Artifact:
benchmarks/cloud-scan-owned-metadata-2026-11-13.json. Each scenario retains seven
cold scan ranges, zero warm scan ranges and one discovery probe per job. Indexed
tracked peaks remain 12,076 bytes (one job) / 29,872 (four), with 6,144 retained
cache bytes; cached peaks remain 12,052 / 29,776 with 10,998 retained. No tracked
capacity or request-count regression from group ownership; whole-runtime heap is
still outside this measurement.

### Projection-aware allocation preflight (November 13, 2026)

inspectCompactAllocations shares the checked parser traversal but suppresses
materialization and conversion visitors. It counts selected structs, fields,
retained lists/slots and borrowed binary views/payload extents. Skipped fields
remain validated without contributing allocations. Projection factories still
select list entries; conversion and range visitors are not invoked. This allows a
future cost policy to reserve one stage extent before decode rather than allocate
one reservation ledger for every metadata field.

Twenty-three parser/envelope tests pass, including generated multi-group footer
parity between preflight counts and actual admission events, projected visitor
suppression, skipped fields and truncation. Parquet TypeScript and formatting
pass; log /tmp/lakeql-projected-preflight-tests.log.

Counts are structural units, not portable heap bytes. Converter output, temporary
bigint/string work and retained ownership still need explicit costing. This helper
is not yet wired to a default byte policy and does not close the decoded admission
or whole-Worker memory requirements.

### Metadata structure/heap calibration evidence (November 13, 2026)

Extended bench/indexed-metadata-retained-heap.mjs with raw allocation profiles for
the envelope, all groups and final group. Profiling runs only after heap snapshots,
so its traversal/construction does not contaminate the measured interval. Each
shape/mode still runs in three isolated fresh Node processes with two warmups,
event-loop boundaries and double GC. Artifact:
benchmarks/indexed-metadata-allocation-profile-2026-11-13.json.

Median retained heap for full/indexed modes: 32 columns × 32 groups,
860,608 / 56,256 bytes; 128 × 64, 6,110,272 / 165,056; 256 × 128,
24,297,440 / 281,488. Largest indexed envelope plus final raw group has 1,283
structs, 5,386 fields, 768 lists/slots, 1,026 binary views and 6,961 borrowed
payload bytes. Full mode across all groups has 131,458 structs and 590,983 fields.
These are raw construction units, not retained converted-object counts or bytes.

Crucially, indexed immediate post-parse heap deltas are 4,580,616 / 2,282,824 /
5,958,032 bytes across these shapes, well above retained heap and not monotonic.
These samples are not peaks: GC timing affects them. They rule out treating the
retained envelope-plus-one-group graph as proof of total transient heap usage.
A cost model must address raw/conversion overlap and garbage awaiting collection;
releasing a reservation does not synchronously collect JS. Worker validation and
reducing materialization remain required. No default byte coefficient was inferred
from these limited Node-only measurements, and no hard whole-runtime cap is claimed.

### Shared column-chunk projections and corpus regression guard (November 13, 2026)

Full/incremental and indexed group decoders now share explicit projections of
fields consumed by column conversion, including nested statistics, key/value,
encoding statistics, size/geospatial statistics and sorting columns. Unknown
payloads are validated but not materialized. Crypto payloads remain opaque.
Tests demonstrate omission of a 1,024-item unknown list and rejection of a
wrong-typed statistics field before its payload is parsed.

The initial strict field-15 projection exposed a real compatibility regression:
data/dict-page-offset-zero.parquet uses a non-scalar legacy column-metadata field
15. Keeping its existing generic representation restores compatibility; a focused
regression test preserves that behavior. The first corpus run reported the extra
ordinary-reader rejection with exit zero, so success was not accepted from exit
status alone. bench/indexed-metadata-corpus.mjs now optionally takes a prior JSON
artifact and fails if a previously validated identical-hash fixture rejects or
is missing. Changed hashes require separate review rather than silent comparison.

Final baseline-checked corpus: 242 validated files / 442 groups, with only the
previous ordinary-reader rejection. Artifact:
benchmarks/indexed-metadata-column-projection-2026-11-13.json. All 27 focused tests,
Parquet TypeScript and formatting pass. Log: /tmp/lakeql-column-projection-tests.log.
The heap profiler now imports the same projections to avoid counting fields the
reader no longer constructs. No new throughput or whole-heap bound is claimed.

### Reduce intermediate BigInts in compact i64 decoding (November 13, 2026)

The compact parser previously created shifted BigInts for every varint byte.
It now accumulates the first seven base-128 digits exactly in Number (at most
49 bits), performs signed zigzag conversion there, and constructs one final
BigInt for small values. Wider encodings promote the exact prefix and retain
checked 64-bit BigInt decoding. Skipped/preflight fields still validate bytes
without decoding their values. No precision is traded for speed.

Tests cover both signs around every power-of-two boundary through 62 bits,
INT64_MIN/MAX, zero, non-minimal encodings and tenth-byte overflow, including
skipped projections. All 36 targeted parser/envelope/indexed-plan tests pass;
Parquet TypeScript/formatting pass. Baseline-checked external corpus retains
242 validated files / 442 groups and the one existing ordinary-reader rejection.
Artifact: benchmarks/indexed-metadata-i64-corpus-2026-11-13.json.

All 18 isolated heap-profile runs complete; artifact
benchmarks/indexed-metadata-i64-profile-2026-11-13.json. Largest full/indexed parse
medians are 80.62/84.12 ms versus 86.47/87.29 in the prior profile, but intervening
projection changes and sample noise prevent attributing this comparison solely
to integer decoding. Post-parse heap deltas are mixed, including higher deltas
on larger indexed cases; no peak-memory reduction is claimed. The smallest full
retained-heap sample also varies substantially, reinforcing the limits of GC-based
snapshots. Retained large-case metadata remains approximately 24.3 MB / 286 KB.

The owned planning scaling matrix also passes parity and cleanup at every size;
artifact benchmarks/indexed-planning-i64-scaling-2026-11-13.json. These local
results do not substitute for cloud-request or whole-Worker allocation validation.

All 28 workerd tests pass after integer-decoding integration; log
/tmp/lakeql-compact-i64-workerd.log.

### Generic public struct decoding and nested schema compatibility (November 13, 2026)

The blanket unannotated-group rejection in schema.ts is removed. Core generic
batch construction already supports struct vectors; direct decoder capabilities
continue to choose the owned fallback for nested shapes. Recursive leaf checks
still reject unsupported decimal conversions. This replaces a format-wide guard
with the existing execution capability boundary rather than query-specific cases.

Generated physical fixtures cover nullable parent structs, nullable nested
containers/members, Unicode, empty strings, signed values, two row groups and
batch sizes 1/2/5. They validate internal owned fallback, public row reads and
public queries against independent expected values. An external Rust nested-struct
fixture checks exact projected values independently compared with DuckDB, including
an integer sum beyond Number's safe range. Multi-file descriptors now include
nested child names/shapes. Numeric compatibility uses only the top-level physical
type, so it cannot accidentally widen a struct after finding a numeric child in
its serialized signature. A conflicting nested schema rejects and cleans up.

Public/adapter/struct checks: 117 pass; full suite before the final nested-signature
change: 1,861 pass / 363 explicit skips. All 28 workerd tests pass after that change.
Parquet TypeScript and formatting pass. Logs: /tmp/lakeql-struct-schema-tests.log,
/tmp/lakeql-generic-struct-full-tests.log, /tmp/lakeql-generic-struct-workerd.log.

Current conformance is 226 passed / 24 failed / one skipped, compared with the
historical 88 / 162 / one. Log: /tmp/lakeql-generic-struct-conformance.log. These
conformance checks assert decoding completes, not full value parity. Remaining
failures include malformed corpus inputs, unsupported codecs/encodings and decimal
conversions; they have not been reclassified as passes. This does not prove all
nested logical semantics, remote nested reads, upstream JS allocation admission,
or whole-Worker bounds. Those acceptance requirements remain open.

### HTTP nested scans expose discovery concurrency gap (November 13, 2026)

Added real localhost HTTP nested-struct fixtures with required/nullable children,
nullable parents, long Unicode strings, four groups, batch size three and four
concurrent public queries. Both HTTP 206 and servers ignoring Range with HTTP 200
produce exact independent expected rows. Warm rounds require zero payload range
requests; final cache eviction returns tracked allocation to zero. The pool is
1 MiB with 512 KiB per-query allocation limits; these are tracked capacity checks,
not whole-Worker heap measurements.

The test initially observed four simultaneous discovery requests despite a shared
maxConcurrentReads=2. withObjectStoreReadControls.head bypassed the semaphore;
HTTP discovery is itself a two-byte range GET. Discovery now uses controlledRead
and competes with payload reads for the same slots. An explicit mixed head/range
unit test holds one discovery slot and verifies queued discovery and payload
requests never start after cancellation. Underlying active head cancellation still
waits for the adapter promise, as required by the existing non-signal head API.

After rebuilding core output, all 16 memory-store tests and both HTTP nested cases
pass; the subsequent cancellation test brings the core file to 17 passing tests.
Logs: /tmp/lakeql-struct-http-tests.log and /tmp/lakeql-head-concurrency-tests.log.
Core/Parquet TypeScript and formatting pass. No new cloud-service latency claim:
these are actual HTTP transport semantics with deterministic local fixture bytes.

### Exact-output LZ4 raw decompression (November 13, 2026)

Added an LZ4_RAW block decoder writing into caller-provided exact output with no
scratch backing. Length extensions, literal extents, match offsets/overlap, output
capacity, truncated sequences and input/output aliasing are checked. Scoped
compressors reserve output before construction; decode failures release the
allocation. Generic numeric page capability checks now include LZ4_RAW with zero
codec scratch, preserving owned admission rather than routing through an
unaccounted decompressor. Legacy Hadoop-framed LZ4 is not treated as raw blocks.
Format reference: https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md.

Tests cover literals, extended lengths, overlapping matches, malformed input,
exact capacity, cap-minus-one and failure cleanup. Both Apache raw-LZ4 fixtures
(4 and 10,000 rows) match independently generated DuckDB full-row digests through
public row reads and queries. Comparison normalizes bigint and the small fixture's
ASCII binary values to DuckDB's JSON representation; public binary values remain
Uint8Array. The initial digest mismatch exposed that representation difference,
not a decompression mismatch.

All 59 LZ4/compressor/numeric-group tests and all 29 workerd tests pass; the new
Worker test verifies exact-output admission and cap-minus-one rejection. Parquet
TypeScript and formatting pass. Logs: /tmp/lakeql-lz4-tests.log and
/tmp/lakeql-lz4-workerd.log. Conformance now reports 228 passes / 22 failures /
one skip; /tmp/lakeql-lz4-conformance.log. Remaining failures and whole-query
allocation accounting are still open. No cloud latency or whole-heap claim.


### Integrated gates after struct, discovery and LZ4 changes (November 13, 2026)

Full suite: 1,870 passes / 363 explicit skips. Repository-wide type checking and
full build pass. Lint initially found import order, unchecked assertions and an
unused test import; those are corrected, and all 669 files now pass without
warnings. The 17 affected parser/owned-group tests pass after cleanup.

Compatibility validation caught the stale struct-rejection evidence. Updated
its record to the actual nullable/external/HTTP tests, documented allocation
limitations, added LZ4_RAW evidence, regenerated compatibility.md and passed the
check. The tracked-file size check passes with subprocess execution permitted;
the initial sandbox attempt could not launch read-only git ls-files. This check
only covers tracked files by its existing contract, not every untracked artifact.

This checkpoint refreshes several release gates but leaves conformance (22
failures), complete decoded allocation admission, realistic near-cap Worker
stress, current reference/coverage and remaining release acceptance unfinished.

### Bounded legacy LZ4 framing (November 13, 2026)

The legacy LZ4 codec now validates Hadoop big-endian block/chunk framing and
historical unframed blocks. Both paths use one caller-admitted exact output;
failed framing validation falls back to independently validated raw decoding,
which must overwrite all output and cannot reference previous attempt state.
Each framed chunk has independent back-reference history and cannot exceed its
remaining block output. Zero progress, truncated chunks, excess output and
trailing bytes reject. No second output or scratch backing is allocated.
Authoritative format context: https://github.com/apache/parquet-format/blob/master/Compression.md
and Hadoop BlockDecompressorStream.java under hadoop-common (big-endian original
block and compressed-chunk lengths). Raw LZ4 remains the separate modern codec.

The three Apache legacy fixtures (two four-row forms and one 10,000-row form)
match the full-row digests of their corresponding raw-LZ4 fixtures previously
verified with DuckDB. DuckDB itself rejects the deprecated LZ4 label, so these
are paired-fixture comparisons, not direct DuckDB decoding of legacy framing.
Tests cover multiple chunks per block, multiple blocks, incomplete/zero-length
chunks, trailing bytes, exact capacity, failure cleanup and Worker cap-minus-one.

All 63 focused LZ4/compressor/numeric-group tests and all 30 workerd tests pass.
Parquet TypeScript, full lint and generated compatibility checks pass. Logs:
/tmp/lakeql-legacy-lz4-tests.log, /tmp/lakeql-legacy-lz4-workerd.log,
/tmp/lakeql-legacy-lz4-lint.log and /tmp/lakeql-legacy-lz4-compatibility.log.
Conformance now has 231 passes / 19 failures / one skip; log
/tmp/lakeql-legacy-lz4-conformance.log. No whole-query heap or cloud throughput
claim follows from these codec checks.

### Remaining conformance census and malformed owned-query cleanup (November 13, 2026)

All 104 reference tests pass after generic structs and raw/legacy LZ4; log
/tmp/lakeql-legacy-reference.log. Refreshed diagnostics over all 243 external
Parquet fixtures: 224 decode, 19 reject. Artifact:
validation/parquet-conformance-legacy-lz4-2026-11-13.json. The broader conformance
suite additionally has seven passing non-Parquet cases and one skip. The census
preserves hashes and complete cause chains; it does not designate rejections as
expected or turn them into passes.

Remaining categories include malformed page/dictionary/level metadata, unknown
ALP encoding, BSON, wide decimal conversions, fixed-binary timestamps, Brotli,
and a shredded-variant range error. Decode-only success remains insufficient for
value correctness, especially malformed/variant fixtures.

Five corrupt real fixtures now exercise public owned queries under a 1 MiB pool,
small batches and disabled encoded retention. All reject and leave zero tracked
charge, with observed peaks within the pool. They initially exposed inconsistent
error boundaries: owned scans leaked upstream RangeError whereas public row reads
wrapped them. The owned adapter now wraps unknown scan errors consistently as
LAKEQL_PARQUET_READ_ERROR with path and original cause. Existing LakeqlError
instances (including budget and cancellation errors) pass through unchanged.
The boundary includes acquisition as well as iteration; lower ownership scopes
remain responsible for cleanup.

All 117 malformed/adapter/public tests pass, plus five focused cause-preservation
checks; Parquet TypeScript and formatting pass. Logs:
/tmp/lakeql-malformed-ownership-tests.log and /tmp/lakeql-malformed-causes-tests.log.
These checks verify failure cleanup and diagnostics, not full upstream malformed
input validation or uninstrumented JS heap limits.

### Timestamp carrier investigation and vector overflow guard (November 13, 2026)

The flba12_timestamp.parquet rejection is not merely a malformed fixture. Current
Parquet LogicalTypes.md specifies TIMESTAMP on FIXED_LEN_BYTE_ARRAY(12) as signed
96-bit little-endian two's-complement counts in the declared unit. Source:
https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp.
The installed DuckDB rejects this fixture's timestamp statistics, so it cannot
supply an independent value oracle for this carrier.

Inspection found a more fundamental execution issue: generic timestamp vectors
store BigInt64Array, and construction previously assigned larger epoch values
without checking, silently wrapping modulo 64 bits. Construction now checks the
explicit signed-64 storage capability and raises LAKEQL_TYPE_ERROR with shape,
storage width, unit and row index instead of corrupting data. Bounds are shared
module constants, not recomputed BigInts per row.

Tests preserve both exact signed-64 boundaries for millis/micros/nanos under exact
16-byte output admission, reject each adjacent out-of-range value, and verify
scope cleanup. All 43 allocation/batch/sort tests pass; core/Parquet TypeScript and
formatting pass. Log: /tmp/lakeql-timestamp-capacity-tests.log.

The 96-bit Parquet parser has deliberately not been enabled ahead of a lossless
vector representation. Full support requires an explicit wider timestamp shape
and compatible generic gather/concat/comparison/operator capabilities, alongside
budgeted backing and tests of metadata pruning. This overflow check is a durable
representation invariant, not completion of that wider-carrier work. Existing
JS timestamp objects can represent larger values but do not establish vector
storage or whole-query allocation safety.

### Fixed-width 96-bit storage foundation (November 13, 2026)

Added internal Int96Storage with one admitted backing allocation of exactly
12 bytes per value. Low 64-bit unsigned and high 32-bit signed typed views use a
structure-of-arrays layout, preserving alignment at odd lengths without padding.
Reads reconstruct signed values; writes validate the full signed-96 range before
mutating either word. No bigint[] storage or per-row backing allocations are used.

Three tests cover signed boundaries around 64/96 bits, zero and negative values,
invalid/overflowing lengths, invalid indices, unchanged storage after rejected
writes, exact capacity and cap-minus-one before any allocation, and competing
child budgets with independently retained ownership. TypeScript and lint pass;
log /tmp/lakeql-int96-storage-tests.log.

This is internal storage, not yet a public vector shape or Parquet feature.
Required integration remains explicit: a wider timestamp vector capability;
automatic bounded promotion when 64-bit storage cannot represent a batch;
scalar access/comparison, gather/slice/concat and all operator dispatch paths;
retained-byte accounting and serialized output; then the 12-byte Parquet carrier
and pruning. The current signed-64 overflow guard remains in force until those
paths preserve wider values end to end.

### Bounded 96-bit storage operations (November 13, 2026)

Internal signed-96 storage now supports borrowed contiguous slices, selected gather
and concatenation. Gather validates indices before admitting one 12-byte-per-row
destination. Concatenation validates plane lengths and total capacity, then copies
typed words into one destination; neither operation creates row-sized BigInt or
selection intermediates. Borrowed slices explicitly require the source owner.

Four storage tests pass, covering duplicate/non-monotone selection, signed limits,
borrowed slices, exact combined output capacity, cap-minus-one before allocation,
invalid selection/ranges and empty concatenation. Core TypeScript and lint pass;
log /tmp/lakeql-int96-operations-tests.log. These remain internal building blocks;
Vector union, semantic timestamp dispatch and Parquet carrier integration are not
yet enabled, and existing public timestamp storage is still signed 64-bit.

### Lossless batch timestamp precision (November 13, 2026)

Timestamp construction previously selected the first non-null value's unit, so
later microsecond/nanosecond values could be truncated. Construction now performs
an allocation-free representation pass over the source, selecting the finest
declared or numerically required precision regardless of row order. The second
pass writes one admitted eight-byte-per-row backing; no row-sized intermediate
array is introduced. Mixed UTC/local semantics and invalid Dates reject with
structured type errors before timestamp backing allocation. Existing signed-64
overflow checks remain; wider storage integration is still outstanding.

All 45 allocation/batch/sort tests pass, including exact capacity, cap-minus-one,
negative epochs, reversed input order, finer numerical precision than declared,
semantic rejection, and cleanup. Two concat expectations now retain the finest
input precision rather than silently inheriting the first input's unit. Core and
Parquet TypeScript builds and targeted lint pass. Log:
/tmp/lakeql-timestamp-precision-tests.log. This fixes value preservation; no CPU
speedup, wide-carrier support, or total Worker heap bound is claimed.

### Planned mixed-unit timestamp concatenation (November 13, 2026)

The generic replayable selected-concat planner now handles timestamp unit
promotion, including nulls and selections reached through nested physical shapes.
It selects the finest declared unit, checks selected epoch scaling for signed-64
overflow before output admission, rejects incompatible UTC semantics, and writes
directly to one typed destination plus optional validity. Extent is exactly eight
bytes per row plus one validity byte when needed. Owned/scheduled concatenation
can reserve the complete extent atomically. Ordinary concatenation uses the same
planner instead of constructing flattened arrays of per-row TimestampValue objects.

All 63 selected-concat, concat-plan, scheduled-concat and vector-sort tests pass;
core/Parquet TypeScript and targeted lint pass. Exact 27-byte nullable mixed-unit
output succeeds; capacity 26 rejects before any tracked output allocation. Tests
also cover negative epochs, precision retention, promotion overflow, incompatible
semantics and cleanup. Log: /tmp/lakeql-timestamp-concat-tests.log. This removes a
known row-sized object intermediate; CPU/remote throughput and whole-runtime heap
improvements have not been measured for this change. Signed-96 integration remains
open, and promotion beyond the current physical capacity rejects explicitly.

### Timestamp fast path and measured scaling (November 13, 2026)

Review caught an unnecessary row scan introduced by the timestamp concat planner:
same-unit signed-64 inputs need no promotion validation. Their plan now reads only
physical extents and their execution retains direct typed copying. A proxy-backed
regression test rejects any indexed epoch access during same-unit planning; all
38 concat-plan tests pass. Mixed-unit validation skips sources already in the
output unit. All 63 focused concat/sort checks passed before that additional test.

bench/timestamp-concat.mjs measures 1,024 / 16,384 / 131,072 rows in eight batches,
two warmups and five samples with alternating mode order. Every output epoch,
exact output charge and cleanup are checked. Artifact:
benchmarks/timestamp-concat-2026-11-13.json records source hashes and raw samples.
At 131,072 rows, mixed-unit direct concat median is 1.94 ms versus 14.84 ms for
materialized timestamp objects using the current lossless builder (about 7.7x).
Same-unit direct copy is 0.52 ms versus 18.04 ms for materialization; the latter
was not the previous same-unit implementation, so it is not a before/after gain.
Same-unit planning stays approximately 0.007–0.013 ms over the tested sizes;
mixed-unit planning scales with conversion validation (0.77 ms at the largest).
Tracked output is exactly 1,048,576 bytes at the largest size in either mode.

These synthetic local operator measurements exclude input backing, transient JS
heap, decoding, transport and Worker execution. They do not establish a whole-query
or cloud speedup or heap cap. The full authorized suite passes 1,888 tests with
363 skips; the later complexity test passes separately. Broad reference/workerd
refresh and the larger outstanding allocation-coverage requirements remain open.

### Explicit signed-96 timestamp vector integration (November 13, 2026)

Added a distinct timestamp96 physical shape with low/high typed planes sharing
one admitted twelve-byte-per-row backing. Generic scalar access, vector length,
slicing, gather, retained backing measurement, physical payload estimation, sort,
predicate fallback and grouping-key reads preserve the full signed epoch. Gather
plans account for twelve bytes per selected row. Retained measurement deduplicates
the shared backing, including borrowed slices. Selected wide concatenation writes
one destination, scales differing units with signed-96 preflight bounds, handles
nulls through its existing selection contract and rejects mixed UTC semantics.
No row-sized timestamp object intermediate is needed for these copies.

All 849 core tests across 98 files pass, including four new wide-vector tests:
signed-96 gather extremes, exact capacity/cap-minus-one, independent output lifetime,
borrowed backing accounting, values differing above signed-64 capacity, sorting,
predicates, grouping keys, mixed-unit wide concat and promotion overflow. Repository
TypeScript and lint (676 files) pass. Logs:
/tmp/lakeql-wide-vector-core-tests.log,
/tmp/lakeql-wide-vector-repo-typecheck.log and /tmp/lakeql-wide-vector-lint.log.

Integration is incomplete: automatic scalar-to-vector promotion, mixed 64/96 concat,
grouped batch reconstruction, wide ISO output, full operator capability acceptance,
and Parquet FLBA12 conversion/pruning remain. The generic CPU capability list does
not yet advertise timestamp96; the default constructor still rejects signed-64
overflow. Thus this is tested physical infrastructure, not a public wide timestamp
support claim, a new fixture-conformance pass, or completion of memory coverage.

### Bounded wide timestamp formatting (November 13, 2026)

Timestamp output now floors negative epochs to the preceding second before
formatting fractional digits, fixing invalid negative fractions around the Unix
epoch. Dates within the native Date range retain its calendar conversion. Outside
that range, exact bigint Gregorian 400-year cycles plus a bounded (at most nine
steps) year search produce the calendar second without year-by-year iteration or
row-sized workspace. Extended years carry a sign and at least six digits; very
large years can require more digits. No Date range truncation or Number conversion
of the unbounded epoch occurs. Existing local/UTC suffix behavior is unchanged.

All 27 focused timestamp/wide-vector/grouping tests pass. One thousand seeded
native-range Date oracles and their positive/negative million-cycle shifts verify
calendar consistency beyond Date range, including negative epochs. Explicit leap
and century cases and fractional milliseconds/microseconds/nanoseconds pass.
All 31 workerd tests across 12 files pass, including wide calendar output and
JSON serialization at both signed-96 epoch extremes. Repository typecheck and
lint (677 files) pass; logs /tmp/lakeql-wide-calendar-{tests,typecheck,workerd,lint}.log.

This covers output formatting, not parsing extended-year input strings, automatic
wide vector promotion, mixed physical-width concat, or Parquet carrier support.
It adds no whole-query heap bound or measured throughput claim. Wide timestamp
construction remains disabled pending those integration checks.

### Automatic timestamp width and FLBA12 fixture integration (November 13, 2026)

Generic timestamp construction now chooses signed-64 or signed-96 backing from
the batch's actual epoch extrema at its resolved precision. Extrema are gathered
in the existing precision pass, with no extra row-sized storage. Eight-byte
storage remains the default when values fit; wider batches admit exactly twelve
bytes per row. Values outside signed-96 reject before backing allocation. The
selected concat implementation now shares one width-aware path for narrow/wide
inputs, promoting scaled narrow epochs when required. Existing same-unit narrow
metadata-only planning and typed-copy execution remain intact. Grouped output
reconstruction can construct wide vectors, and CPU capabilities advertise the
explicit timestamp96 shape. No configuration is required for this choice.

Parquet timestamp parsers accept exactly twelve-byte carriers and decode signed
little-endian two's-complement counts using offset-aware DataView access. This is
distinct from legacy physical INT96's Julian-day layout, handled upstream. The
Apache flba12_timestamp.md supplies an independent six-row epoch oracle, including
year 9999 and year 0001 values outside signed-64 nanos. Public row reads, budgeted
scans at batch sizes 1/2/6, filtering and ordering preserve those documented values.
Offset views, signed-96 extrema and invalid carrier widths are checked separately.

Real localhost HTTP scans test both honored ranges (206) and ignored ranges (200),
four concurrent jobs, max two active reads, shared cache reuse, zero warm data
ranges and final zero tracked charge. These are remote-protocol tests against a
local server, not a fresh R2 production benchmark. The pool is 1 MiB with 512 KiB
per-query allocation limits; this does not establish whole-runtime heap caps.

All 1,904 default tests, 104 reference tests, 854 core tests and 32 workerd tests
pass. The Worker check includes mixed-width concat at exactly 44 bytes including
20 bytes of input backing and 24 output bytes, plus cap-minus-one construction.
Repository typecheck, lint and compatibility generation/check pass. Conformance
improves to 232 passes / 18 failures / one skip; FLBA12 is now a value-verified
success, while remaining failures remain unresolved. Logs:
/tmp/lakeql-wide-promotion-{full-tests,reference,workerd,typecheck,lint,conformance}.log,
/tmp/lakeql-wide-fixture-tests.log and /tmp/lakeql-wide-http-tests.log.

Extended-year ISO parsing remains separate from wide output formatting. The broader
decoded-JS admission, nested/operator/output lifetime coverage, realistic Worker
heap stress and remaining conformance/release gates are still outstanding.

The local concat benchmark was refreshed after automatic promotion:
benchmarks/timestamp-concat-wide-promotion-2026-11-13.json. At 131,072 mixed-unit
rows, median direct concat is 2.48 ms versus 14.41 ms through materialized objects
(about 5.8x). The previous direct median was 1.94 ms: promotion-aware validation
adds work and these short runs are not a controlled attribution of the difference.
Same-unit direct copy remains about 0.49 ms, planning about 0.013 ms. This artifact
still exercises values fitting signed-64 after scaling, not wide-value throughput.
Both modes retain the exact 1 MiB tracked output extent; no total JS heap or cloud
latency inference follows.

### Worker heap stress: returned rows exceed the tracked pool (November 13, 2026)

Added bench/worker-heap.mjs and bench/worker-heap-entry.mjs. The host generates a
real Parquet fixture and uploads it to Miniflare native R2 before starting queries.
The Worker performs ordinary public scans with 1,024-row batches, a shared 8 MiB
encoded cache, 64/128 MiB allocation pools and one/four concurrent jobs. Each
capacity/concurrency scenario starts a fresh isolate and executes cold/warm rounds.
Rows, ID sums and payload character counts are independently asserted; all warm
rounds have zero data ranges. Cache eviction leaves zero tracked allocation charge.
The writer fixture contains unique 256-character strings and compresses well;
encoded file size is not representative of decoded-output memory.

The harness reads Runtime.getHeapUsage from the actual workerd inspector. Counter
semantics are documented at
https://chromedevtools.github.io/devtools-protocol/v8/Runtime/#method-getHeapUsage:
usedSize is JS heap usage, totalSize allocated JS heap, backingStorageSize covers
ArrayBuffer/external-string backing, and embedderHeapUsedSize is embedder GC heap.
All counters remain separate; no assumed sum is presented as Cloudflare billed
memory. No forced collection occurs. Requests for samples are spaced by 5 ms, but
busy isolate execution delays responses: these runs produced only three samples
per round. They cannot establish an exact transient peak. Explicitly retained
materialized rows are verified through a separate endpoint before the post-query
snapshot, then released. Streaming mode retains only checksums.

Artifacts:
- benchmarks/worker-heap-2026-11-13.json: 32,768 rows, materialized output.
- benchmarks/worker-heap-65536-2026-11-13.json: 65,536 rows, materialized output.
- benchmarks/worker-heap-stream-65536-2026-11-13.json: same 65,536-row fixture streamed.

At 65,536 rows with four jobs and a 64 MiB pool, tracked peak is 6,435,046 bytes
(6.14 MiB). Snapshots with output retained show JS usedSize 89,116,488 cold and
92,859,252 warm (84.99/88.56 MiB), individually above the configured pool even
without adding backing storage. Corresponding stream snapshots show 22,345,676 and
33,011,932 bytes (21.31/31.48 MiB), with the same tracked peak and correct outputs.
Streaming backing snapshots are 22,160,481/24,774,986 bytes. JS garbage awaiting
collection and runtime overhead are included; the difference is evidence of output
retention cost, not a portable per-row allocation coefficient. A zero tracked
charge after cache eviction does not mean zero live heap or immediate GC.

Reproduce with `node bench/worker-heap.mjs`, or set LAKEQL_HEAP_ROWS=65536 and
optionally LAKEQL_HEAP_MODE=stream. The scripts bound generated rows to 131,072,
record fixture and bundled-source hashes, and dispose inspector/runtime resources.
They require localhost sockets. This is local workerd using native emulated R2,
not production R2 networking or an enforced production isolate memory limit.

This result moves returned-output coverage from an unproven concern to a concrete
failure of a whole-heap interpretation of maxAllocatedMemoryBytes. Per-batch scan
bounds and fast concatenation do not address `toArray()` accumulation. Required
follow-up is pre-materialization admission and a durable output lifetime contract,
plus decoded-object/runtime headroom and broader metadata/operator stress. Silently
switching to streaming would change the requested materialized API; merely checking
serialized row bytes after allocation would not meet the allocation contract.

### Released ownership drops payload references (November 13, 2026)

Tracing output lifetime found that released OwnedMemory handles retained their
value fields and composite backing lists even after their reservation reached zero.
A caller retaining the handle could therefore keep a large payload strongly
reachable without any charge. Scoped, retained and composite handles now expose
value only while live and clear their payload references on release. The last
composite reference clears its backing owner list. Independently retained handles
remain valid and charged. Release stays idempotent. Access after release now raises
LAKEQL_VALIDATION_ERROR; callers needing later access must retain a live handle.
Raw aliases captured before release remain the caller's responsibility and cannot
be revoked by an ownership handle.

Broader tests exposed an adopted-handle cleanup ordering dependency: adapters may
release the original adopted handle before disposing its scope. Scope cleanup now
uses its captured allocation-index key, clears that key, and releases idempotently
without reading a released payload. A regression covers this producer/consumer
ordering. Existing exact-allocation tests retain raw values before release; the
Parquet allocation spy records backing at allocation time instead of dereferencing
released historical results.

bench/released-ownership-retention.mjs runs with --expose-gc and keeps nine released
handles alive while checking four payload/container cases via WeakRef after real
Node GC. All become collectible, including independent composite siblings.
Artifact: benchmarks/released-ownership-retention-2026-11-13.json. Forced collection
is a test oracle only, not a production lifetime policy or a timing guarantee.

All 115 focused scope/public-Parquet tests and two timestamp-allocation tests pass;
all 32 workerd tests pass. Repository typecheck and lint pass. The refreshed full suite passes 1,906 tests
with 363 skips across 187 passing files and nine skipped files. Logs:
/tmp/lakeql-released-ownership-{targeted,timestamp,workerd,typecheck,lint,full}.log.

The real Worker/native-R2 stress matrix passes its correctness, warm-cache and
tracked cleanup assertions after this change. Artifact:
benchmarks/worker-heap-released-handles-2026-11-13.json. Four materializing jobs
with a 64 MiB pool still show 90,075,656 / 99,043,972 JS heap bytes at cold/warm
retained-output snapshots, with the same 6,435,046-byte tracked peak. Differences
from prior runs include GC/runtime variation and are not evidence of a total-heap
improvement. Caller-output admission and lifetime coverage remain necessary.
The harness now captures query failures until it can dispose the runtime and
bounds each inspector request by a timeout, avoiding unhandled-rejection exits.

### Pre-materialization plans and explicitly owned row output (November 13, 2026)

Added planBatchMaterialization and materializeOwnedBatchRows. The plan traverses
physical vectors and selection before creating rows, decoding packed text, or
constructing nested lists/maps/timestamps. It validates physical extents, column
lengths, packed ranges, cycles and safe allocation arithmetic. UTF-8 text uses an
upper bound of one decoded UTF-16 unit per source byte, avoiding a decoding pass
just to estimate its reservation. Lists/maps/structs and dictionary selections use
the same generic traversal. Map key coercion and overwritten/null-key work may be
over-reserved; no query-specific special case is required.

MATERIALIZATION_CHARGES defines explicit reservation units for object/property,
array/slot (including growth slack), string/code-unit, numeric/bigint/timestamp and
view allocations. These are an allocation-accounting model, not measured portable
VM object sizes or a proven bound on all transient GC heap. Their calibration and
runtime headroom remain acceptance work. Borrowed binary values pin source ownership; native strings now receive output
reservations without pinning unrelated source pages (see integration below); copied packed text/numeric/timestamp output releases its source
pin after construction. The returned OwnedMemory<Row[]> keeps output reservations
alive until its final owner releases. Copied raw aliases remain subject to the
same explicit ownership contract as typed buffers.

Six focused tests pass, including budget rejection before TextDecoder invocation,
exact model charge, selected nested/dictionary/wide timestamp values, borrowed
binary lifetime, decoder failure cleanup and actual multi-group Parquet output
remaining valid after scan/input disposal. All 33 workerd tests pass, including
exact-capacity/cap-minus-one owned materialization and independent source/output
lifetime. Core/Parquet build, repository typecheck and lint pass. Logs:
/tmp/lakeql-materialized-output-{tests,workerd,typecheck,lint,core}.log.

This is a generic owned materialization operation, exported alongside existing
ownership APIs. It does not yet change QueryResult.toArray(), row/SQL fallback
paths, final collector-array allocation, or returned plain-array lifetimes. Those
integrations remain required; the earlier Worker heap failure is not resolved by
adding this operation alone. Runtime weak references/finalizers are available in
workerd, but cannot by themselves track arbitrary extracted nested values and
primitive-string aliases, so they are not treated as proof of complete output
ownership. Explicit owner release supplies a deterministic boundary.

### Query output admission and concurrent Worker evidence (November 13, 2026)

QueryResult.toOwnedArray now admits materialized output on supported owned-vector
projection/filter paths. Its generic collector charges linked chunk bookkeeping
and the final exact-length array before allocation, preserves payload ownership,
and releases all intermediate ownership on rejection or producer failure.
Offset/limit slicing precedes materialization. Plain toArray uses this path when
supported, then transfers returned rows to caller management and releases the
engine owner. Callers retaining results across jobs must retain the explicit
owned result to keep its reservation live. Unsupported owned-output paths reject
before scanning; normal ordering/window/row fallback output remains unfinished.

Native string output is charged even when its decoder lacked a JS payload charge.
It does not retain whole decoded source pages: immutable string references remain
live in the rows themselves. Borrowed binary still retains backing ownership.
Removing the unnecessary string source pin reduced the single-job tracked peak
from roughly 130 MB to 55,182,915 bytes. Field names also receive string charges.
These accounting units remain a model, not a portable whole-VM heap bound.

Three collector tests cover exact capacity/minus-one, append rollback, independent
ownership, abort and producer failure. Three real multi-row-group Parquet tests
cover retained output, offset/limit, early budget failure and contention between
retained results. The concurrent struct HTTP success fixture now uses a 2 MiB
pool (previously 1 MiB) because materialized outputs participate in admission;
separate tight-capacity tests require rejection. Malformed-page tests stream rows
to reach their corruption oracle instead of failing earlier on collection size.
The complete default suite and 104 SQL reference tests pass (checkpoint above).

Fresh local workerd isolates with native emulated R2 read a real 65,536-row Parquet
fixture with 256-character values, under 64/128 MiB pools and one/four jobs:

- One job succeeds at either capacity, with 55,182,915 tracked peak bytes.
- Four jobs at 64 MiB all reject rather than exceed live allocation admission.
- Four jobs at 128 MiB produce one success and three budget rejections.
- Explicit owned output retains about 54.98 MB including cache after a successful
  single query; plain returned arrays no longer retain engine reservations.
- Repeated rejected rounds still reach about 85 MB sampled JS heap with a 64 MiB
  pool; the plain-output 128 MiB warm round reaches about 141 MB JS heap.

Evidence: docs/benchmarks/worker-heap-{output,owned}-detached-2026-11-13.json.
The earlier *-admission and *-owned-output artifacts record the source-pinning
policy before correction. The harness validates values for successful jobs and
records budget failures separately; rejected jobs are not counted as successful
benchmark work. Inspector samples are sparse lower bounds, with no forced GC.
This proves improved live admission, not production Cloudflare heap safety.

Remaining required work includes runtime/GC headroom, untracked metadata and JS
allocations, generic output ownership for remaining physical paths, and preserving
late materialization in the owned path. Owned output currently bypasses the
unowned late-limit fetch optimization; remote byte/request amplification must be
measured and fixed before claiming performance parity. The earlier standalone
materializer section describes its historical integration boundary; this section
supersedes that boundary only for the supported query paths stated here.

### Owned collection cancellation boundary (November 13, 2026)

QueryResult now passes the configured abort signal into collectOwnedRows. A
producer can abort while completing after its final yield; without this signal,
the collector could still allocate its final array and return a successful result.
The regression test uses an owned typed-vector scanner that aborts at completion,
asserts LAKEQL_ABORTED, producer finalization, and zero remaining pool bytes.
All four query-memory tests and the Core/Parquet build pass; repository typecheck
also passes. Logs: /tmp/lakeql-output-cancellation{,-types,-build}.log.

Inspection confirms that lateMaterializedLimitRows and materializeRowRefs retain
unowned reference arrays, rank-key rows and a result Map. They cannot safely be
re-enabled for owned output merely by wrapping their final rows. The required
integration must admit selection/reference storage before construction and keep
borrowed values owned across the deferred range fetch, while preserving bounded
materialization windows. That work remains open.

### Bounded deferred output and observed range transport (November 13, 2026)

Owned projection/filter/limit collection now preserves late materialization.
The generic gatherDeferredColumns operator joins ordered selected positions to
owned deferred batches by absolute row position. It validates ordered unique
indices, safe extents, nonoverlapping fetched batches, complete selected-row
coverage and disjoint retained/deferred columns. It keeps source/fetch ownership
through output lifetime and disposes admitted gather workspace on every exit.
Without a deferred reader, the same operator gathers already-present columns.

Query execution holds one predicate batch and its admitted u32 selection buffer,
then fetches bounded selected row windows. It skips offset matches without
retaining query-sized references and stops at the requested limit. Predicate and
output values remain vectors until admitted output materialization. This removes
the owned path's dependency on the older unowned RankedRowRef arrays, rank-key
objects and result Map. The older streaming/top-k paths still need integration;
this is not a claim of universal owned output coverage. Wildcard projections use
the existing full-column path so missing output dependencies cannot be hidden.

A real HTTP fixture exposed an important transport tradeoff: deferred requests
reduce native-range bytes but amplify traffic when each request returns the
whole file. ObjectHead/ObjectInfo now carry optional observed rangeReadMode
(native/emulated). HTTP discovery derives it from its existing range probe,
including compressed-probe fallback; core preserves the hint through planning.
Emulated transports gather output eagerly. Parquet demand reads prefer whole
requested chunks on such transports when headroom permits, avoiding separate
page-header/payload full-file responses. Pressure, partial row windows and normal
allocation admission still require bounded page reads. No extra user flag is
needed. Unknown transports/listed objects lacking a hint retain deferred behavior.
This hint changes performance planning only, never the correctness contract.

Evidence in docs/benchmarks/deferred-output-http-2026-11-13.json uses a 269,691-byte
Parquet fixture, 512 rows, sixteen groups and deterministic 512-character text,
filter id % 257 = 256, limit one, batch size sixteen:

| Transport | Eager wire bytes | Adaptive wire bytes | Maximum concurrent reads |
| --- | ---: | ---: | ---: |
| Native HTTP ranges | 215,872 | 84,933 | 2 |
| Ignored HTTP ranges | 5,393,820 | 5,393,820 | 2 |

Native-range transfer falls about 61%; no wall-time speedup is asserted from this
fixture. Four cold concurrent jobs and four warm concurrent jobs return identical
owned values; warm runs issue zero data-range requests. The shared pool is 64 MiB,
with tracked peaks below 0.8 MiB in these scenarios, and returns to zero after
owner release/cache eviction. This measures local HTTP transport and tracked
admission, not production Worker heap safety.

Nine generic gather tests cover split fetched batches, sparse windows, invalid
indices, missing rows, overlapping columns, producer failure, admission failure,
and interrupted consumers with retained binary output. Real Parquet query tests
cover offsets across batches, wildcard output, zero/short/oversized limits and
exhausted offsets. A workerd test verifies binary output after both producer
scopes close. A 4,096-row multi-page fixture confirms emulated transport switches
back to page reads under a 4 KiB pool while a 64 KiB pool can read full chunks;
both produce identical values and release all reservations. All 55 owned-scan
tests pass. Full/reference/workerd/typecheck/lint results are in the checkpoint.

This supersedes the earlier owned late-limit bypass limitation for the supported
paths described above. Remaining requirements include whole-runtime/GC headroom,
metadata/JS accounting, remaining physical output paths, and outstanding external
conformance/release gates. The Worker heap failure remains unresolved.

### Final vector top-k materialization admission (November 13, 2026)

The existing owned vector top-k pipeline admitted retained candidates and its
final vector selection, but converted that selection to plain rows outside output
admission. Its final projection now retains the selected vector owner and uses
materializeOwnedBatchRows before transferring plain rows to caller management.
Failures release the projected, selected and candidate owners. Legacy borrowed
adapters retain their existing boundary; this does not add owned capability to
an unowned source.

QueryResult.toArray also returns a completed columnar top-k result directly,
instead of slicing it into ordered batches and pushing every row into a second
result array. Window/qualify execution keeps its existing dispatch. The deferred
rank-reference top-k path remains separate and unowned; neither that path nor
unlimited row sorting is claimed as covered. Explicit toOwnedArray support for
sorted output remains required, as does accounting for results held after a plain
array handoff.

A real 128-row, sixteen-group Parquet fixture verifies descending output under a
512 KiB pool, and rejection specifically at the materialized-output reservation
under a 180 KiB pool (candidate processing fits). Both paths return pool usage to
zero. The ten query-owned-output tests pass, and Core/Parquet build passes.
The full suite passes 1,938 tests (363 skips), all 104 reference tests pass, and
repository typecheck/lint pass. Logs:
/tmp/lakeql-topk-output-{tests,full,reference,types,lint}.log.
The last dedicated workerd suite remains the preceding 34-test checkpoint.

### Worker reclamation diagnostic (November 13, 2026)

Extended bench/worker-heap.mjs with optional LAKEQL_HEAP_DIAGNOSTIC_GC=1. Both
measured cold/warm rounds still run without forced collection. Only after output
release and final cache eviction does the diagnostic request inspector GC and
sample the heap. The request needs a Worker turn to complete in this environment;
a standalone request timed out. The harness now dispatches a harmless /held
request and records a bounded probe failure as unavailable instead of discarding
the successful measured workload. Inspector GC is not a production control.

Command: LAKEQL_HEAP_ROWS=65536 LAKEQL_HEAP_MODE=owned
LAKEQL_HEAP_DIAGNOSTIC_GC=1 node bench/worker-heap.mjs.
Evidence: docs/benchmarks/worker-heap-reclamation-diagnostic-2026-11-13.json.
All query-result/status checks, pool bounds, retained-result checks, cache eviction
and four diagnostic probes pass. The fixture and compiled bundle hashes are
recorded in the artifact.

| Configured pool | Concurrent jobs | JS bytes after release/eviction | JS bytes after diagnostic GC |
| --- | ---: | ---: | ---: |
| 64 MiB | 1 | 47,898,432 | 3,811,140 |
| 64 MiB | 4 | 85,441,060 | 3,908,948 |
| 128 MiB | 1 | 31,592,180 | 3,810,116 |
| 128 MiB | 4 | 87,952,740 | 3,952,380 |

Backing storage after diagnostic GC is 226,763 bytes in each case. Live tracked
reservations are zero after eviction. These observations contradict a large
persistent reachable leak in these completed scenarios; most of the residual heap
is reclaimable. They do not prove that production collection will occur before a
Worker reaches its limit, or that there are no leaks in other paths.

The next allocation-contract work must account for the interval between logical
release and physical reclamation, including decoded typed backing as well as row
objects. A reservation released by ownership disposal is not evidence that the
runtime has reclaimed its bytes. A GC-sensitive admission policy must also prove
progress when callbacks are delayed, avoid treating caller aliases as reclaimed,
and preserve bounded concurrency/cache reuse. Merely forcing GC in a benchmark,
reducing a cap by a fixed percentage, or treating these post-GC samples as peak
measurements would not satisfy the Worker memory requirement.

### Default packed scans and extended heap stress (November 13, 2026)

Allocator-reuse inspection exposed a higher-impact upstream cause: owned scans
with an omitted projection left their requested columns undefined, so the direct
vector capability check declined even when all schema columns were supported.
The adapter now derives the complete top-level schema projection before that
check. Generic shape checks still select fallback for unsupported columns. It
preserves all columns, including nested/nullable fields, rather than requiring
callers to spell out a projection to obtain packed decoding. Text remains packed
through scanning and is decoded by admitted output materialization. No reusable
allocator was introduced without first addressing this avoidable fallback.

Real-fixture adapter tests require packed text/binary leaf storage through any
dictionary wrappers for both omitted and explicit projections; independent owner
lifetime and values remain verified. The broader run exposed a valid DuckDB
unsigned/binary fixture rejected by numeric definition-level decoding: a final
bit-packed run can contain multiple padded groups. The decoder now bounds output
by the page value count and input consumption by the encoded extent, without
visiting or allocating padded values. It retains the format's run-length bound.
V1/V2 tests cover a 64-level encoded run with four actual rows under exactly twelve
bytes of output/scratch admission, plus truncated-run rejection before allocation.
The DuckDB fixture passes again. Reference grammar:
https://parquet.apache.org/docs/file-format/data-pages/encodings/.

Two corrupt-fixture cleanup assertions now require the direct decoder's specific
PLAIN-range/compact-header rejection rather than an upstream RangeError wrapper.
They still reject budget errors and require zero leaked reservations. No external
conformance failure was reclassified as a pass.

Two-round local workerd/native-emulated-R2 evidence is in
`docs/benchmarks/worker-heap-packed-default-2026-11-13.json`. Compared with the
preceding owned diagnostic, a single job's tracked peak rises from 55,182,915 to
63,605,253 bytes because packed output planning includes decoding workspace;
actual sampled JS allocation falls. Four jobs now produce one successful result
at 64 MiB and two at 128 MiB, with the other jobs rejecting admission, instead of
zero and one respectively. This artifact predates the independent padded-level
fix; the fixture contains no such padding. All returned values are checked.

The benchmark now supports LAKEQL_HEAP_ROUNDS (2–32, default two). A twenty-round
run on the final decoder is recorded in
`docs/benchmarks/worker-heap-packed-repeated-2026-11-13.json`:

| Pool | Jobs per round | Successful jobs / attempts | Maximum sampled JS bytes | JS bytes after diagnostic GC |
| --- | ---: | ---: | ---: | ---: |
| 64 MiB | 1 | 20 / 20 | 177,377,864 | 4,093,152 |
| 64 MiB | 4 | 20 / 80 | 310,897,204 | 4,142,988 |
| 128 MiB | 1 | 20 / 20 | 256,106,688 | 4,095,572 |
| 128 MiB | 4 | 40 / 80 | 605,706,860 | 4,157,064 |

Each round releases outputs; final eviction returns tracked usage to zero. No
forced GC occurs during these rounds. This confirms that the short-run packed
improvement does not establish a whole-heap cap: delayed reclamation can dominate
repeated work even while live tracked admission stays bounded. Inspector sampling
is sparse and the local workerd setup does not enforce production Cloudflare's
memory policy; these are neither exact peaks nor production-OOM predictions.
Production runtime headroom and reclamation behavior remain acceptance work.

### Constrained managed-heap runtime evidence (November 13, 2026)

Added bench/worker-heap-runtime.mjs using the installed Miniflare public Runtime
and serializeConfig interfaces. The existing default remains native emulated R2.
An optional LAKEQL_HEAP_RUNTIME_MIB creates a direct local workerd with the V8
--max-heap-size flag and a host HTTP range fixture service. The fixture bytes are
created outside the measured Worker, as before. The backend choice and requested
managed-heap limit are recorded explicitly; HTTP transport is not labeled R2.
These flags are diagnostic controls, not an application setting or a deployment
recommendation. They do not constrain external buffers or total Worker memory.

bench/worker-heap-limit-probe.mjs verifies enforcement in a separate disposable
Worker: with --max-heap-size=32, three million-element arrays were retained; the
next allocation terminated that process with V8's "Reached heap limit" error.
The captured diagnostics show last-resort GC at about 28.5 MB before allocation
failure. Evidence: docs/benchmarks/worker-heap-limit-probe-2026-11-13.json.
The control intentionally exercises OOM and never targets a deployed service.

The actual 65,536-row Parquet workload completed twenty rounds in all eight
capacity/concurrency scenarios below. No forced GC occurred during measured
rounds. Successful output values, budget rejection codes, retained output, cache
reuse, live tracked caps and final eviction were verified. "Successful jobs"
excludes ordinary allocation-budget rejections, which remain expected when
concurrent collected outputs cannot fit.

| V8 heap limit | Tracked pool | Jobs / round | Successful jobs / attempts | Maximum sampled JS bytes | Maximum sampled backing bytes |
| --- | --- | ---: | ---: | ---: | ---: |
| 64 MiB | 64 MiB | 1 | 20 / 20 | 48,055,876 | 31,770,463 |
| 64 MiB | 64 MiB | 4 | 20 / 80 | 49,084,736 | 35,001,741 |
| 64 MiB | 128 MiB | 1 | 20 / 20 | 48,378,640 | 33,246,361 |
| 64 MiB | 128 MiB | 4 | 40 / 80 | 58,441,544 | 47,857,865 |
| 128 MiB | 64 MiB | 1 | 20 / 20 | 112,694,428 | 5,674,680 |
| 128 MiB | 64 MiB | 4 | 20 / 80 | 118,506,940 | 80,114,384 |
| 128 MiB | 128 MiB | 1 | 20 / 20 | 110,831,536 | 6,267,787 |
| 128 MiB | 128 MiB | 4 | 40 / 80 | 115,645,348 | 98,536,589 |

Artifacts: docs/benchmarks/worker-heap-constrained-{64,128}-2026-11-13.json.
Command: LAKEQL_HEAP_ROWS=65536 LAKEQL_HEAP_MODE=owned LAKEQL_HEAP_ROUNDS=20
LAKEQL_HEAP_RUNTIME_MIB=64 (or 128) LAKEQL_HEAP_DIAGNOSTIC_GC=1
node bench/worker-heap.mjs. The independent JS/backing maxima are not necessarily
simultaneous, are not summed, and are not treated as Cloudflare's billable-memory
measurement. Inspector snapshots are still sparse lower bounds on peaks.

The native emulated-R2 backend also passes its existing two-round matrix after
the harness refactor: docs/benchmarks/worker-heap-runtime-refactor-r2-2026-11-13.json.
Runtime failures now preserve completed rounds and stderr in the artifact and
set a failing process exit status. Inspector disconnects reject pending probes
instead of waiting for unrelated timeouts.

Decision: the unconstrained growth alone does not justify a production policy
that waits for FinalizationRegistry callbacks or forces GC. These constrained
runs show normal collection can handle this workload under managed-heap pressure.
They do not establish total-memory safety: external backing, native/runtime
headroom, unsupported execution paths and production Cloudflare enforcement
still require evidence. The original shared-budget and output-ownership scope
remains unchanged, and the goal is not complete.

### Raw compact metadata admission (2026-11-13)

Indexed acquisition now preflights the selected compact structures before constructing
raw metadata objects. The reservation uses the existing materialization charge model
for structs, properties, list slots, field names, scalar boxing, and borrowed binary
views; it does not charge borrowed payload bytes twice. Preflight validates extents
without invoking conversion visitors. This is a conservative accounting contract,
not a portable measurement of JavaScript heap size.

Envelope charges follow acquisition ownership, and each acquired row group retains
its own raw reservation plus its footer backing. If an initial footer read includes
page bytes and raw-envelope admission fails, acquisition releases that read before
retrying with the exact footer extent. Ample budgets retain the single-read path.
Converted schema, logical-type, statistics, and other metadata objects still need
separate admission; the legacy borrowed row-group API is not an owned-memory contract.

Tests cover exact admission and one-byte-short rejection, borrowed binary identity,
visitor suppression before admission, visitor failure cleanup, malformed extents,
source/group lifetime, and real Parquet planning/cache reuse. Adapter fixture budgets
now include the envelope, directory, and largest concurrently live raw group instead
of counting encoded bytes alone. The targeted adapter suite passes (23 tests).
Revalidation passes: 1,943 default tests (363 skipped), 104 reference tests,
34 workerd tests, typecheck, and lint. The heap/transfer benchmark matrices have
not yet been rerun with these additional charges.

#### Raw-metadata benchmark revalidation

The matching 65,536-row owned-output fixture completed all 20 rounds in every
pool/concurrency scenario under both enforced 64 and 128 MiB V8 managed-heap
settings. Success counts are unchanged: single jobs succeed 20/20; four concurrent
jobs succeed 20/80 with the 64 MiB allocation pool and 40/80 with the 128 MiB pool.
Other attempts reject at allocation admission. Checksums, retained output, warm
cache reads, and zero tracked allocations after eviction are verified by the harness.
Single-job tracked peak is now 63,620,659 bytes; concurrent tracked peaks remain
within their pool limits. Sampled JS heap maxima range from 47.9–58.5 MB with the
64 MiB V8 setting and 110.9–118.8 MB with the 128 MiB setting. These are sampled
managed-heap observations, not total Worker memory guarantees. No forced GC was
used during these runs.

Artifacts: `docs/benchmarks/worker-heap-raw-metadata-{64,128}-2026-11-13.json`.
Both record fixture and bundled-code hashes. Commands use `LAKEQL_HEAP_MODE=owned`,
`LAKEQL_HEAP_ROWS=65536`, `LAKEQL_HEAP_ROUNDS=20`, and the corresponding
`LAKEQL_HEAP_RUNTIME_MIB` with `node bench/worker-heap.mjs`.

The real HTTP selective-read fixture also passes after admission changes:
219,157 eager bytes versus 84,933 adaptive bytes (61.2% fewer); ignored-range
servers transfer 5,393,820 bytes on both paths. Maximum concurrent reads remains
two, warm concurrent runs perform zero data-range requests, and tracked peaks are
872,659 bytes (native ranges) and 681,258 bytes (ignored ranges). The eager count
is higher than the older artifact's 215,872 bytes; no claim of identical baseline
request behavior is made. The adaptive count is unchanged. Evidence is recorded in
`docs/benchmarks/deferred-output-http-raw-metadata-2026-11-13.json`.

### Decoded metadata text admission (2026-11-13)

Owned envelope/group parsing now reserves decoded names, paths, key/value strings,
creator strings, and geometry/geography CRS strings before invoking UTF-8 decoding.
The shared helper charges a string header plus two bytes per input byte, a safe
UTF-16 length upper bound including malformed UTF-8 replacement characters. Charges
remain with the supplied metadata scope and its retained owners. Borrowed legacy
conversion remains compatible. This closes the explicit text-decoding gap only:
converted objects/arrays and statistics conversion still require admission.

Five focused cases cover empty/ASCII/multibyte text, exact and one-byte-short limits,
no decoder call on rejection, undefined values, malformed UTF-8, and retained owner
lifetime. Together with real-fixture adapter/indexed planning tests, 40 targeted
tests pass. Full default suite: 1,948 passed, 363 skipped. Core/Parquet build,
typecheck, and lint pass. Runtime benchmark artifacts above predate this text charge.

Text-admission follow-up: BYTE_ARRAY statistics now use the same pre-decode string
reservation while preserving the statistics decoder's `ignoreBOM: true` behavior.
Checked decimal statistics continue through numeric conversion. A direct converter
regression checks BOM/multibyte fidelity, exact capacity, and budget failure
propagation; the shared decoder test also distinguishes the two BOM policies.
The preceding metadata-name change passed 34 workerd and 104 reference tests.
Non-text statistics (including timestamp/decimal conversion) and converted
object/array allocation remain open; no complete metadata accounting claim is made.
Text-statistics follow-up validation: 1,950 default tests pass (363 skipped),
Core/Parquet build and lint pass. Worker/reference results above precede this
follow-up; the real-fixture indexed planner and adapter tests pass with its charges.

### Scalar decimal statistics conversion (2026-11-13)

Checked decimal bounds now call a scalar entry point sharing the batch converter's
precision/scaling implementation. This removes the one-element input array and
`Array.from` output per bound without duplicating arithmetic or changing the rule
that unsafe bounds cannot prune rows. A signed fixed-byte regression covers positive
and negative bounds and omission of an unsafe bound. Decimal, statistics-admission,
and real-fixture indexed planning suites pass (24 tests); Core/Parquet build passes.
This removes allocation work but is not complete non-text statistics admission.

### Converted column-container admission (2026-11-13)

Owned row-group conversion now plans and admits the column chunk object, column
metadata object, statistics/size/geospatial containers, and mapped encoding/path/
key-value/encoding-statistics arrays before constructing them. The plan uses fixed
output property counts and actual list lengths, rejecting invalid list shapes and
unsafe extents. Text output retains its separate admission. Borrowed histogram and
crypto trees remain covered by raw ownership rather than counted as new payloads.

Regression coverage includes exact capacity, one-byte-short rejection before any
reservation succeeds, mapped enum values, borrowed histogram identity, owner lifetime,
and text-budget error cleanup. The real-fixture adapter and indexed planner suites
also pass (38 tests across the targeted run); Core/Parquet build passes. Row-group
containers, envelope converted structures, and non-text scalar conversions are still
open accounting work.
Converted-column validation checkpoint: 1,952 default tests pass (363 skipped),
Core/Parquet build, typecheck, and lint pass. Earlier heap benchmark numbers predate
these additional container charges and must not be treated as current measurements.

### Converted row-group containers (2026-11-13)

Owned row-group decoding now admits its column-list header before construction and
each output slot before appending a converted column. Final conversion reserves the
seven-property row-group object plus the sorting array and its three-property
entries. Non-streamed conversion with a scope also admits its mapped column list;
preconverted columns retain their existing owner rather than being charged twice.
Exact-shape and one-byte-short tests include sorting metadata and empty columns.
The targeted converter, indexed planner, and adapter suites pass (39 tests), as does
the Core/Parquet build. Converted envelope structures and non-text statistics remain
open, along with other execution-path gaps documented earlier in this audit.
Row-group container checkpoint: 1,953 default tests pass (363 skipped); build,
typecheck, and lint pass. Runtime benchmark refresh remains outstanding.

### Metadata-container runtime revalidation and contention finding

The updated code passes 104 reference tests and 34 workerd tests. HTTP wire counts
remain 219,157 eager versus 84,933 adaptive for native ranges, and 5,393,820 each for
ignored ranges. Tracked peaks increase to 928,355 and 709,106 bytes respectively;
read concurrency remains two. Evidence: `deferred-output-http-metadata-containers-2026-11-13.json`.

The matching 65,536-row owned-output matrices complete all 20 rounds without runtime
OOM at both 64/128 MiB V8 managed-heap settings. Single jobs still succeed 20/20.
However, four-job runs now succeed only 20/80 at BOTH allocation pool sizes; the
128 MiB pool previously succeeded 40/80. This is a measured concurrency regression,
not a fully unchanged benchmark result. Tracked peaks remain within the caps:
63,627,633 bytes single-job, 67,049,618 bytes four-job/64 MiB, and 134,184,290 bytes
four-job/128 MiB. Full artifacts: `worker-heap-metadata-containers-{64,128}-2026-11-13.json`.
These managed-heap observations still do not establish total Worker memory caps.

A two-round diagnostic records allocation error details in benchmark reports. At
128 MiB, two concurrent jobs request `materialized output` with actual=134,222,831
against limit=134,217,728 (5,103 bytes over); another rejects at 134,577,932. They
reject before competing failed jobs release their accumulated output. This supports
an admission-timing contention explanation, not an unbounded-memory fix. Preserve
strict admission; investigate generic progress-aware scheduling/retry without
retaining all blocked jobs indefinitely or relying on users to tune concurrency.
Evidence: `worker-heap-metadata-contention-2026-11-13.json`. The harness now retains
allocation details for future regressions. These runs are local HTTP/workerd evidence,
not production Cloudflare measurements or latency comparisons.

### Recovery probe distinguishes contention from output capacity

The Worker harness now has an opt-in `LAKEQL_HEAP_RECOVERY_PROBE=1` mode (owned
outputs only). After a round with budget failures, it attempts one fresh job while
preserving all successful output owners. It validates the extra job's checksums,
all retained values, pool peak, and final zero usage after release/eviction. This is
a diagnostic extra workload, not automatic engine retry or comparable latency data.
Normal benchmark mode does not run the probe.

At 65,536 rows and a 64 MiB V8 managed-heap setting, the 128 MiB pool's cold round
completed one original job and the subsequent probe completed another. Its next
round completed two original jobs; the subsequent third-output attempt rejected.
The 64 MiB pool completed one original job per round and rejected each additional
probe. Thus two outputs still fit the larger allocation pool, while cache/history
and concurrent admission affect progress. The earlier one-success-per-round matrix
remains valid for its recorded workload; it is not an invariant maximum capacity.
Artifact: `worker-heap-contention-recovery-2026-11-13.json` (two rounds, four scenarios,
all completed). The probe changes warm-round cache history and must be labeled when
comparing results. Generic progress-aware admission/scheduling remains unresolved;
unconditional retry with partially retained output risks deadlock and wasted reads.

### Release failed collections before asynchronous producer close

Consumer-side collection failures (append admission or cancellation checked in the
loop) now dispose accumulated output before `for await` invokes/awaits producer
`return()`. Previously those chunks remained pinned until remote/async producer
cleanup completed. The currently yielded handle still releases before iterator
closing, and the outer finally remains idempotent. No automatic retries or weakened
allocation limits are introduced.

A deterministic regression suspends producer closure after the second append exceeds
a 511-byte pool. While closure is suspended, usage must be zero and a sibling must
successfully reserve the entire pool. The test also awaits final rejection and
checks zero usage afterward. The collector suite passes (4 tests), and Core/Parquet
build passes. This fixes consumer-side cleanup timing only; producer-side materialized
output failures from the concurrency benchmark can still unwind asynchronously
before the collector observes them. The broader scheduling issue remains open.
Early collection cleanup checkpoint: 1,954 default tests pass (363 skipped), and
lint passes. No claim that the producer-side contention benchmark is resolved.

### Cancellation while awaiting a producer

Owned collection now installs an abort listener that disposes its accumulated
output immediately, including while `next()` is awaiting remote work. The listener
is removed in the terminal finally. The collector still awaits producer completion
and reports `LAKEQL_ABORTED`; this change does not claim to cancel arbitrary stores
or release allocations still owned by the producer. Checks before iteration,
before append, and before finish remain in place.

A deterministic stalled-producer regression verifies that abort frees prior chunks
synchronously and allows a sibling to reserve the entire pool before the producer
resumes. The collector suite passes (5 tests), and Core/Parquet build passes. This
improves cancellation ownership, not the non-cancelled concurrent-admission policy.
Cancellation checkpoint: 1,955 default tests pass (363 skipped); lint passes.

### Converted footer envelope containers

Owned envelope decoding now admits schema/key-value array headers, their appended
slots, ten-property schema entries, two-property key/value entries, and the outer
three-property envelope plus six-property metadata object before construction.
Decoded text continues to have separate charges. The parser's borrowed binary views
remain under footer ownership. Logical-type objects and non-text statistics remain
open accounting work; this does not claim complete parser heap admission.

A generated real Parquet fixture covers Unicode schema/key-value text, exact measured
capacity, one-byte-short rejection, retained envelope ownership, and zero usage after
release/disposal. Existing indexed planning, adapter, and incremental metadata tests
pass (44 tests before the additional fixture); Core/Parquet build passes.
Envelope checkpoint: 1,956 default tests pass (363 skipped), and lint passes.
Earlier runtime benchmark artifacts predate the envelope container charges.

### Logical metadata object admission

Logical-type conversion now admits recognized objects with their actual property
counts before construction. Its fixed simple/time lookup tables are shared instead
of recreated for each schema field. Geometry/geography text retains the separate
UTF-8 reservation. Unknown logical metadata returns the original raw object without
an extra converted-object charge. Tests cover exact shape admission and one-byte-
short rejection for string, decimal, timestamp, integer, and variant annotations,
plus undefined/unknown preservation with zero capacity. Non-text statistic values,
parser scratch allocations, and the earlier execution-path gaps remain open.
Logical metadata checkpoint: 1,962 default tests pass (363 skipped), Core/Parquet
build and lint pass. Runtime measurements predate these logical object charges.

### Fixed-width statistic admission

Owned INT32/INT64/FLOAT/DOUBLE statistic conversion now reserves a view and numeric
scalar charge before conversion, plus bigint for INT64 and a temporal-value allowance
for date/timestamp annotations. These are model charges rather than VM object-size
measurements. Checked decimal fallback explicitly rethrows budget errors instead of
silently dropping their pruning bound. A real encoded INT64 bound verifies scaled
output at exact capacity, rejection one byte below, and cleanup of the enclosing
column reservation on failure.

Fixed-byte decimal conversion, other parser scratch/derived structures, and the
previously documented execution/scheduling gaps remain open. The new charge does
not imply complete metadata or total Worker heap accounting.
Fixed-width checkpoint: 1,963 default tests pass (363 skipped), Core/Parquet build
and lint pass. Runtime measurements require refreshing after the new charges.

### Accumulated metadata/cancellation validation checkpoint

With envelope/logical/fixed-width admission and early cancelled/failed collection
release in place: 34 workerd tests, 104 reference tests, and typecheck pass. External
conformance remains 232 passed, 18 failed, 1 skipped. Comparing the exact failing
names against the pre-metadata checkpoint finds no additions or resolutions. The
failures remain open release gates; absence of new failures does not establish full
format support. Machine-readable comparison:
`docs/benchmarks/metadata-admission-conformance-2026-11-13.json`.

Refreshed 20-round owned-output matrices (65,536 real fixture rows) complete at both
64/128 MiB V8 managed-heap settings with no runtime OOM. Single jobs succeed 20/20;
four-job rounds succeed 20/80 at both pool sizes, preserving the known contention
regression. Tracked peaks: 63,630,913 bytes single job, 67,063,890 bytes four jobs at
a 64 MiB pool, 134,198,562 bytes four jobs at a 128 MiB pool. Checksums, retained
owners, and zero usage after eviction pass. Artifacts:
`worker-heap-envelope-statistics-{64,128}-2026-11-13.json`. These are tracked-allocation
and local managed-heap experiments; they do not prove a total Cloudflare memory cap.
No recovery probe or forced GC was enabled during these measurements.

### Single-batch owned output handoff

When collection contains exactly one nonempty chunk, finish now returns an
independent retained owner of that chunk's existing array. It avoids allocating a
second result array and does not retain collector bookkeeping. Input release or
collector disposal cannot invalidate the returned owner. Multi-batch assembly keeps
its exact final-array admission; splitting batch-array storage from row payload
ownership is still necessary before redundant arrays can be released there.

A 292-byte boundary regression verifies array identity, no extra peak reservation,
independent source/output lifetime, and zero after release. The existing exact-final-
array boundary test now uses two chunks (711-byte failure/712-byte success), keeping
coverage of actual concatenation. Six collector tests and Core/Parquet build pass.
Single-batch handoff checkpoint: 1,964 default tests pass (363 skipped); lint passes.
This optimization does not resolve multi-batch concurrency admission contention.

### Materialization scratch lifetime

Materialization plans now expose retainedBytes separately from allocationBytes.
Execution reserves the entire original peak extent atomically through a child stage,
then transfers output ownership and releases unused stage capacity. Top-level/nested
field-descriptor arrays and UTF-8 decoding views are transient charges; binary views,
row objects, text, property keys, and borrowed binary backing remain retained.
This changes lifetime accounting, not the pre-decode admission requirement. Planning
itself still has scratch-allocation gaps and map conversion remains conservative.

Tests verify unchanged exact peak admission and pre-decode rejection, a 432-byte
scratch release for the packed-text fixture, nested values, binary lifetime, and
failure cleanup. Full suite: 1,964 passed, 363 skipped; Core/Parquet build and lint pass.
A two-round real-fixture workerd run under 64 MiB managed heap completes all scenarios.
Single-job tracked peak drops from 63,630,913 to 55,350,721 bytes. Four-job runs still
complete only one job at each pool size; the contention issue is not resolved.
Artifact: `worker-heap-materialization-scratch-2026-11-13.json`. This short run is not
a replacement for the 20-round matrix or a total Worker memory guarantee.

Scratch-release extended validation: both 20-round owned-output matrices complete
under 64/128 MiB V8 managed-heap settings. Single jobs succeed 20/20, four-job runs
20/80 at either pool size. Tracked peaks remain 55,350,721 bytes for single jobs,
66,939,406 for four jobs/64 MiB pool, and 134,079,088 for four jobs/128 MiB pool.
Checksums, held output, and zero final tracked usage pass. Artifacts:
`worker-heap-scratch-released-{64,128}-2026-11-13.json`.

The Worker materialization assertion now distinguishes retainedBytes from full
allocationBytes and still checks full peak admission. An added Worker test verifies
borrowed binary backing after stage/source disposal, independent retain/release,
and zero usage after final release. All 35 workerd tests pass; typecheck also passed
before this added test. The scheduling regression remains unresolved and these
experiments still do not establish total production Worker memory limits.

### Broader release-gate refresh after scratch lifetime changes

The full `pnpm build` succeeds, including distribution bundles and declarations.
`pnpm check:packages` passes again against those fresh outputs (temporary consumer
installation and smoke test). Compatibility generation, tracked-file size checking,
and fixture-generation idempotence also pass. Initial sandbox subprocess denials
for package/size checks were resolved by rerunning with subprocess access; they
were not product failures. The size checker covers tracked files only, so it does
not establish that newly added untracked benchmark artifacts satisfy every release
packaging policy. Coverage is being evaluated separately; conformance's 18 known
failures and the substantive ownership/scheduling gaps remain open.

Coverage refresh: default `pnpm coverage` timed out in the gzip fixture matrix
under instrumentation (1 failed, 1,963 passed). Repeating the same suite with bounded
runner concurrency, `pnpm coverage --maxWorkers=4`, passes all configured coverage
thresholds: statements 90.91%, branches 84.75%, functions 95.08%, lines 92.93%.
No tests or thresholds were weakened. This establishes a passing bounded-concurrency
coverage run; the default runner's timeout remains a test-execution reliability issue,
not proof of a gzip decoding failure. Full goal completion remains unproven because
conformance, admission scheduling, and the documented accounting/operator gaps remain.

### Default coverage reliability

The gzip fixture matrix now reports each size/pattern combination as an independent
case. All six sizes, two patterns, four compression strategies, and three levels
remain tested (144 combinations); exact byte comparisons are unchanged. Per-case
reporting avoids imposing one test timeout on the entire instrumented matrix and
identifies the failing fixture if a regression occurs. No timeout or coverage
threshold was increased.

Default `pnpm coverage` now passes: 1,975 tests passed, 363 skipped, statements
90.91%, branches 84.75%, functions 95.08%, lines 92.93%. The eleven-test count
increase is solely matrix parameterization. A direct file-size inventory also finds
no benchmark artifact above 1 MiB; the audit itself is below that threshold. These
checks include the newly added artifacts rather than relying solely on tracked-file
inventory. Substantive conformance, memory ownership, and scheduling work remains.

### Independent inspection of two external failure fixtures

Reproducing `bad_data/ARROW-GH-41317.parquet` through the public reader exposes the
upstream cause `parquet unsupported page type: INDEX_PAGE`; `ARROW-GH-41321.parquet`
exposes `parquet bitpack offset 9 out of range` during definition-level decoding.
These are decoding failures, not allocation-budget rejections.

An independent DuckDB `SELECT * FROM read_parquet(...)` initially rejects both for
GeoParquet metadata (`geometry` lacks geometry types). With the installed engine's
`enable_geoparquet_conversion=false`, full reads instead reject with
`TProtocolException: Invalid data` and `Decode bit width too large`, respectively.
This strengthens the malformed-input suspicion but does not by itself classify the
fixtures as expected negative cases or prove the precise corruption boundary. No
conformance expectation or decoder behavior was weakened. In particular, blindly
skipping an INDEX_PAGE would not be justified by this evidence. Explicit malformed
fixture policy and bounded, deterministic error behavior remain acceptance work.

### Malformed HTTP fixture ownership regressions

Added real-fixture tests for ARROW-GH-41317 and ARROW-GH-41321 over both HTTP 206
range responses and HTTP 200 ignored-range responses. Each scenario runs twice with
an explicit 64 MiB pool, disabled range cache, batch size three, and read-concurrency
limit two. It requires the specific INDEX_PAGE/bitpack decoder cause wrapped in
LAKEQL_PARQUET_READ_ERROR, so a budget rejection cannot masquerade as validation.
Tracked usage returns to zero after every failure and peak never exceeds the pool.
All four scenarios pass; test lint passes. These tests verify error ownership over
remote transport, not whether each external fixture should count as an expected
negative conformance case. The broad conformance gate is unchanged.

### Avoid descriptor arrays during materialization preflight

Preflight now enumerates own enumerable top-level and struct fields directly instead
of constructing `Object.entries` descriptor arrays, including repeated per-row
nested descriptors. Execution's descriptor charges remain unchanged because the
materializer still constructs them under peak admission. A prototype-bearing nested
fixture verifies inherited fields are ignored exactly as with Object.entries and
that both output values and allocation extents match an own-field-only batch.

This removes a concrete pre-admission allocation source; recursive cycle tracking,
VM enumeration internals, and other planning allocations remain outside a complete
portable heap model. No latency speedup is claimed without measurement. Full suite:
1,980 passed, 363 skipped; Core/Parquet build and lint pass. This count includes the
four newly added malformed HTTP ownership cases.

### Nested struct execution: allocation/CPU tradeoff

Struct value materialization now enumerates own fields directly, removing each
row's Object.entries descriptor array and pair arrays. Corresponding execution
scratch charges are removed from the planner. Top-level descriptors remain shared
once per batch. Prototype-bearing nested fixtures and the full suite pass (1,980
passed, 363 skipped); Core/Parquet build passes.

A reproducible local microbenchmark (`node bench/struct-materialization.mjs`, after
build) compares the prior entries loop with current conversion for 50,000 eight-field
numeric structs, checking checksums and alternating order after warmup. Median times
in this run: 9.15 ms prior versus 9.58 ms current (~4.7% slower). This is an explicit
CPU/allocation tradeoff: avoid per-row transient descriptor allocations for bounded
memory operation, not a claim of universal speedup. The benchmark is narrower than
a full Parquet workload and does not measure GC/Worker behavior. Artifact:
`docs/benchmarks/struct-materialization-2026-11-13.json`. Future shape caching must
itself have bounded ownership before replacing this allocation contract.

### Arbitrary data-key materialization consistency

Selected-batch materialization now uses the existing safe record setter, matching
single-row and nested conversion for `__proto__`. Generic column-vector/struct maps
and merged Parquet vector maps use null-prototype dictionaries to avoid inherited
setters during construction. Unit regressions verify own data properties, preserved
ordinary row prototypes, selected/single-row parity, and batchFromColumns roundtrip.

A real generated Parquet fixture with `__proto__` and `constructor` reveals an
additional unresolved path: the full query still drops `__proto__` upstream of these
materialization boundaries. The generic fixes therefore do not establish end-to-end
arbitrary-column-name correctness. This remains acceptance work rather than a
passing fixture claim. Initial selected-output suite passed 1,981 tests; expanded
map-construction validation is recorded below when complete.
Expanded map-construction checkpoint: 1,982 default tests pass (363 skipped),
Core/Parquet build and changed-file lint pass. Full Parquet key preservation remains open.

### Prototype-like Parquet key follow-up

The additional loss occurred in vector projection's ordinary object map. Projection
now stores selected columns and aliases in a null-prototype dictionary. A generated
Parquet fixture with `__proto__` and `constructor` now survives both unfiltered owned
scan and explicit projection with an integer predicate, at batch size one and a
64 KiB allocation pool. Tests assert own column values, normal output row prototypes,
and zero tracked memory after each owner release. This resolves the specific full
query reproducer from the preceding entry; it is not a claim that every dictionary
throughout every operator has been audited. Targeted adapter/projection suites pass
(28 tests), and Core/Parquet build passes.
Parquet data-key checkpoint: 1,983 default tests pass (363 skipped), typecheck and
changed-file lint pass. The full goal's memory/scheduling and conformance gaps remain.

### Synchronous producer failure releases collected output

Owned row collection accepts a source factory with a terminal failure callback.
The callback disposes accumulated rows synchronously and throws the original error.
Owned projected and deferred-limit materialization invoke it when materialization
fails, before async scan iterator cleanup unwinds. This complements consumer-side
failure/abort cleanup without adding retries, waits, eviction of live successful
results, or weaker admission. Other upstream failure locations still rely on their
existing unwind paths; this is not a general scheduling guarantee.

A deterministic producer failure test suspends its finally block and verifies zero
collector usage, a full-capacity sibling reservation, and exact original error
identity. Full suite: 1,984 passed, 363 skipped; Core/Parquet build and lint pass.
A short real-fixture Worker run restores two successful concurrent jobs at the
128 MiB allocation pool, while the 64 MiB pool still completes one. Extended
matrices below verify whether that recovery persists.

Both 20-round matrices confirm the recovery: single jobs 20/20, four-job runs
20/80 at a 64 MiB pool and 40/80 at a 128 MiB pool, under both 64/128 MiB V8
managed-heap settings. Tracked peaks are 55,350,721 bytes single-job, 66,965,942
four-job/64 MiB pool, and 134,150,917 four-job/128 MiB pool. All scenarios complete
without runtime OOM; checksums, retained output, and zero final usage pass. Artifacts:
`worker-heap-early-failure-{64,128}-2026-11-13.json`. The observed concurrency regression
is resolved for this workload through lifetime cleanup, not a global progress/fairness
policy. Total production Worker memory guarantees remain unproven.

### Query-level early-failure regression

A controlled owned scanner now exercises the public `run().toOwnedArray()` wiring:
a small first output fits, a large second UTF-8 output exceeds a 4 KiB pool, and
scanner shutdown remains suspended. During suspension, tracked usage is zero and
a sibling reserves the entire pool. Final rejection must retain the original
LAKEQL_BUDGET_EXCEEDED `materialized output` purpose. This supplements the generic
collector test and measured real-Parquet Worker matrices, ensuring the query actually
uses the early-release notification. Five query-memory tests and changed-file lint
pass. It is a deterministic scanner-lifecycle test, not an HTTP latency simulation.
Early-failure integration checkpoint: all 35 workerd tests and typecheck pass.

### Deferred-output failure notification coverage

A second public-query regression exercises explicit projection + predicate + limit,
with separate predicate and deferred-column scans. The first text output succeeds;
the second exceeds an 8 KiB pool and stalls during its deferred scanner's cleanup.
Previously collected output must already be released, while current predicate/gather
backing may remain until outer cleanup. Final failure retains the materialized-output
budget purpose and final tracked usage is zero.

The six query-memory tests pass. A controlled negative run temporarily disabled only
the terminal notification: this test failed with 2,551 bytes still held versus the
less-than-1,024-byte assertion. The implementation was restored immediately. This
confirms the test detects the intended lifetime regression rather than merely eventual
cleanup. It complements the real-Parquet multi-job measurements; it is not a transport
performance benchmark or a general deadlock/fairness proof.

### Reconciled current summary and coverage

The opening summary now separates historical unconstrained/public-R2 measurements
from current constrained Worker evidence, describes implemented metadata admission,
and preserves remaining acceptance gaps. Default coverage passes with 1,986 tests,
363 skips, statements 91.13%, branches 84.88%, functions 95.16%, lines 93.18%.
This validates the accumulated changes, including both early-failure query paths;
it does not close external conformance or whole-query/Worker allocation coverage.

### Scalar decimal output and exponent scratch admission

Checked scalar decimal metadata conversion accepts the owning scope and reserves a
numeric output charge before conversion. Scale >22 also reserves bounded string
scratch for formatting the safe unscaled integer, safe scale, and combined exponent
notation, releasing scratch after execution. The bound depends on safe-integer text
lengths, not the encoded byte-array length or scale magnitude. The existing converter
still rejects unsafe integer values and never constructs input-sized bigints.

Tests cover 65,536-byte sign extension with a 16-byte scalar-output model, exact
344-byte large-scale peak admission with 16 bytes retained, and one-byte-short
pre-conversion rejection. Fixed-width decimal error propagation still passes;
intermediate unscaled-statistic charges follow their enclosing scope until disposal.
Full suite: 1,988 passed, 363 skipped; Core/Parquet build and changed-file lint pass.
These modeled charges do not cover all VM/parser scratch or close the broader
whole-query memory requirement.

### Retire unscaled decimal-statistic intermediates

Checked INT32/INT64 decimal bounds now decode the unscaled statistic under a child
scratch scope and keep only the scaled output charge in the metadata owner. Scratch
ends in finally on success, unsafe-bound fallback, and budget rejection. Fixed-byte
bounds do not create this unused intermediate scope. The exact peak test retains
the original combined admission requirement while checking a smaller post-conversion
charge and zero reservation after failed output admission. Targeted decimal/statistic
suites pass (17 tests), and Core/Parquet build passes.
Decimal intermediate checkpoint: all 1,988 default tests pass (363 skipped);
changed-file lint passes. Broader allocation coverage and conformance remain open.

### Fixed-width statistic view scratch lifetime

Numeric/date/timestamp metadata conversion now reserves peak view-plus-result space
atomically in a child stage, transfers the scalar owner, and releases the temporary
DataView charge. Integer-decimal conversion also benefits: unscaled view scratch
ends before final decimal output admission, while the intermediate scalar stays
charged until conversion completes. Peak requirements reflect simultaneous lifetimes
rather than summing non-overlapping scratch.

Direct encoded INT64 and microsecond timestamp regressions verify output values,
exact peak versus retained bytes, one-byte-short rejection, and error cleanup.
The existing checked-decimal boundary test is updated for the shorter overlap.
This still uses explicit model units rather than portable VM sizes; other parser
scratch and whole-query gaps remain open.
Statistic-view checkpoint: 1,990 default tests pass (363 skipped), Core/Parquet
build and changed-file lint pass. Runtime metadata peaks need a later refresh.

### Indexed schema-leaf derived ownership

Indexed metadata sources now plan/admit the copied schema-leaf array and records
before construction. A single pass replaces filter().map(), avoiding an intermediate
array. Copies preserve original annotations before GeoParquet marking, as required
for deferred statistics conversion. Derived records and the directory are built in
a child scope and transferred together; failure rolls both back. They remain owned
by the source, and completed row groups need not retain this lookup structure.
Footer-prefix narrowing now includes leaf-copy charges in its headroom calculation.

Exact-capacity and one-byte-short indexed-footer tests include copied records; a
real decimal metadata fixture and indexed planner/cache/lifetime tests pass (38
targeted tests), as does Core/Parquet build. This admits one derived structure;
GeoParquet parser allocations and other metadata/runtime scratch remain open.
Indexed-leaf checkpoint: 1,990 default tests pass (363 skipped); full lint passes.

### GeoParquet post-parse allocation boundary

Inspection of the installed `hyparquet/src/geoparquet.js` confirms that marking
annotations performs JSON.parse on the entire `geo` key-value string, builds a Map
for every declared WKB column, formats CRS strings, and creates logical-type objects
before walking eligible root schema leaves. Both indexed source construction and
incremental full metadata conversion call this routine. Text admission covers the
input string, and indexed-leaf admission covers copied schema records; neither covers
this parsed JSON tree, annotation Map, derived CRS strings, or logical records.

This remains a specific uncovered expansion path, including metadata entries that do
not correspond to selected schema leaves. A complete fix must admit JSON expansion
before parsing and give retained annotations their own lifetime, with bounded scratch
for unused declarations. Avoid treating input-text bytes as equivalent to parsed JSON
heap or charging only the final selected annotations after parsing has already occurred.
Tests should distinguish absent geo metadata, large irrelevant column declarations,
malformed JSON, CRS conversion, nested/root eligibility, and budget rejection before
expansion. Existing GeoParquet vector tests provide real WKB operation coverage but
do not establish these allocation bounds. No behavior was changed at this checkpoint.

### GeoParquet JSON tree and retained annotations — 2026-11-13

Implemented stackless lexical allocation preflight for parsed metadata JSON. It
charges containers, properties, string storage, and scalar/slot storage before
`JSON.parse`; escape sequences conservatively use their encoded extent. The
input string remains separately owned. This models parsed-tree storage, not VM
parser workspace or a portable total-heap bound.

Indexed footer GeoParquet conversion now parses under a temporary child scope
and retains only eligible root-column annotations and derived CRS strings under
the construction scope. Parsed JSON storage is released after marking, and
indexed construction failures roll back all retained annotation allocations.
The unscoped incremental metadata API keeps its existing unscoped contract.
There is no intermediate Map or Object.entries array for every declared column.
Nested flattened schema subtrees are skipped completely. Existing logical types
are preserved. Only eligible WKB columns have CRS identifiers interpreted;
identifiers require a string authority and string or finite numeric code.
Irrelevant metadata declarations do not produce annotations or CRS errors.

Evidence: ten new JSON/annotation tests cover exact parse admission, rejection
before JSON.parse at one byte below admission, escaped/non-ASCII strings,
containers/scalars, malformed syntax rollback, scratch release, absent metadata,
and nested-schema eligibility. An encoded Parquet footer test independently
compares metadata and row-group conversion against hyparquet, exercises the
`crs.ids` fallback, and verifies zero retained bytes after indexed construction
fails at peak minus one. The full default suite passed 2,000 tests with 363
skipped before that last encoded-footer test was added; the updated footer file
then passed all three tests. Core/Parquet TypeScript build and changed-file lint
passed. The first sandboxed full run failed localhost HTTP tests; the completed
rerun with localhost permissions passed in 13.51 seconds.

Still required: measure end-to-end cloud read and constrained-runtime behavior
with these charges, refresh release gates, and account for JSON parser workspace
and other outstanding decoder/operator/output boundaries. This change does not
establish a total Cloudflare Worker memory cap.

### GeoParquet transport validation and refreshed heap stress — 2026-11-13

`geo-http.test.ts` constructs encoded Parquet containing WKB points and GeoParquet
JSON, including unused JSON payload. Four concurrent scans run cold and warm over
actual localhost HTTP, separately honoring ranges (206) and ignoring ranges (200).
Both cases return identical geometry bytes, perform zero warm data reads, respect
the two-read concurrency limit, and return shared-pool usage to zero after cache
eviction. Both tests pass. The workerd fixture suite also passes all 35 tests.

`worker-heap-geo-admission-64-2026-11-13.json` refreshes the existing 65,536-row
owned-output stress fixture for twenty rounds in a local workerd runtime with a
64 MiB managed-heap flag. This fixture is ordinary string Parquet, not GeoParquet;
it measures regression effects of current shared metadata/output changes, while
the dedicated HTTP test exercises GeoParquet behavior.

| Shared pool | Jobs per round | Successful jobs | Peak tracked bytes |
| --- | ---: | ---: | ---: |
| 64 MiB | 1 | 20/20 | 55,351,969 |
| 64 MiB | 4 | 20/80 | 66,967,926 |
| 128 MiB | 1 | 20/20 | 55,351,969 |
| 128 MiB | 4 | 40/80 | 134,154,661 |

All four scenarios completed; successful-job counts match the preceding
failure-cleanup checkpoint. Failed admissions remain bounded budget rejections.
The script verifies successful checksums and retained results and eviction cleanup.
This remains a managed-heap experiment, not a total Worker memory cap.

**Next uncovered ownership boundary:** `readOwnedParquetMetadata` passes its
scope to footer acquisition but `readFooterMetadata` invokes the unscoped
`incrementalParquetMetadata`, then decimal normalization. Consequently this full
metadata fallback owns footer bytes without admitting its complete parsed metadata
tree (including GeoParquet JSON). Indexed-source admission does not close this
alternate path. A durable fix must thread ownership through full envelope/group
conversion and normalization, account for retained group arrays and temporary raw
objects, and verify full-metadata cache lifetime under actual fallback workloads.

### Full-metadata decoder admission foundation — 2026-11-13

Added `decodeOwnedFullMetadata`, a transactional full-footer decoder built on the
existing admitted envelope and checked row-group converters. It admits the group
array and final result record, retains converted metadata under the caller's scope,
and releases a temporary row-group directory and leaf-reference list before handoff.
GeoParquet parsing and annotations use the same construction scope. Failure at any
stage rolls back the complete construction without disturbing caller allocations.
Raw compact record charges remain conservatively retained with metadata; finer
scratch ownership is still outstanding.

The generated encoded-footer test checks normalized-reader parity, owner retention
beyond scope disposal, temporary-storage release, and zero usage after failure at
measured peak minus one. A separate external decimal fixture test checks parity
with the existing normalized full reader. This is an internal decoder foundation;
`readFooterMetadata` has not yet switched to it. The next step is to connect the
scoped path and revise tests that previously assumed footer bytes alone were the
full allocation cost, preserving exact footer-acquisition coverage separately.

### Full-metadata fallback integration — 2026-11-13

Connected the scoped `readFooterMetadata` path to `decodeOwnedFullMetadata`.
`readOwnedParquetMetadata` now retains admitted decoded metadata as well as footer
backing, including full row-group storage, checked decimal conversion, and
GeoParquet annotations. Unscoped callers retain their existing API behavior.
The owned path avoids the old extra decimal-normalization pass by using the
checked group converter directly.

Acquisition and construction now share a transactional child scope. If decoding
fails its allocation budget while the initial footer buffer includes speculative
prefix bytes, release that entire attempt and retry using the exact footer extent.
An exact-footer failure propagates without further retries. This preserves bounded
allocation and trades repeated footer traffic for usable decoding headroom only
when necessary; it does not retry malformed input or transport errors.

Validation: core/Parquet TypeScript build passed; full default suite passed 2,005
tests (363 skipped) in 13.88 seconds. Subsequently added an exact-capacity regression
and reran all seven metadata ownership tests successfully. That regression uses a
100,000-row encoded Parquet file, measures the decoder allocation requirement,
asserts both requested ranges exactly, verifies successful metadata and retained
charges at the required cap, and verifies budget rejection plus zero residual
usage at cap minus one. Cache acquire/eviction lifetime tests pass. The external
byte-decimal test now derives its capacity from actual admitted decoder costs and
still matches normalized metadata and decoded row extrema.

Kept footer-only assembly/refetch and 1,024-byte initial-probe boundary checks at
the acquisition API, where their original byte-only contract belongs, rather than
loosening those assertions to accommodate decoded objects. Remaining limitations:
raw compact records are conservatively charged for the metadata lifetime; parser
workspace and total runtime memory remain separate open boundaries. Shared cache
eviction cost currently adds its existing metadata-size estimate to the expanded
owned cost; review that conservative duplication before claiming improved cache
capacity. Cloud fallback-specific and Worker validation still need refreshing.

### Full-metadata cloud ownership validation — 2026-11-13

Extended the actual HTTP GeoParquet fixture to call `readOwnedParquetMetadata`
directly, since indexed scans alone do not exercise the fallback. For native 206
and ignored-range 200 responses, four concurrent cold consumers receive complete
row groups and CRS annotations. Four warm consumers hit the parsed metadata cache
with no HTTP requests; eviction while their owners are live preserves metadata,
and releasing all owners returns each child budget and finally the shared pool
to zero. The existing server-side two-read concurrency assertion still passes.
Both transport tests pass with the additional fallback checks.

Added a workerd test using the emulated R2 bucket binding and encoded Parquet with
GeoParquet metadata. It independently verifies that owned bytes exceed the encoded
file length, four warm metadata owners issue no additional R2 reads, and metadata
remains valid after shared-cache eviction until all owners release. This is a
metadata test; its byte column is not offered as valid decoded WKB geometry.
The workerd suite now passes 36 tests (14 files) in 6.85 seconds. Repository
typechecking passed, including the new runtime test.

The cache-size duplication identified previously cannot simply be removed from
`sharedParquetMetadataCache.setOwned`: `OwnedMetadataCache` is publicly exported
and accepts arbitrary owners without certifying complete decoded-object coverage.
Removing its estimate globally would undercount legacy/external backing-only
owners. A future cache-cost optimization needs an explicit ownership-coverage
contract or an internal certified path, plus tests for both owner categories.
Exact-footer retry has cap/cap-minus-one local ObjectStore coverage; a dedicated
HTTP transfer-cost retry test remains to be added. These tests do not establish
production R2 latency or a total Cloudflare Worker memory cap.

### Exact-footer HTTP retry cost — 2026-11-13

`metadata-retry-http.test.ts` closes the dedicated HTTP retry coverage gap using
an encoded 100,000-row Parquet file and an actual localhost server. For both native
206 ranges and ignored-range 200 responses, the test derives the exact full-decoder
capacity, verifies successful metadata at that cap, and verifies budget rejection
at cap minus one with zero residual reservations. It asserts the two exact requested
suffix ranges and no further retry. Both tests pass.

Transfer evidence explicitly differs by transport: native responses send the
initial requested suffix plus the exact footer; ignored ranges send two whole
objects. The server records the response payload extent, not a sampled throughput
estimate. Tracked engine allocations remain within the cap in both cases; runtime
Fetch chunk buffers remain outside that model. Thus exact retry is a bounded-memory
fallback with a real cloud-bandwidth cost, not an unconditional I/O improvement.

Coverage refresh after HTTP retry validation: 203 test files passed, nine skipped;
2,008 tests passed, 363 skipped. Statements 91.16%, branches 84.92%, functions
95.17%, lines 93.20%. No timeout or coverage-threshold changes were needed. This
refresh does not resolve the previously recorded external conformance failures
or the remaining complete-allocation/whole-runtime acceptance gaps.

### Fixed-byte statistic conversion admission — 2026-11-13

Review of temporary metadata storage found that `convertMetadata` creates a
DataView for every non-BOOLEAN/non-BYTE_ARRAY statistic, including FIXED_LEN_BYTE_ARRAY
values that it returns by alias. The scoped conversion previously admitted only
INT32/INT64/FLOAT/DOUBLE cases, leaving that view and FLOAT16 numeric results
unreserved. Scoped conversion now admits the view for all applicable physical
types, releases it immediately after conversion, and retains numeric result
charges only for numeric conversions. Borrowed fixed-byte values remain aliases
and do not acquire an extra retained payload charge.

Two new boundary tests verify exact peak, scratch release, numeric-versus-alias
identity, and rejection at peak minus one. Two real external FLOAT16 fixtures
verify normalized metadata parity, explicit min/max values (including negative
zero), and null counts. Full suite: 2,012 passed, 363 skipped, 14.03 seconds.
The fixture file was rerun successfully after adding explicit bound assertions.
Core/Parquet TypeScript build passed. Coverage percentages above predate this
small conversion change and require final refresh.

Raw compact records cannot all be treated as temporary: converted column metadata
retains histogram lists, geospatial-type lists, opaque crypto metadata, legacy
bloom-filter payloads, scalar bigints, and borrowed binary views. Releasing their
aggregate reservation after conversion would undercount these retained aliases.
Their current conservative lifetime remains until an ownership partition covers
all projected field shapes. Unchecked legacy decimal conversion workspace is also
not certified by the new view charge; normal owned indexed/full metadata uses the
checked scalar decimal converter.

### Metadata ownership scaling census — 2026-11-13

Added reproducible `bench/metadata-ownership.mjs` and recorded
`docs/benchmarks/metadata-ownership-scaling-2026-11-13.json`. The benchmark generates
real two-column Parquet files with eight rows per group, admits exact footer
backing for both paths, checks total row counts and the final numeric statistic,
and verifies zero usage after disposal. The indexed path releases each decoded
group before advancing; the full path must return all groups. These are explicit
allocation-model measurements, not VM heap samples or cloud timing results.

| Groups | Footer bytes | Full retained bytes | Indexed peak bytes | Raw group charges retained by full path |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 175 | 25,667 | 26,587 | 11,022 |
| 16 | 1,998 | 293,002 | 28,538 | 176,352 |
| 256 | 32,551 | 4,573,039 | 61,015 | 2,821,632 |

Raw group charges use the same compact projection and streaming column visitor
shape as the admitted decoder. At 256 groups they represent about 62% of full
retained charges. They are not all reclaimable: raw numeric scalars, lists and
opaque field trees can be aliased by converted output. Nevertheless this evidence
prioritizes a partition of raw scratch versus retained aliases over reducing a
small constant schema-directory charge. Indexed traversal already bounds live
group charges; full metadata inherently retains converted groups, but need not
retain unrelated parser record/key overhead for every group. The next change must
establish that ownership partition for all projected shapes and test aliases after
scratch disposal, rather than releasing the raw reservation wholesale.

Benchmark executes successfully and passes lint. No production behavior changed
in this measurement step, so existing runtime validation is not superseded.

### Row-group raw scratch / retained alias partition — 2026-11-13

Row-group raw parsing now uses a temporary child scope disposed after conversion.
A separate retained reservation covers forwarded raw chunk fields and metadata
values. Decoded paths/key-value strings and enums rely on their existing output
admission; opaque crypto payloads, legacy bloom fields, histograms, geospatial data,
statistics and encoding-stat trees remain conservatively owned. Whole raw subtrees
are retained where conversion forwards selected children, avoiding an unsafe
assumption that their contents are all temporary. Footer payload backing remains
separately owned. Duplicate aliases are conservatively charged more than once.

The partition admits retained aliases while the original raw reservation is still
live. This explicit overlap increases one-group peak: the scaling fixture changes
from 26,587 to 30,499 indexed peak bytes. At 256 groups, however, full retained
charges fall from 4,573,039 to 2,752,879 bytes (about 40%); full peak falls from
4,575,183 to 2,765,533. Indexed peak changes from 61,015 to 64,927 bytes, while
its retained source charge stays 43,323. Artifact:
`metadata-ownership-partitioned-2026-11-13.json`. No wall-time speedup is asserted.

Added alias-accounting tests for nested opaque trees, binary-view charges without
payload duplication, forwarded column shapes, and row-group scalar retention.
Existing exact-capacity planning helpers previously equated retained usage with
peak; they now measure the conversion peak separately and continue asserting exact
retained usage and source-independent group ownership. The initial full run exposed
these stale assertions; targeted indexed planning now passes all twelve tests.
TypeScript build and changed-file lint pass. A final full run is pending below.
Further acceptance still needs stronger encoded opaque-field alias lifetime cases,
Worker/cloud stress refresh, and measured CPU overhead for recursive alias counting.

Final partition verification: full default suite passes 2,014 tests (363 skipped),
204 files passed and nine skipped, in 13.85 seconds. No production assertions were
relaxed; test capacity models now distinguish conversion peak from retained bytes.

### Encoded alias lifetime and runtime verification — 2026-11-13

Added an actual compact-protocol row-group fixture encoded by hyparquet-writer.
The test decodes under a budget, confirms parser scratch has ended (peak exceeds
retained usage), retains the resulting metadata owner, and disposes the producing
scope. Histogram lists, geospatial-type lists, opaque legacy bloom data, nested
crypto metadata, encrypted binary views, and sorting values remain valid and
charged until the independent owner releases. Usage then returns to zero. This
is encoded row-group metadata coverage, not a claim that arbitrary crypto blobs
are decryptable data-page fixtures. All three alias tests pass.

Refreshed workerd: 36 tests pass across 14 files in 6.52 seconds. Reference queries:
104 tests pass across four files in 4.53 seconds. Refreshed constrained workload:
`worker-heap-metadata-partition-64-2026-11-13.json`, twenty rounds, 65,536 rows,
owned output, localhost HTTP in workerd with a 64 MiB V8 managed-heap setting.

| Shared pool | Jobs per round | Successful jobs | Peak tracked bytes |
| --- | ---: | ---: | ---: |
| 64 MiB | 1 | 20/20 | 55,344,535 |
| 64 MiB | 4 | 20/80 | 67,054,326 |
| 128 MiB | 1 | 20/20 | 55,344,535 |
| 128 MiB | 4 | 40/80 | 134,132,359 |

All scenarios complete and preserve prior successful-job counts. The benchmark
checks successful output checksums, retained owners, warm reads, and final eviction
cleanup. Small peak differences include concurrent scheduling; this is not evidence
of a general latency improvement. Total runtime/production Worker memory and CPU
overhead of alias counting remain separate acceptance work.

### CPU cost of metadata lifetime partition — 2026-11-13

Added `bench/metadata-partition-cpu.mjs`, with results in
`docs/benchmarks/metadata-partition-cpu-2026-11-13.json`. It compares the frozen
pre-partition raw-retention policy against the current partitioned decoder using
the same current conversion kernels. Each measurement decodes four passes over
256 real encoded row groups; four warmup rounds precede twelve alternating-order
measurements. Both paths validate total rows and final numeric statistics and
release all allocations. Timing includes scope disposal but excludes file reads,
envelope parsing and directory construction.

Median retained-raw baseline: 47.397 ms. Median partitioned: 53.067 ms. Ratio:
1.1196, approximately 12% more CPU time in this local metadata-only workload.
This is a material measured tradeoff for the previously observed roughly 40%
reduction in full-metadata retained allocation charges, not an unconditional
speedup. It does not predict whole-query/cloud latency, where reads, decoding,
filtering and output work also contribute. No threshold or user flag was added
based on a single local microbenchmark. Any adaptive lifetime policy must consider
remaining group count, ownership duration and available headroom, and retain the
same alias safety and admission checks. The benchmark passes lint and completes
with checksum/zero-usage assertions intact.

### Transfer raw admission instead of duplicating it — 2026-11-13

The lifetime partition now uses the existing exclusive reservation-partition API.
Before parsing a row group, a child stage admits the entire preflighted raw tree.
Retained aliases consume partitions of that original admission and transfer to the
output scope; remaining capacity covers transient parser records until conversion
finishes. Converted output containers/strings continue to be independently admitted.
No capacity is released and reacquired during transfer, and no new reservation is
required for a value that already exists in the raw tree.

This removes the duplicate-admission peak introduced by the first partition:
`metadata-ownership-transfer-2026-11-13.json` records the same 2,752,879-byte full
retained footprint at 256 groups, with full peak 2,761,621 bytes. Indexed peak is
61,015 bytes, exactly the original pre-partition scaling result, versus 64,927 in
the intermediate version. Indexed source retention remains 43,323 bytes. The
one-group indexed peak likewise returns to 26,587 bytes.

CPU rerun in `metadata-partition-transfer-cpu-2026-11-13.json`: baseline median
49.300 ms, transfer-based partition median 53.825 ms, ratio 1.0918. This run still
shows about 9% metadata CPU overhead; differences from the earlier 12% run are not
sufficient to claim a reliable CPU improvement. The memory reduction and removal
of duplicate peak admission are deterministic allocation-model results.

Validation: all 2,015 default tests pass (363 skipped), including encoded opaque
alias lifetime, exact-cap/cap-minus-one decoding, source disposal, full fallback
retry and real HTTP transport cases. Core/Parquet TypeScript build and changed-file
lint pass. Remaining acceptance includes a final runtime/conformance refresh and
whole-query/whole-Worker allocation coverage; this optimization does not close
those broader boundaries.

### Post-transfer conformance and next decoder boundary — 2026-11-13

External conformance refresh after reservation transfer remains 232 passed,
18 failed, one skipped. Failure names match the preceding checkpoint exactly
(after normalizing checkout-absolute fixture paths); no newly failing or resolved
fixtures. Evidence: `metadata-transfer-conformance-2026-11-13.json`. The existing
18 failures remain unresolved rather than being relabeled expected success.

Inspection of fallback vector construction identified the next concrete allocation
boundary in `vector-batches.ts`: `arrayFlatVector` builds string and binary outputs
with `values.slice(start, end).map(...)`, producing two unadmitted JS arrays.
`nullableArrayFlatVector` creates string/binary arrays with `new Array(length)` and
fills binary null slots using new empty Uint8Array objects, also without output
admission. Numeric branches already use `allocateVectorBuffer`, and timestamp
branches use scoped `vectorFromSource`; therefore the supplied allocation scope is
available but not consistently used across value shapes. String/binary branches
need a single admitted construction pass, accounting for newly created views or
coerced values while preserving aliases to existing decoded payloads. Tests should
exercise required and optional fields, null-only windows, sliced windows, actual
encoded fallback pages, and owner lifetime under tight budgets. Do not assume the
packed/direct numeric page tests exercise these fallback array branches.

### Fallback string/binary vector array admission — 2026-11-13

Replaced required string/binary `slice().map()` output construction with a single
admitted array and fill pass. Nullable string/binary branches use the same array
admission. Newly constructed binary view headers (ArrayBuffer/DataView wrappers
and empty null-slot views) are admitted; existing Uint8Array payload views remain
aliases. Existing decoded strings remain aliases rather than being copied. Decoder
arrays that mix incompatible value kinds now raise a typed Parquet read error
instead of invoking arbitrary string coercion or silently producing empty binary
values. Numeric and timestamp paths retain their existing constructors.

`decoded-array.test.ts` covers exact array capacity and cap-minus-one rejection,
borrowed byte-view identity, admitted wrapper/empty views, failed-conversion
rollback, and rejection without invoking an object's coercion hook. Actual
DELTA_BYTE_ARRAY Parquet fixtures cover strings and binary with/without nulls,
Unicode and empty values, batch size two, owned output correctness and zero shared
pool usage after output release. All seven new tests pass. The full default suite
passed 2,015 tests (363 skipped) after production changes and before adding this
new test file. Core/Parquet TypeScript build and changed-file lint pass.

This closes output array/view construction for these branches, not upstream
fallback decoder arrays, reconstruction scratch, or all nested vector conversion.
Further validation needs explicit sliced/null-only windows and branch coverage,
transport/runtime refresh, and measured output-allocation savings/CPU behavior.

### Fallback array branch and sliced-window validation — 2026-11-13

Expanded encoded DELTA_BYTE_ARRAY tests with [1,3) null-only, [2,5) leading-null,
and [3,5) non-null row windows for strings and binary values. Each test retains a
yielded batch through iterator shutdown, checks values against the source window,
then verifies zero usage after owner release. All nine tests pass.

Targeted instrumentation exposed that the original no-null fixtures still had
OPTIONAL schema fields: they exercised only nullable construction. The fixtures
now explicitly select REQUIRED or OPTIONAL repetition (and supply type through
schema only, as required by the writer). Coverage confirms both required branches
and both nullable branches execute: `arrayFlatVector` twice and
`nullableArrayFlatVector` eight times; constructor statement counts are 1/1/4/4.
Evidence: `decoded-array-branch-coverage-2026-11-13.json`. The targeted diagnostic
command disabled global coverage percentages only for that single test file;
repository configuration was unchanged and normal full coverage gates run
separately. The initial explicit-schema fixture attempt supplied both schema and
columnData type and was rejected by the writer; that fixture setup was corrected.

Normal full coverage gates pass after the fixture correction: statements 91.21%,
branches 85.00%, functions 95.21%, lines 93.26%. No repository timeout or threshold
was changed. This refresh validates current default execution, not the unresolved
external conformance or total Worker allocation boundaries.

### Packed delta binary decoder foundation — 2026-11-13

Upstream `dataPageValues` still falls back to unscoped `readDataPage` for
DELTA_BYTE_ARRAY and DELTA_LENGTH_BYTE_ARRAY. Those routines allocate length arrays
and per-value reconstruction buffers before our vector output admission. Added an
internal `readPackedDeltaBinaryValues` foundation using the existing admitted
integer-delta decoder. Prefix/suffix lengths are temporary child-scope arrays;
all lengths, predecessor-prefix bounds, total expansion and input extents are
validated before output payload allocation. One admitted range table and one
packed reconstruction payload replace per-value arrays. Length-only encoding
borrows its contiguous input payload and admits its new view header. Reader offset
commits only after successful construction; failures release the entire attempt.

Tests encode actual delta byte streams using hyparquet-writer and verify empty
input, empty values, Unicode prefix sharing, repeated values, exact consumption,
budget failure at measured peak minus one, rollback, negative suffix lengths and
invalid first prefixes. All three tests pass; TypeScript build and changed-file
lint pass. This helper is not yet connected to page decoding: the next step is
integration with existing v1/v2 definition-level and decompression handling,
followed by actual Parquet page, malformed extent, cloud ownership and benchmark
validation. Output descriptor/view model boundaries remain subject to the broader
allocation audit; this foundation is not a completed fallback admission claim.

### Scoped packed delta page integration — 2026-11-13

Connected DELTA_BYTE_ARRAY and DELTA_LENGTH_BYTE_ARRAY BYTE_ARRAY columns to the
existing scoped flat page reader for text and unannotated binary leaves. Both
page versions reuse existing level validation and scoped decompression, then build
packed utf8-view/binary-view vectors (with dictionary-style presence indices for
nullable rows). Length/prefix scratch and expanded payload now pass admission
before construction rather than falling through to uninstrumented upstream delta
arrays. Nested/repeated and unsupported logical conversions retain their separate
fallback boundary.

Full default suite after production integration: 2,027 passed, 363 skipped.
Subsequent targeted tests add four v1/v2 × delta encoding cases, proving the packed
path is selected, null placement is correct, and peak-minus-one capacity rejects.
Actual external `delta_byte_array.parquet` and `delta_length_byte_array.parquet`
owned queries with batch size 17 match the unscoped upstream-decoder reader and
release the 64 MiB shared pool to zero. The page tests and all four delta decoder
tests pass; TypeScript build and changed-file lint pass. External fixture tests
skip explicitly when the checkout lacks those files.

The earlier fallback-array branch census predates this integration: these delta
fixtures now exercise the packed path for scoped compatible leaves. Fallback array
coverage must retain separate unsupported-shape/unscoped cases; do not reuse the
historical census as evidence that current scoped delta queries hit those branches.
Next acceptance work includes malformed/truncated expansion cases, all-null pages,
cloud/Worker transport ownership, and measured allocation/CPU comparison against
upstream reconstruction. No broad delta performance claim is made yet.

### Delta expansion and Worker ownership validation — 2026-11-13

Added exhaustive truncation of a real encoded stream for both delta byte-array
encodings: every shorter extent rejects, leaves the reader offset unchanged, and
returns temporary allocations to zero. Invalid first prefixes and negative lengths
remain covered. An oversized first prefix is rejected after only sixteen bytes of
length-array admission. A separate valid prefix-compression case encodes 1,024
repeated 1,024-byte values in under 2 KB; a 32 KiB budget rejects the 1 MiB expanded
payload before allocation, with a measured 16,384-byte peak for prefix/suffix
scratch plus ranges and zero residual usage. All eight decoder tests pass.

Actual all-null Parquet pages now have public owned-query tests for both encodings,
using batch size one. They return three null values and release the shared pool.
The combined targeted decoder/output suite passed before the final expansion case
was added; that updated decoder file subsequently passed all eight tests.

Added workerd/emulated-R2 tests for both encodings with 128 Unicode values/nulls,
two row groups, batch size three, and four concurrent owned queries sharing a
64 MiB pool. All outputs match source values while retained simultaneously, then
release pool usage to zero. The workerd suite passes 38 tests across 14 files in
6.98 seconds. These are runtime/ownership checks, not production R2 latency or
whole-isolate memory measurements. CPU/expanded-buffer benchmark comparison and
native-versus-ignored HTTP transfer validation remain open for this decoder path.

### Packed delta measurements and HTTP ownership — 2026-11-13

Added reproducible `bench/delta-binary.mjs` and artifact
`docs/benchmarks/delta-binary-2026-11-13.json`. For 8,192 shared-prefix Unicode
values (1,342,378 decoded bytes), four warmup rounds precede twelve alternating
measurements against the upstream decoder. Every output byte is checked outside
timing, reader consumption is exact, and packed scopes release to zero.

| Encoding | Encoded bytes | Upstream median | Packed median | Packed tracked peak | Packed retained bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Delta length | 1,342,717 | 0.250 ms | 0.164 ms | 98,432 | 65,664 |
| Prefix delta | 14,930 | 3.965 ms | 0.536 ms | 1,473,450 | 1,407,914 |

These are local payload-only timings. Packed timing includes admission, upstream
does not. Input backing is held by the harness, excluded from these allocation
figures. Length-only output borrows that backing; prefix output owns reconstructed
bytes. No upstream heap or total runtime memory comparison is inferred, and no
cloud latency speedup is claimed from this microbenchmark.

Added actual HTTP tests for both encodings × native 206/ignored-range 200 responses.
Four cold and four warm jobs per case retain owned outputs simultaneously and
compare all Unicode/null values. Outputs release before the next round; warm rounds
perform zero data reads, server-side concurrency stays at two, and final cache
eviction returns the shared pool to zero. All four cases pass with a 2 MiB shared
pool and 512 KiB per-query cap, providing pressure coverage below the user's usual
Worker budgets. Benchmark and test files pass lint. Real production networking,
whole-isolate accounting, unsupported logical/nested delta paths, and broader
release acceptance remain outside these results.

### Release reconstructed delta decompression scratch — 2026-11-13

Packed binary page decoding now uses the same alias-aware decompression release
helper as numeric conversion. Prefix-delta output has independently reconstructed
payload bytes, so tracked decompressed input can end immediately after successful
reconstruction, before nullable presence-index allocation. PLAIN and length-delta
payloads continue retaining decompressed backing because their byte vectors alias
it. The helper never releases caller input or untracked custom-compressor output.

Two real-GZIP page regressions verify both lifetime outcomes and decoded Unicode
values. The prefix case confirms decompression output is already absent from the
scope; the length-only case confirms it remains charged until explicitly released
at the end of the test. All six page tests pass. Core/Parquet TypeScript build and
changed-file lint pass. This reduces overlapping page lifetimes without changing
admission bounds or making a total-heap guarantee. A full-suite refresh follows.

Full-suite refresh after delta scratch release: 2,044 passed, 363 skipped;
208 test files passed and nine skipped, 13.67 seconds. External conformance and
whole-runtime acceptance retain their separately documented unresolved status.


### Release-gate and acceptance-summary refresh — 2026-11-13

Full build, package-install smoke test, repository lint (768 files), compatibility
check and tracked-file size check pass after packed delta integration and scratch
release. Initial lint found only formatting in three newly generated benchmark
JSON artifacts; those were formatted without changing measured values. Initial
package/size runs could not spawn their required child processes in the sandbox;
reruns with appropriate subprocess access passed. No test gate was disabled.

Updated the opening acceptance summary to distinguish latest default tests,
coverage, Worker/reference checkpoints and the still-unresolved conformance set.
The implementation record now includes full-metadata admission and alias transfer,
scoped fallback vector arrays, and packed delta encodings. Complete query allocation
coverage, unsupported nested/logical fallback decoding, adaptive scheduling across
operators, and total production Worker memory remain open. Passing packaging gates
does not establish release readiness while those requirements and conformance
failures remain unresolved.

### Conditional admitted fallback compaction — 2026-11-13

Fallback page decoding previously used unconditional `Array.filter` to remove
null/undefined placeholders, copying even fully dense decoded arrays and never
admitting the new array. The shared decoded-array helper now counts present values,
returns dense arrays and typed arrays unchanged, and allocates exactly the necessary
slot count only when compaction is required. Existing payload values remain aliases.
Both v1 and v2 fallback call sites supply their allocation scope. Conversion consumes
the page-local result; no other consumer requires the old dense array to remain an
independent copy. This does not account for upstream creation of that input array.

The new regression verifies zero-capacity reuse for dense/typed arrays, payload
identity, no source mutation during compaction, exact array charge, cap-minus-one
rejection without retained usage, and all-null empty-array admission. All twelve
decoded-array tests pass. Core/Parquet TypeScript build and changed-file lint pass.
Full-suite outcome follows. Logical conversion allocations after compaction remain
a separate open boundary; this change is not a claim that the complete fallback
page is allocation-safe.

Full default suite after fallback compaction: 2,045 passed, 363 skipped;
208 test files passed, nine skipped, 14.11 seconds.

### Scoped temporal conversion admission — 2026-11-13

Known DATE, TIMESTAMP_MILLIS/MICROS and logical TIMESTAMP conversion after scoped
physical decoding now admits one result array plus conservative per-value temporal
and bigint charges before invoking scalar parsers. It fills that array directly,
removing the upstream Array.from/map intermediate for legacy temporal annotations.
Byte-carrier parser views receive a conservative additional charge. The existing
physical input remains separately owned; malformed scalar conversion rolls back
the complete new output reservation.

Tests prove exact admission, rejection before parser invocation at cap minus one,
negative date and nanosecond timestamp correctness, and cleanup when a parser
throws. All three targeted tests pass. Full default suite before the last parser
failure regression: 2,047 passed, 363 skipped, 14.10 seconds. TypeScript build and
changed-file lint pass. These costs describe the built-in parsers, not arbitrary
custom parser heap behavior. INT96 decoding, fallback conversion call sites,
other logical types and temporal vector re-materialization scratch remain separate
coverage work; no complete temporal pipeline bound is claimed yet.

### Shared scoped fallback logical conversion — 2026-11-13

Both v1 and v2 fallback page conversion calls now forward their allocation scope.
Non-dictionary conversion uses the same `convertDecodedValues` dispatch as the
admitted physical decoder, eliminating duplicate decimal dispatch and extending
known temporal/decimal admission to fallback physical representations. Dictionary
dereference keeps its separate existing semantics and remains a distinct ownership
boundary; this does not certify arbitrary upstream decoder allocations.

Full suite after production change: 2,048 passed, 363 skipped, 14.10 seconds.
A subsequent regression reads the real external FLBA12 timestamp fixture through
an owned public query (batch size three), compares every value with the unscoped
reader and confirms zero pool usage after release. All four temporal tests pass.
Missing external fixture data is reported as an explicit skip. TypeScript build
and changed-file lint pass. INT96 rich conversion, other logical parsers and
upstream physical fallback arrays remain open rather than being hidden behind
the new scoped dispatch.

### Admitted INT96 temporal conversion — 2026-11-13

Scoped legacy INT96 rich conversion now joins the shared temporal converter rather
than upstream Array.from/map. It preserves the existing Julian-day/high-word and
nanoseconds-of-day/low-word interpretation. Output array and temporal/bigint values
are admitted before parsing; five scalar bigint charges provide temporary arithmetic
workspace, released after conversion while output ownership survives. This model
assumes the physical decoder supplied the format's bounded INT96 carrier, not
arbitrary caller-created unbounded bigints.

The boundary regression verifies the nanosecond immediately before Unix epoch,
epoch, and 123 ns after epoch, exact peak and retained charges, scratch release,
and cap-minus-one rejection with zero residual usage. Owned queries over real
FLBA12, `int96_timestamp_order.parquet`, and `int96_from_spark.parquet` fixtures
match the unscoped reader and release their pools. All five temporal tests pass;
TypeScript build and changed-file lint pass. This closes rich conversion admission,
not upstream INT96 physical decoding or all temporal vector materialization.

Full default suite after INT96 conversion admission: 2,050 passed, 363 skipped;
209 test files passed, nine skipped, 13.46 seconds.

### Admitted PLAIN INT96 physical carriers — 2026-11-13

Added scoped PLAIN INT96 decoding and enabled it in the flat admitted page reader.
Input extents validate before reservation; output admits one array plus bounded
96-bit bigint carriers, with three temporary bigint charges for scalar assembly.
The scratch stage releases after output handoff and reader offset advances only on
success. Scoped dictionary page calls that already use `readPlainValues` inherit
this admission as well. Unsupported encodings and nested reconstruction remain
separate paths.

The decoder combines an unsigned low 64-bit word with a signed high 32-bit word.
This avoids the upstream expression's low-word sign extension overwriting the high
word when bit 63 is set. Valid timestamp nanoseconds-of-day are below that bit,
so ordinary timestamp fixture results remain unchanged; the raw-carrier test also
checks general 96-bit assembly, including -1 and a positive nonzero high word.

Boundary tests cover misaligned input, exact retained/peak charges, cap-minus-one
rejection, and truncated extents rejected before admission. Plain-value and
real timestamp fixture files pass all fifteen tests. TypeScript build and
changed-file lint pass; full default-suite outcome follows. This admits physical
carrier storage, not all framework descriptors, bigint runtime internals or total
Worker heap.

Full default suite after INT96 physical admission: 2,051 passed, 363 skipped;
209 test files passed, nine skipped, 13.11 seconds.

### PLAIN binary carrier admission (2026-11-13)

Scoped `readPlainValues` now admits the retained binary carrier array and each
borrowed value-view header before constructing them. Variable-width offset tables
are validated and held in a disposable child scope; fixed-width input needs no
offset table. Payload backing remains borrowed and requires the caller's existing
page/footer owner. Reader offsets commit only after successful admission.
Missing fixed widths are explicitly rejected rather than interpreted as variable
length. Invalid widths and all truncated extents reject without allocating.

Evidence: core/parquet TypeScript build passed; full default suite 2,058 passed,
363 skipped. An additional exhaustive truncation test subsequently passed in the
focused 18-test PLAIN suite. Exact retained charges, peak-minus-one rejection,
unchanged failure offsets, borrowed backing identity, and scratch release are
asserted. Existing real-fixture regressions ran in the default suite.

Coverage boundary: this closes scoped binary dictionary carrier construction;
plain text/unannotated binary data pages already use packed vectors. Nontext
logical binary data-page fallbacks and their conversion allocations still need
separate admission coverage. These results do not establish a total Worker heap
cap or a cloud latency improvement.

### Flat logical binary PLAIN data-page admission (2026-11-13)

Extended the shared flat page decoder's physical PLAIN capability to BYTE_ARRAY
and FIXED_LEN_BYTE_ARRAY regardless of logical annotation. Text and unannotated
binary retain their packed-vector path; other logical values now construct their
physical carrier arrays through the admitted PLAIN reader before conversion.
This applies to both v1 and v2 pages, preserving existing level validation.
Binary carriers borrow their decoded page, so that allocation remains retained
instead of being mistaken for disposable scratch because the outer carrier is a
JavaScript array. Logical conversion remains a separate accounting boundary:
decimal and temporal conversions are admitted, while other conversion families
still require follow-up. Nested/repeated fallback pages are not covered by this
flat capability extension.

Evidence: core/parquet build passed. Focused page tests cover nullable v1/v2
logical binary carriers, peak-minus-one budget rejection, backing identity,
and retention of a tracked decompressor output through an independently held
owner. The latter is an injected compressor ownership test, not a cloud transport
claim. Full default suite: 2,062 passed, 363 skipped; actual workerd/R2 regression
suite: 38 passed. No new end-to-end latency claim is made.

### FLOAT16 logical conversion admission (2026-11-13)

Scoped FLOAT16 conversion now validates two-byte physical carriers, reserves the
logical output array plus modeled numeric values, and fills it directly. This
removes upstream Array.from followed by map and its simultaneous intermediate
array. Null/undefined carriers preserve upstream undefined output. Wrong-width
or nonbinary carriers fail before output admission. Conversion precedence is
unchanged for converted-type annotations; this path handles the logical FLOAT16
annotation with no converted type, after the existing decimal dispatch.

Evidence: all 65,536 half-float bit patterns match upstream conversion, with
explicit signed-zero/infinity/NaN assertions, exact output charge and one-byte
under-budget rejection. Both parquet-testing FLOAT16 fixtures ran through owned
batch-size-three queries and matched unscoped results; releasing results returned
the shared budget to zero. These fixture reads use memoryStore, not cloud I/O.
Core/parquet build passed. Full default suite: 2,066 passed, 363 skipped. This is
allocation/operation evidence, not a measured latency or total-heap claim.
Other logical conversion families and nested physical fallbacks remain open.

### JSON logical admission and heterogeneous-vector finding (2026-11-13)

Added a scoped JSON converter that admits output slots, decoded UTF-8 text, and
parsed-tree charges before construction. Each text reservation is disposed after
parsing; a construction scope rolls back all earlier trees if any value fails.
The existing stackless lexical JSON allocation estimator is reused. Parser VM
workspace remains outside this explicit model. Focused tests prove exact peak
admission, peak-minus-one rollback, Unicode/escaped strings/prototype-named keys,
null/scalar/container output, and rollback after malformed JSON.

The real parquet-testing json.parquet fixture exposed a correctness issue:
canRepresentDirectVectorLeaf claimed JSON scalar-vector support, but arbitrary
objects/arrays became null in arrayFlatVector. JSON now declines that scalar
capability and uses general column materialization. The fixture's mixed object
and array shapes reveal a deeper generic engine limitation: batch size three
raises the existing mixed-vector-types error. Batch size one preserves all four
fixture values and releases the owned budget to zero. Tests assert both behaviors
so silent null substitution cannot return.

This is NOT completion of JSON support or bounded JSON scans: general column
fallback still invokes upstream conversion, and the new admitted converter is
available only to scoped decoding callers. Generic heterogeneous/union vector
representation plus admitted fallback physical decoding remain required work.
Do not promote batch-size-one as the product solution or claim cloud coverage
from this memoryStore fixture. Core/parquet build and six focused tests passed.

Full regression run found only the old JSON direct-capability expectation
(2,071 passed, one failed, 363 skipped). Updated that expectation to decline
JSON while retaining direct support for the other selected scalar columns.

### Generic shape inference allocation order and union-vector boundary (2026-11-13)

`vectorShape` previously constructed unadmitted string arrays during inference
and left validity allocations in the caller scope when inference rejected mixed
shapes. It now constructs under a child scope, admits string array/slots before
allocation, and transfers successful inference allocations to the caller. Failed
inference disposes the child. Strings remain aliases of caller values; the charge
covers the new container, not duplicated payloads. Validity and strings retain
separate allocation identities.

An initial two-pass approach failed the existing single-evaluation string
expression test and was replaced. The final approach preserves that performance
contract. Focused tests cover exact admission, cap-minus-one rollback, mixed
string/number and negative/unsigned-bigint rejection without residual usage.
The existing map-child budget now includes 112 bytes for three string-key slots
and their array header (168 total instead of 56). This is newly accounted storage,
not an additional physical array.

The JSON fixture requires a genuine generic union-vector contract, not a JSON
special-case or a batch-size workaround. Required implementation surface:

- A bounded tag/index representation with compact children; preserve nulls,
  scalar types, nested objects/lists, and signed/unsigned integer distinctions.
- Infer child domains without constructing unbounded per-row routing objects;
  admit tags, offsets, children, and inference scratch before allocation.
- Extend length/value traversal, slicing/selection, concatenation, ownership
  discovery/copying, retained-byte accounting, and materialization together.
- Express each operator's union capability explicitly. Projection and transport
  must preserve values; numeric/order/group operators must use defined domain
  semantics or reject before execution rather than erase unsupported values.
- Validate heterogeneous JSON with normal batch sizes, nested unions, null-only
  children, budget boundaries, and local/HTTP/R2 fixtures independently.

Union representation remains unimplemented. Current inference still rejects
mixed domains, now rolling back inference allocations. JSON general fallback
allocation coverage also remains incomplete.

Final inference checkpoint: core/parquet build and changed-file formatting passed;
21 targeted inference/allocation/projection tests passed. Full default suite:
2,074 passed, 363 skipped. This also verifies the corrected JSON capability test
from the preceding checkpoint. No union-vector completion claim is made.

### Admitted dense union routing foundation (2026-11-13)

Added internal `buildUnionRouting` as the first implementation component of the
generic union contract. Stable child IDs use byte tags (1–256 domains), u32
child-local offsets, and grouped source-row indices. The grouped index allows
child construction to read only its rows instead of rescanning all source rows
for every child. Null can be a normal explicit domain. Classification happens
exactly once per row; no per-row object or per-child JavaScript row array is
constructed. The byte-domain count is a representation capability, not a query
or JSON-specific limit.

Admission includes all routing backing, four retained view headers, the result
record/properties, and a temporary u32 count table plus its view. Retained backing
is 9*N + 4*(K+1) bytes; modeled descriptors are added explicitly. Count scratch
is 4*K plus a view header and releases on return. A single stage admits the
peak before any callback or buffer allocation. Invalid tags and classifier
exceptions roll back all construction; successful ownership transfers to the
caller scope. Extents are validated against u32 capacity.

Evidence: core TypeScript build and formatting pass; nine focused tests pass.
Coverage includes empty input, all 256 tags, stable order, empty children,
exact-cap success/cap-minus-one preallocation rejection, invalid tags/extents,
throwing classifiers, 1/1,024/8,192-row scaling with exactly N classifier calls,
and a maximum-u32 request rejected under 64 MiB with zero allocation peak.

This helper is not yet wired into Vector inference or operators and is not
publicly exported. Mixed JSON batches still reject; no end-to-end union or cloud
claim is made. Next work must integrate child construction/ownership and generic
vector operations before advertising heterogeneous support.

### Compact union child construction (2026-11-13)

Added internal `buildUnionChildren`, composing admitted routing with existing
vector constructors through grouped indexed sources. Child construction visits
only that child's source rows, rather than rescanning the entire input per
domain. The child directory and construction result have explicit modeled
charges. Routing and all child allocations remain in a construction scope until
every child succeeds; failure disposes everything. Ownership transfers together
on success. The grouped source index currently remains retained with routing;
splitting construction-only routing from retained tags/offsets remains an
optimization to integrate with final vector ownership.

Evidence: core build passed; 26 routing/construction/inference/allocation tests
passed. Three construction tests cover interleaved struct/list/string/null and
empty domains, exact peak admission and peak-minus-one rollback, failure in a
later child after earlier work, independently retained ownership, and source
read complexity independent of unused domains (1,024 rows, 256 declared domains,
exactly N classifications and 2*N numeric source reads).

Seven JSON tests plus the three construction tests passed together. The real
parquet-testing JSON fixture is read through memoryStore, then its four parsed
values are routed into compact children. This is construction integration, not
an end-to-end union query or cloud test. Existing struct semantics normalize
missing fields to null: the first {a:1} becomes {a:1,b:null} when sharing a child
with {a:1,b:null}; array and null rows remain distinct and correct. Exact JSON
property-presence preservation would need a separate explicit representation
contract; do not claim byte-for-byte JSON roundtripping.

The internal result is intentionally not yet a Vector variant and is not publicly
exported. Existing recursive vector constructors still govern child allocation
coverage; this wrapper does not establish complete nested object/header accounting.
Operator support, recursive unions, direct JSON scan integration, and cloud
validation remain outstanding.

### Union vector storage/traversal integration (2026-11-13)

The shared Vector type now includes dense union tags, child-local offsets, and
children. Logical length/value/scalar traversal, zero-copy slicing, shared-index
gathers, retained backing measurement, physical byte estimation, serialized-size
traversal, and materialization admission handle this representation. Gathers of
multiple union columns share one admitted index buffer and retain input; slicing
preserves child backing. Materialization traverses the selected child and charges
its logical output, including nested values. Invalid child tags, missing offsets,
and child-index overflow fail instead of substituting defaults.

Ownership discovery in concatenation now visits union children conservatively.
Union concatenation itself explicitly rejects pending domain reconciliation;
group-key encoding declines union scalar capability and rejects unsupported
union keys. The CPU planner's advertised shape list still excludes union. No
automatic mixed-source inference is enabled yet: enabling it before recursive
construction, concatenation and complete operator capabilities would overstate
support. Union values can be constructed internally for traversal tests.

Evidence: all-package TypeScript build passed; focused storage tests cover mixed
string/list/null values, scalar access/rejection, slices, repeated/reordered
shared gathers, exact gather admission, retained backing, serialization byte
sizes, admitted materialization, and malformed rows. Full default suite:
2,089 passed, 363 skipped. Worker/R2 regression suite: 38 passed. Worker tests
are regression evidence, not end-to-end union cloud coverage. Automatic mixed
JSON queries remain unsupported pending the remaining generic integration.

### Planned union concatenation with retained children (2026-11-13)

Added `planUnionConcat` and integrated it into generic concat allocation planning
and execution whenever a direct or dictionary-selected input has union shape.
It preserves inputs as children and constructs only routing and child directories;
input payload is not materialized/copied. Direct, null, dictionary and union
inputs can coexist. Each node has at most 256 children for byte tags. Larger
input lists use a balanced bounded-fanout tree, with u32 row extents validated
before construction. Per-node admission covers 5 bytes/row routing plus views,
record/properties, child array/header/slots. The complete recursive plan is
admitted as one stage before construction and transferred atomically on success.

Owned concatenation conservatively pins source owners while shared union children
remain reachable. The allocation plan integrates with existing scheduled concat
admission; no special JSON branch is used. This does not yet flatten previously
nested input unions or reconcile identical child domains: repeated sequential
concat can add depth. Measuring/rebalancing that case remains required before
claiming optimal scaling for incremental accumulation.

Evidence: seven focused tests pass. Input counts 1/2/256/257/1,025 preserve row
values; exact planned bytes succeed, one-byte-less caps reject with zero peak,
single inputs alias without new allocation, and empty child inputs work. Mixed
union/null/dictionary batches match expected rows and retain the tracked producer
backing after producer handles close, releasing to zero with the output owner.
Oversized u32 row extents reject during planning. Source fixture reading/cloud
behavior is unchanged; these are operator/storage tests, not cloud measurements.
Automatic mixed-shape inference and complete operator capabilities remain open.

The first full concat run caught an existing performance-contract regression:
using selectVector merely to detect union shape inspected dictionary payload
lengths. Shape detection now unwraps dictionary types only, within the existing
column pass, without selectors or payload access. All 45 concat-plan/union-concat
tests pass, including the existing linear-lookup/no-payload-inspection guard.

Final union-concat checkpoint: all-package TypeScript build passed; full default
suite 2,096 passed, 363 skipped. Repeated incremental concatenation depth and
automatic recursive union inference remain unverified/open.

### Automatic recursive union inference and real HTTP JSON (2026-11-13)

Generic vector inference now selects union representation when nonnull physical
value domains differ. Homogeneous columns keep their established fast paths,
including single-evaluation string inference. Speculative inference storage is
released before mixed construction. Routing uses a fixed shared domain table;
children recursively invoke generic construction, enabling mixed values inside
lists and struct fields. Union construction and its output descriptor are owned
by a child scope and transfer only on success. Unscoped callers use an operation-
local accounting scope with the safe-integer ceiling, disposed after construction;
scoped callers remain constrained by the caller's actual pool. This does not
add an implicit memory cap to previously unscoped APIs.

Mixed negative/unsigned-64 bigint domains retain their existing explicit rejection;
no numeric coercion was introduced. Unsupported values (e.g. symbols) still fail
and roll back. CPU planner operator shape lists still do not advertise union;
group keys explicitly reject it. Full planner/operator capability work remains
open even though general scan materialization now preserves mixed values.

Evidence: recursive list/struct/scalar/null tests prove values, homogeneous shape
stability, exact-cap success, cap-minus-one rollback and zero released usage.
The actual parquet-testing JSON fixture succeeds with batch size three and owned
results. Existing struct missing-property-to-null normalization remains explicit.
Two HTTP tests use those real fixture bytes with native 206 and ignored-range
200 responses: four concurrent retained results, cold and warm rounds, a 2 MiB
shared pool and 512 KiB per-query admission, shared cache reuse, at most two
concurrent reads, no warm data reads, and zero usage after cache eviction. These
are HTTP fixture tests, not proof of total Worker process memory.

All-package build passed. The first full inference checkpoint passed 2,097 tests
(363 skipped); descriptor admission and HTTP cases were added afterward and
verified in focused runs. Final full-suite results follow separately.

Final recursive-inference checkpoint: 2,099 default tests passed, 363 skipped.
This includes both new HTTP fixture tests and admitted union descriptors.

### Recursive JSON unions in actual workerd/R2 execution (2026-11-13)

Added a generated real Parquet fixture with 48 records across four row groups:
JSON objects containing mixed nested arrays, heterogeneous list values, strings,
and physical nulls alongside a numeric id. The writer receives logical objects
and undefined physical nulls; it performs JSON serialization itself. The first
test attempt incorrectly pre-serialized JSON inputs and was corrected rather
than treating double-encoded strings as valid union evidence.

The actual workerd test uploads through R2ObjectStore, then runs four concurrent
owned scans twice with batch size three, a 64 MiB shared pool, 1 MiB per-query
limits, and a 256 KiB shared range cache. Every result matches all generated
records, including nested mixed values. A spy on the real store's getRangeOwned
method proves cold range reads occur and the warm round performs none. It then
filters numeric ids below 17 and projects only the mixed JSON payload; values
remain correct through that operator path. All result owners release, cache
entries evict, and pool usage returns to zero. Peak accounted usage stays under
the pool cap. This is Worker/R2 operational evidence, not total runtime heap
measurement or proof that upstream JSON parser workspace is admitted.

Final Worker suite: 39 passed (14 files). Existing CPU planner shape lists and
unsupported union grouping remain open; this test proves the exercised scan,
filter and projection paths, not every physical operator implementation.

### Flat JSON scans now use admitted logical conversion (2026-11-13)

Re-enabled flat JSON leaf capability after generic union representation became
available. Complete-page decoding routes JSON through `logicalPageVector`, which
expands compact physical presence and dictionary indices through a replayable
indexed source. It does not choose the output representation from the first
nonnull value and does not allocate another row-sized reconstruction array.
Generic inference handles mixed scalar/list/struct values recursively. Presence
length/domain, compact value count, and dictionary index bounds are validated.

This connects flat scoped JSON scans to existing admitted PLAIN physical carriers,
JSON text/tree conversion, and union construction instead of the general upstream
column materializer. JSON dictionaries use logical conversion before index
expansion. The source cursor rewinds across inference/child construction passes;
allocation accounting for parser VM workspace, other physical-encoding fallback
carriers, and repeated/nested Parquet fallback paths remains open.

Because complete pages now share struct children before slicing batches, absent
JSON object fields normalize to null even with batch size one. Tests explicitly
compare normalized objects; raw JSON property-presence roundtripping is still not
claimed. This aligns page construction with existing struct-vector semantics.

Evidence: core/parquet build passed. Sixty-two focused tests passed, including
nullable direct/dictionary mixed values, malformed counts/levels/indices, real
JSON fixture operation, and updated direct capability assertions. Full default
suite: 2,102 passed, 363 skipped. Actual workerd/R2 suite: 39 passed, including
recursive JSON concurrency, warm zero-range-read reuse, filtering and projection.
The two native/ignored-range HTTP fixture tests also passed in the default suite.

### Release union routing construction indices (2026-11-13)

Split routing allocation identities into retained tags/offsets and independently
owned source-row indices, child-start directory, and full routing descriptor.
`buildUnionChildren` places the latter in a construction scratch scope, creates
a compact result containing only tags/offsets/children, and disposes scratch
before transferring final ownership. No released source index escapes in the
returned construction result. Standalone routing callers retain all components
unless they explicitly supply a scratch scope, preserving that helper's contract.

Deterministic retained routing backing drops from 9*N + 4*(K+1) to 5*N bytes,
plus the applicable retained view headers. Construction peak is unchanged; child
payloads have separate accounting. Tests retain valid tags/offsets after scratch
release and assert exact released charges. Existing recursive construction,
cap-minus-one rollback and retained-owner tests pass.

`bench/union-routing.mjs` and `docs/benchmarks/union-routing-2026-11-13.json`
record current-code retain-versus-release modes at 1,024/8,192/65,536 rows. At
65,536 rows and ten domains, retained modeled bytes fall 590,700 -> 327,936
(about 44.5%), with the same 590,868-byte construction peak. Every tag and offset
is checked outside timing; disposal returns accounting to zero. Timing samples
are noninterleaved and do not establish a historical CPU speedup. This is a local
routing measurement, excluding child payload, cloud transport, and total VM heap.

All-package build passed. Full default suite: 2,103 passed, 363 skipped. Actual
Worker/R2 suite: 39 passed, including recursive JSON concurrency/cache coverage.

### Union concat scaling diagnosis and dense tree layout (2026-11-13)

Added `bench/union-concat-scaling.mjs`, checking every output value and releasing
all owners to zero for one-shot versus incremental owned concat at 32/128/512
appends. Baseline artifact: `union-concat-scaling-2026-11-13.json`. This confirms
an unresolved asymptotic problem: at 512 appends, incremental concat retains
1,011,202 modeled bytes and has union depth 513 for only 514 rows. Earlier roots
remain pinned through coarse source ownership, and every append adds routing.
This must not be presented as acceptable bounded performance merely because the
pool eventually rejects growth. Generic ownership-aware compaction/rebalancing
is required. The existing query top-k accumulation path also calls repeated
owned concat, so exercising heterogeneous payload accumulation there is a
priority once planner capabilities permit the shape.

The same experiment found an independent one-shot planning inefficiency: grouping
513 inputs with stride ceil(N/256) created 171 mostly empty intermediate nodes.
The planner now fills lower levels using power-of-256 strides. At 513 inputs,
the root has three children: two complete 256-input nodes and one aliased leaf.
Byte-sized tags remain valid, and arbitrary input counts still fit a bounded
fanout hierarchy. Exact allocation planning follows the same layout.

Improved artifact: `union-concat-packed-tree-2026-11-13.json`. At 512 appends,
one-shot retained modeled bytes fall 131,542 -> 20,673 (~84.3%), depth remains
three, and all values/ownership checks pass. Incremental retention/depth remain
unchanged and unresolved. These are single-run local diagnostic timings; no CPU
speedup or cloud latency claim is made. Source construction is included, and
figures are explicit allocation accounting rather than measured VM heap.

Forty-six focused concat tests pass, including exact admission, payload-free
shape inspection, source ownership, >256 inputs, and the new dense-layout guard.

Dense-tree checkpoint: all-package TypeScript build passed; full default suite
2,104 passed, 363 skipped. The goal remains incomplete, with measured incremental
union concat growth explicitly prioritized rather than hidden by this one-shot fix.

### Internal selective backing retention foundation (2026-11-13)

Incremental union compaction requires releasing obsolete routing without releasing
aliased child payload. Added internal provenance for allocation/composite ownership
handles and `retainSelectedAllocations` in memory-scope.ts. It traverses trusted
composite backing and pins only atomic allocations selected by a caller predicate.
Selection is atomic with respect to the destination: a child scope rolls back
partial pins on predicate or admission failure. An independent source guard
remains live through target-budget reclamation, including reclamation that closes
the original producer. Unknown external ownership handles are retained whole;
no provenance is inferred from their public value. Released handles discard their
provenance entries so a caller retaining a released handle cannot retain stale
provenance arrays through that map.

This is a low-level building block, not automatic reachability discovery. Callers
must retain every reachable allocation and all borrowed aliases; mixed allocation
units cannot be split by this helper. Union concat has NOT yet been connected to
it, and the measured incremental growth remains unresolved. Metadata/ownership
bookkeeping allocations still use the existing ledger model, not a total VM heap
claim. Deep provenance traversal is currently recursive and needs bounded-stack
work before using it to compact arbitrarily deep accumulated concat histories.

Evidence: all-package build passed before the final provenance-release cleanup;
core rebuild and ownership tests cover that cleanup. Full default run: 2,108
passed, 363 skipped. An additional reclamation test then passed in the focused
suite. Tests cover detaching obsolete routing, source independence, cross-budget
admission rollback, predicate errors, disposed targets, released inputs, unknown
third-party owners, and producer closure during destination reclamation.

### Bounded selective traversal and nonrecursive composite release (2026-11-13)

Replaced recursive selective backing traversal with explicit linked frames.
Each frame is admitted using the shared object/property charge model before
construction, and released when traversal pops it. Output pins and traversal
workspace share the destination budget. Admission or predicate failure disposes
all workspace and partial output while preserving the guarded source. The
reservation-unit constants now live in allocation-charges.ts and remain
re-exported through materialization-plan.ts, avoiding a memory-scope/materializer
runtime dependency cycle.

Composite backing release now uses an intrusive queue in existing backing
records, eliminating recursive release across deep ownership histories without
allocating a pending-owner array. If a third-party release throws, remaining
queued owners still release; the first error propagates after draining. This
change covers composite release and selected traversal specifically; other APIs
such as retainTreeAllocations and cross-budget composite retainIn have separate
traversal contracts and are not claimed to be fully iterative.

Evidence: a 12,000-level owned history is constructed, selected and released;
only the selected 100-byte payload remains before final release, then usage is
zero. The traversal peak includes admitted frames. A 511-byte destination cannot
hold the two required 256-byte frames and fails before the leaf predicate, with
zero residual destination usage. Existing cross-budget, reclamation, disposed
scope and unknown-owner tests pass. A throwing external release test verifies
remaining backing drains and subsequent releases still function. All-package
build passed; 34 focused ownership tests passed. Full-suite checkpoint follows.

This removes the selected-traversal stack prerequisite for incremental concat
compaction. Reachability discovery and concat integration remain outstanding;
the measured quadratic retained-routing issue has not yet been resolved.

Final bounded-ownership checkpoint: 2,112 default tests passed, 363 skipped.

### Budgeted vector reachability and safe concat-routing selection (2026-11-13)

Added an iterative vector-descriptor reachability walk. A modeled Set entry is
admitted before each insertion, and linked queue nodes are admitted before
construction and released when popped. Shared references and cycles are visited
once; traversal does not materialize payloads or allocate Object.values arrays.
The returned Set and entry charges remain with the caller scope; failure rolls
back the complete construction scope. Descriptor edges include dictionaries,
unions, lists, maps and own struct fields.

Concat-created nodes now have internal weak provenance identifying their atomic
allocation as routing/directories only, excluding borrowed child payload. This
allows selected backing retention to discard such an allocation only when it is
no longer reachable, while keeping every other allocation conservatively.
Tests demonstrate dropping an obsolete routing node while retaining its payload.

Descriptor identity alone is NOT sufficient: sliceVector creates new descriptors
that borrow old routing buffers. Added conservative alias recognition covering
union child-directory identity, routing/validity/indices/offset buffers, packed
byte backing, timestamp96 storage, numeric views, and legacy binary elements.
Sliced unions and a Float64Array reinterpretation of routing backing are both
recognized as live aliases even when the original descriptor is unreachable.
This alias check currently scans reachable descriptors per routing allocation;
indexing aliases is needed before claiming linear compaction complexity.

Evidence: all-package build passed before the final alias helper; core rebuild
and five focused reachability tests cover that helper. Tests include a cyclic
shared graph, admission rollback, actual selected-owner routing release,
collection/dictionary references, slicing/backing aliases, and 12,000 nested
descriptors without recursion. Full suite before the final added alias test:
2,116 passed, 363 skipped. Production concat compaction is not yet connected;
measured incremental retention growth remains open.

### Indexed routing alias checks (2026-11-13)

Replaced per-allocation scans over all reachable vectors with `indexVectorStorage`.
It visits the already discovered descriptor set once, admitting Set headers and
each distinct backing-buffer/union-directory entry before insertion. Numeric,
validity, dictionary-index, collection-offset, packed-byte, timestamp96, binary
array and union buffers are covered. Repeated views of one backing add one entry.
The result descriptor is admitted, construction failures roll back, and the
caller must retain the input descriptor set alongside the index.

Concat routing retention now uses four identity lookups: descriptor, child
directory, tag backing and offset backing. It does not inspect payload values
or iterate descriptors for each allocation. This removes the previous
reachability-times-routing-count scan; indexing still necessarily visits legacy
binary array entries once to discover their backing.

Evidence: core build and 23 targeted reachability/selection/concat tests passed.
New tests assert deduplicated backing charges, exact-cap success, one-byte-under
rollback, and one getter read during indexing followed by 1,024 routing checks
with no additional getter reads. Existing slice/reinterpretation alias checks
and 12,000-deep traversal tests continue to pass. Production concat compaction
and its measured incremental scaling regression remain unfinished.

### Owned union concat compaction integrated with dynamic depth trigger (2026-11-13)

Added admitted iterative flattening of trusted concat-created routing. Arbitrary
unions, dictionary selections and slices remain opaque because their row order
cannot be replaced by concatenating children. Frame/leaf workspace is charged
before construction; final leaf-directory storage is exact, and all traversal
scratch releases. Tests cover a 1,200-level history, exact-cap and cap-minus-one,
row order, slices and reordered dictionary selections.

`concatOwnedBatches` now compacts trusted routing histories whose depth reaches
2 + ceil(log2(max(2,rowCount))). This row-derived slack avoids rebuilding every
append while keeping directly accumulated concat history logarithmic in row
count. Compaction rebuilds balanced routing, indexes reachable backing, and uses
selected ownership to drop only proven-obsolete concat routing. Unknown/other
allocations stay pinned conservatively. Source guards remain live throughout;
failure releases new output/workspace without invalidating caller inputs.
Compaction stages admit against the actual destination budget synchronously.

The first implementation compacted every nested concat and was rejected on
measured CPU cost: the 512-append diagnostic took ~4.51 s. Artifact
`union-concat-compacted-2026-11-13.json` records that experiment honestly.
The dynamic trigger artifact is `union-concat-dynamic-compaction-2026-11-13.json`:
512 appends retain 52,712 bytes, peak 224,658 bytes, depth seven, versus the
uncompacted 1,011,202 bytes and depth 513. Timing is ~413 ms versus the historical
~29 ms diagnostic. Runs are single samples, not interleaved CPU benchmarks; the
memory improvement is deterministic, while CPU cost remains a material tradeoff
and optimization target. One-shot concat remains uncompacted (20,673 bytes).

Coverage boundary: scheduled concat and unowned concat do not yet invoke this
owned-history compactor. Dictionary-wrapped accumulated histories remain opaque
and can still pin earlier routing, notably selected/top-k outputs; their generic
selection compaction needs separate work. Non-routing allocations remain
conservatively retained, so this is not complete ownership pruning. Do not claim
all incremental accumulation now has optimal memory/CPU complexity.

Evidence: all-package build passed; full default suite 2,123 passed, 363 skipped.
Tests verify automatic compaction across 128 appends, complete output values,
accounted retention below 64 KiB, final zero release, exact compaction admission,
one-byte-under rollback, and readable source ownership after failure. Worker
regression checkpoint follows separately.

Owned-history compaction Worker checkpoint: 39 workerd/R2 regression tests passed.

### Avoid per-leaf ownership traversal frames (2026-11-13)

Selective ownership traversal now processes atomic and opaque child handles
under the existing parent frame. Only composite children with descendants need
a new admitted traversal frame. Source guards still protect lifetime during
predicate evaluation and target admission; unknown third-party ownership remains
conservatively retained. Empty composite children require no descent.

This removes one frame allocation/release per visited leaf without changing
selection semantics, output retention, or the dynamic compaction trigger.
A 1,024-leaf backing set now traverses with a single 256-byte frame under an
exact 256-byte destination workspace budget. Existing failure/reclamation/deep
history tests and compaction exact-cap tests pass. The earlier two-frame leaf
workspace boundary test was updated to the actual one-frame boundary (255-byte
failure before the predicate), not loosened to hide additional allocations.

Diagnostic artifact `union-concat-leaf-traversal-2026-11-13.json` preserves the
512-append memory result: 52,712 retained bytes, 224,658 peak bytes, depth seven.
The single elapsed-time sample is ~374 ms versus the previous ~413 ms, which is
not sufficient for a reliable speedup claim; deterministic per-leaf frame work
was removed. CPU overhead versus the uncompacted ~29 ms diagnostic remains
substantial. Selected/dictionary-wrapped histories and scheduled integration
remain open.

Core build and 11 focused selection/compaction tests passed. All-package build
and the full regression checkpoint follow below.

Leaf-traversal checkpoint: all-package TypeScript build passed; full default
suite 2,124 passed, 363 skipped.

### Iterative row resolution for selected union histories (2026-11-13)

Added internal `visitRoutedRows` to resolve each logical row through dictionary
and union routing to a terminal physical vector/index. Repeated selections and
row order are preserved; null vectors and null validity entries produce an
explicit undefined/-1 visitor result. No scalar, string, binary or nested payload
is materialized. The visitor owns its own effects and allocations.

Descriptor discovery uses the existing admitted iterative reachability walk.
The finite descriptor count bounds per-row routing steps, rejecting cycles rather
than recursing forever. Every routed index is checked against physical length,
and missing child tags/offsets fail. Workspace releases on success, malformed
routing, admission failure, or visitor exceptions. The root-directory allocation
is explicitly admitted as well.

Evidence: four focused tests cover interleaved dictionary/union routing,
reordered duplicates, physical nulls, exact workspace boundary rejection before
callbacks, invalid indices, cycles, visitor failure, and 12,000 dictionary levels.
A packed UTF-8 leaf with a throwing payload getter resolves successfully, proving
routing does not inspect/decode its data storage. This primitive is not yet wired
into selected-history compaction: constructing bounded multi-domain output and
releasing its obsolete ownership remains the next integration task.

### Selected routing construction and CPU profile checkpoint (2026-11-13)

`compactSelectedUnion` now reconstructs logical row routing without copying leaf
payloads. Blocks contain at most 256 rows and therefore at most 256 distinct leaf
domains; the existing concat planner combines blocks. This supports more than
256 total physical child vectors while preserving reordered and duplicate rows.
Construction, scratch maps, routing arrays, and directories are admitted before
allocation, and failure releases the construction scope. Routing ownership
provenance is separate from contiguous concat-order provenance: selected blocks
must not be flattened into physical child order.

Four tests cover 700 selected rows across 600 physical leaf vectors, reordered
duplicates, packed UTF-8 aliasing, nulls, exact peak admission and one-byte-short
rollback, late invalid indices, empty output, and provenance separation. The
latest full default suite passed 2,132 tests, with 363 skipped (224 passing files,
9 skipped). This builder is not yet integrated into automatic selected-history
compaction; conservative payload and dictionary ownership remains an open bound.

The incremental-concat CPU regression remains unresolved. A Node CPU profile of
all local scaling cases reproduced the 512-append result: 52,712 retained bytes,
224,658 peak charged bytes, depth 7, and approximately 343 ms in this profiled
run. Aggregated samples and benchmark results are recorded in
`docs/benchmarks/union-concat-cpu-profile-2026-11-13.json`. Garbage collection and
`trackOwner`/`ownedMemory` are prominent sampled frames, making ownership-handle
churn a concrete next optimization target. Samples include startup and all cases;
they do not isolate compaction, establish statistical timing significance, or
prove cloud-read performance. The explicit allocation model does not include VM
heap/GC overhead. Preserve admission, rollback, source guards, cross-budget
retention, and alias correctness while reducing this churn.

Ownership optimization experiment: replacing atomic handles with a class sharing
prototype methods failed three existing scope tests. Consumers wrap handles via
object spread and override individual methods; inherited methods disappear from
those wrappers. The experiment was reverted rather than weakening that contract.
Future handle-churn changes must preserve enumerable, independently callable
ownership methods and should reduce the number of pins/traversals instead of
assuming prototype substitution is API-compatible. No speedup is claimed for
this rejected experiment.

Selected-history integration: compaction depth detection now follows dictionary
and union routing, including dictionaries hiding old concat chains. Explicit
owned compaction resolves opaque nested routing through `compactSelectedUnion`,
while trusted concat roots with terminal children retain the faster flatten path.
This also reaches the existing automatic owned-concat compaction entry point.
A new integration test selects [last, first, last, second] from a 20-append history,
checks exact values and removal of excessive routing depth, verifies exact-budget
success and one-byte-short rollback, and confirms the source survives rejection.
Payload and dictionary-index allocations are still retained conservatively;
this is not a proof of memory bounded solely by selected output cardinality.
The local concat benchmark was rerun in
`docs/benchmarks/union-concat-selected-history-2026-11-13.json`; timing remains
diagnostic and the ownership CPU optimization remains open.

Validation for selected-history integration: all-package TypeScript build passed;
full default suite with local HTTP listeners permitted passed 2,133 tests, with
363 skipped. The sandboxed attempt failed because loopback listeners were denied
(EPERM), so it is not the authoritative transport-test result.

### Repeated selection scaling evidence (2026-11-13)

Added a public-operator integration test alternating `concatOwnedBatches` and
`gatherOwnedBatch` for 128 appends. Each step keeps [newest, previous], checks
both values after releasing the prior source, checks concat routing depth, and
finally verifies all charged ownership releases. All four compaction tests pass.

`bench/union-selection-scaling.mjs` records the corresponding local scaling
experiment in `docs/benchmarks/union-selection-scaling-2026-11-13.json`. Although
output stays at two rows and final routing depth stays at two, retained charges
grow from 10,689 bytes (32 appends), to 36,801 (128), to 141,249 (512). Peak charges
are 15,478 / 41,590 / 146,038 bytes. The 512-append diagnostic timing is about
1.11 seconds. This is affirmative evidence that routing compaction alone does
not bound selected-history retention by output cardinality. Conservative
non-routing ownership must be addressed before claiming that guarantee. These
are explicit-accounting local results, not measured Worker heap or cloud I/O.

### Pruning unreachable typed backing (2026-11-13)

Owned compaction now omits known atomic ArrayBuffer-view allocations when their
backing buffer is absent from the output storage index. Buffer identity, rather
than view identity or byte range, preserves the whole charge for reachable
subarrays and differently typed reinterpretations. Opaque external owners remain
conservatively retained by the ownership selector. Non-view atomic allocations
retain the prior conservative policy.

A new test drops 4 KiB of obsolete backing while preserving a Float64 view of a
Uint8 allocation and an individual binary-cell subarray. It verifies exact
remaining charges, values after source release, and final zero ownership.
The focused compaction/reachability/selection suite passes 21 tests; all-package
TypeScript build passes. The rerun benchmark artifact is
`docs/benchmarks/union-selection-pruned-buffers-2026-11-13.json`: two-row retained
charges are 9,935 / 34,511 / 132,815 bytes at 32 / 128 / 512 appends. This removes
obsolete buffer retention but leaves roughly 256 bytes of growth per append;
metadata ownership is still unresolved. Diagnostic 512-append timing was about
503 ms, not a statistically established CPU speedup.

Full default suite after typed-backing pruning: 2,135 passed, 363 skipped,
with local HTTP listeners enabled (224 passing files, 9 skipped).

### Bounded compaction metadata history (2026-11-13)

Compaction-created batch records, column directories, and null-domain descriptors
now carry private weak provenance distinguishing metadata-only allocations from
payload-owning allocations. Selection drops obsolete registered batch records,
keeps registered vector descriptors only when reachable, and keeps registered
column directories when reused as a reachable struct's fields. Unknown records
remain conservative. This prevents each compaction from retaining all prior
compaction metadata; it does not infer ownership from an object's apparent shape.

The repeated public concat/gather test now runs 512 appends under a 16 KiB pool,
checks retained charges below 4 KiB after every selection, checks both output
values after source release, and verifies final zero ownership. Additional tests
cover column-directory reuse as struct fields, reachable null domains, and
conservative unknown metadata.

`docs/benchmarks/union-selection-pruned-metadata-2026-11-13.json` reports identical
2,255-byte retained and 7,588-byte peak charges at 32, 128, and 512 appends (two
output rows, routing depth two). Before metadata pruning the corresponding
retention grew to 132,815 bytes. Diagnostic 512-append timing is about 121 ms;
this is a single local run, not a statistical timing or cloud/Worker claim.
This closes the demonstrated metadata-history growth for this workload, not
all possible payload retention, scheduled operators, or total VM memory bounds.

Metadata-pruning validation: all-package TypeScript build passed; full default
suite passed 2,137 tests, with 363 skipped (225 passing files, 9 skipped), with
local HTTP listeners permitted for the transport fixture tests.

### String-slot history and Worker checkpoint (2026-11-13)

The selection benchmark now includes 128-character string payloads. Before slot
pruning, retained charges grew from 4,791 bytes (32 appends) to 43,191 (512),
recorded in `docs/benchmarks/union-selection-payload-scaling-2026-11-13.json`.
The retained string-slot directories were known atomic allocations, but their
ownership had no provenance permitting removal. String vector construction now
registers each slot array once as directory-only metadata. Compaction retains
such arrays if any reachable UTF-8/binary vector uses that exact directory;
unknown arrays remain conservative. Referenced string payload is not included
in these slot-array charges and this does not establish a total JS-heap bound.

The public concat/gather regression now runs both numeric and string cases for
512 appends under 16 KiB, verifying every output and <4 KiB retained at every
step. `docs/benchmarks/union-selection-pruned-slots-2026-11-13.json` reports flat
retained/peak charges at 32, 128, and 512 appends: numeric 2,175/7,580 bytes;
strings 2,391/7,676 bytes. Values are checked and final ownership is zero in all
benchmark cases. Single-run local timings remain diagnostic only.

Slot-pruning validation: all-package TypeScript build passed; full default suite
2,138 passed, 363 skipped; Worker runtime suite 39 passed across 14 files.
Both suites ran with required local listeners permitted. The Worker suite is
regression evidence, not a measurement of this benchmark's total Worker heap.

### Binary selection and append-only cross-check (2026-11-13)

The public concat/gather regression now includes binary payloads supplied as
128-byte subarrays of 256-byte buffers, with distinct row IDs encoded in the
bytes. All seven compaction tests pass. Across 512 operations the binary case
checks both complete byte sequences after prior owners release, retained charges
below 4 KiB under a 16 KiB budget, and final zero ownership. No additional runtime
change was needed for this shape: scoped binary construction packs payloads and
the existing reachable-buffer pruning handles them.

`docs/benchmarks/union-selection-binary-scaling-2026-11-13.json` records constant
2,559-byte retained and 8,340-byte peak charges at 32, 128, and 512 appends for
binary selection, alongside the numeric and string cases. This is local explicit
accounting, not a cloud or total Worker-heap measurement.

The separate append-only cross-check is
`docs/benchmarks/union-concat-pruned-metadata-2026-11-13.json`: 512 incremental
appends retain 43,405 bytes with a 201,826-byte peak and routing depth 10, but take
about 377 ms in this diagnostic run. Thus the selected-history fix has not
resolved the previously identified append-only CPU regression. Keep that item
open; do not extrapolate selected-workload timing improvements to all concats.

### Removing per-leaf flatten scratch ownership (2026-11-13)

Trusted concat flattening now visits immutable inputs twice: count terminal
children, allocate the exact output directory, then fill it in row order. It
retains admitted iterative traversal frames but no longer allocates an owned
linked scratch record for each leaf. Opaque selections remain opaque, and source
payloads are not read or copied. The 1,200-level traversal and exact/one-byte-short
rollback tests pass. A new 256-leaf case proves flattening fits in output slots
plus two 256-byte traversal frames (4,672 bytes total peak), retaining only the
4,160-byte output directory afterward.

All-package TypeScript build and nine existing focused flatten/compaction tests
passed before adding the wide-case regression. The benchmark artifact is
`docs/benchmarks/union-concat-two-pass-flatten-2026-11-13.json`: 512 incremental
appends retain 43,405 bytes with a 201,826-byte overall peak and depth 10, unchanged
from the preceding run. Diagnostic elapsed time was about 321 ms versus 377 ms
in that preceding single run. This removes a known source of handle churn but
is not a statistically established speedup or resolution of the original CPU
regression. Overall compaction peak is governed by other stages in this workload.

Full suite including the wide-case regression: 2,140 passed, 363 skipped,
with required local HTTP fixture listeners permitted.

### Terminal reachability without queue ownership (2026-11-13)

Reachability now records every descriptor in the admitted identity set but only
queues descriptors with vector children (dictionary, union, list, map, struct).
Terminal payloads require no queue visit; their buffers remain indexed by the
separate alias-aware storage pass. Shared/cyclic graph behavior and iterative
deep traversal are unchanged. A 1,024-terminal-root test proves discovery fits
exactly in the set header and entries (65,600 bytes), without per-leaf queue
records, and that one-byte-short admission rolls back to zero.

All-package TypeScript build and 19 focused reachability/compaction/routed-row
tests pass. `docs/benchmarks/union-concat-terminal-reachability-2026-11-13.json`
records 512 incremental appends retaining 43,405 bytes, unchanged, while peak
charges fall from 201,826 to 139,042 bytes. Diagnostic runtime is about 279 ms
versus the previous 321 ms single run. These timings remain non-statistical;
the original append-only CPU regression is still open. This optimization removes
known queue-handle churn and reduces admitted scratch without loosening bounds.

Full default suite after terminal-queue removal: 2,141 passed, 363 skipped,
with local HTTP fixture listeners permitted.

### Repeated ownership benchmark baseline (2026-11-13)

Added `bench/union-ownership-repeated.mjs`: it launches fresh Node processes for
both concat and selection scaling scripts, discards one complete run per script,
then records seven measured runs. Every child checks output values and final zero
ownership. The runner additionally rejects inconsistent retained/peak charges or
routing depth across repeated cases and always removes its temporary directory.
The artifact includes raw runs, Node version, and descriptive min/median/max.
The first sandboxed invocation could not spawn child processes (EPERM); the
completed invocation permitted local child processes.

`docs/benchmarks/union-ownership-repeated-2026-11-13.json` gives the 512-append
incremental concat median 278.34 ms (274.17–288.46 ms), retained 43,405 bytes and
peak 139,042 bytes. Corresponding 512-selection medians are numeric 107.12 ms,
strings 64.06 ms, binary 85.88 ms; memory/depth metrics are identical across all
seven runs. Timing order within each script remains fixed; these are repeatable
current-state baselines, not controlled before/after speedup estimates.

The refreshed profile in
`docs/benchmarks/union-concat-current-profile-2026-11-13.json` still places GC,
atomic owner construction, and scope-handle tracking among prominent sampled
frames. These artifacts support targeting ownership representation/transfer
costs next while preserving spread-compatible handles, admission and rollback.
No runtime change occurred in this checkpoint; the previous full-suite result
remains the latest runtime regression evidence.

### Compatible atomic ownership handles (2026-11-13)

Atomic ownership now uses private instance state and shared method/getter
implementations. Public retain, retainIn, and release remain enumerable bound
functions; value remains an enumerable configurable own getter. Object-spread
wrappers and detached callbacks therefore preserve their existing behavior. This
avoids the compatibility failure of the earlier prototype-only experiment.
Added a regression covering exact public keys, detached methods, idempotent
release, released access rejection, and independent pins.

All-package TypeScript build passed; full default suite 2,142 passed, 363 skipped;
Worker runtime suite 39 passed. Final repeated-run evidence is in
`docs/benchmarks/union-ownership-shared-handles-2026-11-13.json`. At 512 appends:

- oneShot: median 4.10 ms, range 3.90–5.27 ms.
- incremental: median 225.72 ms, range 220.79–236.97 ms.
- selected: median 93.39 ms, range 91.42–96.46 ms.
- strings: median 72.30 ms, range 66.05–75.83 ms.
- binary: median 62.73 ms, range 60.28–68.88 ms.

Memory/depth metrics remain deterministic across the seven runs. Compare with
the prior repeated baseline (incremental median 278.34 ms); these sequential
local runs support improvement but are not randomized paired measurements.
Selection modes need individual comparison; do not claim every workload speeds
up. Total VM heap remains outside the explicit accounting model, and append-only
CPU cost remains above the original unbounded-history implementation.

### Admitted struct discovery and descriptors (2026-11-13)

Generic struct vector construction now admits its field-discovery set and entries,
sorted field-name directory, output field directory, and struct descriptor.
Discovery uses own-property iteration instead of allocating an Object.keys array
per input row. A child construction scope rolls back field children and metadata
on failure; scratch ends before successful ownership transfer. Field-directory
and vector-descriptor provenance lets later compaction prune obsolete metadata
while preserving directories reused as reachable struct fields. The unscoped API
uses the same construction path without claiming a caller-configured memory cap.

A new wide-struct test includes 102 fields and an own `__proto__` key, checks
lexical field order and values, and proves exact-peak admission plus one-byte-short
rollback to zero for a non-null input. Preexisting validity storage outside this
struct stage remains governed by the caller scope. Runtime sort workspace, source
objects, and other unaccounted constructor/runtime allocations are not claimed as
covered by these explicit metadata charges.

All-package TypeScript build passed. Full default suite passes 2,143 tests with
363 skipped, including existing real JSON/struct HTTP fixture tests. This is a
broader allocation-coverage improvement; it does not resolve the remaining nested
Parquet fallback paths or establish a whole-Worker heap cap.

Worker runtime regression after struct admission: 39 tests passed across 14 files.

### Constant-workspace field ordering and nested selection evidence (2026-11-13)

Extracted the existing string-key collision heapsort into a generic dense-array
helper and reused it for struct field ordering. Both callers now use the same
in-place O(n log n), constant-auxiliary-storage algorithm; struct construction no
longer delegates field sorting to runtime Array.sort workspace. Tests cover
empty/singleton/wide typed inputs, repeated values, Unicode including surrogate
code units, lexical field ordering, and existing collision-workspace behavior.
All-package TypeScript build and 11 focused tests pass.

Extended selection scaling with nested structs and nested heterogeneous lists.
`docs/benchmarks/union-selection-nested-scaling-2026-11-13.json` disproves a general
nested-selection retention bound: struct retained charges grow from 35,468 bytes
at 32 appends to 480,908 at 512; lists grow from 33,026 to 478,466. Both retain two
rows with routing depth two, check nested values, and release all charges when
finished. Nested child construction/ownership needs further pruning; the scalar
numeric/string/binary results must not be generalized to these shapes. These are
local explicit-accounting measurements, not cloud or Worker total-memory tests.

Full default suite after shared heapsort: 2,145 passed, 363 skipped, with local
HTTP fixture listeners permitted.

### Pruning nested-union construction history (2026-11-13)

The nested retained-growth increment matched three separately owned metadata
allocations per inferred union: child directory (224 bytes), construction
wrapper (384), and output descriptor (320). These now carry private provenance.
Compaction can discard obsolete wrappers/descriptors and keeps child directories
whenever a reachable union references them, including a different alias descriptor.
Payload and routing buffers retain their independent alias-aware ownership.

The public concat/gather regression now covers nested structs and heterogeneous
lists for 512 iterations under 32 KiB, verifying full nested values and retained
charges below 16 KiB after each selection (including history slack between
compactions). Scalar cases retain their previous
16 KiB budget and 4 KiB retained limit. A separate metadata test verifies child
directory alias preservation. All-package TypeScript build passed.

`docs/benchmarks/union-selection-pruned-nested-2026-11-13.json` records identical
charges at 32, 128, and 512 appends: structs retain 6,476 bytes with a 20,481-byte
peak; lists retain 4,034 with a 15,897-byte peak. Prior 512-append retention was
480,908 and 478,466 respectively. Nested values and final zero ownership pass.
This closes the demonstrated nested metadata-history growth, not all nested
payload-copy, source-retention, or decoder allocation bounds. Timings are single
local diagnostic runs; no cloud or total Worker-memory claim is made.

The first per-step nested test used an 8 KiB retained assertion derived from
benchmark endpoints; it failed at 9,307 bytes for a struct between compactions.
Endpoint retention is not an every-step bound. The final test uses a 16 KiB
retained bound under the same 32 KiB pool, including logarithmic history slack,
and passes all 512 steps. Twelve focused tests pass; Worker runtime tests pass
39/39. This assertion change does not alter allocation admission or compaction
behavior and must not be reported as an 8 KiB universal nested bound.

Final full default suite for nested pruning: 2,148 passed, 363 skipped.

### Concurrent nested selection in workerd (2026-11-13)

Added Worker-runtime tests for four cooperatively interleaved nested-selection
jobs under each 64/128 MiB shared pool. Every job runs 128 concat/gather rounds
with a 32 KiB child budget, retains its current output across microtask yields,
checks complete nested values after prior owners release, and stays below 16 KiB
retained between operations. A 256 KiB independently owned backing allocation
remains live across all jobs; final output release leaves precisely that charge,
and disposing its scope returns the pool to zero.

At round 64 each job attempts compaction into a separate 1 KiB child budget.
Admission rejects, the rejected budget returns to zero, and source values remain
readable before the job continues. Per-job peak is checked against 32 KiB and
shared peak against cached backing plus four child limits. Both scenarios pass;
the full Worker suite now passes 41 tests across 15 files.

This is generated-data operator/lifetime coverage in local workerd. The retained
backing simulates cache lifetime; the test does not exercise an object-store cache
API, network transport, production Cloudflare enforcement, or total VM heap.
Existing real-fixture HTTP/R2 tests remain separate evidence for those paths.

Repository typecheck passes after correcting the new test helper's Batch return
type and explicitly checking that the selected column exists.

### Avoiding unnecessary concat source discovery (2026-11-13)

Direct and scheduled concat share a source-release decision. It previously built
an input descriptor set before checking whether the output must borrow payload.
It now checks output shapes first with an empty known-dictionary set: union,
UTF-8, binary, and nested shapes containing them already establish borrowing.
Only undecided cases perform source discovery and the existing dictionary-alias
check. This removes unnecessary full-input walks without changing source-release
semantics. All-package TypeScript build, 14 focused scheduled/compaction tests,
and the full suite (2,148 passed, 363 skipped) pass.

Repeated evidence in
`docs/benchmarks/union-ownership-shared-output-check-2026-11-13.json` was refreshed
without concurrent tests after an initial overlapping run. The 512 incremental
append median is 225.07 ms (223.50–229.99), effectively unchanged from the prior
225.72 ms baseline; no append-only timing win is claimed. Selection medians are
numeric 59.24 ms, strings 55.05, binary 60.18, structs 243.58, lists 193.91.
Intervening nested changes mean these are current baselines, not isolated causal
comparisons. Scheduled history compaction itself remains open: it needs a complete
admitted construction/workspace plan, rather than allocating outside its stage.

### Real HTTP fixture selection ownership (2026-11-13)

Extended the existing external `parquet-testing/data/json.parquet` HTTP test to
exercise decoded vector ownership through 128 concat/selection rounds. It scans
four mixed struct/list/null rows into an independently retained batch owner,
closes the scan iterator, evicts shared transport cache entries, then repeatedly
concatenates and selects into a separate 512 KiB consumer budget. Every iteration
checks complete nested values. Releasing the source returns producer usage to
zero while the selected consumer output remains readable; final release returns
consumer usage to zero, and final pool cleanup remains zero.

Both proper 206 byte-range and ignored-range 200 response cases pass. The existing
cold/warm concurrent scan checks and two-read concurrency limit remain part of
the same tests. This directly covers real decoder/transport backing lifetime,
not just generated core vectors. These are local HTTP protocol tests, not actual
remote-object-store latency or production Cloudflare memory enforcement.

The initial test bridge handle was scope-owned and correctly rejected access
after bridge disposal; the test now takes an independent retain before disposing
that bridge. No runtime behavior was weakened. Final targeted fixture result:
two tests passed, neither skipped. No runtime code changed in this checkpoint.

### List/map descriptor admission and construction rollback (2026-11-13)

Generic list and map vector construction now shares an isolated nested-construction
helper. It admits descriptor storage before building offsets/children and transfers
all new ownership only after success; failures dispose the construction scope.
Descriptor charges are 256 bytes for lists (object plus three properties), 320
for maps (object plus four). Private descriptor provenance allows obsolete nested
metadata to be pruned without releasing reachable child storage. Unscoped callers
use the same construction path without a configured allocation-cap claim.

New tests cover nested heterogeneous lists/maps, exact-peak success, one-byte-short
rollback, and late unsupported child values releasing already constructed backing.
Map materialization retains its existing string-key object semantics. Existing
exact typed-storage tests were updated by the explicit descriptor charge, keeping
exact and one-byte-short boundaries and sibling-budget no-double-charge checks.
The oversized-list test still rejects before flattening once descriptor/offset
admission is available. Preexisting root validity allocation is outside this
construction scope and still follows caller-scope lifetime.

All-package TypeScript build and 29 focused allocation/gather tests pass; Worker
runtime tests pass 41/41. Initial full-suite failures identified obsolete typed-only
budget expectations and an incorrect Map-versus-object test expectation; runtime
semantics were not relaxed to satisfy those assertions.

Final full default suite for list/map admission: 2,150 passed, 363 skipped.

### Same-budget reservation retention (2026-11-13)

Reservation retainIn now takes the direct pin path when the destination is the
same budget. That budget already holds the allocation, so this path requires no
admission and cannot invoke reclamation; an extra temporary source pin/release
was unnecessary. Cross-budget retention keeps its existing guard around target
admission, where a reclaimer may release the source. Released-handle validation
still precedes either path.

Added a full-budget test checking same-budget pins do not reclaim, remain charged
through independent releases, reach zero only after the final pin, and reject
retention from a released handle. All-package TypeScript build and the preceding
47 focused ownership tests pass. Repeated measurements are in
`docs/benchmarks/union-ownership-same-budget-retain-2026-11-13.json`: 512 incremental
appends median 221.73 ms, range 205.50–234.68. This overlaps the previous baseline;
no meaningful timing improvement is established. The change removes known guard
churn without altering allocation limits. Recent nested descriptor charges also
mean nested results are current-state evidence, not isolated timing comparisons.

Final validation including the new full-budget test: default suite 2,151 passed,
363 skipped; Worker runtime suite 41 passed.


### Refreshed external conformance after ownership changes (2026-11-13)

The current conformance run remains 232 passed, 18 failed, one skipped. Full-corpus
public-reader diagnostics decode 225 fixtures and reject 18; the test suite also
includes other conformance checks, so those totals differ. No failures were added
or resolved relative to the metadata-transfer checkpoint. Evidence:
`docs/benchmarks/ownership-conformance-checkpoint-2026-11-13.json` and
`docs/benchmarks/parquet-conformance-ownership-refresh-2026-11-13.json` (fixture
hashes and complete error chains).

The failures include malformed fixtures documented by the vendored corpus,
unsupported BSON/Brotli/encoding paths, high-precision decimal policy, and remaining
variant/malformed dictionary paths. They remain test failures: no blanket
expected-error classification or skips were introduced. Several malformed inputs
still surface upstream TypeError/RangeError causes, so robust decoder validation
remains incomplete. Decode completion alone establishes neither correct values
nor bounded allocations. Broader codec/nested decoder admission and real near-cap
cloud validation remain required; this checkpoint is regression evidence only.

### Shared bounded level-run traversal (2026-11-13)

Extracted the existing flat decoder's hybrid RLE/bit-packed visitor into
`level-runs.ts` for reuse by nested-level validation. The visitor does not allocate
level arrays, bounds emitted values by the requested count, and consumes encoded
padding only within the encoded extent. It now validates bit widths (0–32), safe
nonnegative output count/reader offset, and RLE values fitting the encoded width
before passing them to consumers. Logical schema maxima remain caller checks.

New tests cover widths 0/1/2/8/16/32, unsigned 32-bit values, padded packed runs,
explicit extents, invalid dimensions rejected before callbacks, truncated runs,
RLE output overflow, out-of-width values, and strict trailing-byte rejection.
All-package TypeScript build and 62 focused level/numeric/delta tests pass.

This is a shared primitive, not completion of nested decoding. The malformed
repetition-level fixture still enters upstream nested reconstruction; a nested
reader must use this visitor and enforce schema/state constraints before building
child arrays. Do not claim that fixture fixed or nested allocation admission
complete merely because flat paths reuse the hardened visitor.

Full default suite after shared level traversal: 2,160 passed, 363 skipped,
including real-fixture HTTP tests with local listeners permitted.

### Admitted nested level decoding primitive (2026-11-13)

Added `readNestedLevels`, which admits compact repetition/definition arrays before
hybrid decoding and retains a charged result descriptor only on success. Domains
up to 255 use bytes; wider domains use Uint32 storage. It validates safe dimensions,
repetition maximum not exceeding definition maximum, schema-level bounds, and
first-column-page repetition zero. Continuation pages explicitly allow a nonzero
initial repetition level. V1 length prefixes and V2 explicit lengths share the
same bounded visitor; row-start/present counts and payload offset are returned
without reading payload values. All construction rolls back on decode/admission
failure.

Tests cover V1/V2 values, counts and untouched payload offset, exact admission,
one-byte-short rollback, valid continuation pages, invalid first-page continuation,
and definition values above the schema maximum. A real vendored
`bad_data/ARROW-GH-45185.parquet` page is parsed from its metadata/header and rejected
by this primitive before nested child reconstruction, with zero residual charge.
Four focused tests pass, including the real fixture (not skipped); all-package
TypeScript build passed before adding the fixture-only test.

This primitive is not yet wired into the public nested fallback, so public
conformance behavior remains unchanged. It is preparatory allocation/validation
coverage, not a claim that the public malformed-file issue or nested decoder
memory bounds are finished. Decompression, page ownership, row continuation
state and reconstruction integration remain required.
