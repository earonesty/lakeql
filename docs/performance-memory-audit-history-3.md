# Performance and memory audit: historical execution log 3

[Current requirements and evidence](performance-memory-audit.md).


### Nested PLAIN physical page contract (2026-11-13)

Added `readNestedPlainPage` to combine admitted nested levels with existing PLAIN
physical-value readers and scoped decompression. Its explicit capability is PLAIN
values with supported hybrid level streams; unsupported value/legacy level
encodings return undefined before allocating. Logical conversion and nested
reconstruction remain separate stages. V1 whole-page and V2 value-only compression
are handled separately, preserving V2 level bytes outside decompression. Header
extents, decoded sizes, V2 null/row counts and complete value-byte consumption are
validated. Boolean PLAIN bits decode into an admitted Uint8Array; other physical
types reuse the scoped PLAIN reader. Caller ownership must keep borrowed input
views alive; decompressed outputs remain in the construction scope.

Five page tests plus four level tests pass: V1/V2 and raw/GZIP combinations,
physical values/counts, exact admission, one-byte-short rollback, truncated page
extent rejection, booleans, unsupported level capability without allocation, and
the prior real malformed-level fixture. All-package TypeScript build passes.
The new page helper is not integrated into public nested scanning yet. Therefore
this checkpoint does not change public conformance or prove a complete bounded
nested decoder. Dictionary/delta encodings, logical conversion, row assembly and
page-boundary ownership integration remain required.

### Shared flat/nested dictionary index decoding (2026-11-13)

The nested physical-page helper is now `readNestedPage`, supporting PLAIN plus
RLE_DICTIONARY/PLAIN_DICTIONARY when dictionary cardinality is available. It
returns physical indices and an explicit dictionaryEncoded flag; dictionary
payload remains borrowed and is not inspected. Missing cardinality declines
before allocation. Both flat and nested paths share `readDictionaryIndices`,
which admits Uint32 storage, validates cardinality/count/bit width, and validates
run values against cardinality before filling the output. This removes the flat
path's separate per-index bounds-validation pass. Zero-width dictionaries retain
the existing implicit-zero representation.

Nested tests cover both dictionary encoding names, correct indices, exact-budget
success, one-byte-short rollback, out-of-range references releasing the nested
construction scope, and missing dictionary capability with a zero-byte budget.
Additional helper tests cover zero-bit dictionaries and invalid extents before
allocation. All-package TypeScript build passed; full default suite before the
last two helper-only tests passed 2,171 with 363 skipped. The final focused helper
and nested-page run passed nine tests. Public nested scan integration remains
open; shared flat decoding is the runtime integration in this checkpoint.

### Nested integer delta and byte-stream-split capabilities (2026-11-13)

Nested physical pages now reuse the bounded integer-delta reader for INT32/INT64
DELTA_BINARY_PACKED and the bounded byte-stream-split reader for INT32/INT64/FLOAT/
DOUBLE. Unsupported encoding/type combinations decline before allocation; physical
values still remain separate from logical conversion and row reconstruction.
Six new cases verify exact values (including INT64 beyond Number's safe range),
exact-budget admission and one-byte-short rollback. All-package TypeScript build
and 26 numeric/delta focused tests passed.

Added real-fixture comparison for all four repeated columns in
`data/repeated_primitive_no_list.parquet`: the new page helper matches upstream
physical dictionary indices and repetition/definition levels, with definition
levels normalized for upstream's all-present omission. Each scope releases to
zero. The fixture test is local page decoding with no HTTP/cloud transport; it
checks physical values/levels rather than complete row assembly. Final focused
fixture/page run passes 14 tests, including the real fixture (not skipped).
Public nested scan integration, byte-array delta representations, logical
conversion and bounded reconstruction remain open.

### Packed nested byte-array pages (2026-11-13)

Nested page decoding now exposes packedBinary storage for PLAIN BYTE_ARRAY/
FIXED_LEN_BYTE_ARRAY and DELTA_LENGTH_BYTE_ARRAY/DELTA_BYTE_ARRAY. It reuses the
existing validated packed readers rather than allocating one view per physical
value. The nested wrapper conservatively admits the packed descriptor and view
headers before invoking those readers. PLAIN and length-delta payloads borrow
page backing; prefix-delta payload reconstruction is separately admitted. The
returned values field is undefined for packed binary pages; consumers must use
the explicit packed representation. Missing fixed-width metadata rejects.

Three encoding tests check multibyte UTF-8 bytes, shared prefixes, empty values,
backing-buffer alias/copy behavior, exact-budget success and one-byte-short
rollback. All-package TypeScript build passes; 25 focused nested-page, real
repeated-column fixture, and delta-binary tests pass. This completes the planned
packed byte-array capability in the physical helper, not logical conversion or
public nested scan integration. Calling packed value accessors creates borrowed
views; downstream output/materialization accounting remains its own contract.

Full default suite after nested packed pages: 2,183 passed, 363 skipped, including
real-fixture HTTP tests with required local listeners enabled.


### Shared logical conversion boundary for nested decoding (2026-11-13)

Extracted physical-to-logical conversion from `vector-batches.ts` into
`logical-values.ts`, keeping the flat reader on the same implementation. Decimal,
JSON, FLOAT16, and temporal conversion retain their existing scoped allocation
paths. Dictionary indices remain a separate operation: converting indices as
physical leaf values would corrupt logical semantics. Input backing remains
caller-owned.

Validation: `pnpm exec tsc -b` passed; six targeted suites passed all 45 tests,
including real temporal, FLOAT16, JSON, decimal, and repeated-page fixtures and
existing admission/rollback checks. These fixture checks use local or memory
sources, not cloud transport. This extraction makes the shared conversion
boundary available; nested logical vector construction and schema reconstruction
are still outstanding, and public nested fallback behavior has not changed.
No timing improvement is claimed for this refactor.


### Nullable decimal allocation admission (2026-11-13)

Inspection of the shared logical conversion boundary found a scoped fallback
that returned an uncharged `Array.from` whenever decimal values contained null
or undefined. It now reserves the output array, slots, and conservative numeric
cell charges before constructing that array. Dense typed decimal output retains
its existing representation and accounting. Physical input remains caller-owned.

A regression checks mixed positive/negative/null/missing values, unchanged input,
exact-budget success, cap-minus-one rejection before allocation, zero retained
bytes after disposal, and rollback when a later value exceeds exact integer
precision. The late error retains the nested column path. All 13 decimal tests
and `pnpm exec tsc -b` passed; changed-file Biome checks passed. This closes the
nullable output-array gap, not all decimal/parser scratch accounting, and is not
a cloud-transport or whole-runtime memory-cap claim.


### Large-scale decimal conversion workspace (2026-11-13)

Decimal batches with scale greater than 22 now admit the bounded string workspace
already modeled for scalar decimal statistics. One serial workspace reservation
is reused across the batch and released before returning; only output stays
charged. Dense and nullable output use the same conversion/admission path.
Empty batches need no conversion scratch. Ordinary-scale dense output keeps its
existing charge. The workspace bound depends on safe integer string lengths,
not the scale magnitude or input binary width.

Both dense and nullable regression cases prove exact peak admission, retained
output-only charges, cap-minus-one rejection, and complete rollback on a late
precision failure. All 15 decimal tests, TypeScript build, and changed-file lint
passed. This adds modeled string scratch; it does not establish total JS runtime
heap bounds or eliminate other decoder/parser accounting gaps.

Full regression refresh after decimal admission changes: `pnpm test` passed
2,186 tests, with 363 skipped (233 passing files, nine skipped). This includes
the existing HTTP fixture coverage; no new transport behavior is introduced.


### Compact nested leaf vector boundary (2026-11-13)

Added `nestedLeafVector`: it produces only present leaf values while preserving
physical repetition/definition levels for later reconstruction. Numeric typed
storage is borrowed, including unsigned reinterpretation; boolean values retain
boolean vector semantics. Logical dictionary vectors and compact indices remain
shared instead of expanding per row. UTF8/raw packed binary retains payload and
range backing. Other packed logical annotations explicitly decline before
allocation; their bounded conversion adapters remain outstanding.

Construction charges descriptor/view capacity before conversion and transfers
new allocations only on success. The caller must retain physical and dictionary
owners; the helper does not implicitly take ownership. Tests cover unsigned
64-bit extremes, unchanged levels, exact/cap-minus-one admission, dictionary
identity and invalid-index rollback, Unicode/empty packed strings, and allocation-
free unsupported dispatch. Three helper tests passed; the preceding combined
run also passed all 19 tests across the helper and physical-page suites, including
the real repeated-column physical fixture. TypeScript build and changed-file
lint passed. The new helper tests are synthetic; real-fixture logical integration,
cloud transport, and public nested schema reconstruction remain open. This is
not a public fallback replacement or evidence of whole-Worker memory bounds.


### Real repeated-fixture logical leaf verification (2026-11-13)

Extended `nested-page-fixtures.test.ts` beyond repetition/definition levels and
physical indices: all four dictionary-encoded columns in
`repeated_primitive_no_list.parquet` now decode their actual dictionary payload,
build compact logical dictionary/leaf vectors, and compare every present leaf
value against independent upstream PLAIN decoding and logical conversion indexed
by upstream data-page indices. This exercises both INT32 and BYTE_ARRAY leaves,
including repeated fields within a group. The test also checks compact length,
shared dictionary/index identities, and zero tracked allocations after disposal.
The fixture test passed (not skipped), and changed-file lint passed.

This establishes local real-fixture logical leaf operation; it does not assemble
nested rows, exercise HTTP range reads, or prove the public fallback has been
replaced. Those integration steps remain outstanding.


### Packed logical nested leaf conversion (2026-11-13)

The compact nested leaf converter now accepts packed decimal, JSON, FLOAT16,
and logical TIMESTAMP carriers through the shared scoped conversion functions.
It reserves the temporary physical-view array and each byte view before creation,
then releases that workspace immediately after conversion. Raw binary/UTF8 still
share packed storage. Unsupported annotations such as BSON decline before
allocation. Converted-value arrays remain conservatively retained with output;
further ownership pruning is still possible.

Synthetic cases exercise nested JSON/null, signed decimals, FLOAT16 negative
zero/infinity, and FLBA12 pre-epoch nanoseconds. Each checks exact-peak success,
cap-minus-one failure, and zero retained allocations after cleanup. Malformed
JSON after a valid value rolls back the entire construction. The real repeated
fixture regression remains part of the targeted run. Build and changed-file lint
passed. These conversions remain internal; public schema reconstruction and
remote integration are still pending.


### Packed logical real-page fixture coverage (2026-11-13)

Expanded nested page/leaf fixture verification to six real files: repeated
primitives, both FLOAT16 fixtures, JSON, fixed-length decimal, and FLBA12 timestamps.
Dictionary tests now cover real half-float dictionary values; direct-page tests
cover JSON and decimal plus all three timestamp columns. Expected physical and
logical values come from upstream page decoding/conversion. Expected values then
use the standard core vector representation: missing JSON struct fields normalize
to null, matching existing public vector behavior. An initial direct-object
comparison exposed that expected representation difference and was corrected;
no production JSON semantics changed.

Direct-page cases rerun the complete physical-plus-logical path at measured exact
peak and cap-minus-one, and check cleanup to zero. All six fixture tests passed
without skips, and changed-file lint passed. Fixtures here are uncompressed local
pages; compressed logical pages, remote transport, and public nested assembly
still require integration coverage.


### Page-local nested row indexing (2026-11-13)

Added a generic index from top-level row segments to both level-event offsets and
compact present-value offsets. Repetition level zero starts a new row; deeper
repetition levels stay in the same segment. A leading nonzero repetition level
is explicitly marked as a continuation, not counted as a newly started row.
The last segment is deliberately not marked complete: a subsequent page or the
column boundary must determine that. No row objects or expanded values are built.

The index validates level-array lengths and recorded row/present counts before
allocation, then admits two exact-size typed offset arrays and a descriptor in
an atomic construction scope. Tests cover leading continuation, deeper repetition,
null/empty events, required nonrepeated values, empty pages, exact admission,
cap-minus-one rollback, and inconsistent-count rejection before allocation.
Real repeated/FLOAT16 dictionary fixtures now validate index extents alongside
logical values. TypeScript build and changed-file lint passed. Full schema-level
Dremel transition validation, cross-page coordination, and public nested assembly
remain outstanding; this index alone does not supply them.


### Schema-aware repeated-ancestor transition validation (2026-11-13)

Added `validateNestedTransitions`, which derives each repeated ancestor's required
definition level from the schema path. Nonzero repetition must refer to an
ancestor present in both the preceding and current event. The caller carries
only the returned last definition level across pages; empty pages preserve it.
A first-page continuation without a preceding event is rejected. Level maxima,
array extents, and recorded row/present counts are checked as well.

Temporary threshold storage is admitted by schema repetition depth and released
on success or failure. Tests validate an optional/two-repeated-level schema at
every page split (including empty pages), absent ancestors on either side,
out-of-schema levels, and exact/cap-minus-one workspace admission. Real repeated
and FLOAT16 fixture paths also pass validation. All 14 targeted tests, TypeScript
build, and changed-file lint passed. This validator is available to the future
page coordinator; it is not yet called by the public nested fallback. Full
schema reconstruction, cross-column alignment, and public integration remain.


### Atomic nested column-page pipeline (2026-11-13)

`decodeNestedColumnPage` now composes physical decode, schema transition checks,
row indexing, and compact logical conversion in one construction scope. It
returns the next definition-level state only after all stages succeed. Unsupported
physical/logical capabilities return undefined and release all new allocations;
malformed pages and budget failures likewise roll back. Input bytes/dictionaries
remain separately owned, and a previous page remains intact after failed decoding.

Real direct JSON, decimal, and FLBA12 fixtures now exercise this combined path at
exact peak and cap-minus-one. A synthetic two-page repeated row verifies leading
continuation, new-row offsets, readable prior output after invalid continuation,
and retry with unchanged continuation state. Orphan first-page continuation and
unsupported BSON rollback are covered. All eight targeted tests, build, and
changed-file lint passed. This is a per-page coordinator, not yet a column stream:
row sealing, cross-column alignment, schema assembly, transport integration, and
replacement of the public fallback remain outstanding.

Full suite after column-page integration: `pnpm test` passed 2,214 tests,
363 skipped, across 237 passing files and nine skipped files. Existing HTTP
coverage remains green; the new column-page pipeline is still internal.


### Nested leaf slot expansion (2026-11-13)

Added schema-derived leaf-slot expansion to the atomic column-page result.
Optional leaf nulls produce routing/validity slots only when their parent exists;
absent ancestors and empty repeated elements produce no child slot. Compact
payload remains borrowed, using a dictionary routing vector rather than expanded
value objects. When no null insertion is required, the original vector is
returned with no allocation. All-null leaf slots can route an empty dictionary
because their validity suppresses access.

Tests cover missing parents versus null leaves, mixed/all-null slots, no-op
identity at zero budget, inconsistent compact counts, exact capacity, and failure
rollback. The column-page test includes a null leaf in a continued repeated row.
All ten targeted tests (including real-fixture page regressions), TypeScript build,
and changed-file lint passed. Parent struct/list/map construction and slot
alignment across columns are still required before public nested integration.


### Workerd column-page concurrency and cache eviction (2026-11-13)

Added four workerd cases combining 64/128 MiB shared accounting pools with raw
and GZIP versions of the actual first JSON fixture page. Four cooperatively
interleaved jobs each decode eight pages with a 256 KiB per-job cap. The input
sits in a real SharedMemoryCache allocation; jobs acquire leases, the entry is
evicted while all jobs retain decoded output, and subsequent rounds continue
using those leases. Every round checks complete leaf-slot values, including JSON
objects/lists and a null. Per-job usage returns to zero after each round, shared
usage returns to zero after final lease release, and tracked peak stays within
cache admission plus the four job caps.

The embedded page records its original fixture SHA-256. The Node fixture test
compares its exact bytes to the checked-out JSON Parquet page. GZIP recompresses
those real page bytes; the source Parquet file itself is uncompressed. All 45
workerd tests across 16 files passed; repository typecheck and changed-file lint
passed, and all six local fixture tests passed. These are real runtime/cache
lifetime checks, not HTTP/R2 reads or measurements of total Worker heap, and they
do not yet exercise public nested schema assembly.


### Schema-node indexing and repeated-dimension vectors (2026-11-13)

Added page-local schema-node indexing for optional, required, and repeated nodes.
Indices capture occurrence event offsets, validity, and leading continuation at
that node's repetition depth. Optional null nodes retain slots when their parent
exists; empty repeated nodes have no element occurrence. This runs after schema
transition validation and does not independently validate the full Dremel stream.

Added generic repeated-dimension assembly from aligned parent/child node indices
and an existing child vector. It builds only admitted list offsets/descriptor and
borrows parent validity and child storage. It rejects children under null parents,
misaligned extents, and child counts exceeding the core signed-offset capability.
Page fragments remain fragments; the helper does not seal continued containers.

Five tests cover node depths, absent parents, empty lists, nullable elements,
continuation semantics, sharing, exact-capacity admission, and rollback after
misalignment. A complete example materializes `[null, [], [7, null], [9]]`.
TypeScript build and changed-file lint passed. Initial build checks caught and
resolved core list representation mismatches (Int32 offsets and `child` field).
These helpers are not yet orchestrated into public schema assembly; structs,
maps, cross-column alignment, and cross-page container sealing remain open.


### Real repeated lists and multidimensional composition (2026-11-13)

Real repeated-primitive fixture tests now assemble complete lists for all four
columns, including those inside the required group. An upstream-oracle mismatch
revealed a compatibility issue: upstream `assembleLists` returns null for empty
legacy REPEATED INT32 fields. Direct DuckDB `read_parquet` verification returns
empty lists, matching the new generic assembler. Tests freeze independently
verified numeric/string row values and explicitly account for the upstream
null/empty difference. The public fallback has not changed yet; its future
replacement must record this correctness/compatibility change rather than claim
bit-for-bit equivalence with upstream behavior.

A composed two-dimensional example passes through transition validation, compact
leaf null expansion, node indexing, and repeated-dimension assembly, producing
`[null, [], [[1, null], null, [], [2]], [[3]]]` with no intermediate row arrays.
This exercises null/empty lists and nullable elements at multiple depths. All
12 targeted tests passed, and changed-file lint passed. Public orchestration,
struct/map assembly, and cross-page sealing remain open.


### Struct assembly with explicit child alignment (2026-11-13)

Added bounded struct assembly with an explicit per-field alignment contract:
`present-parents` children omit absent parents; `all-parents` children already
include them. Lengths and parent validity are checked before allocation. Compact
fields share one admitted parent-routing index array and borrow child payload;
already aligned children retain identity. The output field directory has a null
prototype, preserving own field names such as `__proto__` safely.

Tests combine compact numeric/text/boolean fields with an already aligned list,
null parents, shared routing identity, exact capacity/cap-minus-one rollback,
empty structs, and invalid child lengths rejected before allocation. Both tests,
TypeScript build, and changed-file lint passed. Cross-column event/row alignment
remains the coordinator's responsibility; matching lengths alone do not establish
that independent column fragments describe the same rows. Public assembly and
map construction remain outstanding.


### Shared container offsets and map vectors (2026-11-13)

Extracted list offset construction into `nestedContainerOffsets` and reused it
for bounded map assembly. Lists/maps now share signed-offset capability checks,
parent/child event-range validation, null-parent rejection, and atomic rollback.
Map construction verifies key/value cardinality before allocation and borrows
both payload vectors plus parent validity. It preserves the existing core map
representation and key semantics; schema and cross-column alignment remain
explicit prerequisites rather than assumptions inferred from equal lengths.

Tests cover null maps, empty maps, null values, a `__proto__` key, payload identity,
exact capacity, cap-minus-one rollback, mismatched key/value counts, and invalid
parent alignment. Existing multidimensional list and real-fixture regressions
also pass after the shared-helper extraction. Build and changed-file lint passed.
Public map schema orchestration and real map fixture assembly are still pending.


### Real compressed nested-map assembly (2026-11-13)

Added a fixture test assembling `nested_maps.snappy.parquet` from three independent
leaf streams: outer string keys, inner integer keys, and inner boolean values.
It runs scoped Snappy decoding, dictionary decoding where present, atomic column
page conversion, schema-node indices, and two levels of generic map assembly.
All six output rows match values independently obtained from DuckDB, including
null and empty inner maps. Inner key/value indices align exactly; outer and inner
streams have different event counts, so parent occurrence validity is compared
at corresponding schema depths rather than comparing raw event offsets.

The fixture also exposed an older framing convention: the inner integer column's
reported data offset points to a dictionary page despite no dictionary offset in
metadata. The test reader now consumes that inline dictionary before the data
page. Production column streaming must preserve this behavior; a direct seek to
an assumed data-page header is insufficient.

The fixture test passed at measured exact tracked peak; cap-minus-one failed,
and cleanup returned tracked usage to zero. Changed-file lint passed. This is
local fixture orchestration and scoped decoder evidence, not public query or
cloud transport integration. Test-driver metadata/objects are not included in
the decoder budget measurement. General column-stream alignment is still pending.


### Atomic bounded dictionary-page decoding (2026-11-13)

Added `decodeNestedDictionaryPage` for modern PLAIN and legacy PLAIN_DICTIONARY
headers. It validates counts/byte extents, uses scoped decompression and physical
readers, preserves packed binary/text storage, and invokes shared logical leaf
conversion. Reader/conversion descriptors are admitted scratch and released
before return. Partial allocation, trailing payload, malformed headers, and
unsupported logical dictionaries roll back atomically. BOOLEAN dictionaries are
explicitly declined rather than entering the unaccounted generic PLAIN fallback.

Real repeated/FLOAT16 and Snappy nested-map fixture tests now use this production
helper instead of their hand-built dictionary decoding. Unit cases check both
header encoding names, shared packed backing, exact peak, cap-minus-one rollback,
trailing payload rejection, and unsupported BSON cleanup. All ten targeted tests,
TypeScript build, and changed-file lint passed. Stream discovery/ownership of
inline dictionaries remains a column coordinator responsibility; public streaming
integration is still pending.


### Shared page framing for nested stream integration (2026-11-13)

Extracted page header mapping, bounded header probing, prepared body validation,
and owned body reads into `page-window.ts`. Flat vector batches retain compatibility
re-exports; numeric page grouping/alignment import the shared module directly.
The chunk-reader parser uses the same exported header mapper. This avoids a
second nested-reader implementation of cloud/local extent handling and removes
numeric alignment's runtime dependency on the full vector-batch module for page
preparation. Header probe sizes, projected fields, cancellation checks, and owned
body transfer behavior are unchanged.

The first build/regression caught the chunk path's additional parser call site;
it now imports the shared parser. The corrected build, changed-file lint, and
all 41 page preparation/group/header planning tests passed. This refactor enables
nested column streaming but does not implement its page/dictionary lifetime loop.

Full regression after shared framing extraction: 2,229 tests passed, 363 skipped
(243 passing files, nine skipped), including existing real HTTP fixture tests.


### Owned nested column-page iterator (2026-11-13)

Added `readNestedColumnPages`, which uses shared page framing and requires owned
transport reads. It discovers dictionary pages in the stream (including legacy
inline dictionaries), retains one dictionary, validates total event counts, and
carries scalar definition-level continuation between data pages. Each yielded
owner is an iteration lease; callers explicitly retain it to outlive advance or
close. Retained page ownership includes its input backing and dictionary pin.
Exhaustion, early return, malformed counts, and failed admission dispose iterator
state; duplicate/late dictionary pages are rejected. Index-page bodies are skipped.

Tests stream all three nested-map fixture columns through memoryStore's owned
read adapter, covering normal exhaustion and early close. Retained scalar/packed/
dictionary output remains readable after iterator and caller scope disposal, and
final release returns accounting to zero. Incorrect metadata counts and a one-byte
read budget reject cleanly. Both fixture tests, build, and changed-file lint passed.
This is local owned transport evidence; HTTP/R2 streaming and multi-page row sealing
still need verification. Public query fallback integration remains outstanding.


### Real HTTP nested column streaming (2026-11-13)

Added real-server HTTP coverage for the owned nested column iterator using the
Snappy nested-map fixture. Both compliant 206 ranges and ignored-range 200 full
responses pass. Three independent column streams share a two-read transport
limit, 2 MiB allocation pool, 128 KiB per-stream caps, and a 128 KiB shared range
cache. The warm round performs zero network requests. Server-observed concurrency
reaches two and never exceeds it.

Cold and warm outputs are explicitly retained beyond iterator/scope disposal.
After cache eviction, every retained scalar/packed/dictionary value is checked
again; final release returns the shared pool to zero. Both HTTP tests and
changed-file lint passed. Fixture metadata is bootstrapped locally so this test
isolates column transport/decoding, not remote footer loading. It does not yet
assemble public query rows, exercise R2 for this iterator, or establish total
runtime heap caps. Multi-page row sealing and public schema orchestration remain.


### Multi-page continuation and failure lifetime regression (2026-11-13)

Added serialized V1 column-stream edge cases using the writer's actual page-header
encoder: one repeated row spans a data page, an empty page, and a following data
page that also starts a new row. The owned iterator preserves the prior definition
state through the empty page, marks the next leading segment as a continuation,
and preserves compact/nullable leaf values. A malformed later continuation is
rejected while an independently retained first-page result remains readable.
Iterator/scope disposal plus final owner release returns accounting to zero.

All four continuation/local-fixture stream tests and changed-file lint passed.
These new boundary cases are generated column streams, complementing the real
Snappy fixture coverage; they are not additional externally sourced fixtures.
The iterator exposes correct fragments, but joining them into sealed rows for
cross-column/schema assembly is still outstanding.


### Owned sealed row fragments across pages (2026-11-13)

Added `readNestedColumnRows`, which seals a row only when the next row start or
column end is observed. It holds page leases for pending fragments and admits
fragment records plus the final directory/row descriptor under the same budget.
Empty pages do not seal or erase a pending row. The result preserves event and
compact-value spans rather than materializing row objects; each row lease can be
retained independently of iterator advance/close. A spanning row that cannot fit
fails allocation rather than accumulating unbounded storage.

Generated V1 stream tests now seal the row across two nonempty pages separated
by an empty page, retaining definition levels `[2,1,2]` and compact values `[7,9]`,
then seal the next row at end of column. Retained fragments remain readable after
iterator/scope disposal. A malformed later continuation fails before exposing the
incomplete first row. Exact measured pipeline peak succeeds; cap-minus-one fails
and returns accounting to zero. Both extended tests, build, and changed-file lint
passed. Cross-column row synchronization and schema orchestration remain pending;
sealed fragment directories are not yet public query output.


### Bounded cross-column sealed-row synchronization (2026-11-13)

Added `readAlignedNestedRows`: it advances sealed-row iterators with bounded
concurrency (default four, capped by source count), validates ordinal row order
and simultaneous end-of-stream, and returns independently retainable aligned row
leases. Iterator/result/column directories and result records are admitted before
construction. Concurrent work drains before error cleanup; all iterators close,
and an original decode error is preserved if cleanup also fails. Runtime Promise
and async-generator engine overhead is not a total-heap accounting claim.

Synthetic tests prove two-way concurrency, early-close cleanup, retained output,
row-count mismatch, row-order mismatch, and failure cleanup across three sources.
The real Snappy nested-map fixture now streams and seals all three columns, then
aligns six rows despite differing per-column event/value counts; final usage is
zero. All six targeted tests, build, and changed-file lint passed. General schema
orchestration over aligned fragments, row batching/performance, and public query
integration remain outstanding.


### Sealed fragment normalization for schema assembly (2026-11-13)

Added `joinNestedRowFragments`: validates event/value extents and exactly one
leading row start, builds admitted contiguous repetition/definition metadata,
and routes compact values through shared page vectors. Full payload spans keep
identity; partial spans get admitted dictionary indices. Multiple fragments use
the existing generic bounded-fanout union concatenation planner, now exposed from
the core entry point, rather than a Parquet-specific union implementation.
Input row ownership remains an explicit caller lifetime requirement.

The cross-page continuation test now normalizes its sealed row to levels
`rep=[0,1,1]`, `def=[2,1,2]` and compact values `[7,9]`, with exact-capacity success
and cap-minus-one rollback. Five targeted continuation/union tests, build, and
changed-file lint passed. Normalization copies level metadata and retains a
conservative routing directory; payload values remain shared. General schema
orchestration and batched public scanning are still outstanding.


### Schema container layout normalization (2026-11-13)

Added an admitted schema-layout classifier for leaf, struct, LIST, and MAP nodes,
using both logical and converted annotations. LIST compatibility preserves tuple
and repeated-element groups where required; MAP identifies children by position,
accepts omitted values, and recognizes legacy MAP_KEY_VALUE outer annotations.
Malformed container shapes and optional map keys reject before output allocation;
unknown group annotations decline instead of silently becoming structs. The
classifier borrows schema nodes and admits only its layout descriptor.

Rules were checked against Apache's primary specification:
https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#nested-types
Six tests cover legacy list layouts, modern logical LIST annotations, map naming,
missing values, malformed keys, and cap-minus-one admission. Build and changed-file
lint passed. This classifies layouts; recursive/iterative schema orchestration,
public scanning, and batching are still outstanding.


### Cross-column repeated-ancestor placement validation (2026-11-13)

Added `validateNestedNodeAlignment` before general schema composition. Equal
node counts and validity are insufficient: children can be distributed differently
among repeated ancestors. The validator compares the common schema prefix,
projected node validity, continuation state, and repetition boundaries clamped to
that node's depth. Different descendant event counts remain valid when their
projected ancestor structure matches. All temporary node indices are budgeted and
released on success or error.

Adversarial tests reject identical occurrence counts with different ancestor
placement while accepting a deeper repeated descendant with equivalent projected
shape; exact/cap-minus-one admission and cleanup are covered. Real Snappy nested
maps now validate both outer and inner key/value ancestor placement before map
assembly. All three targeted tests, build, and changed-file lint passed. General
schema orchestration and public scanning remain pending.


### Reusable iterative nested schema plans (2026-11-13)

Added `compileNestedSchema`, producing generic leaf/struct/list/map operations plus
explicit wrappers for unannotated repeated nodes. Each node records physical
schema depth and leaf-source ordinal; LIST compatibility layouts determine whether
the repeated wrapper is preserved or unwrapped. Plan nodes, child directories,
layout descriptors, and traversal frames are admitted. Compilation uses an
explicit stack and releases traversal frames rather than recursing through the
JavaScript call stack. Schema trees and source paths remain borrowed metadata.

A real Snappy nested-map schema compiles with the expected nested map operations
and source ordinals. A separately constructed 2,000-level schema compiles under
a 4 MiB accounting cap. Exact measured peak succeeds; cap-minus-one and missing
leaf inputs roll back to zero. Both tests, build, and changed-file lint passed.
The plan is reusable across rows; its iterative executor and public batch scan
integration are still outstanding. Source-path matching currently scans paths
per node, so large-schema compile-time scaling remains a measurement target.

### Compiled nested-map execution checkpoint (2026-11-13)

The explicit-stack schema executor now has real-fixture coverage: the Snappy
nested-map fixture is decoded through the physical column decoders, compiled from
its schema, and assembled with generic map/leaf operations. All six output rows
match both the existing independent assembly and the frozen DuckDB-verified
expectations, including null and empty inner maps and unequal key/value event
counts. The fixture exercises ancestor alignment across independent leaf streams.
The complete fixture pipeline succeeds at its measured tracked peak, rejects at
one byte below that peak, and releases its charges after disposal. This is an
allocation-accounting boundary test, not a total-process heap measurement.

Validation: TypeScript project build passes; the map fixture and schema-plan
suites pass (three tests, including deep iterative compilation). Execution of deep
schemas, compiled LIST/struct combinations, streamed row integration, cloud reads
through this executor, and public reader integration remain to be validated.
No throughput improvement is inferred from these correctness tests.

### Streamed compiled assembly checkpoint (2026-11-13)

Extended the real Snappy map stream test through the complete internal row path:
owned column pages → sealed column rows → concurrency-two row alignment → joined
row fragments → compiled schema execution. All six assembled rows match frozen
map expectations, covering null and empty inner maps and different event counts
between outer keys and inner entries. Each row has its own assembly scope and
adopts the aligned row lease. The final assembled vector remains readable after
stream exhaustion and parent-scope disposal; releasing its independent owner
returns tracked use to zero. Four tests pass across the real streaming and
synthetic page-continuation suites. This uses an in-memory object store; it does
not establish remote latency, cloud cancellation behavior, or public adapter
integration for compiled assembly.

### Compiled rows over HTTP and shared cache (2026-11-13)

The actual HTTP nested-column fixture test now also runs aligned row streaming,
fragment joining, schema compilation and execution. Both proper 206 ranges and
servers ignoring ranges with 200 responses produce all six expected nested-map
rows on cold and warm passes. Each assembly job has a 128 KiB child budget under
the shared 2 MiB test pool; the shared cache is capped at 128 KiB. Cold passes
perform network reads, warm passes perform none, and observed concurrent server
requests reach two without exceeding the configured two-read limit. All twelve
assembled row owners remain readable after their job scopes are disposed and the
shared cache is evicted. Releasing them and the independently retained page owners
returns pool usage to zero. Both HTTP tests pass.

This proves the internal compiled assembly path against real HTTP response
semantics and cache lifetimes, not production cloud latency or R2 integration.
Metadata still comes from the fixture harness outside the job budget. These small
fixtures do not establish whole-isolate memory bounds or throughput improvements;
public reader integration and broader nested shape coverage remain outstanding.

### Compiled LIST/struct alignment edges (2026-11-13)

Added executor regression coverage for an optional struct containing an optional
three-level LIST: absent struct, null list, empty list, null element, and present
elements remain distinct. The complete assembly succeeds at its tracked peak and
rolls back to zero at peak minus one. A separate legacy repeated primitive case
checks that empty repeated fields stay aligned across absent optional structs.
Both tests pass and dispose to zero. These synthetic level streams target shape
semantics; they complement rather than replace the real map/HTTP fixtures, and
real LIST fixtures still need to exercise compiled assembly.

### Real repeated primitive compiled execution (2026-11-13)

All four columns of `repeated_primitive_no_list.parquet` now also run through the
compiled executor. Dictionary-backed integer and string lists match the frozen
DuckDB-verified rows, including empty integer lists. Disposing each assembly scope
restores its pre-assembly tracked use; a budget smaller than the retained assembly
charge rejects atomically with zero outstanding charges. The six-test physical
fixture suite passes. This covers real legacy repeated primitives, not modern
three-level LIST encodings; those still require fixture-level compiled coverage.

### Modern LIST fixtures expose zero-width dictionary framing (2026-11-13)

Added streamed whole-schema execution for nested_lists.snappy.parquet,
list_columns.parquet, and null_list.parquet. Frozen expectations were checked
against the upstream reader (missing nullable fields normalized to core null).
These cover three nested LIST dimensions, sibling primitive columns, INT64 lists,
UTF8 lists, null elements, null lists and empty lists.

The Snappy fixture exposed a real decoder defect in its constant sibling column:
a zero-bit-width dictionary index stream contains a valid bit-packed run header.
The decoder previously returned immediately after the width byte, causing strict
page-consumption validation to reject the remaining header. It now parses present
hybrid headers with the shared bounded run visitor while preserving support for
writers omitting zero-width runs. Strict page extent validation remains intact.

### Zero-width run regression matrix (2026-11-13)

Added direct dictionary tests for omitted runs, padded zero-width bit-packed
headers, single and multiple zero-width RLE runs, and malformed zero/truncated/
overlong/insufficient runs. Exact output-byte budgets succeed and scopes release
to zero. The nested-reader plus dictionary regression suite passes: 102 tests in
24 files, including both real HTTP response modes. An initial sandboxed run passed
100 tests but could not bind the HTTP server (EPERM); rerunning with local-server
permission passed all tests. No assertions or HTTP coverage were removed.

### Deep compiled execution and admission rollback (2026-11-13)

Extended the depth-2,000 schema test through execution, traversing the resulting
struct vectors iteratively to verify the leaf value. Execution succeeds under a
4 MiB tracked budget and at its measured exact peak; peak minus one rejects with
zero outstanding execution charges. Independent input/plan ownership survives the
failed attempt. Compilation and execution tests both pass. This establishes stack
safety for these internal stages, not recursive upstream metadata parsing or
recursive consumer materialization. Schema-prefix scans remain a potential
quadratic CPU cost at extreme depth and require measured optimization separately.

### Owned compiled row operator (2026-11-13)

Added `readNestedSchemaRows` to consolidate normalized fragment joining and
compiled execution behind an owned async stream. It admits the temporary input
directory, pins each aligned source row, enforces exactly one output row, releases
scratch before yielding, and expires output leases on advance/close while allowing
independent retention. A for-await boundary propagates closure to the source.
All three real modern LIST fixtures now exercise this operator rather than a
harness-local copy of assembly logic. TypeScript build and those three fixture
tests pass. Dedicated early-close/error ownership tests and public adapter wiring
remain required before this becomes the public nested read path.

### Owned row-stream closure and failure evidence (2026-11-13)

All three real LIST fixtures now exercise early close with independent output
retention, assembly admission failure under a zero-byte destination budget, and
schema/column-count mismatch. Closing or failing the assembly stream returns the
source job to its pre-stream charge, demonstrating cleanup through alignment,
column-row sealing and page iterators. Independently retained assembled output
remains readable after stream closure and parent-scope disposal; final release
returns use to zero. The admission test initially exposed an unadopted retained-row pin when cross-budget
adoption failed. The operator now releases that pin on adoption failure.
This verifies internal
operator ownership with real bytes; cancellation during remote in-flight reads
and public adapter selection are still separate outstanding integration work.

After the pin-release fix, all three expanded fixture tests pass, along with the
TypeScript project build and changed-file lint checks.

### Cross-budget row pins (2026-11-13)

Inspection clarified the previous failure: adopt requires identical budget identity,
so passing a retained owner from another budget rejected before admission. Row
assembly, row alignment and column-row sealing now retain into the destination
budget before adoption, with explicit release if adoption itself fails. This
supports separately bounded stages and makes zero-budget tests actual admission
failures. A new alignment test supplies an 8 KiB source row to a 2 KiB destination,
verifying source closure and zero charges in both budgets after rejection.

### Shared-pool stage transfer and public integration boundary (2026-11-13)

Successful alignment now has a regression test with separate 16 KiB input/output
stage caps under a 64 KiB pool. An 8 KiB source row transfers into the aligned
output, survives source closure and both scope disposals through an independent
output owner, and releases the pool to zero. All six alignment tests pass.

Public integration inspection: `readOwnedParquetVectorBatches` currently chooses
the direct flat-vector reader or `readOwnedParquetColumnBatches`, whose fallback
uses a fresh requested-batch stage. The internal nested row operator must be
wrapped in projection-aware, bounded batch assembly before replacing that branch.
Integration must preserve rowStart/rowEnd offsets, row-group predicate pruning,
residual-predicate metadata, read metrics, abort signals, and caller-owned batch
lifetimes. Returning single rows alone would not establish the desired batching
or performance behavior. No public branch has been switched at this checkpoint.

### Bounded nested vector batching operator (2026-11-13)

Added `readNestedVectorBatches`, grouping owned row vectors through the existing
generic union-concat planner. The requested batch-size directory is admitted
before allocation, source rows are pinned into the destination budget, and concat
routing is budgeted. The stream expires yielded leases on advance/close and closes
its source on early return. Four tests cover sizes 1/2/4, nested values retained
after early close and parent disposal, partial final batches, empty input, exact
row order, and release to zero. TypeScript build passes. This establishes an
internal row-count-bound operator; memory-pressure-driven early flushing, real
fixture batching, and public projection/pruning integration remain outstanding.

### Real fixture batching and pending-batch failures (2026-11-13)

All three modern LIST fixtures now run the full owned page/row/schema pipeline
through batch sizes 1, 2 and 4. Every batch respects the requested maximum and
preserves exact row order and nested values, including the final partial batch.
Each completed run restores the plan-only charge. The seven combined fixture and
initial batching tests pass. Two additional batching tests then pass for a source
failure with a partially accumulated batch and a cross-budget admission failure;
both close the source and leave input/output budgets at zero (six batching tests
total). These remain internal pipeline tests; no end-to-end public speedup or
adaptive memory-pressure flushing is claimed.

Test correction: independent root budgets reject ownership transfer before byte
admission. Failure tests now use capped sibling/child budgets sharing a root so
they exercise actual capacity rejection. The source-failure case likewise uses a
shared root, allowing its first row to be retained before the injected error.

### Full regression checkpoint after nested operators (2026-11-13)

Default suite: 2,269 passed, 363 skipped (253 passing files, nine skipped).
Typecheck passes. Actual workerd suite: 45 passed across 16 files. These runs cover
the shared zero-width dictionary fix and existing public-reader/ownership behavior
alongside the new internal nested operators. They do not establish public nested
operator integration, resolve the previously recorded conformance failures, or
prove whole-isolate 64–128 MiB bounds. External conformance was not rerun at this
checkpoint. Runtime/GPU limit warnings in the Node suite were non-failing.

### Nested row-window operator (2026-11-13)

Added a lease-preserving half-open row-window operator before batch assembly.
It validates safe integer extents before opening the source, consumes no rows for
an empty interval, closes without advancing beyond the requested end, and rejects
truncated input or non-single-row vectors. Five tests cover empty/prefix/middle/
final windows, exact source-advance counts, cleanup, invalid intervals and short
input. Build and changed-file lint pass. This provides the rowStart/rowEnd primitive
for integration; skipped prefixes are still decoded, so page-index skipping is a
separate performance opportunity rather than an implied optimization here.

### Real row-window/batch composition (2026-11-13)

The three modern LIST fixtures now combine compiled streaming, empty/prefix/tail
row windows and size-two vector batching. Exact selected rows match expectations,
and every run restores the plan-only charge. Eight fixture/window tests pass.
Batching now explicitly rejects empty or multirow input vectors, so the requested
batch-size limit is a row bound rather than only a vector-count assumption. Both
malformed input shapes close their source and release to zero; all eight batching
tests pass. This preserves the internal single-row contract pending public wiring.

### Explicit nested top-level projection plans (2026-11-13)

Added `compileNestedProjection` to compile requested root fields in order using
only their provided leaf paths. Selected subtrees still require complete leaf
coverage; unselected siblings do not. Projection directories and descriptors are
admitted atomically. Missing and duplicate field requests reject with rollback.
The real map-schema test verifies selecting a sibling with a reduced source-path
set and source index remapping; both plan tests pass and TypeScript builds.
Empty projection is explicitly unsupported by this operator because row counts
must come from metadata rather than a leaf source. Public handling of empty and
missing columns must retain the existing reader contract during integration.

### Real projected nested streams (2026-11-13)

Each top-level field in the three modern LIST fixtures now executes independently
through a projected schema with only its selected leaf sources, row alignment,
compiled assembly and size-two batching. Result objects contain exactly that
field and preserve all expected values and row counts; disposing each projection
restores the prior charge. Three fixture tests pass. Projection compilation also
passes at its measured exact peak and rejects at peak minus one with zero retained
charges; both schema-plan tests pass. The harness selects sources explicitly;
public selection and remote request avoidance are not yet established by this test.

### ZSTD multi-column struct fixture (2026-11-13)

Added the real Rust nested-struct fixture to the projected streaming matrix,
selecting its six-column `roll_num` struct from 36 top-level fields. Frozen bigint
values were checked with the upstream reader using the configured ZSTD codecs.
The pipeline preserves signed/unsigned INT64 values through independent column
streams, struct assembly, projection, windows and batching; early-close and
failure cleanup checks also pass. All four fixture tests pass. This file's fields
are required, so it does not establish nullable sibling-struct coverage. The first
oracle invocation lacked ZSTD registration and failed; the configured-codec read
succeeded. No fixture failure was suppressed or classified as expected.

### Nullable sibling alignment rejection (2026-11-13)

Added a compiled execution case with independently nullable sibling leaves under
an optional struct. Absent parent, left-null/right-present, left-present/right-null,
and both-present rows assemble correctly. Changing one sibling's parent validity
while preserving its present-value count causes alignment rejection, rolls back
all attempted output charges, and leaves previously assembled output readable.
All three executor tests pass. This is deliberately an adversarial level-stream
test; the real Rust struct fixture covers required siblings separately.

### Projected row-group orchestration (2026-11-13)

Added `readNestedProjectedBatches` as the shared internal row-group entry point:
selected leaf streams, bounded alignment, projected schema execution, row windows,
and batch assembly now share one lifetime scope. Selected-path/source directories
are admitted and schema/metadata remain caller-owned. All four real fixture tests
exercise the wrapper and return to their prior charge; TypeScript builds. Public
adapter capability selection, metrics/pruning and Batch conversion remain separate
integration requirements. Runtime generator bookkeeping is not fully heap-accounted.

### Public Batch conversion finding (2026-11-13)

Inspection found a concrete integration boundary: nested vector batching currently
uses union routing over complete row structs. `batchFromVectors` accepts a column
directory, not a union-of-structs root. Core's existing `concatBatches` already
supports matching struct/list/map layouts through `structConcatInputs` and
`planConcat`, while `selectStructField` handles dictionary selection but not union
roots. Therefore public conversion cannot safely assume every assembled batch is
a direct struct or materialize JavaScript rows to recover fields. The integration
needs column-wise batching or a generic union-aware field projection with admitted
routing and propagated validity. Reusing existing concat capabilities must be
measured against leaf-payload copying and retained page lifetimes; switching the
public branch before resolving this would hide an allocation/performance change.
Relevant sources inspected: core batch.ts, vector-selection.ts, union-concat.ts,
struct-concat.ts, concat-plan.ts and vector-sort.ts.

### Generic struct routing concat (2026-11-13)

Added core `concatStructRouting` for matching struct layouts. Each output field
uses existing bounded-fanout union routing into borrowed field vectors; parent
validity is concatenated separately. Directories, validity and routing are admitted
atomically. Nested batching now preserves a struct root when its rows are structs,
resolving the public column-directory shape boundary without materializing rows or
copying leaf payloads. Other vector shapes retain generic union concatenation.

Thirteen focused tests pass across the core operator, nested batching and four
real fixture pipelines; TypeScript builds. Core coverage verifies parent nulls,
field payload identity, exact tracked peak success and peak-minus-one rollback.
Public adapter conversion/selection is still unwired; CPU/scaling measurements
and broader malformed-layout cases remain required for this new core operator.

### Struct routing shape and fanout validation (2026-11-13)

The generic struct routing operator now has tests for 513 inputs, verifying every
result value and that all routing nodes stay within 256 children. A field named
`__proto__` remains an own field in a null-prototype directory. Mismatched field
sets, child lengths, parent-validity lengths and negative row extents reject before
output allocation, demonstrated with a zero-byte budget. All three core operator
tests pass; changed-test lint passes after avoiding deprecated accessor syntax in
the prototype-name assertion. This checks routing shape, not a CPU speedup.

### Struct routing measurement: small dense payload tradeoff (2026-11-13)

Added reproducible `bench/struct-routing.mjs` and artifact
`docs/benchmarks/struct-routing-2026-11-13.json`: prebuilt one-row INT32 structs,
128/512/4096 rows, one/eight fields, four warmups and twelve alternating samples.
Every output cell is verified outside timing and every scope releases to zero.
At 4096 rows/eight fields, routing median is 3.779 ms versus copying 3.460 ms;
tracked retained/peak routing charges are 941,952/1,007,552 bytes versus copying
131,072/131,072 bytes. At 128 rows/one field medians are 0.073 versus 0.033 ms.
The 512-row/eight-field timing is close (0.638 versus 0.657 ms); no universal
speedup is established.

Input ownership/backing is excluded. The older concat path's descriptor accounting
is less complete, so tracked totals are not equal-coverage total-heap estimates.
Nevertheless routing metadata alone is expensive relative to tiny INT32 payloads.
A smart public policy must consider physical shape and payload size; routing is
not justified as a universal default. Large variable/nested payload comparisons,
accounting parity, retained source-page costs and end-to-end timings remain needed.
The current internal routing path prioritizes borrowed leaf semantics, not a
measured throughput win; it has not replaced the public reader.

### Struct routing measurement: variable and nested payloads (2026-11-13)

Added `bench/struct-routing-payloads.mjs` and
`docs/benchmarks/struct-routing-payloads-2026-11-13.json`, with the same four-warmup,
twelve-alternating-sample protocol. All payload elements are verified outside the
timed region, and scopes release to zero. At 512 one-field rows:

| Payload per row | Routing median ms | Copying median ms | Routing tracked peak | Copying tracked peak |
| --- | ---: | ---: | ---: | ---: |
| 32 binary bytes | 0.112 | 0.122 | 23,968 | 20,480 |
| 4096 binary bytes | 0.187 | 1.112 | 23,968 | 2,101,248 |
| 32 INT32 list elements | 0.122 | 0.264 | 23,968 | 67,588 |
| 4096 INT32 list elements | 0.267 | 3.857 | 23,968 | 8,390,660 |

This supports a shape/size-aware choice: routing avoids substantial copy work for
large nested/packed payloads, while dense scalar results favor copying in the
previous experiment. Inputs stay alive in both benchmark arms and their backing
is excluded from reported charges. In production copying may permit earlier input
page release; routing pins those pages. These figures therefore do not establish
end-to-end retained memory or cloud-query gains. The older copy path also retains
its descriptor-accounting caveat. No fixed crossover threshold is inferred from
four local cases.

### Adaptive concat planning dependency (2026-11-13)

Inspected `planSelectedConcat` as a candidate for automatic dense-field copying.
Its advertised bytes cover typed backing/validity, while selection descriptors,
plan objects and several output descriptors are not included. Using that number
against fully charged union-routing bytes would bias the decision. It also creates
recursive selection descriptors for dictionary chains and nested children. The
current row batch owns whole source-row leases, so copying just one field would
not by itself free that field's page backing while other fields remain borrowed.

Consequently an automatic choice needs comparable admitted output/workspace costs
and field-granular ownership (or a conservative whole-batch retention model).
A bare `selected.bytes < routing.bytes` branch is not an accurate measured-memory
policy. Existing benchmark results remain valid as scoped implementation timings,
with the documented accounting and source-retention exclusions. Next integration
work must preserve those limits rather than report copied fields as freed inputs.

### Retire schema-index workspace before output transfer (2026-11-13)

Compiled schema execution now scopes node event offsets and index descriptors to
one node's assembly. Only parent validity buffers needed by output containers are
pinned into the output construction scope; entry-node validity stays scratch.
`indexNestedNode` accepts an optional same-budget validity owner to retain that
buffer at its original allocation boundary. This avoids retaining assembly-only
indices for the full output lifetime. An attempted post-transfer exact-allocation
lookup correctly failed (composite ownership is not an exact allocation registry);
the final implementation pins at allocation time instead.

A regression verifies validity remains readable after index workspace disposal,
tracked use decreases, and final output disposal returns use to zero. Build and
changed-file lint pass. Real fixtures and deep/nullable execution checks pass;
this is a lifetime reduction, not yet a measured end-to-end speed improvement.

### Borrowed struct-to-Batch conversion (2026-11-13)

Added generic core `batchFromStructVector`: an all-present struct exposes its
existing field directory as Batch columns with an admitted descriptor, preserving
explicit row counts even for zero-column structs. It rejects parent nulls,
malformed validity lengths, mismatched field lengths and non-struct roots before
allocation; nullable parent semantics must never be discarded by conversion.
Two core tests and all four real projected fixture tests pass through conversion;
TypeScript builds. This supplies the public representation boundary without row
materialization, but the adapter's capability/default selection remains unwired.

### Shared nested physical capability predicate (2026-11-13)

Extracted `supportsNestedValueEncoding` and made page decoding use it, so future
preflight selection can share the physical type/encoding contract rather than
copy a divergent allowlist. A matrix covers all eight physical types, plain,
dictionary, delta, byte-stream-split and unsupported value RLE/bit-packed cases.
Dictionary BOOLEAN is declined consistently with the dictionary decoder. Dictionary
availability, level encodings, compression and logical conversions remain separate
requirements; metadata RLE entries can describe levels and are not alone evidence
of an unsupported value encoding. This predicate is not a complete public-reader
capability check. Build and focused physical/fixture tests pass.

### Shared packed logical capability (2026-11-13)

Extracted `nestedPackedCapability` from the leaf decoder and made decoding consume
that shared classification: binary view, text view, logical conversion, or decline.
This preserves existing precedence and unsupported BSON/INTERVAL behavior while
making future preflight use the same decision. Tests cover raw/UTF8/STRING,
JSON/decimal/FLOAT16, unsupported annotations and conversion precedence. Build and
focused logical/real-fixture tests pass. This is representation capability only;
physical widths, decimal precision, temporal parameters and malformed annotation
validation still belong to decoding, and complete preflight remains outstanding.

### Column metadata preflight (2026-11-13)

Added `canReadNestedColumn` and applied it to every selected column before the
projected reader opens any streams. It shares value-encoding/packed-logical checks,
verifies decoder/metadata type and codec agreement, declines unregistered codecs,
empty or level-only encoding sets, and unsupported value encodings. RLE metadata
is allowed as a level encoding; actual page headers still enforce decoding rules.
The initial fixture run exposed BIT_PACKED metadata on a supported dictionary
column. Like RLE, this may describe levels, so preflight allows it as an ancillary
entry while actual page headers remain authoritative.

This is not a proof against inaccurate metadata: unsupported encodings omitted
from metadata, malformed headers, and logical parameter errors remain fatal during
streaming. The public reader must never catch those after yielding and restart a
fallback stream. Schema projection compilation remains an additional preflight.

After that metadata distinction, all six preflight/real-fixture tests pass.

### Projection-aware no-I/O preflight regression (2026-11-13)

Across all four real fixture pipelines, replacing a selected column's codec with
unregistered BROTLI now verifies rejection before a trap owned-read callback is
invoked. No attempted reads occur and tracked use returns to its prior baseline.
Adding an unsupported column outside the requested projection leaves valid output
unchanged, demonstrating preflight is restricted to selected sources. All four
expanded fixture tests pass. The mutated metadata cases test selection ordering;
they do not claim Brotli decoding support or a real Brotli network transfer.

### Single-struct concat identity (2026-11-13)

`concatStructRouting` now returns a single validated input by identity, avoiding
an unnecessary field directory, parent-validity copy and struct descriptor for
one-row/final-singleton nested batches. The no-op succeeds under a zero-byte
output budget while malformed input still rejects before return. Source ownership
remains the caller's responsibility, already pinned by the batching operator.
Core identity and real fixture/batching tests pass; TypeScript builds. This is a
deterministic allocation elimination, with no separate timing claim.

### Public ownership and offset bridge (2026-11-13)

Added `readNestedBatchOutputs`, converting internal expiring vector leases into
caller-released `ParquetColumnBatch` owners. Batch/offset/predicate descriptors are
admitted; field vectors and source backing remain pinned without row materialization.
The bridge advances row offsets by actual emitted batch length and preserves the
supplied residual-predicate flag. A regression retains two outputs across iterator
closure and parent disposal, checks offsets 7/9 and values, then releases to zero.
Build and the bridge test pass. Adapter selection, pruning/metrics and decoded-cache
policy still require integration; this bridge does not itself choose a default path.

### Public batch bridge rejection paths (2026-11-13)

The ownership bridge now validates nonnegative safe row offsets and rejects offset
addition overflow before yielding. Added rejection tests for zero-capacity shared-
pool destinations, invalid non-struct output shape, and overflow at MAX_SAFE_INTEGER.
Each closes the input stream and returns the shared pool to zero. All four bridge
tests and TypeScript build pass. These validate the conversion boundary, not yet
adapter selection or cache compatibility.

### Legacy LIST and omitted MAP values expose empty dictionary framing (2026-11-13)

Added old_list_structure.parquet and map_no_value.parquet to the complete projected
fixture matrix. The legacy two-level nested LIST matches its fixture documentation.
The MAP fixture exposed zero-index dictionary streams containing one width byte;
readDictionaryIndices now consumes an optional valid width for count zero and
rejects additional payload or invalid width. Strict page-consumption checks stay
intact. Direct edge tests cover omitted width, widths 0/32, invalid 33 and extra
bytes. Build and dictionary/fixture suites pass.

Compatibility distinction: the omitted-value MAP is assembled as keys mapped to
null, consistent with the schema/layout contract. The upstream fallback currently
returns a key_value wrapper for that field. This intentional semantic difference
must be documented and independently checked at public integration; it is not
silently normalized into upstream output. The public default remains unchanged.

### Independent legacy oracle and shared decoder regression (2026-11-13)

The installed DuckDB Node API confirms old_list_structure.parquet as [[1,2],[3,4]].
It rejects map_no_value.parquet with "MAP_KEY_VALUE requires two children", so it
cannot serve as an output oracle for that fixture. Exact engine version and results
are recorded in docs/benchmarks/nested-legacy-oracle-2026-11-13.json. Python DuckDB
was unavailable; the existing repository Node dependency supplied the check.

The Apache Parquet logical-type specification explicitly permits an omitted value
field and permits representing it as an all-null-valued map or a set of keys:
https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
The new reader chooses the map representation consistent with core Map vectors.
This is specification-backed behavior, not a claimed DuckDB-equivalence result.
Shared dictionary, flat vector and nested decoder regressions: 84 passed, eight
skipped across four passing files and one skipped file.

### In-flight cancellation through projected orchestration (2026-11-13)

All six fixture schemas now test aborting the composed reader while its first
owned range requests are pending. A controlled transport waits for one/two active
requests (according to selected column count), aborts the parent scope, and rejects
on each read scope's signal. The bounded advance layer drains those requests,
starts no additional columns, and returns tracked use to the pre-operation baseline.
The first run exposed unhandled rejections when synchronous abort occurred inside
the allocation callback that started concurrent work. Admission now completes
before async startup, ensuring its promise is always awaited. This verifies cancellation propagation and draining
with a deterministic transport; actual network abort behavior remains covered by
separate transport tests and still needs composed-reader HTTP integration.

After separating admission/startup, all twelve fixture/alignment tests pass with
no unhandled errors; TypeScript builds.

### Cancellation admission retained until drain (2026-11-13)

The analogous-pattern audit found a stronger existing core contract:
`MemoryScope.allocateAsync` admits before startup and holds its reservation until
the promise settles, even after scope disposal. Row alignment now uses it instead
of a synchronous marker followed by await. The marker fix prevented orphaned
promises but could release workspace charges while canceled work was still live.

A delayed-settlement regression confirms charges remain positive after parent
scope disposal, source closure waits for settlement, and final cleanup returns to
zero. All thirteen alignment/fixture tests pass without unhandled rejections;
TypeScript builds. Focused searches found no additional production synchronous
allocate callbacks directly starting mapConcurrent/Promise/fetch/iterator-next work;
this is a targeted pattern audit, not a whole-program proof.

### Composed projected reader HTTP cancellation (2026-11-13)

Extended both actual HTTP fixture modes (proper 206 ranges and ignored-range 200)
to abort the projected reader when the server receives its first cold-cache range
request. The shared-cache/owned-transport/alignment pipeline rejects with abort,
then scope cleanup and cache eviction return the shared pool to zero. Both tests
pass with no unhandled errors. Existing cold/warm correctness, retained output and
two-read concurrency assertions remain. The abort is before response delivery;
mid-body interruption and shared-read waiter isolation require their separate
transport/cache regressions and are not newly proven by this case.

### Nested row-group adapter checkpoint (2026-11-13)

Added `readNestedRowGroupBatches` to compose the projected pipeline with requested
row intervals, row-group pruning, read metrics, residual-predicate metadata and
caller-released public batch owners. Selected decoder descriptors and lookup-path
arrays are admitted; output offsets advance across row groups. All six real fixture
cases now run through the adapter and verify each public column value, output
offset and cleanup baseline. TypeScript builds and all six tests pass.

This adapter is not yet selected by `readOwnedParquetVectorBatches`. The existing
schemaPathIndex tree and default-name discovery still allocate outside this new
stage's explicit ledger, and decoded-column-cache integration remains pending.
Multi-row-group/pruning metrics and boundary tests are also required before default
selection. Current fixtures validate single-row-group operation only.

### Multi-row-group boundaries and pruning (2026-11-13)

Added a serialized Parquet fixture with three two-row groups, a scalar id and an
optional struct containing an optional score. Reading [1,5) preserves global batch
offsets [1,2,4], requested projection order and null semantics. With id >= 2
row-group pruning, the leading group is skipped and offsets are [2,4]; emitted
groups carry a true residual-predicate-satisfied flag. Both tests pass and stage
charges return to zero before parent disposal. This verifies real serialized
multi-group operation; detailed metric counts and decoded-cache reuse still need
separate integration coverage.

### Row-group metrics and partial predicate proof (2026-11-13)

Expanded the serialized three-group adapter test to check exact rowGroupsRead,
rowGroupsSkipped, rowsDecoded and columnsRead counts. A predicate cutting through
a group's min/max interval emits that group's rows with residualPredicateSatisfied
false; a later fully matching group reports true. The adapter leaves rowsMatched
unchanged because residual filtering belongs downstream. All three cases pass.
rowsDecoded here follows the existing requested-output count convention; it does
not measure physical prefix values visited while seeking a row window.

### Eliminate repeated lazy schema-path construction (2026-11-13)

Schema compilation previously accessed node.path repeatedly and leaf.path inside
each prefix comparison. With schemaPathIndex's lazy getters, those accesses built
repeated ancestor arrays. Compilation now admits one temporary node-path snapshot
per planned node and compares against already supplied schema-path element names,
then releases the snapshot immediately. Root parent lookup reads its path once.
The depth-2,000 test now uses allocating path getters and asserts at most depth+3
getter calls during compilation. Build and eleven plan/fixture/multi-group checks
pass; the updated two plan tests also pass.

This removes repeated lazy-array construction, not all depth-dependent CPU work:
ancestor-name comparisons still scale with depth, and the initial root path plus
schemaPathIndex's tree remain outside complete explicit accounting. No elapsed-time
speedup is inferred solely from the getter-count regression.

### Compile child sources from existing ancestor paths (2026-11-13)

Follow-up removes the per-node path snapshot entirely: child compilation compares
candidate sources to the parent's existing schema-path ancestors and the child's
final name. LIST/MAP wrapper levels use the same ancestor-prefix rule. Only the
initial requested root path is materialized. The depth-2,000 allocating-getter
regression now requires exactly one getter call; twelve plan/executor/real-fixture
tests pass and TypeScript builds. This supersedes the preceding per-node snapshot
approach while retaining iterative traversal and atomic output admission.
Ancestor comparisons remain depth-dependent, so no linear-CPU claim is made.

### Admit nested schema-index construction (2026-11-13)

`schemaPathIndex` now accepts an optional memory scope. Scoped construction admits
linear tree/directory storage and a worst-case traversal stack before building;
workspace is released on return, and malformed schemas or denied admission roll
back all construction charges. The nested row-group adapter uses a planning scope
for this index, the root lookup and default projection names, released on normal
completion, failure or iterator closure. Decoder lookup paths remain charged to
their existing row-group scopes rather than double charged by the index.

All thirteen schema-index, real nested-fixture and multi-row-group tests pass, and
TypeScript builds. New boundary coverage verifies exact-peak success, one-byte-under
failure, malformed-tree rollback and workspace release. These are explicit
reservation units, not measured VM Map/closure heap sizes. Lookup results and lazy
path-name arrays remain caller-owned; unscoped consumers retain their existing
behavior. Full runtime heap accounting, decoded-cache integration and default
public-reader selection remain incomplete.

### Nested decoded-page cache integration (2026-11-13)

The nested row-group/projected/page pipeline now passes decoded-cache context and
reuses owned dictionary vectors and decoded nested pages. Cache entries use a
separate nested-page namespace including object identity key, byte length, ETag,
physical column path, page extent and preceding definition state. Cached owners
are admitted into each requesting scope through the existing cache contract;
cache admission retains the existing policy and shared-pool bounds. Page headers
are still read and stream extents/event totals checked on warm reads. This does
not imply that a decoded-cache hit alone eliminates all transport operations.

A real Snappy nested-list fixture proves cold/warm equivalence, actual decoded
page hits, ETag invalidation, and outputs remaining readable after their job scopes
close and cache entries are evicted. Releasing all outputs returns the pool to
zero. Ten cache/real-fixture/row-group tests pass; TypeScript builds. Both actual
HTTP fixture modes (206 ranges and ignored-range 200) also pass with decoded-cache
hits asserted, warm range-cache reads remaining zero, retained output checks and
cold-cache cancellation. Public reader selection and adaptive concatenation remain
unfinished. Cache-key strings and runtime object overhead are not yet completely
represented in the explicit allocation ledger.

### Avoid replacement dictionary retention on cached pages (2026-11-13)

A decoded nested-page cache hit already owns its original dictionary backing.
Previously, if the standalone dictionary entry had been evicted, the reader also
attached the newly decoded replacement dictionary to that cached page's output.
The reader now pins the current dictionary only when decoding a page on a miss.
The dictionary remains available in the column lifetime for subsequent page misses.

A real Snappy dictionary-column regression evicts only the dictionary cache entry,
verifies a dictionary miss and page hit, and checks that the retained warm-page
charge does not grow when the dictionary is replaced. Its output remains readable
after all cache entries are evicted; final release returns pool use to zero.
Both decoded-cache tests pass, TypeScript builds, and focused lint passes.

Broader checkpoint: `pnpm test` passes 2,303 tests with 363 skipped (262 passing
files, 9 skipped); `pnpm test:workerd` passes all 45 tests in 16 files. Existing
Worker tests do not yet prove the complete new nested row-group/cache pipeline
under concurrent Worker jobs. External conformance, performance crossover policy,
and the remaining audit requirements are not established by this checkpoint.

### Complete nested reader in workerd with R2 and shared cache (2026-11-13)

Added workerd integration coverage for the complete nested row-group adapter,
including owned R2 ranges, Snappy dictionaries/levels, schema assembly, two-row
batch output and decoded-page cache reuse. The fixture is the complete upstream
nested_lists.snappy.parquet file, embedded with its SHA-256 for Worker loading;
the Node fixture test verifies embedded bytes equal the source file exactly.

Each 64/128 MiB accounting-pool case reserves all but 1 MiB for other admitted
work, runs four concurrent jobs with 128 KiB per-job caps for two rounds, and
checks every output value and global offset. All warm page lookups hit the cache.
Outputs remain readable after scopes close and cache eviction; releasing their
owners returns every child budget and the pool to zero. Both Worker cases pass,
as do the two Node decoded-cache tests and repository typecheck.

This uses the local workerd/Miniflare R2 binding, not production R2 latency. The
occupied capacity is a reservation, not a large live heap allocation. It verifies
shared-budget pressure and Worker operation on the real fixture, not a measured
whole-isolate heap cap or large-data throughput. Public default-reader selection,
adaptive concatenation and the rest of the audit remain open.

### Validate nested windows before planning (2026-11-13)

Nested row-window validation is now shared by row selection, projected reads and
the row-group adapter, and runs before schema allocation or transport. Invalid
negative/fractional/nonfinite/reversed requests can no longer be hidden by
row-group clipping. Concurrency settings are checked even on empty reads. Empty
intervals return without planning allocations; a start beyond EOF with an omitted
end remains an empty result. Explicit ranges extending past EOF retain existing
row-group clipping semantics.

A zero-budget transport-trap test verifies validation errors precede allocation,
empty intervals have zero tracked peak, and neither case performs I/O. Fifteen
window, multi-row-group and real-fixture tests pass; TypeScript builds. This
closes an adapter contract gap but does not complete public-reader selection or
the remaining memory/performance work.

### Scoped nested capability preflight and public-integration findings (2026-11-13)

Extracted selected-column descriptor/path construction into `nestedColumnPlan`,
shared by the row-group adapter and a new scoped metadata/layout capability probe.
The probe compiles selected schema layouts before any transport, declines only
unsupported capabilities, and propagates malformed-schema/admission failures.
Tests cover supported projection, empty/missing fields, selected versus unselected
unsupported codecs, zero-budget rejection and malformed-schema rollback.

An attempted default public selection exposed unresolved behavior, so the public
selection change was reverted rather than normalizing failing expectations:

- The existing vector capability predicate includes LIST/MAP paths backed by its
  old nested fallback, so checking it first bypasses the new reader. Routing must
  distinguish actual flat decoding from nested representation capability.
- A retained-owner test measured total live iterator workspace as output ownership.
  New schema-stage workspace legitimately disappears on close; integration needs
  an owner-specific lifetime assertion, not a relaxed leak check.
- The Spark INT96 fixture has a real oracle discrepancy. Its source document
  `fixtures/external/parquet-testing/data/int96_from_spark.md` specifies the last
  timestamp as 9089380393200000000 microseconds. The raw final dictionary record
  has signed low word -32509551616000 and signed Julian-day word -105862232.
  The existing upstream reader sign-extends its low word into the day word,
  producing -192420178035842064384 nanoseconds. The new decoder preserves the
  high word, but its direct Julian-day conversion still disagrees with the
  documented Spark timestamp. A generic, justified overflow/compatibility contract
  and independent fixture oracle are needed before changing public defaults.

The attempted integration full-suite run had two failures (ownership accounting
expectation and timestamp equivalence); the added public fixture route also exposed
legacy omitted-value MAP representation. Those public routing edits were reverted.
The shared planning/preflight implementation remains; public-reader integration is
still incomplete, and the timestamp discrepancy is not classified as a passing
compatibility result.

### Spark INT96 overflow oracle resolved (2026-11-13)

`bench/int96-spark-oracle.mjs` now reproduces every dictionary record in the real
Spark fixture from its documented microsecond values, verifies the physical words
byte-for-byte, and reconstructs those values. It implements the arithmetic in
Spark v3.4.3 `DateTimeUtils.toJulianDay/fromJulianDay`: signed 64-bit microsecond
addition/multiplication wraps, and the nanosecond remainder is signed. The extreme
value produces Julian day -105862232 and nanos -32509551616000; wrapping the inverse
microsecond calculation restores 9089380393200000000 exactly. This is a source-based
arithmetic reproduction, not execution of a Spark JVM.

Evidence is stored with the fixture hash and DuckDB version in
`docs/benchmarks/int96-spark-oracle-2026-11-13.json`. DuckDB's epoch_us result for the
extreme row is -7207070780300583936, so it is not an agreeing independent oracle.
All other documented dictionary records agree with the Spark arithmetic too.

Source: https://raw.githubusercontent.com/apache/spark/v3.4.3/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/DateTimeUtils.scala

Implementation must distinguish a writer's microsecond-wrap compatibility contract
from ordinary INT96 nanosecond decoding; applying wrapping globally would silently
truncate other writers' valid extended-range or sub-microsecond timestamps. Next
work is metadata-driven decoder context, explicit override semantics where writer
identity is absent, and real-fixture/boundary tests. Public selection remains
unchanged pending that implementation.

### Metadata-driven INT96 interpretation in nested reads (2026-11-13)

The internal nested reader now resolves INT96 interpretation from writer metadata:
`org.apache.spark.version` selects Spark's signed microsecond-wrap contract;
otherwise decoding keeps the existing Julian-day/unsigned-nanosecond contract.
Internal overrides (`auto`, `julian-nanos`, `spark-micros`) support absent writer
identity. This option is not yet advertised on public scan APIs; all-path public
plumbing and default reader selection remain pending.

Spark conversion reproduces signed day subtraction and 64-bit microsecond overflow,
then returns exact nanoseconds. It rejects sub-microsecond physical values under
that explicitly microsecond contract; ordinary INT96 preserves them. Decoder
metadata charges include the interpretation field, conversion scratch is admitted,
and nested cache keys include interpretation so one mode cannot reuse the other's
converted pages/dictionaries.

The real Spark fixture matches all six documented rows, including the year-290000
value. Tests alternate auto/ordinary/Spark modes through one shared cache and
verify cleanup. Boundary round trips cover signed 64-bit minimum/maximum,
pre-epoch values and ordinary sub-microsecond precision. Seventeen targeted tests
pass and TypeScript builds. An additional temporal admission regression verifies
exact-peak success and one-byte-under rejection before scalar parsing.

### Public owned-reader nested selection enabled (2026-11-13)

The public owned reader now asks the existing vector capability probe for flat
shapes explicitly. Non-flat or otherwise unsupported flat projections undergo
scoped nested schema/physical-column preflight, then use the nested row-group
pipeline when supported. Unsupported capabilities retain the existing fallback;
malformed data and admission failures are not swallowed or retried after output.
The outer reader still supplies indexed-group iteration, global offsets, signals,
and bounded prefetch behavior.

All six nested fixture cases now verify both the internal adapter and public owned
entry point. This exposes the documented omitted-value MAP representation as maps
with null values rather than an incidental physical key_value struct. The owned
Spark INT96 test now uses the fixture's documented microseconds for its independent
expected values; borrowed upstream decoding still has its known extreme-value
mismatch. Earlier-output lifetime checks compare retained owner bytes, separating
output backing from temporary schema workspace that correctly disappears on close.
Corrupt level data is rejected explicitly by the bounded level decoder before the
old upstream bounds exception; the malformed-fixture test asserts that exact error
and unchanged zero-leak invariant.

The concurrent Worker/R2 nested-cache cases now use the public owned entry point.
`pnpm test` passes 2,309 tests (363 skipped) across 264 passing files (9 skipped).
`pnpm test:workerd` passes 47 tests in 17 files; TypeScript builds. This validates
integration correctness/lifetimes, not throughput: automatic concat crossover,
large-workload benchmarking, full-path INT96 override consistency, external
conformance and other audit items remain incomplete.

### Integrated nested-reader benchmark exposes per-row overhead (2026-11-13)

`bench/nested-reader.mjs` serializes nullable structs with optional integer fields
and required UTF8 payloads, compares the public owned nested path with the legacy
owned column reader, and verifies every value/offset outside timed advances. It
runs memory storage and an actual local HTTP range server with 2 ms response delay,
without caches. Two warmups precede four alternating measured samples. Failures
are recorded, never counted as fast samples; all success/error runs check cleanup.
Results: `docs/benchmarks/nested-reader-2026-11-13.json`.

This is a material regression, not a performance success. At 1,024 rows with
16-byte text and batch size 128, median memory-source time is 196.91 ms nested
versus 3.67 ms legacy. HTTP medians are 212.36 versus 45.78 ms, despite nested
reducing range reads from 18 to 8 and bytes from 65,211 to 9,048. At 128 rows the
nested path remains much slower too. The nested tracked peak is 778,378 bytes for
the 1,024-row small-payload case versus 33,671 legacy; these ledgers have different
coverage, so this is not a measured heap comparison.

For 1,024 rows with 4,096-byte payloads, the nested path verifies every result
(memory median 197.74 ms; HTTP 235.93 ms). Legacy decoding fails with `parquet struct
parsing error` in every measured run, so there is no valid relative speed claim.
The failure is preserved in the artifact rather than omitted from the matrix.

The evidence changes the priority: amortize level joining, schema execution and
ownership across bounded row groups within each requested batch, rather than
constructing and then concatenating a complete schema vector for each row.
Per-row scope/frame/vector churn currently overwhelms the saved payload copying
and network work. Preserve page-boundary continuation, unequal column page shapes,
exact requested intervals, admission and early-close ownership in that redesign.
Automatic concat choice alone cannot resolve this much upstream per-row overhead.
Public integration correctness tests pass, but performance acceptance is explicitly
not achieved; the new default remains work in progress pending this redesign.

### Execute nested schema once per bounded batch (2026-11-13)

The projected reader now groups aligned physical rows up to the requested batch
size before joining levels and executing the compiled schema. It no longer builds
a complete nested vector for every row and then concatenates those vectors.
`joinNestedFragments` validates complete row counts and coalesces adjacent spans
of one page into a single shared-payload route. Requested windows are applied
before assembly; input leases, directories, level buffers and output remain
admitted and independently retained through the existing ownership contract.

Serialized continuation tests cover a row crossing an empty page, a following row
on the same page, equal charges for split/coalesced spans, exact-peak success,
one-byte-under rollback, wrong row counts and an invalid initial continuation.
The existing real-fixture matrix exercises batch sizes, projection, row windows,
null/empty containers and cache cleanup through the updated reader.

The saved per-row baseline is now
`docs/benchmarks/nested-reader-per-row-2026-11-13.json`; the updated report remains
`docs/benchmarks/nested-reader-2026-11-13.json`. On 1,024 small-payload rows, local
median decreases from 196.91 to 42.77 ms; HTTP median from 212.36 to 60.34 ms.
Tracked peak falls from 778,378 to 310,890 bytes. All outputs still verify, including
the large-payload case where legacy decoding fails. These are successive local
runs, not a controlled production throughput claim. Legacy small-payload decoding
is still faster: approximately 3.75 ms local and 44.37 ms HTTP in the new run.

Full validation passes 2,309 tests (363 skipped), and all 47 Worker tests pass;
TypeScript builds. Remaining per-row column sealing/alignment and owner metadata
still dominate enough CPU to require further batch-level work. This is a measured
improvement, not completion of performance acceptance or adaptive budget sizing.

### Seal and align bounded nested row blocks (2026-11-13)

Column sealing now accepts a maximum row count and requested-window boundaries.
It coalesces adjacent segments in each page into one pinned fragment and emits
complete blocks, retaining row-spanning continuations across empty pages. Alignment
validates each block's start/count across columns and advances once per block.
Schema assembly accepts these blocks, validates requested batch/window boundaries,
and preserves the single-row compatibility contract for existing consumers.

Tests add partial final blocks, cross-page continuation with same-page coalescing,
and mismatched block counts with complete source cleanup. Existing real fixtures
exercise nonzero window starts, projection and batch boundaries. Full validation:
2,311 passing tests (363 skipped), all 47 Worker tests, build and typecheck pass.

Saved the previous stage as
`docs/benchmarks/nested-reader-batch-schema-2026-11-13.json`. The final current
benchmark was rerun after all validation processes exited to avoid competing test
load. For 1,024 small-payload rows, median nested time is 3.09 ms local and 20.19 ms
HTTP, versus 42.77/60.34 ms in the previous stage. Tracked peak falls from 310,890
to 87,562 bytes (initial per-row implementation: 778,378). Current legacy medians
are 8.12 ms local and 45.91 ms HTTP; run-to-run legacy variance means these are
local samples rather than a universal speedup claim.

The 128-row small-payload case is still slower than legacy (1.42 versus 0.54 ms
local; 9.74 versus 5.13 ms HTTP). Large-payload 1,024-row output remains fully
verified while legacy consistently fails its struct assembly. Automatic sizing,
small-read transport overhead, larger scaling/pressure matrices and the remaining
audit requirements still need work.

### Reuse complete bodies already present in header probes (2026-11-13)

Nested page reads now opt into same-scope reuse when the bounded header probe
already contains the complete page body. The retained view/descriptor is admitted
before construction; when that optional charge does not fit, the probe is released
and the existing body-read path remains available. Other page-window consumers
keep the default release-after-inspection behavior. A different execution scope
never borrows this retained view, including after the inspection scope is disposed.
No probe-size increase or whole-chunk speculative allocation was introduced.

Four focused tests verify complete/incomplete probe behavior, insufficient optional
headroom and cross-scope safety. Twelve probe/real-fixture/cache tests and 59 owned
reader/continuation/actual HTTP tests pass; TypeScript builds.

Previous measurements are preserved in
`docs/benchmarks/nested-reader-row-blocks-2026-11-13.json`; the current benchmark
still verifies every value and cleanup. The 1,024-row small-payload HTTP case drops
from eight range requests/9,048 bytes to six/8,835 bytes, with median 16.30 ms versus
20.19 ms previously. Peak tracked allocation remains 87,562 bytes. The 128-row
cases have no complete probe-contained bodies, retain four requests and show no
meaningful improvement. Small-read transport overhead is therefore still open;
this change removes redundant reads only where already fetched bytes suffice.

### Admit batch-directory slots by actual physical blocks (2026-11-13)

After block-level decoding, the schema batch directory still reserved one slot per
requested logical row. It now admits the array header and each actual incoming
block slot before growth. A block containing thousands of rows needs one directory
entry; batches composed of smaller blocks remain bounded by the existing requested
row limit and each slot is still admitted before allocation. This removes excessive
reservation without treating any directory growth as free.

A serialized single-row-group fixture with 8,192 required nested records verifies
every output value within a 256 KiB budget and returns tracked use to zero. A
controlled regression check temporarily restored the prior directory reservation:
the same test failed at that allocation with LAKEQL_BUDGET_EXCEEDED. The change was
then restored and validation rerun. Thirteen row-group/real-fixture/cache tests
pass, focused lint passes, and TypeScript builds. A smaller 128 KiB trial could not
fit the actual decoder allocations; that unsuccessful trial is not claimed as a
supported cap. This improves directory admission, not adaptive sizing of the
payload/levels or the remaining overall memory-accounting requirements.

### Add owned-query corpus conformance and fix unused dictionary offsets (2026-11-13)

The existing conformance suite exercised the borrowed object reader, so its
unchanged 18 failures did not validate the new owned path. Added all 243 corpus
files through the public owned query reader with 128-row batches and a 64 MiB
shared/per-query cap. Each successful scan must match metadata row count; cleanup
and peak-budget assertions run on success and failure. No failures were skipped,
reclassified as expected success, or hidden. This expands operation/ownership
coverage, not independent per-value oracle coverage for every file.

The initial owned matrix exposed 20 failures, including an additional zero-offset
case and the noncompliant optional-key MAP fixture. The zero-offset fixture is
PLAIN-only but stores an unused dictionary_page_offset of zero. Shared dictionary
offset resolution now ignores that sentinel only when no dictionary encoding is
declared, preserving real zero-based dictionary streams. Flat decoding, prefetch
range planning, row-group planning and nested page traversal use the same rule.
A real fixture regression verifies all 39 values through an owned query and final
pool cleanup, plus unit checks for real zero-offset dictionaries.

Build and 63 targeted tests pass. Expanded conformance now reports 456 passed,
37 failed, one skipped: 18 borrowed failures and 19 owned failures. The recorded
failure inventory is `docs/benchmarks/conformance-owned-2026-11-13.json`. The
optional-key MAP compatibility case and previously known unsupported/corrupt
fixtures remain unresolved; these counts are not a claim of conformance success.

### Accept legacy optional-key MAP annotations with value validation (2026-11-13)

The nested schema compiler now accepts OPTIONAL as well as REQUIRED MAP keys,
covering the legacy Presto/Trino/Athena schema documented alongside the upstream
incorrect_map_schema fixture. Repeated keys remain invalid. During execution,
optional key-node validity is checked from admitted level indexes before map
construction; actual null keys fail with LAKEQL_PARQUET_READ_ERROR and rollback.
This does not rewrite definition levels or materialize key strings for validation.

The real fixture verifies its documented {name: report, parent: another} map through
the public owned query path with final pool cleanup. Generated level cases verify
present optional keys and null-key rejection with unchanged pre-operation charges.
Fifteen focused layout/fixture/key tests pass; TypeScript builds. Both borrowed and
owned conformance cases for this fixture now pass. Expanded corpus totals improve
to 457 passed, 36 failed, one skipped (18 failures in each reader path); the machine
inventory is updated without hiding or reclassifying remaining failures.


### Nested decoded-cache inspection lifetime (November 13, 2026)

Nested page inspection now uses a separate child scope. A decoded dictionary or
page cache hit releases its new header-probe storage before retaining output;
a miss transfers the inspected body backing to the decode scope, preserving the
existing complete-probe body reuse and avoiding another cloud range request.
The same ownership boundary applies to dictionary and data pages. Exceptions,
index pages, cancellation and iterator close dispose the attached inspection scope.

The real `nested_lists.snappy.parquet` regression now compares cold and warm page
ownership, then evicts only the dictionary and finally the entire cache while
checking retained values and zero final pool charges. A control mutation that
reattached inspection backing failed: warm output retained 3,956 bytes versus
3,535 for cold output (421 unnecessary bytes). The corrected path retains equal
backing across those runs. This is tracked ownership evidence, not a total-heap
measurement or an assertion that every codec has identical storage behavior.

Validation: 11 targeted tests pass, covering nested pages, decoded cache, row
groups, real HTTP 206 and ignored-range 200 responses; all 47 workerd tests pass,
including concurrent local R2 fixture reads and cache reuse. The earlier broad
refresh passed 2,321 default tests (606 skipped), 104 reference tests, full build,
compatibility and tracked-file size checks. External conformance still has 36
failures; adaptive byte-aware batching, full runtime allocation coverage and final
release gates remain open. No whole-Worker 64–128 MiB heap guarantee is claimed.

### Nested batch budget feasibility sweep (November 13, 2026)

`node bench/nested-reader.mjs --budget-sweep` reuses the timing harness's serialized
optional-struct fixture and every-value oracle, with 8,192 rows, 16-byte string
payloads, batch limits 128/1,024/8,192 and budgets 128/256/512/1,024 KiB. It exercises
both memory storage and a real HTTP range server with 2 ms response delay. The
machine-readable results are in `docs/benchmarks/nested-budget-sweep-2026-11-13.json`.
The sweep fails on unexpected exceptions, incorrect values/offsets/counts, output
batches exceeding the requested limit, leaked reservations, or an over-budget
failure peak. Budget errors are recorded with their allocation purpose. Each
configuration runs once: timing samples are diagnostic, not speed comparisons.

Both transports have the same feasibility boundary on this fixture:

| Requested batch rows | 128 KiB | 256 KiB | 512 KiB | 1,024 KiB |
| --- | --- | --- | --- | --- |
| 128 | row-offset admission fails | Snappy output admission fails | passes | passes |
| 1,024 | row-offset admission fails | Snappy output admission fails | passes | passes |
| 8,192 | row-offset admission fails | Snappy output admission fails | node-validity admission fails | passes |

Local tracked peaks for successful runs are 452,311 bytes at batch 128, 473,181
at batch 1,024 and 567,544 at batch 8,192. All failures clean up their charges.
These are allocation-accounting observations, excluding fixture generation,
metadata, server and untracked runtime heap. They do not establish Worker heap caps.

This proves a real adaptive-batching opportunity: the requested 8,192-row upper
bound could succeed within 512 KiB by emitting smaller batches. It also shows why
reducing row counts alone cannot solve the 128/256 KiB failures: page-level decode
allocations fail before output assembly. The durable implementation must coordinate
one block boundary across all projected columns, account for page and dictionary
backing plus assembly workspace, and preserve the user's row upper bound. Choosing
sizes independently in concurrent column readers would violate the alignment
contract. Retrying an already-advanced stream after yielding rows would risk
repeated rows or inconsistent cloud reads. Neither workaround has been introduced.
This sweep is the concrete acceptance fixture for coordinated adaptive assembly;
page-level memory admission remains a separate open requirement.


### Adaptive nested assembly from admitted fragments (November 13, 2026)

Nested schema assembly now treats the requested batch size as an upper bound.
Each assembly attempt has its own disposable construction scope. On allocation
admission failure, the attempt is discarded and the same already-owned input is
assembled in smaller row windows. The successful smaller limit is remembered for
later blocks in the row group. Other errors propagate unchanged; a one-row failure
propagates instead of looping. No source iterator is rewound, no physical decode
is repeated, and no cloud reads are issued by the retry path.

Row windows use binary searches in each page's existing root-row event/value
indexes, preserving continuations across pages and sharing compact payloads.
Every projected column receives the same window. Newly allocated window metadata,
levels, routes and schema output are admitted, and output ownership pins the
original fragments independently of iterator lifetime. This does not reduce the
page/dictionary storage already required to reach assembly, nor dynamically shrink
the upstream column block. Repeated full fragment-directory visits under extreme
fragmentation still warrant scaling measurements and potential indexing.

The prior budget report is preserved as
`docs/benchmarks/nested-budget-sweep-fixed-2026-11-13.json`; the current report now
passes the requested 8,192-row / 512 KiB case over both transports: five outputs,
largest 1,798 rows, peak admitted 522,248 bytes. HTTP uses eight requests and 67,416
transferred bytes. The public-reader regression compares an ample-budget run and
this constrained run, verifies every value and offset, confirms identical read
counts, and checks cleanup. A separate early-close test retains an adaptive output
and verifies it after closure. Serialized cross-page continuation tests cover
windows containing a row spanning an empty page and the following independent row.

The isolated timing refresh retains the small-payload 1,024-row result (about
3.19 ms local / 16.03 ms HTTP versus 3.15 / 16.31 previously). The 128-row local
median changed from 1.47 to 1.70 ms and HTTP from 10.11 to 9.24 ms; four samples do
not establish the cause or significance of that variation. Small-fixture overhead
remains an open performance concern. Prior timing evidence is preserved in
`nested-reader-before-adaptive-2026-11-13.json`.

Validation: full default suite 2,323 passed / 606 skipped, all 47 workerd tests,
TypeScript build/typecheck and repository lint passed. The new budget sweep also
verifies batch-size limits, every successful value, and zero final charges for
successes and failures. The 128/256 KiB page-decode failures remain; no total-heap
or all-operator memory guarantee is inferred from adaptive nested assembly.


### Indexed adaptive fragment windows (November 13, 2026)

Resolved the repeated full-fragment scans identified in the preceding checkpoint.
On the first partial assembly, each projected column now builds one admitted index
of fragment row ends and page-local starting segments. It also retains one fragment
directory for the block. Later retries and outputs reuse these indexes; the ample-
budget full-batch path does not build them. Index storage is scoped to assembly
and is not attached to emitted output backing. Index construction is atomic, with
all allocations released on admission or validation failure.

Window selection performs a binary search over fragment row ends, then visits only
the selected fragments and at most one following boundary fragment. Equal row-end
entries preserve continuation-only pages. Construction still searches each page's
existing event index once; across many windows the bound is O(F log P + W log F +
selected fragment visits), rather than O(W F log P), for F fragments, P page-local
row boundaries and W windows. This claim concerns fragment selection, not all
schema execution or codec work.

Deterministic tests at 128, 1,024 and 8,192 fragments verify one construction visit
per fragment and at most three total page-index visits per fragment when emitting
one-row windows. Tests also cover seven consecutive continuation-only pages,
invalid windows, orphan continuations, inconsistent value/event boundaries,
exact-budget admission, one-byte-under rejection and zero residual charges. The
serialized cross-page tests remain green. The real public-reader regression proves
no index creation on the ample-budget path and at most one per column per row group
on the constrained path, with unchanged transport read counts and correct values.

The refreshed local/HTTP budget sweep still passes the 512 KiB case in five batches,
with maximum output 1,798 rows, peak charged 522,248 bytes, and eight HTTP requests
transferring 67,416 bytes. The preceding report is preserved as
`nested-budget-sweep-before-index-2026-11-13.json`. These one-run timings are not a
performance comparison. Validation: 2,328 default tests passed (606 skipped), all
47 workerd tests passed, TypeScript build/typecheck and repository lint passed.
Page decoding under tighter budgets, broader allocation coverage, conformance and
final release acceptance remain open.


### Compact nested page shape for schema assembly (November 13, 2026)

The schema-assembly path was building `slots` vectors that it never consumed.
Nullable leaves allocated index and validity arrays to expand compact values into
leaf slots, then schema execution rebuilt the necessary nesting from physical
levels and compact values. This redundant expansion is now an explicit decoder
capability: the projected schema pipeline requests compact pages; direct leaf-slot
consumers retain the expanded-page decoder. Both use the same physical decoding,
transition validation, row indexing and page-stream implementation.

Decoded data-page cache identities distinguish compact and expanded shapes, so a
cache hit cannot substitute an incompatible output. Dictionary cache identities
remain shared because dictionary decoding is independent of slot expansion. The
cache regression alternates both readers cold and warm, verifies their shapes and
values, proves dictionary reuse across shapes, and releases all pool charges.

A serialized nullable-leaf page test verifies compact decoding skips at least five
bytes per slot, admits at its measured exact budget, rejects one byte below it and
rolls back allocations. Existing actual nested-list fixtures, cross-page null and
continuation tests, public adaptive-reader comparisons and Worker/R2 checks pass.

The refreshed 8,192-row sweep at batch 128 reduces peak tracked allocation from
452,311 to 418,917 bytes; batch 1,024 changes from 473,181 to 439,787. At requested
batch 8,192 with a 512 KiB cap, both local and HTTP reads now emit three batches
instead of five, largest 3,596 rows, with peak 501,777 rather than 522,248 bytes.
HTTP still uses eight requests and transfers 67,416 bytes. The 1,024-row small-
payload timing fixture now peaks at 80,281 rather than 85,530 bytes; its median
is 3.32 ms local / 15.49 ms HTTP. These timing samples do not establish a general
speedup. Before-change reports are preserved with the `before-compact` suffix.

The 128 KiB failure remains row-offset admission; 256 KiB remains Snappy page-output
admission (attempted total now 296,520 bytes). Compact shape selection reduces
avoidable decode retention but does not implement streaming decompression or prove
a whole-runtime heap bound. Validation: 2,330 default tests pass (606 skipped),
all 47 workerd tests pass, and TypeScript build/typecheck and lint pass. The cache
shape/dictionary-sharing test was also rerun after its final assertion update.


### Decompression backing follows physical aliases (November 13, 2026)

Nested physical decoding now allocates decompression output in a distinct child
scope. Decoded level arrays and reconstructed values own separate storage; when
neither packed payload nor numeric output aliases the decompressed buffer, that
buffer is released before committing the physical page. Packed payloads and aligned
numeric views retain the decompression owner. Uncompressed input backing remains
caller-owned and does not create an empty retained decompression handle.

Serialized V1/V2 GZIP tests compare live charges against equivalent uncompressed
pages. Reconstructed DELTA_BINARY_PACKED values retain no extra decompression bytes;
aligned PLAIN numeric output retains exactly the decompressed payload/page size.
Packed binary tests likewise retain backing for PLAIN and DELTA_LENGTH_BYTE_ARRAY,
while reconstructed DELTA_BYTE_ARRAY releases it. Values remain correct after
scope closure with an explicit retained owner. Existing exact-budget, one-byte-under,
malformed-page rollback and cleanup checks pass.

The ordinary Snappy optional-struct sweep is unchanged: its borrowed values need
the decompressed backing. The 512 KiB case still peaks at 501,777 bytes with three
outputs and eight HTTP requests; the 256 KiB Snappy-output admission failure remains.
No improvement for that workload is claimed. Its preceding report is preserved as
`nested-budget-sweep-before-decompression-release-2026-11-13.json`. The benefit is
removing unnecessary retained decode storage for reconstructed physical encodings;
peak decompression requirements and streaming-codec work remain separate.

Validation: 2,334 default tests passed (606 skipped), all 47 workerd tests passed,
TypeScript build/typecheck and lint passed. After avoiding the empty handle on
uncompressed reads, the 25 page/adaptive/cache tests and local/HTTP budget sweep
were rerun successfully. The goal remains incomplete: runtime allocation coverage,
page-level admission, conformance and final acceptance work remain open.


### Current verification gate refresh (November 13, 2026)

Refreshed the full build, fixture regeneration, isolated package installation,
reference suite, coverage and external conformance after the recent decoder work.
`pnpm build`, `node scripts/check-fixtures.mjs`, `pnpm check:packages`,
`pnpm test:reference` (104 tests) and `pnpm coverage` all pass. Fixture generation
was byte-for-byte idempotent. Package checks packed the built packages, installed
them in a temporary consumer and exercised runtime/type entrypoints.

Coverage passes unchanged repository thresholds: statements 91.41%, branches
85.20%, functions 95.43%, lines 93.51%. These aggregate numbers do not establish
complete lifecycle coverage. In particular, `core/src/owned-stream.ts` has only
37.5% function and 62% branch coverage, making direct stream ownership, cancellation
and reservation-transfer edge tests a useful next verification target.

Conformance remains 457 passed, 36 failed and one skipped. An exact comparison of
all failure names matches the preceding owned/borrowed inventory, rather than
merely matching the count. No failures were skipped or reclassified to make the
gate green. Malformed-fixture expectations and remaining unsupported feature
contracts still need explicit treatment; the full release gate remains failing.
Machine-readable evidence is recorded in
`docs/benchmarks/performance-audit-gates-2026-11-13.json`.

This refresh closes stale verification evidence, not the overall objective.
Whole-runtime allocation coverage, remaining performance requirements and the
conformance/release boundary remain unproven.

### Direct stream ownership and actual HTTP decoding checks (November 13, 2026)

Added direct tests for bounded unknown-length stream growth, preflight reservation
transfer, shared-pool ownership and cancellation. A nine-byte budget admits a
four-byte input buffer plus an exact five-byte replacement when geometric growth
would require twelve bytes. Tests verify copied contents, peak overlap and zero
final charges. A preflight eight-byte reservation shrinks to a learned three-byte
decoded extent without double charging. Retained trimmed views preserve their full
eight-byte backing charge across source-scope disposal and transfer to another job.

Pending-reader cancellation is synchronized on the actual stream pull rather than
a timeout. Both resolving and rejecting source cancellation preserve the abort
error, release accumulated buffers and unlock the reader. Partial source failures
retain their original error. Decoded-size/preflight validation errors likewise
survive a rejecting cancel callback. Null and empty bodies work with zero capacity.
No production change was needed for these cases.

A real local HTTP server serves the checked-in nested-list Parquet fixture through
identity, gzip and chunked responses. Each transport accepts the exact decoded
byte limit and rejects one byte less; gzip advertises its smaller compressed
Content-Length, proving that header is not trusted as decoded size. Successful
owners retain every fixture byte after scope close, while failures return all
charges and unlock the body. These tests validate Fetch/HTTP behavior separately
from the synthetic stream tests. They do not bound Fetch's own runtime chunk or
transparent-decompression heap, which remains outside engine buffer accounting.


Validation: the full coverage run passes 2,346 tests (606 skipped) and unchanged
repository thresholds: 91.51% statements, 85.27% branches, 95.65% functions and
93.59% lines. Direct stream coverage improves from 64.04% statements / 62% branches /
37.5% functions / 73.23% lines to 96.62% / 92% / 100% / 98.59%. Repository lint passes.
The remaining uncovered defensive partition-result check is not evidence of a
missing normal transfer path. Broader heap coverage and conformance remain open.


### Range guard and obsolete pipeline consolidation (November 13, 2026)

The Parquet store bridge now validates finite safe-integer object sizes and ranges
before forecasts, counters or I/O. Negative, reversed, fractional, nonfinite and
out-of-object ranges produce typed Parquet read errors. Tests use trap adapters to
prove invalid extents issue no store request and allocate nothing. This corrects a
malformed dictionary fixture that previously reached the store with an invalid
range and surfaced as object-not-found.

The conformance harness now tests eight corruptions explicitly documented in the
vendored `bad_data/README.md` as rejection cases. Both public readers must return a
read/validation error; budget or arbitrary errors do not satisfy the assertion.
Owned cleanup is checked. Production metadata loading is exercised instead of
letting the harness's upstream metadata parser fail first. The 235 other files per
reader keep their success expectations, including the valid zero-bit-width case.
The resulting 465/20/1 counts reflect corrected expectations, not twenty-six format
fixes or waived unsupported cases. The preceding inventory is preserved.

A cross-change review found `nested-schema-rows.ts` and `nested-vector-batches.ts`
had no production callers. Their obsolete per-row assembly/concatenation path has
been removed. Six real-fixture suites now exercise `readNestedSchemaBatches`
directly for single-row and multi-row batches, projections, row windows, partial
outputs, source failure, budget failure, column mismatch and retained output after
close. Eight tests specific to the deleted vector-concatenation implementation
were removed; relevant lifecycle assertions are exercised on the production path.
No exported package entrypoint is removed. This eliminates two implementations
rather than introducing another ownership abstraction.


Consolidation verification: the full coverage run passes 2,344 tests (598 skipped)
and all unchanged coverage thresholds (91.52% statements, 85.29% branches, 95.64%
functions, 93.60% lines). All 47 workerd tests, TypeScript build/typecheck and lint
pass. The finite acceptance checklist above preserves the original scope and
makes the remaining review units explicit; none is silently waived by this cleanup.


### F1 review: incremental kernel versus snapshot continuation (November 13, 2026)

Reviewed the original group-by finding against update, restore and merge code.
`GroupMemoryMap` maintains its total on insertion/replacement/deletion/clear;
aggregate updates refresh deltas, and merge refreshes in a finally block even on
failure. Restore measures inserted groups once. A new lifecycle test restores
1,024 groups, repeatedly merges existing/new groups and updates them, and proves
no traversal of the accumulated target map; its final accounting matches the
independent full-scan estimator and exact/cap-minus-one checks.

The wider F1 integration requirement is still open. `query.ts`'s columnar grouped
aggregate continuation passes a full snapshot as `priorOutput`; the CPU physical
`grouped-reduce` path restores it and snapshots the whole state again for every
batch. This is separate from constant-time per-row budget enforcement. It must not
be hidden by calling the kernel optimization complete. Late-materialized and other
owned-state execution paths are not characterized by this measurement.

`bench/grouped-batch-continuation.mjs` measures the actual CPU physical continuation
contract, counts prior-group key reads during restore, and validates every final
group. With 128-row batches, 2,000 / 4,000 / 8,000 input rows produce 15,360 /
63,488 / 249,984 restore visits. With 512-row batches the counts are 3,072 /
14,336 / 61,440. This is quadratic growth in accumulated group revisits for fixed
batch size. Single-run timing is recorded only as diagnostic evidence in
`docs/benchmarks/grouped-batch-continuation-2026-11-13.json`; it is not cloud latency
or total-memory evidence.

The next F1 action is to reconcile in-process continuation with the existing
owned aggregate lifecycle, using serialization at explicit checkpoint/backend
boundaries. Avoid a second special-purpose grouping engine or a query-specific
shortcut. Snapshot compatibility, admission, cancellation and backend selection
must remain covered. The present turn changes tests/evidence only, not execution.


### F1/M1 correction: aggregate ownership is not yet a shared lifecycle (November 13, 2026)

Inspection disproves the premise that general grouped aggregation already has a
pool-admitted persistent state to reuse. `CpuPhysicalBackend` advertises allocation-
scope operators only for selection, projection and top-k. Aggregate queries do not
pass an allocation scope to the snapshot reduction path. Existing owned aggregate
scan tests assert input release, not allocation of growing aggregate state.

`node bench/grouped-state-admission-gap.mjs` reproduces the gap through public
`Lake.path().groupBy().aggregate()` with an owned two-row scanner. Both 128 and
1,024 distinct output groups succeed under a 16-byte pool, whose reported peak
stays 16 bytes. All result values and final reservation cleanup are checked. This
is a counterexample to complete group-state admission, not a memory-safety success.
The machine-readable report is `grouped-state-admission-gap-2026-11-13.json`.

The top-level acceptance description has been corrected. F1 and M1 must be solved
together: persistent reduction state needs explicit ownership/admission and backend
continuation/checkpoint semantics. Merely retaining today's uncharged maps would
fix repeat work while preserving the wrong memory contract. No new execution API
or unsafe state-cache optimization was introduced in this verification turn.


### F1/M1 shared ordinary group-creation boundary (November 13, 2026)

Consolidated the three duplicated group-creation sequences in general batch
updates, encoded/value updates and direct batch-row lookup. They now share the
group-count guard, lazy key-value construction, accumulator creation, insertion
and existing serialized-budget check. Existing groups bypass the constructor.
This removes duplicate mechanisms without changing public state/snapshot formats.
Restore and merge construction remain distinct and must be included in the upcoming
admission contract; this refactor does not claim to fix the 16-byte-pool reproducer.

Verification: 25 focused group/accounting/aggregate tests, all 2,345 default tests
(598 skipped), TypeScript build/typecheck and lint pass. F1/M1 remain open pending
pool-admitted growing aggregate state and reusable physical continuation.


### F1/M1 common buffered-growth preflight (November 13, 2026)

Median, quantile, mode and distinct accumulators now share `bufferedAppendBytes`
for prospective row-count and serialized-byte checks before collection insertion.
The previous paths mutated their array/map/set and then rejected an over-limit
state. A rejected item now leaves the collection unchanged; later admitted updates
can continue. Existing serialization metrics, duplicate handling and incremental
measurement are preserved. Merge remains incrementally applied, not an atomic
transaction across all source items. Distinct sorted-run normalization is separate
from item insertion and is not claimed to be allocation-admitted by this change.

The helper consolidates repeated growth accounting without constructing a new
collection or scanning existing contents per append. Tests cover byte and row
rejection plus subsequent successful updates for all five buffering operations.
The 39 focused aggregate/group tests, 2,350 default tests (598 skipped), all 47
workerd tests, TypeScript build/typecheck and lint pass. This is a pre-mutation
boundary needed for ownership integration, not a fix for the shared-pool admission
gap: that reproducer and persistent reduction continuation remain open.


### F1/M1 accumulator construction admission (November 13, 2026)

The shared accumulator constructor now optionally reserves the state directory,
accumulator objects and empty containers before creation. A child scope rolls back
partial construction on invalid specifications or budget rejection, preserving
pre-existing caller allocations. Successful construction transfers its reservations
to the caller scope. Distinct construction also reuses its one empty Set for
measurement instead of allocating a second temporary Set.

This is explicitly a construction-only internal boundary. Updates, restore, merge
and public query execution do not yet use a complete aggregate ownership contract;
the shared-pool admission counterexample remains unresolved. Persistent physical
continuation must share that contract rather than add another independently owned
copy of the group state.

Tests cover all 17 accumulator operations at exact capacity and one byte below,
retained lifetime, and rollback after partial construction while caller backing
remains live. Verification: 31 focused tests, 2,368 default tests (598 skipped),
47 workerd tests, TypeScript build/typecheck and lint pass. These tests prove
reservation behavior, not a bound on JavaScript engine heap usage.

### F1/M1 shared list mutation boundary (November 13, 2026)

Median and quantile updates and merges now use one typed buffered append helper,
removing the identical measure/check/push/account implementations. Their value
validation remains operator-specific. This gives future growth admission one
mutation boundary without introducing a second ownership mechanism.

Added merge rejection tests for numeric quantiles and Unicode string medians:
only the admitted prefix remains, the source stays unchanged, serialized-byte
accounting matches the retained prefix, and subsequent updates succeed with
sufficient capacity. All 33 focused aggregate/group/construction tests pass;
TypeScript build and typecheck passed for the production refactor, and formatting
and lint checks pass for both changed files. No new pool-bound or performance
claim follows from this refactor. Scope retention captures allocations already
present; a durable mutable-state owner must also cover later growth before public
query integration. F1/M1 remain open.

### F1/M1 lifetime decision and expanded admission counterexample (November 13, 2026)

Re-inspected CPU reduce/grouped-reduce execution and MemoryScope retention. Both
physical reductions restore prior snapshots and create new snapshots per batch.
`retainAllocations` explicitly excludes future allocations; the existing tree
snapshot tests also verify that boundary. Retaining a construction snapshot and
then allowing mutation is therefore not an adequate mutable-state owner.

The continuation contract must have an exclusive mutable phase, own reservations
for construction and every growth operation, and release on cancellation/error.
It must permit batch-to-batch updates without snapshot serialization. Capturing
an independently retained result must end mutation of that state, or explicitly
copy/checkpoint into separately admitted storage. Checkpointing must account for
source/destination overlap. This applies to both reduce and grouped-reduce;
backend capabilities must advertise the complete contract before planner routing
uses it. Existing snapshot interchange remains an explicit checkpoint boundary.

Expanded `bench/grouped-state-admission-gap.mjs` to include no-key median and
count_distinct in addition to grouped count. All six cases (128/1,024 input rows)
return correct results with a 16-byte pool and report a 16-byte peak; cleanup is
zero. The machine-readable artifact was regenerated. This proves the missing
admission includes buffering accumulators, not just group directories. It is a
local owned-scanner diagnostic, not cloud transport, heap or performance evidence.
The next implementation must cover list/map/set growth, retained scalar payloads,
restore/merge overlap, and finalization scratch/output under that same lifecycle.
No production behavior changed in this verification turn.

### F1/M1 mode checkpoint allocation simplification (November 13, 2026)

Mode snapshot now writes output entries directly instead of materializing the
complete Map-entry array and then mapping it. Restore likewise fills its Map
directly, avoiding the mapped tuple array and an additional Set of all keys used
only to initialize accounting. Empty state retains its serialized size of two;
nonempty state retains the existing unmeasured sentinel until a budget requests
measurement. Duplicate snapshot keys still use the last value with the original
insertion order. Checkpoint values, including bigint serialization, are unchanged.

The two new checkpoint tests verify duplicate keys/order, bigint round trips,
independent entry objects and empty-state accounting; all 38 existing focused
aggregate/group/accounting tests also pass. TypeScript build passed and changed
files pass formatter/lint checks. This removes full-cardinality intermediates but
does not yet admit checkpoint storage or fix aggregate pool ownership.

### F1/M1 scoped aggregate checkpoints (November 13, 2026)

`snapshotVectorAggregateStates` now accepts an optional allocation scope and
reserves a conservative checkpoint extent before constructing output. The forecast
covers all 17 accumulator kinds, directories/entries, arrays, string payloads,
bigint/timestamp serialization and distinct sort/run-merge work. It reads source
state without constructing snapshot containers or decimal strings. Bigint decimal
length is counted arithmetically in 18-digit chunks. Reservation units are explicit
managed-allocation estimates; VM arithmetic/GC/runtime scratch is not a heap cap.
Distinct run forecasts conservatively count duplicates and merge work, so retained
checkpoint reservation can exceed final output size. Input remains charged during
copying. Copy failure releases the checkpoint reservation while preserving caller
allocations; retained output survives producing-scope closure.

Sorted distinct snapshots now copy their sorted-values array rather than sharing
mutable array backing with the accumulator. Unscoped snapshots retain their existing
format and skip allocation forecasting. The scoped API is not yet wired into public
query execution, and neither mutable state growth nor per-batch snapshot elimination
is solved by this change.

Verification: all 17 types at exact admitted capacity and one byte below while
source backing remains live, retention/cleanup, throwing copy rollback, Unicode,
large signed bigint and timestamp serialization, sorted-array independence and
duplicate run merging. The full default suite passed 2,392 tests (598 skipped), all
47 workerd tests passed, and build/typecheck/lint passed. After optimizing decimal
counting to 18-digit chunks, the 21 checkpoint tests (including decimal-boundary
cases) and TypeScript build pass. These are checkpoint admission tests, not cloud
transport or whole-Worker memory evidence. F1/M1 remain open.

### F1/M1 scoped checkpoint restore (November 13, 2026)

`restoreVectorAggregateStates` now optionally reserves reconstructed state before
allocating its directory, accumulators, arrays/collections and decoded scalar
payloads. Its forecast covers all 17 types, counts duplicate entries conservatively,
and bounds bigint decoding from serialized input length without decoding first.
The source checkpoint stays charged throughout restore. Decode errors and existing
serialized-budget rejection roll back the restore reservation without releasing
caller backing. Restored storage can be retained independently of its source.
The unscoped path also avoids a full Object.entries directory array.

Tests exercise every accumulator at the exact combined checkpoint/restore extent
and one byte below, source cleanup while retained restored state survives, invalid
bigint decoding after partial construction, buffered-row rejection, Unicode and
large bigint/timestamp values. All 78 focused tests, 2,412 default tests (598 skipped),
TypeScript build/typecheck and lint pass. The prior 47-workerd result predates this
restore change; no new Worker-memory claim is made here.

This admits reconstruction only. The mutable-state owner must still charge future
growth and finalization, and persistent physical continuation must replace ordinary
per-batch checkpoint/restore. Neither CPU reduction capability advertising nor public
query routing has been changed. The 16-byte admission diagnostic remains an open
acceptance failure until that integration is complete.

### F1/M1 finalization admission and reusable distinct merge scratch (November 13, 2026)

`finalizeVectorAggregateStates` now accepts an optional allocation scope. It admits
the retained result row separately from transient sorting/merge workspace, and
releases scratch before returning. Sequential finalizers reserve the maximum
workspace rather than summing their workspace extents. Scalar result payloads are
charged independently of the accumulator lifetime. Empty arrays avoid sort scratch;
median conservatively bounds a possible returned string without sorting during
forecasting. Result/scratch admission and finalizer failures roll back without
releasing caller backing. This remains an explicit reservation contract rather
than a VM heap/GC guarantee.

Distinct sorted-run merging now mutates and reuses the popped heap entry instead
of allocating a new entry for every visited value. Scratch entries therefore scale
with nonempty run count. Checkpoint forecasts were reduced to reflect that contract;
duplicate output candidates remain conservatively counted. Tests validate unchanged
merge results and constant finalization scratch across 16/1,024/8,192 values with
the same run count, including duplicate and empty runs.

All 17 finalizers are tested at exact combined source/result/scratch capacity and
one byte under, with retained output, immediate scratch release, and rollback when
mixed-type median sorting throws. A multi-finalizer test verifies shared peak
scratch and retained Unicode/timestamp payloads. The 97 focused tests, 2,431 default
tests (598 skipped), 47 workerd tests, build/typecheck/lint pass. The subsequently
added multi-finalizer test passes with all 60 checkpoint/restore/finalization tests.

Construction, checkpoint, restore and finalization now have scoped boundaries.
Mutable growth/merge ownership and persistent physical/query continuation are still
missing, and no reduction capability was newly advertised. F1/M1 and the public
16-byte counterexample remain unresolved until that integration is complete.

### F1/M1 exclusive mutable accumulator owner (November 13, 2026)

Added internal `OwnedAggregateState`: it owns private accumulators and admits
per-value list/collection growth and scalar replacement before mutation. It exposes
no mutable state; checkpoint returns independently admitted snapshots and finish
returns an admitted result while closing mutation. Closing or parent cancellation
clears state references as well as reservations. Replaced scalar backing is released;
duplicate collection values do not grow retained storage. Key lookup scratch is
charged separately from retained collection entries, including overlapping key
construction in the existing updater. Scoped construction/checkpoint/finalization
boundaries are used by the owner rather than creating another snapshot format.

All 17 accumulator kinds have value-update/checkpoint/final-result lifecycle tests.
Additional tests cover competing-pool rejection before collection/list growth,
continuation after capacity returns, repeated last-value replacement, duplicate
Unicode distinct values, cancellation, invalid construction and throwing finalization.
All 25 owner tests and 60 checkpoint/restore/finalization tests pass, along with
TypeScript build/typecheck and lint. The owner is internal and not yet used by query
execution. Per-value reservation metadata and conservative losing min/max candidate
admission need scaling/performance assessment before integration.

Remaining integration work is explicit: batch expression/vector extraction admission,
merge/restore ownership in the mutable owner, group-directory/key admission, and
persistent backend/query continuation. Existing public aggregate paths still fail
the complete-pool-accounting acceptance diagnostic. No capability advertisement,
public API promise or cloud/whole-Worker memory guarantee has changed.

### F1/M1 owned checkpoint merges (November 13, 2026)

The exclusive accumulator owner now merges checkpoints. It validates all aliases
and operation kinds before mutation, restores into admitted temporary storage,
and separately admits newly retained target list/map/set entries and scalar
replacements. Existing distinct keys do not add retained reservations; mode counts
merge with their weights rather than replaying observations. Fixed-size numerical
accumulators reuse the existing merge implementation. Temporary restored storage
is released on every exit. Merge remains prefix-applied on growth rejection;
accepted target entries retain their reservations and the source is unchanged.
Checkpoint input is borrowed, so its caller remains responsible for source ownership.

Tests compare owned and existing merges across all 17 accumulator kinds, exercise
billion-count mode weights, repeated Unicode distinct duplicates, complete alias
validation before mutation, restore rejection cleanup, and target rejection after
an accepted median prefix. All 46 owner tests and the combined 140 focused tests
pass; TypeScript build/typecheck and lint pass. This internal API is still not
connected to batch extraction or physical/query continuation. No full-suite or
cloud performance claim is inferred from these focused tests.

### F1/M1 scoped scalar vector extraction (November 13, 2026)

`scalarVectorValue` accepts an optional scope for packed UTF-8 decoding, binary
views, bigint reads and timestamp/timestamp96 construction. Admission propagates
through dictionary and union indirection. Nulls and already-materialized borrowed
scalars avoid new reservations. Packed strings use their selected range length,
including row offsets, rather than the whole column's byte length. Rejection occurs
before accessing packed payload storage. Returned binary views still borrow input
backing; the caller must keep that input owned.

`OwnedAggregateState.updateVectorValue` uses a per-value scratch scope, extracts
with admission, updates the independently charged accumulator, and releases scratch
on success or rejection. Tests cover Unicode/range windows, fixed lengths,
dictionary/union indirection, null/borrowed values, signed/unsigned bigint and wide
timestamps, 1,024 repeated decoded distinct values without retained scratch growth,
and binary aggregate rejection cleanup. The full suite passed 2,483 tests (598
skipped), 47 workerd tests passed, and build/typecheck/lint passed. The additional
bigint/wide-timestamp boundary test then passed with all six scalar-admission tests.

This is scalar extraction, not complete expression admission. `batchExprValues`
currently scopes certain masks/buffers but does not yet propagate per-value scratch
through all lazy expression/call evaluation. Batch expression support, group state
and persistent physical/query continuation remain open. Public aggregate routing
and capability advertising are unchanged; existing real-fixture suites provide
regression coverage, not evidence that public reduction is now fully admitted.

### F1/M1 per-value expression scratch propagation (November 13, 2026)

`BatchExprValues.valueAt` now accepts a per-value scope separately from the scope
that owns compiled batch masks. Columns forward it into scalar extraction;
arithmetic, CASE result branches and call arguments propagate it recursively.
Casing/trim/substring results reserve output string storage before construction.
Casing permits Unicode expansion and substring conservatively bounds by input
length. Coalesce no longer constructs a temporary array of argument results; its
existing eager evaluation behavior is preserved.

Tests cover nested Unicode casing at exact/one-byte-under capacity, independent
CASE-mask and per-row scratch lifetimes over repeated evaluation, selected-branch
allocation only, and propagation through coalesce/nullif and rejected arithmetic.
The focused expression/call tests, 2,487 default tests (598 skipped), TypeScript
build/typecheck and lint pass. The previous workerd result predates these expression
changes; no new Worker validation is claimed here.

Scope propagation alone does not make every call admitted. Replacement/regex/cast
result construction, spatial constant construction and predicate-evaluation scratch
still require completion. No bounded expression/reduction capability or query route
was enabled. These gaps remain prerequisites to owned batch/physical continuation.

### F1/M1 replacement and string-cast admission (November 13, 2026)

String-search replace now forecasts native substitution semantics before building
output: $$, matched text, prefix and suffix tokens, nonoverlapping matches, and
empty-search boundaries. It counts output units and match-index workspace without
materializing a match list or result. No-match calls return their input without a
new reservation. Scoped lazy replace calls use this boundary; unscoped behavior
continues to call native replaceAll directly.

String casts now admit conversion based on scalar shape, including arbitrary-width
bigints, binary values and wide timestamps. Existing strings remain borrowed. The
decimal-size helper moved to a shared scalar-allocation module, so aggregate and
expression forecasts reuse it. These are managed reservation bounds, not portable
measurements of native conversion or GC internals.

Tests compare 420 replacement combinations against native output, asserting the
output-length/match-count extent at exact capacity and one byte below with caller
backing present. They include empty strings/searches, overlaps, dollar tokens,
Unicode and an unpaired surrogate. Cast tests cover special numbers, signed wide
bigints, empty/nonempty bytes, intervals, normal/wide timestamps, borrowed strings
and lazy-expression propagation. All 118 focused expression/aggregate tests,
TypeScript build/typecheck and lint pass; the expanded replacement matrix also
passes after adding the standalone-dollar case. No new full-suite/workerd run is
claimed for this change.

Regex result/compilation work, numeric object-conversion scratch, spatial constant
construction and predicate scratch remain open before complete expression admission
can be advertised. Owned batch/group/physical/query integration remains incomplete.

### F1/M1 reusable owned batch reduction and real-fixture operation (November 13, 2026)

The exclusive owner now updates directly from batches and selections without
checkpoint/restore between batches. Count-star uses selected-row count directly.
Expression evaluation plans are admitted per accumulator/batch, and selected rows
use per-value scratch. Input vector/expression allocation capabilities are checked
before accumulator updates. The assessment explicitly rejects shapes whose
allocation contract is unfinished (including regex, CASE predicates and spatial
construction); this internal boundary is not a new public query-language limit.
No backend reduction capability or public route was enabled.

All 17 accumulators match existing selected-batch results across multiple batches.
Repeated packed-string expression batches retain a stable reservation baseline;
unsupported expressions are rejected before another accumulator is updated. All
125 focused owner/checkpoint tests, TypeScript build/typecheck and lint pass.

`bench/owned-aggregate-fixture.mjs` validates the real sales Parquet fixture through
owned decoding and this persistent internal reducer. Its independent generator
oracle checks count, sum, median and uppercase-region distinct count. Memory-store
and local HTTP 206 transport both pass at batch sizes 1/7/64 (100/15/3 batches), with
10,476 peak admitted bytes and zero remaining reservations. HTTP uses seven range
requests and transfers 1,013 bytes for every batch size. The output remains charged
and readable after the producing parent closes. Evidence is recorded in
`owned-aggregate-fixture-2026-11-13.json`. This is operation evidence, not a timing
benchmark, production cloud latency claim, public-query integration or Worker heap
measurement.

Before public integration, assess and reduce per-value reservation-handle overhead
for buffering accumulators. Remaining work includes complete expression/predicate
admission, group-directory/key ownership, backend continuation/checkpoint contracts
and replacing public per-batch snapshots. The original public admission-gap
reproducer is still an unresolved acceptance failure.

### F1/M1 measured accounting overhead and chunked growth (November 13, 2026)

The new accounting diagnostic exposed a major issue before public integration:
65,536 buffered median values created 65,536 growth reservations, retained only
2,098,314 admitted bytes, but retained 117,563,520 bytes of Node heap after explicit
GC. Per-value ownership metadata dominated the data. This contradicted the intended
low-memory use case even though the reservation cap itself was respected.

Append-only aggregate growth now uses shared bounded chunks rather than one owner
handle per value. Chunk sizing grows dynamically up to 64 KiB; admission failure
falls back to exact required payload plus explicitly charged ledger metadata.
Unused capacity registers as reclaimable at the shared pool, while committed and
pending-mutation payload stays charged. Reclamation may partition a chunk, so
rollback releases the current partition rather than a stale handle. Scope closure
releases all chunks. Mutable state never escapes this exclusive lifetime; existing
checkpoint/finalization paths independently admit retained output.

At 65,536 values the new diagnostic records 38 growth reservations, 2,179,626
admitted bytes and 666,272 retained Node heap bytes after GC. The single update
sample changed from 185.49 ms to 5.15 ms; these timings are diagnostic, not a repeated
performance claim. The 1,024/8,192/65,536-row reports are preserved separately in
`owned-aggregate-accounting-before-2026-11-13.json` and
`owned-aggregate-accounting-after-2026-11-13.json`. Heap deltas vary with runtime/JIT
and do not prove a whole-Worker heap bound.

Tests cover bounded reservation counts, exact fallback and cap-minus-one rejection,
sibling reclamation, protected pending payload, rollback after partitioning, parent
closure and reentrant mutation rejection. The full suite passed 2,514 tests (598
skipped), all 47 workerd tests passed, and build/typecheck/lint passed. The final
reentrancy/closure guard also passed the 70 focused allocator/owner tests and build.

The real sales fixture still passes memory/HTTP transports at batch sizes 1/7/64,
with unchanged results, seven HTTP ranges and 1,013 transferred bytes. Its admitted
peak rises from 10,476 to 17,600 bytes because chunks and ledger metadata are now
explicitly charged; optional spare is reclaimable under pressure. The earlier
fixture report is preserved as `owned-aggregate-fixture-before-chunk-accounting-2026-11-13.json`.
Public aggregate routing, complete expression/group admission and physical
continuation integration remain open.

### F1/M1 scalar replacement decisions and NaN ownership (November 13, 2026)

Min/max comparison is now shared between ordinary updates and owned admission.
Losing candidates, nulls and equal values require no replacement reservation;
winning candidates still admit new payload before replacing the old value. This
also avoids rejecting a logically allocation-free update when another job fills
the shared pool. Type and timestamp comparison semantics remain centralized.

Ownership identity checks now use Object.is. The prior equality check treated a
newly retained NaN as unequal to itself and released its payload reservation.
Tests cover NaN retention for min/max/first/last/any and signed-zero last-value
replacement, in addition to timestamp comparisons. For each of min and max,
10,000 losing candidates produce zero reserve calls under a full shared pool;
a winning candidate is rejected without altering the existing result.

All 96 focused owner/vector-aggregate tests, TypeScript build/typecheck and lint
pass. This is allocation-count/correctness evidence, not a new latency or Worker
heap benchmark. Public continuation integration and the previously listed
expression/group allocation gaps remain open.

### F1/M1 CPU physical reduction continuation (November 13, 2026)

The physical backend interface now has an optional explicit reduction-continuation
factory. CPU reduce sessions keep the owned accumulator alive across ordered batch
appends; appends do not checkpoint or restore. Explicit checkpoints have independent
ownership and can seed a new continuation. Finish returns independently retained
rows and closes mutation. Failed appends close the session because a prefix may
have committed, preventing accidental replay of the same input into partial state.
Cancellation, elapsed limits and final output-row limits release session state.

This factory does not imply support in the ordinary execute path or automatic
planner selection. CPU grouped continuation explicitly rejects until group/key
admission is implemented, and input expressions still pass the allocation-capability
assessment. Public aggregate query loops continue to use their prior snapshot path;
planner/query routing is not silently switched to this narrower capability set.

Tests prove 100 batch appends produce zero snapshot calls, explicit checkpoints
remain independent across subsequent updates, checkpoint restore creates a separate
session, returned rows survive parent closure, and failure/cancellation/row/time
limits clean up. All 19 focused physical tests, 2,527 default tests (598 skipped),
47 workerd tests, TypeScript build/typecheck and lint pass.

The real sales Parquet fixture benchmark now calls the CPU physical continuation,
not the internal owner directly. Memory and HTTP 206 transports still pass batch
sizes 1/7/64, with 17,600 admitted peak bytes and complete cleanup. HTTP remains
seven ranges / 1,013 transferred bytes. The previous direct-owner report is archived
as `owned-aggregate-fixture-before-physical-continuation-2026-11-13.json`.
Group ownership, complete expression/predicate admission, continuation placement
and public query integration remain required before F1/M1 can be closed.

### F1/M1 shared accumulator arena and releasable ledger (November 13, 2026)

Generalized append-only accounting into `MutableMemoryLedger`: committed payload
can be released after replacement, capacity can be reused, and reclamation can
remove multiple unused chunks together with their ledger metadata. Pending mutation
remains protected. Ten thousand equal-size replacements reuse one growth reservation.
Tests cover over-release rejection, multi-chunk trimming, rollback and zero cleanup.
The obsolete append-only source/generated module was removed after the rename.

Extracted accumulator construction, per-value mutation and state merge from the
query-lifetime wrapper into `AggregateStateMemory`. One arena can own many directories
using a shared ledger; it tracks replacement payload by accumulator identity and
rejects foreign states. Construction admits directory/alias/state and membership
metadata before creation. The existing owned reducer delegates to this arena, so
there is one implementation of admitted mutation and merge. This avoids giving every
future group an independent scope, reservation map and reclaimer. Enclosing reducers
must keep raw state private and independently admit any escaping output.

The diagnostic compares separate owners with a shared arena at 128/1,024/8,192
directories. At 8,192, reservations fall from 16,384 to 72; admitted bytes fall from
36,831,232 to 4,412,272; retained Node heap after GC falls from 42,475,992 to 972,072
bytes. Single construction samples are 65.03 ms versus 3.38 ms, not repeated timing
evidence. Full values and final cleanup are checked. The report is
`aggregate-arena-accounting-2026-11-13.json`; it is not grouped-query or Worker-heap
proof.

All 87 focused arena/ledger/owner/continuation tests, 2,532 default tests (598 skipped),
47 workerd tests, TypeScript build/typecheck and lint pass. The real sales fixture
still passes through CPU continuation over memory and HTTP at batch sizes 1/7/64,
with seven HTTP ranges, 1,013 transferred bytes and complete cleanup. Its admitted
peak is now 21,304 bytes; the earlier report is archived as
`owned-aggregate-fixture-before-shared-arena-2026-11-13.json`.

Group-directory/key encoding and lookup admission, grouped continuation, complete
expression/predicate admission, planner placement and public query routing remain
unfinished. The shared arena prepares those changes without per-group ownership
machinery; it does not itself satisfy the grouped admission acceptance tests.

### F1/M1 group directory and shared key encoding (November 13, 2026)

Extracted the existing typed group-key encoding into one shared implementation.
The legacy grouping operator uses the same encoding as the new scoped path;
scoped callers reserve conservative encoding scratch before constructing strings.
Tests preserve composite-key separation, Unicode, sparse/nullish tuples, numeric
special values, large bigints and timestamp identity, including exact and
insufficient allocation limits.

Added an internal owned group directory backed by the shared accumulator arena.
It reserves persistent keys and directory entries before insertion and updates
the existing operator-size metric by accumulator deltas. Lookup and update do
not rescan earlier groups. This consolidates ownership at the reducer lifetime
instead of creating a scope and reclaimer for every group. Raw groups remain
internal; escaping checkpoints and final rows still require independent admission.

All 40 focused key/directory/legacy-group tests pass, including 8,192 groups,
repeat lookup, typed composite keys, group limits, foreign-group rejection and
repeated nested admission failure with full rollback under an 8,192-byte pool.
Typecheck and lint also pass. These are component tests, not a new real-fixture
or end-to-end benchmark result. Grouped batch execution, checkpoint/finalization,
physical continuation and public query routing remain unfinished; the public
aggregate memory-admission counterexample remains open.

### F1/M1 owned grouped checkpoints and final rows (November 13, 2026)

The owned group directory now admits independently retained checkpoints and final
row arrays. Group-key serialization shares the existing checkpoint representation;
bigint and timestamp decimal output is admitted before conversion. Final rows
preserve key insertion order and aggregate alias precedence. Both key properties
and aggregate properties in the destination row are admitted, in addition to the
aggregate finalizer's temporary row and workspace.

A checkpoint failure releases partial output while leaving the directory usable.
Finalization closes the directory on success or failure, and successful outputs
remain charged after parent closure until their handles are released. Tests cover
those lifetimes, full-pool failures, empty output, output-row limits, alias overlap,
independent checkpoints across mutation, and mixed timestamp/200-digit bigint keys
round-tripping through the existing restore implementation.

The four-suite grouping/aggregate-snapshot gate passes 89 tests; the added mixed-key
round-trip also passes in the nine-test directory suite. Typecheck and lint pass
for the implementation. No new real-fixture performance claim is made. Grouped
batch input, checkpoint restore into the owned arena, physical continuation and
public routing are still required; this does not close the public admission gap.

### F1/M1 grouped batch continuation input (November 13, 2026)

The owned directory now accepts selected batches directly. It preflights key and
aggregate input capabilities before mutation, compiles expression readers once
per batch, admits decoded key tuples and scalar scratch, and updates shared arena
states without checkpointing between batches. Scratch lifetime is one selected
row for keys and one aggregate value for expression/decode results. Existing groups
are looked up directly; no previous-group traversal is needed for updates.

Tests compare all 17 aggregate operations with the existing grouping implementation
over 16 selected batches, with null values and multiple groups. Packed UTF-8 keys
and uppercase distinct expressions retain no extra bytes across 128 repeated
batches. Missing columns and invalid selection lengths fail before group insertion.
The combined owned/legacy grouped and ungrouped gate passes 122 tests; typecheck
and lint pass. This is component correctness and lifetime evidence, not a new
end-to-end performance result. Owned checkpoint restore, physical continuation,
planner placement and public-query routing remain open.

### F1/M1 grouped restore and CPU continuation (November 13, 2026)

Owned grouped checkpoints can now merge into the shared arena. The complete
checkpoint is preflighted for key arity and accumulator specification before
mutation. Each source group's key decoding and accumulator reconstruction has a
temporary scope; retained target keys and state receive separate admission. Merge
uses the same arena implementation as ungrouped reduction and updates the legacy
operator metric incrementally, including a successfully applied prefix on failure.

CPU `openReduction` now supports grouped reduction through the existing
continuation class. Cancellation, elapsed limits, failed-append closure and explicit
checkpoint ownership share the ungrouped implementation. Group count limits,
empty grouped output and final-row enforcement are covered. Planner capabilities
and public query routing have not yet been switched to this execution path.

All 17 aggregate operations pass checkpoint restore/repeated-merge comparisons.
Continuation tests prove 100 grouped batch appends perform no implicit snapshots,
explicit restore resumes independently, outputs survive parent disposal, and
failure/cancellation release state. The full default suite passes 2,596 tests
(598 skipped); 47 workerd tests, TypeScript build/typecheck and lint pass.

`bench/owned-aggregate-fixture.mjs --grouped` checks actual sales Parquet decoding
and grouped CPU continuation against the fixture generator's independent oracle:
count, sum, median and uppercase-region distinct values for each region. Memory
and local HTTP 206 transports pass batch sizes 1/7/64 (100/15/3 batches). All six
runs peak at 46,788 admitted bytes and finish with zero reservations; HTTP uses
seven ranges totaling 1,013 bytes. Results are saved in
`owned-grouped-aggregate-fixture-2026-11-13.json`. This proves the explicit physical
continuation with real decoded data, not public-query integration, production cloud
latency, or a whole-Worker heap cap. Public routing, complete input-capability
coverage and the existing public admission counterexample remain open.

### F1/M1 continuation-aware physical placement (November 13, 2026)

Added an explicit `requireReductionContinuation` planning contract, separate from
ordinary `execute` allocation-scope support. Placement requires a batch input,
one terminal reduction operator and a backend implementing `openReduction`.
Existing backend policy and cost assessment still apply, including required
accelerator policy. This prepares public routing without hard-coding CPU selection
or claiming that snapshot-producing execution has become allocation-owned.

CPU continuation assessment checks referenced key/aggregate column shapes and
expression allocation capabilities. Expression assessment shares the owned runtime
implementation through a column-validation callback. Dictionary children are
checked recursively. Union metadata currently lacks child descriptors, so planner
placement rejects union inputs even though direct continuation can validate actual
union vectors. This is a metadata-contract gap to close, not a query-language
restriction. Regex/CASE and other unadmitted expression shapes remain rejected.

The 137-test planner/continuation/owned-reducer gate passes. Tests distinguish
ordinary scoped fragments from continuation planning, reject backends without the
continuation method, preserve required accelerator policy, and reject unsupported
expressions and nested dictionary payloads before execution. Public query loops
still use the old snapshot path; routing and its admission counterexample remain
unfinished.

### F1/M1 recursive union placement metadata (November 13, 2026)

Closed the union metadata gap noted above: physical column descriptors now include
ordered child descriptors for unions, recursively including dictionary payloads.
CPU continuation placement accepts unions only when every child has an admitted
shape. Missing child metadata and unsupported children remain explicit rejections.
Runtime physical-input validation compares these descriptors recursively, so a
changed union child cannot reuse a plan built for a different input shape.

The 18-test physical/continuation gate passes, including accepted mixed numeric and
dictionary-string children, missing metadata, binary child rejection and runtime
nested-child mismatch. Typecheck and lint pass. Public query routing remains open;
this change makes its placement contract accurate for heterogeneous vectors.


### F1/M1 public columnar reduction continuation (November 13, 2026)

Consolidated the grouped and ungrouped columnar aggregate loops. The shared loop
requests continuation-aware backend placement, keeps admitted mutable state across
supported batches, and uses explicit owned checkpoints when the selected backend
changes or continuation is unavailable. Required-accelerator policy still applies.
Selection runs as a separately scoped physical operator before append. Final rows
are independently admitted, and ordering/slicing reserve output arrays and sort
workspace before materialization. The ordinary Row[] API releases library ownership
at its return boundary; retaining those returned rows is caller responsibility.

Regression tests now cover the former 16-byte-pool counterexample: grouped count,
median and distinct count reject at admission and clean up, while sufficient pools
produce all expected values. The refreshed diagnostic tests 128/1,024 input rows
with 16-byte and 64 MiB pools. At 1,024 rows, successful tracked peaks are 1,692,624
bytes for grouped count, 114,906 for median and 285,072 for distinct count. Historical
unsafe results are preserved in
`grouped-state-admission-before-public-continuation-2026-11-13.json`; the current
`grouped-state-admission-gap-2026-11-13.json` records the fixed route's assertions.
A backend-transition test proves state survives two reopenings around a batch whose
placement changes. Scan-owner tests now assert scan-handle release independently
of live reducer state instead of incorrectly expecting an empty pool between batches.

Real sales Parquet public queries now run via an owned-only scanner over both memory
and HTTP 206, using batch sizes 1/7/64 and the independent fixture-generator oracle.
Grouped results peak at 46,788 admitted bytes; ungrouped results peak at 21,384.
All runs release every reservation. Public HTTP execution makes eight range requests
and transfers 1,015 bytes (including its object-stat probe), compared with seven
ranges/1,013 bytes for the direct physical benchmark. Reports are
`public-grouped-aggregate-fixture-2026-11-13.json` and
`public-aggregate-fixture-2026-11-13.json`. This is local HTTP simulation, not
production cloud latency or total Worker heap evidence.

Local Parquet and native Worker R2 tests locate the minimum successful configured
cap, verify success at that cap and budget rejection one byte below, and check
cleanup in every attempt. The unconstrained peak is not a valid minimum-cap oracle:
shared arena spare capacity can be reclaimed when the cap is tightened. Full default
validation passes 2,603 tests (598 skipped); 47 Worker tests, TypeScript build,
typecheck and lint pass.

This closes the recorded public counterexample for the supported columnar
continuation route. It does not close F1/M1 overall: legacy snapshot fallback,
late-materialized grouped execution, remaining expression/predicate admission,
spill and resumable operator-state paths, public scaling evidence and the broader
acceptance checklist still require work. External conformance remains unresolved.


### F1/M1 owned deferred aggregation through bounded vector gathers (November 13, 2026)

Owned late-materialized aggregation now uses the existing `gatherDeferredColumns`
operator and CPU reduction continuation. Selected positions join early and late
vectors in bounded read windows; no query-wide early-row copies, row-reference map,
or separate per-row accumulator-update implementation is used on this route.
Inputs and fetched vectors stay owned through gathering, and joined batches feed
exactly the same admitted aggregate implementation as eager columnar execution.
The prior 4,096-reference product cutoff is unnecessary for this route: transient
selection/gather work is batch-bounded and retained state is pool-bounded.

Expression allocation capabilities are checked before speculative I/O; actual
joined vector shapes are checked by the reducer. Unsupported capabilities or
speculative allocation admission failure clean up and restart eager execution
under the same pool. Mandatory group/buffer/time limits still propagate. Queries
without a predicate skip the speculative early read, since their selection density
is already known to be 100%. Objects advertising emulated ranges also choose eager
execution. The existing density threshold remains for native-range selective work.

Six owned-scanner tests cover grouped/ungrouped selective windows, dense fallback
and empty results, checking requested columns/positions, iterator cleanup and zero
remaining reservations. A real Parquet/native Worker R2 fixture proves a predicate
on the early `id` column causes the deferred `value` scan to request rows 0–2 only,
with exact group results and full cleanup. The default suite passes 2,609 tests
(598 skipped); 48 Worker tests, TypeScript build/typecheck and lint pass.

The real sales selective grouped fixture passes over memory and HTTP for batch
sizes 1/7/64. Tracked peaks are 18,550/18,565/18,726 bytes; all reservations are
released. Its HTTP results expose a scheduling tradeoff rather than a speedup:
11/50/14 range requests transfer 1,104/7,572/1,647 bytes. Batch size 1 takes the dense
fallback after the first selected row; seven-row deferred windows repeatedly fetch
page bytes in this uncached decoder harness. Evidence is
`selective-public-grouped-aggregate-fixture-2026-11-13.json`. Density alone is not a
sufficient remote-read cost model; shared page-cache reuse and measured/window-cost
selection remain open. The fixture harness uses real decoding and local HTTP,
not production latency or a whole-Worker heap guarantee.

Borrowed-only legacy deferred aggregation, legacy physical snapshot fallback,
remaining expression/predicate allocation gaps, spill/state APIs and the broader
acceptance checklist are still unfinished. The owned deferred path is now covered;
this is not a declaration that every aggregate route has a complete memory bound.


### M1/M2 fractional decoded-cache admission below 128 MiB (November 13, 2026)

The balanced/IO decoded-cache policy previously refused every decoded page or work
product when configured capacity was below 128 MiB. A 16 MiB cache inside the same
64 MiB query pool therefore retained nothing in the selective fixture, even though
individual reusable pages were tiny. Removed that absolute cutoff. Balanced/IO
admission now consistently limits each decoded work product to one eighth of
configured cache capacity; latency policy retains its explicit full-capacity rule.
Shared-cache priority, eviction and pool admission remain authoritative. The change
adds no cache-wide traversal or additional ownership structure to a page update.

Tests cover small-page admission, oversized-entry rejection, protection of
higher-priority entries, admission after space returns, and owned reuse/eviction
with sibling query budgets in 64/128 MiB pools. All 54 focused cache/page tests,
2,612 default tests (598 skipped), 48 Worker tests, build, typecheck and lint pass.

Added a `--cache` comparison to the real sales fixture harness. Its 16 MiB decoded
cache is charged to the same 64 MiB root pool, surviving query completion and then
being explicitly evicted; retained cache bytes and final zero usage are both
recorded. Seven-row selective windows now report 12 cache hits and 2,340 retained
cache bytes, with a 20,143-byte tracked peak versus 18,565 without cache. The prior
all-rejected result is archived as
`cached-selective-public-grouped-aggregate-before-fractional-admission-2026-11-13.json`;
current results are in
`cached-selective-public-grouped-aggregate-fixture-2026-11-13.json`.

This change has **not reduced HTTP traffic** in that fixture: seven-row windows
still make 50 requests / transfer 7,572 bytes. Numeric header preparation precedes
decoded-cache lookup, and a header probe can fetch the entire small page payload.
A whole-chunk-prefetch experiment did not change the result and was reverted;
ordinary prefetch/concurrency behavior is preserved. Header/page-plan reuse and a
remote-read cost model remain the next performance work. Cache hits here prove
reuse and lifetime correctness, not a measured wall-time improvement or a total
Worker heap cap. Configuring no decoded cache still leaves that cache disabled.


### M1/M2 owned header reuse and cache-hit body avoidance (November 13, 2026)

Added shared page-header descriptors through the existing decoded cache. Keys
include source namespace, file length/etag and exact chunk/page offsets. Escaped
key strings and fixed-shape projected header metadata are admitted before I/O.
Only scalar header records and extents are cached: no probe bytes, decoder,
inspection scope or source handle escapes. Callers independently pin descriptors
across eviction, and source-identity changes miss the cache. Numeric aligned reads
and ordinary single-column window reads share the descriptor implementation.

The ordinary window reader also used to read a matching page body before checking
its decoded-vector cache. It now looks up and pins that vector before deciding
whether body transport is necessary. The captured pin survives cache eviction
between lookup and publication. Misses use the existing decode and bounded read
paths; whole-chunk prefetch and parallel range-read limits are unchanged.

Unit tests cover key admission before I/O, invalid extents, cache eviction with a
live caller, changed etag, descriptor reuse without probe backing, and cancellation
during the pending header read. Real Parquet tests cover one and two numeric
columns: after a cold requested window, a warm window issues zero owned source
reads, remains correct after cache eviction and releases all reservations.
The default gate passes 2,616 tests (598 skipped), and 48 Worker tests pass. A
subsequently added cancellation regression passes in the three-test header suite;
TypeScript build/typecheck and lint pass after that addition.

For cached seven-row selective public HTTP windows, requests fall from 50 to 26
and transferred bytes from 7,572 to 1,671. Twelve decoded-cache hits still occur.
HTTP tracked peak rises from 20,143 to 31,299 bytes, and retained cache storage rises
from 2,340 to 13,656 bytes; eviction returns pool usage to zero. At batch size 64,
there are no decoded-cache hits and traffic remains 14 requests / 1,647 bytes, so
this is a measured reuse tradeoff, not an all-workload improvement. The before
report is `cached-selective-public-grouped-aggregate-before-header-reuse-2026-11-13.json`;
the current report remains `cached-selective-public-grouped-aggregate-fixture-2026-11-13.json`.

Repeated source/stat probes, dictionary transport, whole-chunk cache lookup timing,
uncached small-window scheduling and the broader memory/compatibility checklist
remain open. These fixture results do not establish production-cloud latency or
a whole-Worker memory cap.

### M1/M2 dictionary cache lookup before body transport (November 13, 2026)

Deferred dictionary pages now use the same lookup-before-transport ordering as
ordinary data pages. Dictionary cache-key construction is shared with the decoder;
a hit pins the decoded dictionary in the column scope before skipping its encoded
body. A miss retains the existing decode path and page backing lifetime. Cached
hits release temporary header transport ownership immediately rather than retaining
it as though a newly decoded dictionary could alias that body.

A real SNAPPY/RLE_DICTIONARY string fixture checks a cold and warm selected window.
The warm window makes zero owned source reads, remains correct after cache eviction,
and returns shared-pool usage to zero after output release. The final numeric-page
and page-cache lifetime gate passes 39 tests; 48 Worker tests, TypeScript build,
typecheck and lint pass. This extends the prior data-page/header reuse contract;
no new production latency or all-workload performance claim is made. Whole-chunk
cache lookup timing, repeated source probes and uncached scheduling remain open.


### F1/V2 public grouped continuation scaling (November 13, 2026)

Added `bench/public-grouped-continuation.mjs` to exercise the public query API with
an owned synthetic scanner. Each case has one warmup and three sequential timed
samples, with explicit GC before samples when available. A measured CPU backend
counts continuation opens, appends and checkpoints. Every group value, scan cleanup,
pool cap and zero final reservation count is asserted outside the timed query.

| Groups | Batch rows | Median ms | Peak admitted bytes | Inter-batch checkpoints |
| --- | --- | --- | --- | --- |
| 2,000 | 128 | 42.80 | 3,213,536 | 0 |
| 4,000 | 128 | 91.25 | 6,331,120 | 0 |
| 8,000 | 128 | 181.84 | 12,633,952 | 0 |
| 2,000 | 512 | 34.26 | 3,213,536 | 0 |
| 4,000 | 512 | 84.20 | 6,331,120 | 0 |
| 8,000 | 512 | 166.75 | 12,633,952 | 0 |

Each run opens exactly one continuation, and append count equals the number of
input batches. Within this measured range, time and admitted storage grow roughly
linearly with cardinality; increasing batch size does not change retained peak.
The prior snapshot diagnostic counted 15,360/63,488/249,984 prior-group restore
visits at 128-row batches. The public continuation has no inter-batch checkpoint
round trips. The old diagnostic excluded compilation/instrumentation and had only
one timing sample, so its elapsed values are not a controlled speedup comparison.

Full samples and conditions are in
`public-grouped-continuation-2026-11-13.json`. This closes the missing public scaling
evidence for supported grouped count, not all aggregate operations, spill, legacy
fallback, cloud latency or concurrent-worker acceptance. No runtime heap claim is
inferred from admitted bytes. The broader F1/V1/V2 checklist remains open.

### V1/M1 concurrent public group-state admission matrix (November 13, 2026)

Extended `bench/public-grouped-continuation.mjs --concurrent` with two/four
asynchronously overlapping public queries, each creating 16,000 distinct groups
from owned 128-row batches. Root pools are 64/128 MiB and per-query limits are
16/32 MiB. The harness observes the requested number of simultaneously live scan
iterators, checks every successful result, requires typed budget errors for rejected
queries, and verifies per-query and root cleanup after each run.

| Shared pool MiB | Per-query MiB | Jobs | Completed | Admission rejected | Root peak bytes |
| --- | --- | --- | --- | --- | --- |
| 64 | 16 | 2 | 0 | 2 | 33,451,046 |
| 64 | 16 | 4 | 0 | 4 | 66,800,230 |
| 64 | 32 | 2 | 2 | 0 | 42,048,512 |
| 64 | 32 | 4 | 3 | 1 | 67,107,874 |
| 128 | 16 | 2 | 0 | 2 | 33,451,046 |
| 128 | 16 | 4 | 0 | 4 | 66,800,230 |
| 128 | 32 | 2 | 2 | 0 | 42,048,512 |
| 128 | 32 | 4 | 4 | 0 | 75,936,960 |

The 16 MiB failures occur during aggregate-state construction against the child
limit, including with unused root capacity. The single observed 64 MiB/32 MiB
four-job rejection occurs against the root limit. Thus neither spare global
capacity nor sibling work bypasses the user's query limit. The harness requires
limits and cleanup, not a fixed winner or completion count: scheduling may change
which query encounters pressure first. All final reservation counts are zero.

Results are in `public-grouped-concurrent-2026-11-13.json`; the benchmark passes its
assertions and lint. These are synthetic owned scans in one Node process, not
simultaneous CPU threads, cache-reuse evidence, cloud latency or Worker heap limits.
They complement rather than replace real fixture/HTTP/R2 tests. Mixed operators,
cold/warm caches, pinned outputs, fairness and full constrained-runtime acceptance
remain open under V1/M2.

### M1 predicate scratch lifetimes and streaming IN (November 13, 2026)

Generic comparison, scalar predicate, null-check and IN evaluation now propagate
allocation scopes into scalar reads. Decoded strings and expression outputs are
released at the row boundary rather than escaping admission or accumulating over
the batch. Existing typed-vector comparison and null-check paths remain intact.

IN no longer builds a candidate-value array. It evaluates candidates sequentially,
retaining the target and the current candidate's scratch. All candidate expressions
still execute after an early match or a NULL target. Comparison errors are deferred
until candidate evaluation completes so a later expression error retains its prior
precedence. This is a lifetime simplification: avoid retaining intermediate values
when their only consumer can run immediately. An initial array-admission approach
failed six existing low-budget IN tests; streaming eliminates that unnecessary
allocation without loosening their limits. It is not a measured throughput claim.

`predicate-scratch.test.ts` covers packed UTF-8 expansion through comparisons, IN,
null checks, BETWEEN and CASE predicates; one and 100 rows; exact admission and
one-byte-under rejection; row-bounded scratch; eager evaluation after a match or
NULL target; evaluation-error precedence; and cleanup on failure. Together with
existing IN and batch tests, 36 focused tests pass. The rebuilt default suite passes
2,626 tests with 598 skipped; the actual Workers suite passes all 48 tests.

This establishes scalar scratch ownership for these predicate paths, not complete
expression-plan admission. CASE remains excluded from owned aggregate capability
advertising pending its remaining allocation review. The broader audit and runtime
heap acceptance remain open.

### M1 nested CASE predicate workspace release (November 13, 2026)

CASE branch compilation now uses the existing `predicateSelection` ownership
boundary for each WHEN. That boundary transfers only the completed truth mask and
releases nested predicate scratch, including nested CASE choice buffers. Previously
those nested choice buffers survived in the outer compilation scope until the
entire compiled expression was disposed. Reusing this boundary avoids a separate
CASE-specific ownership implementation.

A nested false-CASE condition repeated across 20 or 256 WHEN branches now retains
only the outer choice buffer: 100 or 200 bytes for 100 rows. Peak admitted mask
storage is 300 or 400 bytes respectively, independent of branch count within each
choice-width class. The tests execute the selected ELSE expression and verify
cleanup. These measurements cover mask backing, not compiled closure/array heap;
expression-plan admission remains an explicit outstanding prerequisite to enabling
CASE in owned aggregate capability assessment.

All 21 focused predicate/CASE/expression tests pass, including existing compact
choice-width, first-match, NULL, empty-branch and failure-cleanup cases. The rebuilt
full default suite passes 2,628 tests with 598 skipped, including existing Parquet
fixture tests. Typecheck and repository lint pass. No new cloud I/O behavior or
whole-runtime memory guarantee is inferred from this mask-lifetime change.

### M1 eager scalar-call scratch lifetimes (November 13, 2026)

COALESCE now evaluates each argument in a child value scope. It retains the first
non-NULL argument's scope and immediately disposes every unused argument's scope,
including on evaluation failure. Evaluation remains eager: later arguments still
run and can throw after a result has been found. NULLIF similarly separates left
and right operand scopes, always releases the right side, and retains the left
side only when returning a non-NULL left value. Comparison failure releases both.
The common rule across IN, CASE predicate compilation and these calls is to retain
only values that can escape the current consumer; existing scope ownership handles
the cleanup without introducing another allocation ledger.

Packed UTF-8 tests use a Unicode upper-case expansion as each COALESCE argument.
One hundred arguments now have the same admitted scratch peak as two: the selected
result and one current candidate. Exact-peak admission succeeds, one byte less
rejects, and final disposal clears all reservations. NULLIF tests cover equal,
unequal and type-error operands, verifying returned payload ownership and cleanup.
These are scoped payload measurements; compiled argument arrays and runtime scope
object overhead remain separate audit concerns. No throughput claim is made.

The production revision passes the full default suite (2,632 passed, 598 skipped),
all 48 Workers tests, TypeScript build/typecheck and lint. A subsequently added
NULLIF comparison-error regression also passes in the eight-test expression-scratch
suite. Existing real Parquet fixture regressions are included in the full run;
these scalar changes do not alter transport or cloud range-read policy. CASE
capability expansion and the broader memory/concurrency release gates remain open.

### M1/V2 scalar lifetime performance and reader capabilities (November 13, 2026)

Added `bench/scalar-call-lifetimes.mjs` to compare the former eager COALESCE loop
(retaining every argument until row disposal) with the bounded loop, using the
same current scalar readers. It checks every result and zero final reservations,
warms each variant, alternates order and records five samples per case over 4,096
rows. Numeric literals, numeric columns and packed UTF-8 upper-case expansion are
measured at two and 32 arguments. This is a Node microbenchmark, not a cloud or
whole-runtime heap result.

The first run exposed avoidable scope construction for allocation-free numeric
literals: 32 arguments took 16.50 ms versus 1.21 ms for the retained loop, despite
zero payload allocations. `BatchExprValues` now explicitly advertises
`allocationFree` only for literals and scalar vector shapes that return borrowed
or primitive values without payload allocation. Dictionary and union readers
inherit the property only when their children qualify. Packed strings, bigint,
timestamps and unknown readers remain conservative. COALESCE/NULLIF skip child
scope construction only for readers with this capability. Tests verify borrowed
numeric/string versus packed string classification and zero-budget numeric reads.

The final recorded medians for 32 arguments are 0.50 ms versus 1.54 ms for literals,
1.85 ms versus 2.13 ms for numeric columns, and 559.59 ms versus 507.28 ms for
expanded strings (bounded versus retained). Tiny numeric timings are sensitive to
JIT/order effects and do not establish a general speedup. For strings, peak admitted
scratch falls from 8,512 to 532 bytes (16-fold), at roughly 10% additional elapsed
time in this run. The bounded default preserves the user's allocation contract;
no configuration is required to avoid scope overhead for known allocation-free
readers. Compiled plan and scope-object heap accounting remain open.

Raw samples and runtime version are in `scalar-call-lifetimes-2026-11-13.json`;
the initial regression is preserved in
`scalar-call-lifetimes-before-reader-capability-2026-11-13.json`.

Validation for this revision: 24 focused scalar/predicate tests; full default suite
2,634 passed and 598 skipped; all 48 Workers tests; TypeScript build/typecheck and
repository lint pass. The broader acceptance checklist remains incomplete.

### M1/V2 allocation-free predicate readers and nested shapes (November 13, 2026)

Scalar predicate, fallback comparison, null-check and linear IN evaluation now
skip temporary scope creation when their readers explicitly advertise no scalar
payload allocation. IN also avoids candidate scopes for allocation-free candidates
when another operand still requires a row scope. Unknown readers remain scoped;
no inference is made from a particular row's value. This reuses the scalar-reader
capability introduced for COALESCE/NULLIF rather than introducing function-specific
numeric shortcuts.

Tests exercise a dictionary whose values are a union of NULL and strings. Borrowed
strings advertise allocation-free reads and execute under a zero-byte payload
budget; substituting a packed UTF-8 child makes the whole reader require scratch,
charges both returned decoded values, and rejects a zero-byte budget. All scopes
clean up. Existing exact-boundary, eager-error-ordering, mixed IN and packed-string
predicate tests continue to pass (49 focused tests).

The compound predicate harness was rerun at 10,000/100,000 rows and 2/32/128
operands, checking every result against its independent three-valued-logic oracle.
All cases retain one mask and peak at two masks, regardless of operand count.
Samples are in `compound-predicate-reader-capability-2026-11-13.jsonl`. The run
overlapped regression tests, so its elapsed samples are diagnostic and are not
used for a before/after speedup claim. This does not establish compiled-plan or
whole-runtime memory bounds.

Validation: rebuilt core; 2,636 full-suite tests passed with 598 skipped; all 48
Workers tests passed; typecheck and lint passed. Remaining expression-plan admission
and broader release requirements remain open.

### M1 bigint comparison fast-path scratch admission (November 13, 2026)

The bigint-vector/literal comparison fast path now reserves one reusable bigint
scalar workspace while reading typed-array elements. The reservation is released
when the loop finishes or fails; it does not accumulate with row count. Empty
vectors require no scalar workspace. Safe integer number literals now use native
mixed number/bigint relational comparison, avoiding an unnecessary BigInt literal
conversion while preserving exact ordering in the admitted safe-integer domain.
The existing fast-path selection still excludes unsafe integer number literals.

New boundary tests compare signed 64-bit extremes, safe-integer boundaries, zero
and negative values with numeric and bigint literals. They verify the independent
relational oracle, exact mask-plus-one-bigint admission, one-byte-under rejection,
retained-mask-only accounting and rollback. Empty unsigned vectors work with zero
budget. These charges represent live scalar workspace, not total V8 allocation or
GC behavior. Timestamp parsing/conversion fast paths still need their own review;
this does not unblock CASE capability advertising by itself.

Validation: 39 focused tests; rebuilt core; full suite 2,643 passed and 598 skipped;
all 48 Workers tests; typecheck and lint pass. Existing real Parquet fixture tests
remain in the full regression run. The overall goal remains incomplete.

### M1 timestamp precision correction and bounded comparison workspace (November 13, 2026)

Reviewing the timestamp fast path uncovered a correctness defect: converting a
literal to the vector's coarser unit truncated fractional ticks before comparison.
For example, a millisecond value at epoch zero incorrectly compared equal to a
literal one nanosecond later. New oracle tests reproduced failures for both
millisecond and microsecond columns before the production change.

The fast path now computes the truncated tick quotient and signed nanosecond
remainder once. If a stored tick equals the quotient, the remainder determines
exact ordering. This handles both sides of epoch zero without per-row timestamp
object construction or nanosecond multiplication. Four bigint workspace units are
reserved before conversion and released after comparison, independent of batch
size; empty vectors skip the workspace. The mask retains its separate ownership.

Tests cover all six comparisons, all three units, positive/negative fractional
boundaries, typed literals, ISO literals, reversed operands, NULLs, exact-budget
success and one-byte-under failure. The real written TIMESTAMP_MICROS Parquet
fixture additionally checks a nanosecond literal between stored ticks through the
public query API: less-than returns the first row and equality returns none.
This is serialized Parquet coverage via the memory store; it is not a new remote
transport experiment. ISO parsing itself and generic timestamp comparison parsing
still require allocation-contract review before CASE can be advertised as bounded.

Validation: 2,648 full-suite tests passed with 598 skipped; all 48 Workers tests;
TypeScript build/typecheck and lint passed. The subsequently expanded real Parquet
timestamp test also passes. The goal and remaining expression-plan audit stay open.


### M1 shared ISO timestamp parsing admission (November 13, 2026)

`timestampValueFromIso` now accepts an optional allocation scope. It rejects lengths
outside its 20–30-code-unit grammar before regex work, reserves a fixed workspace
for regex captures, bounded conversion strings and bigint intermediates, and admits
the returned timestamp separately. Parser workspace releases on success, invalid
input or failure. Only a valid result remains owned by the supplied scope. Existing
unscoped callers retain their API behavior and still need caller-specific review.

Fast timestamp predicates parse into a short-lived child scope. Generic comparison,
IN and NULLIF pass comparison scopes through the same parser and dispose parsed
values after comparison. The read-allocation-free capability does not imply that
comparison is allocation-free: literal timestamp versus literal ISO comparisons
still reserve parsing workspace even though neither operand read allocates.

Tests cover a negative-epoch nanosecond result, exact parser peak admission,
one-byte-under failure and cleanup, invalid dates/grammar, oversized input, and
100-row literal comparisons through comparison, IN and NULLIF. Only output masks
remain after predicate execution; parser payload does not accumulate per row.
This closes the ISO parsing gap for these predicate routes, not every timestamp
conversion caller or complete compiled expression-plan admission.

Validation: the production revision passes 2,654 default tests (598 skipped), all
48 Workers tests, TypeScript build/typecheck and lint. The subsequently expanded
13-test timestamp memory suite also passes, including IN and NULLIF scope propagation.
The broader acceptance checklist remains open.


### F1/M1 CASE plan admission in owned aggregate continuations (November 13, 2026)

The shared runtime/planner expression assessment now accepts CASE when every
condition and branch has an allocation contract. Its preflight recursively covers
comparison, IN, BETWEEN, null checks, logical/not predicates and the existing
admitted scalar functions. CASE branch-reader arrays, IN reader arrays and recursive
compiled descriptors are included in the aggregate plan reservation before
construction. Predicate masks and scalar/parser workspace remain separately
admitted at runtime. Scalar positions still reject predicate-only AST shapes;
regex/spatial and unsupported casts remain excluded, even in unreachable branches.

Both `OwnedAggregateState` and `OwnedGroupDirectory` already use this shared
preflight and plan reservation, so no CASE-specific reducer or query branch was
introduced. The same assessment informs CPU continuation planning. This enables
supported CASE expressions on the owned continuation path instead of routing them
to a legacy snapshot path solely because their expression kind is CASE.

New tests exercise Unicode packed-string IN conditions in grouped and ungrouped
CASE sums, full and selected batch updates, unchanged retained state after repeated
updates, correct final results and complete cleanup. They find the actual minimum
executable budget and verify admission there plus rejection one byte below it.
A roomy peak is not assumed to be the minimum: the existing arena can reclaim
spare capacity under pressure. An unsupported unreachable branch is rejected before
another accumulator mutates. The real written microsecond Parquet fixture now also
runs a public CASE sum against a nanosecond comparison literal and returns the
independent expected count.

Validation: 123 owned aggregate/directory/CASE tests; the real Parquet fixture;
full default suite 2,659 passed and 598 skipped; all 48 Workers tests; rebuilt core,
typecheck and lint. This closes the CASE capability blocker for the admitted subset,
not all expression functions, ordinary projection/filter plan admission, legacy
fallback/state, mixed-workload scheduling or total runtime memory acceptance.

### V1/V2 CASE aggregates over real Parquet, HTTP and native R2 (November 13, 2026)

Extended `bench/owned-aggregate-fixture.mjs` with `--case`, adding a conditional sum
whose expected result is computed independently from the sales fixture recipe.
The harness now counts planner backend continuation opens; ordinary public grouped
and ungrouped runs require exactly one continuation across the complete scan.
The count observes the configured planner backend, not direct continuation creation
inside the owned deferred path, so zero in a selective run does not mean snapshot
fallback. Existing final result, cache eviction and reservation cleanup assertions
remain active.

All 18 scenarios pass: public grouped, public ungrouped and selective cached grouped
CASE aggregation, each at batch sizes 1/7/64 through memory and local HTTP 206 stores.
Ordinary grouped runs peak at 51,540 admitted bytes; ungrouped at 23,460. All ordinary
HTTP runs make eight range requests transferring 1,015 bytes, unchanged from the
non-CASE fixture route. Selective cached HTTP runs use 11/26/14 requests and
1,104/1,671/1,647 bytes, with peaks 23,239/34,525/34,589 admitted bytes. Final pool
usage is zero after cache eviction. These are correctness/admission/traffic results,
not latency samples or total-heap measurements.

Reports are `case-public-grouped-aggregate-fixture-2026-11-13.json`,
`case-public-aggregate-fixture-2026-11-13.json` and
`case-cached-selective-public-grouped-aggregate-fixture-2026-11-13.json`.

The native R2 workerd test now runs both plain and CASE sums over a written
100-row, ten-row-group Parquet object. Both preserve the selective owned window:
only the deferred value column for rows [0,2) is gathered, results are exact, the
configured 1 MiB pool is respected and final reservations are zero. The Workers
suite passes all 49 tests; typecheck and lint pass. This is native local workerd/R2
coverage, not production cloud memory enforcement or the outstanding mixed-operator
64/128 MiB concurrency acceptance matrix.


### V3 package and compatibility gate refresh (November 13, 2026)

Refreshed the full distribution build (`pnpm build`), temporary packed-package
installation (`pnpm check:packages`), generated compatibility evidence
(`pnpm check:compatibility`) and tracked-file size gate (`pnpm check:large-files`).
All pass on the current worktree. The package check installs the newly built
artifacts in its temporary environment; it is not merely a workspace import test.

Compatibility initially failed because its decoded-cache evidence still named the
old test asserting that constrained balanced caches reject decoded pages. Updated
`docs/compatibility.json` to reference the actual small-page admission test matching
the implemented fractional policy. The feature claim and generated compatibility
table did not change. All 17 decoded-cache tests pass, and the metadata edit passes
lint. Package/size checks initially hit sandbox subprocess restrictions; they passed
when rerun with the required execution permission, without changing either gate.

These refreshed gates do not waive the 20 known external conformance failures,
replace final coverage/reference/fixture runs, or close the M1/M2/V1 work. Release
acceptance remains incomplete.

### V3 current conformance causes and shared failure clusters (November 13, 2026)

Reran the external conformance suite on the current implementation: **465 passed,
20 failed, one skipped**. No expectations, exclusions or support claims changed.
Added `bench/conformance-failure-causes.mjs` to reproduce the existing failing files
through both readers, expose nested error causes and verify owned-pool cleanup.
Its report is `conformance-failure-causes-2026-11-13.json`; it is diagnostic evidence,
not a substitute for the full gate. All owned failures release their 64 MiB pool.

| Shared cause | Failed cases across both readers | Current evidence / implementation boundary |
| --- | --- | --- |
| Precision above 15 decimal rejection | 10 | `fixed_length_decimal` and shredded variant 026–029 hit the same schema guard. Removing it alone would expose lossy conversion; exact decimal representation/conversion and its allocation contract must be resolved together. |
| ALP encoding unsupported | 2 | The vendored fixture documents ALP; current decoder reports unsupported encoding `undefined`. Requires encoding dispatch and bounded decoding, not a fixture exclusion. |
| BSON conversion unsupported | 2 | Current conversion throws `parquet bson not supported`. Requires a defined value representation and bounded conversion. |
| BROTLI codec absent | 2 | Current compressor registry has no BROTLI implementation. A decoder must meet the cross-runtime workspace contract. |
| Dictionary page extent | 2 | `nation.dict-malformed` reaches an invalid typed-array length in page reading. The filename alone is not sufficient evidence to classify it as an expected rejection. |
| Shredded variant decoding | 2 | Case 030 reaches a DataView bounds error inside variant object decoding. Requires an independent expected-value/format investigation. |

The largest shared cluster is decimal handling, not five unrelated fixture bugs.
Any fix must preserve exact values and memory admission; bypassing the precision
guard or reducing the conformance corpus would not satisfy acceptance. These are
still release failures. No production behavior changed in this diagnostic pass.

### V3 general value-checked scalar decimal capability (November 13, 2026)

The precision-25 fixed-width conformance fixture stores small coefficients (100,
200, …, 2400 at scale two), despite its wider declared precision. Existing raw
scalar decimal decoding and `decimalConverter` already support signed big-endian
byte arrays with incremental safe-integer checks, including long sign extension.
The schema capability was narrower than that implementation: it allowed checked
high-precision INT64 leaves only.

Renamed the internal capability to `allowValueCheckedScalarDecimals` and applied it
to top-level INT32, INT64, BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY decimal leaves. All
callers use the same capability. This changes admission by physical shape, not by
fixture name or declared-precision exceptions. Each value still passes the existing
coefficient check before scaling; unsafe coefficients still reject. Nested decimal
leaves remain excluded because the checked normalization route is scalar-only,
an actual conversion boundary rather than a feature-stage shortcut.

The new real-fixture test checks every expected value 1–24 through both public
readers and owned batch sizes 1/7/64, with pool limits and zero final reservations.
Existing unsafe-value, signed-byte conversion and nested-schema rejection tests
continue to pass. Vendored shredded-variant expected values 026–029 contain
coefficients such as 123456789987654321, outside this safe-number contract; those
remaining failures require exact representation/conversion work rather than a
schema-guard bypass.

Conformance now reports **467 passed, 18 failed, one skipped**, with no exclusions
changed. Current inventory/cause reports are refreshed; preceding reports are
archived with `before-scalar-decimal` in their filenames. Validation also passes
2,660 default tests (598 skipped), all 49 Workers tests, core build, typecheck and
lint. This resolves two release failures while preserving the precision guard for
unsupported values; the overall conformance gate and broader goal remain open.

### V2 decoded fixed-width decimal boundary evidence (November 13, 2026)

Expanded `scalar-decimal-fixture.test.ts` to verify the widened schema capability
at the actual coefficient boundary through both public readers. Tests preserve the
real fixture's uncompressed page layout, uniquely locate the first two known PLAIN
coefficients and replace the first 11-byte signed coefficient with each of
±(2^53−1) and ±2^53. The reads are unfiltered: the original footer statistics are
not asserted to describe the modified value and are not used for pruning.

Both readers return the expected once-scaled value for safe coefficients and all
remaining rows unchanged. Both reject unsafe coefficients with the typed
`decimal-value-precision` error. The owned reader uses batch size one, stays within
its 1 MiB pool and returns to zero reservations on success and rejection. All five
fixture tests pass. No production code or conformance expectations changed in this
pass; the conformance gate still has 18 unresolved failures.

### F3/M1/V1 owned top-k output and mixed retained-output matrix (November 13, 2026)

The new mixed public-query harness exposed a concrete missing integration:
`orderBy(...).limit(...).run().toOwnedArray()` rejected with “Execution path lacks
owned row materialization capability”. The bounded top-k operators already owned
their candidates; the public collector did not expose final materialization ownership.

The public owned-output route now uses the existing bounded columnar top-k path
and transfers the final independently admitted row owner to the caller. Projection
descriptors are admitted before construction; empty output also has an owner.
Existing unowned callers still receive ordinary arrays. Explicit capability checks
require an owned scanner, a bounded limit and supported filter shape, and exclude
window/distinct/Hive/projection-expression paths lacking this integration. The owned
route bypasses the borrowed deferred top-k implementation; owned deferred sorted
reads and broader full-sort/fallback support remain unfinished. No new sorting
algorithm or query-specific reducer was introduced.

Tests cover descending order, offset, projection, retained independent handles,
empty results, admission failure and zero final reservations on a real written
Parquet fixture. The focused owned-output/top-k suite passes 18 tests. Full default
validation passes 2,666 tests (598 skipped), all 49 Workers tests, core build,
typecheck and lint.

`bench/mixed-query-memory.mjs` writes a 2,048-row Parquet fixture with 4,096-byte
strings (8,408,467 encoded bytes). It retains a prior collected output charging
17,406,400 bytes, then starts four overlapping public jobs: full collection,
filtered collection, bounded descending top-k and aggregate sum. Every successful
row/result is checked against independent input-derived expectations. Jobs share
two-read transport control, 32 MiB per-query caps and a 64/128 MiB root pool.
The matrix covers memory and local HTTP, 4/16 MiB shared raw caches, and cold/warm
rounds: all 64 jobs complete. All job owners release, cache eviction preserves the
prior output, and releasing that final pin returns the pool to zero.

| Transport/cache | Peak admitted bytes | Cold HTTP requests / bytes | Warm HTTP requests / bytes |
| --- | --- | --- | --- |
| Memory / 4 MiB | 48,628,722 | n/a | n/a |
| Memory / 16 MiB | 53,422,588 | n/a | n/a |
| HTTP / 4 MiB | 49,161,418 | 37 / 8,471,464 | 37 / 8,471,464 |
| HTTP / 16 MiB | 53,955,284 | 37 / 8,471,464 | 4 / 8 |

Peaks are identical for the two root-cap settings in this workload. The fitting
cache's warm traffic is asserted to consist only of the four discovery probes;
the smaller cache thrashes and offers no warm traffic reduction. This is evidence
for a real cache-capacity tradeoff, not an automatic recommendation to raise caps.

`mixed-query-memory-2026-11-13.json` includes outcomes, pinned/cached reservations
and separate Node heap/external/RSS samples after each round. Runtime samples
substantially exceed admitted bytes and include fixture generation, server backing,
previous rounds and delayed GC; they are neither peaks nor whole-Worker cap evidence.
This advances mixed-operator local/HTTP acceptance but does not close constrained
workerd/production-cloud, cancellation/fairness or all remaining allocation paths.


### F3/V2 stable owned top-k through public Parquet queries (November 13, 2026)

Expanded the public owned-output fixture suite with 64 rows spread over eight
Parquet row groups, repeated primary/secondary keys and NULL primary keys. At batch
sizes 1, 7 and 64, queries exercise both directions, both NULL placements, descending
secondary keys, filtering, hidden sort columns, offset/limit, out-of-range offsets
and zero limits. An independent comparator uses input row IDs only as an encounter
order tie breaker; the query's order specification does not contain that ID key.
Every projected row matches the oracle and every output owner releases its pool.

The 15-test owned-output fixture suite passes, along with typecheck and changed-file
lint. Refreshed the independent reference gate on the current production revision:
all 104 tests pass. No production code changed in this pass. The added evidence
covers stable merging and pagination through the public owned route; it does not
close owned deferred top-k, unbounded sort or the broader runtime-memory contract.

### M1/M2 owned top-k cancellation with a pinned sibling (November 13, 2026)

Added a real-Parquet cancellation regression that retains output from a completed
sibling query, starts an owned sorted query in the same pool, and aborts after the
first yielded scan batch has been consumed into top-k candidates. The test confirms
additional candidate memory was live, the scanner iterator closes, the query rejects
with `LAKEQL_ABORTED`, and pool usage returns exactly to the pinned sibling baseline.
The sibling rows remain readable and releasing their final owner returns usage to
zero. This exercises interruption after work begins rather than only a pre-aborted
request. Raw scan caching is disabled to make the expected remaining owner exact.

All 16 public owned-output tests pass, along with typecheck and changed-file lint.
No production change was needed. This adds cancellation evidence for the local
public owned top-k route; simultaneous pending HTTP/R2 reads, fairness and broader
operator cancellation still require their separately scoped evidence.


### V3 coverage and reproducible fixture refresh (November 13, 2026)

The current `pnpm coverage` gate passes all configured thresholds: statements
91.48% (minimum 90), branches 85.21% (minimum 84), functions 95.73% (minimum 90),
and lines 93.62% (minimum 90). The instrumented suite passes 2,670 tests, with 598
skipped, across 288 passing files and nine skipped files. Thresholds and exclusions
were not changed.

After that suite completed, `node scripts/check-fixtures.mjs` regenerated the
repository fixtures and confirmed that content hashes match the pre-generation
snapshot. Running generation after the suite avoided changing fixture files during
active reads. A compact checkpoint is archived in
`release-gate-refresh-2026-11-13.json`.

This refresh verifies coverage and fixture idempotence only. External conformance
still fails 18 cases, and the documented M1/M2/V1 integration gaps prevent release
completion regardless of these passing gates.

### F3/M1 owned candidate selection boundary for deferred ranking (November 13, 2026)

Inspection of the borrowed deferred top-k path confirms that its count-bounded
`RankedRowRef` heap does not own candidate payloads or account for replacement
overlap. Added an internal `OwnedBoundedTopK` wrapper around the existing stable
heap. It admits directory/candidate descriptors, retains input ownership before
mutation, releases rejected/evicted candidates immediately, and admits final
ordering/output arrays before transferring independent winner ownership. Comparator
failure closes the selector because a comparator can interrupt heap mutation.
Output-admission failure also releases selector state while preserving independently
held caller inputs. `BoundedTopK.clear()` drops references without sorting or
allocating, supporting deterministic failure cleanup.

Tests verify stable winners across 100 offers, retained-memory plateau after the
heap fills, eviction cleanup, output lifetime beyond parent disposal, comparator
failure and output-admission failure under sibling pressure. The six owned/plain
heap tests, core build, typecheck and changed-file lint pass.

This is an internal ownership primitive, not completed deferred-read integration.
The existing public deferred top-k path is unchanged. Ranking-key materialization,
window planning, gathered output ownership and cloud/local read tradeoffs must be
connected and verified before this closes the deferred top-k allocation gap.

### Regular PR preparation boundary (November 13, 2026)

Prepare the completed changes as one regular PR; size alone is not a reason to
split the shared ownership and execution contracts. The preceding deferred-ranking
primitive entry records exploratory work, not code included in this PR. Its three
standalone helper/test files and the single-row materialization and heap-clear
additions have been preserved outside the proposed PR in
`/tmp/lakeql-deferred-wip-46s30jfs`, with full snapshots, SHA-256 hashes and a
reviewable restoration patch. This local archive is not a durable repository
artifact. Deferred owned ranking remains open.

After removing those unused additions, the full build and nine materialization
and heap tests pass. The previous complete coverage checkpoint remains 2,670
passing tests; final PR-wide verification still needs refreshing. External
conformance remains at 18 failures, and the complete audit goal remains active.

The PR-preparation refresh now passes the full distribution build, lint,
typecheck, compatibility, tracked-file size and package-install checks; 2,670
default tests (598 skipped), 49 Worker tests and 104 reference tests pass.
Coverage passes unchanged thresholds at 91.48% statements, 85.21% branches,
95.73% functions and 93.62% lines. Fixture regeneration is hash-identical.
The 16 public owned-output tests also pass separately. This refresh excludes the
archived unfinished helpers and does not resolve the external conformance gate
or the broader outstanding allocation and scheduling requirements.

### Deferred ranking follow-up after PR #21

PR #21 contains the validated checkpoint without the unfinished deferred-ranking
helpers. Follow-up work restores those helpers in the working tree to continue
M1/F3 integration. Single-row materialization admits one selected row without a
batch-sized mask or scanning unrelated rows. Owned candidate selection admits
replacement overlap and transfers independent winner ownership. Added tests prove
that cross-budget admission failure preserves the source owner and releases all
target reservations, and zero-capacity selection retains no candidate payload.
Heap swaps now use scalar references rather than constructing temporary arrays.
All 17 focused heap/materialization tests pass. Public deferred-read routing and
window/gather ownership remain unimplemented; this is prerequisite evidence, not
closure of the deferred-ranking requirement.

Deferred ranking now selects the owned candidate heap when its scanner supplies
owned batches, reserves scalar materialization before constructing each candidate,
and retains ordered references through late-column gathering. Predicate masks are
scoped on this route. The shared scan-input adapter preserves legacy column-batch
fallback. The query, public owned-output and owned-heap suites pass 102 tests after
that fallback correction. This covers the ordinary returned-row deferred route's
ranking phase; final gather/map/projection allocations are still not fully owned,
and `toOwnedArray` continues using its eager owned route. Whole deferred-output
ownership and real-fixture boundary/traffic measurements remain required.

Real-fixture follow-up and serialized-limit regression: see
`docs/validation/deferred-ranking-followup-2026-11-13.json` (22 tests pass).

### 2026-11-14 — admitted Hive partition parsing

Extracted the shared Hive parser into `hive-partitions.ts` with an optional owning
scope. Segment traversal no longer allocates a path-wide split array and scans
only within each segment. Scoped parsing admits the output record, entry keys and
decoded values, and encoded-value overlap before allocation. Failed decoding or
admission releases partial output while preserving caller-owned siblings. Duplicate
keys retain last-value semantics; conservative storage charges include overwritten
entries. Prototype-named keys are defined as own data properties.

Validation: `pnpm exec tsc -b` passed; targeted Hive and query suites passed 87 tests.
New checks cover Unicode decoding, embedded equals signs, duplicate keys, malformed
percent escapes, prototype names, exact peak capacity and one byte below, rollback,
and 10,000 non-partition path segments without increased admitted storage. This is
a task-planning prerequisite: callers do not yet pass the scope, and queued task
ownership remains unfinished. These checks do not establish cloud runtime memory
bounds or replace the final full-suite and runtime gates.

### 2026-11-14 — queued task ownership and window execution

Added `planOwnedTasks()` to query builders/results and a shared owned task planner.
Task arrays/records, copied range records, scoped Hive partitions, paths/etags,
shared projected columns, detached residual predicates, and scoped window topology
are admitted before retained construction. Per-object physical-column lists and
callback arguments remain charged through asynchronous planner settlement,
including cancellation. Plain `planTasks()` transfers storage out of accounting
at return. Window task execution holds the task owner through both legacy and
owned executor callbacks and releases it on success, failure, cancellation, or
unavailable-topology fallback.

New tests prove exact capacity and one-byte-below rejection, shared predicate and
column identity across tasks, independence from mutated predicate binary backing
and adapter ranges, and delayed planning/execution cleanup on all three outcomes.
A generated real Parquet fixture verifies two simultaneously retained plans,
cold/warm encoded-read reuse, cache eviction while tasks remain pinned, exact
measured capacity, and final zero reservations. The fixture's earlier capacity
covered only adapter workspace; the revised assertion includes query metadata.

Remaining boundaries: object discovery lists, explain/manifest output ownership,
adapter-internal returned plan workspace, and total runtime-memory headroom still
need their separate audit evidence. These changes do not establish production
Cloudflare Worker caps or remote spill atomicity.

Verification after integration: build and typecheck passed; the full default suite
passed 3,550 tests with 629 skips. Focused task-lifetime tests passed 7 and the
Parquet adapter suite passed 25. Evidence:
`docs/validation/queued-task-ownership-2026-11-14.json`.

### 2026-11-14 — fingerprint encoding and admitted serialization

Fingerprint hashing now emits UTF-8 bytes directly into the existing FNV-1a hash,
avoiding the full TextEncoder byte-array copy. Compatibility tests compare against
TextEncoder across every UTF-16 code unit, supplementary pairs, lone surrogates,
empty strings, embedded nulls and large Unicode inputs. Existing manifest tests
preserve identifiers and fingerprint semantics. `fingerprint(value, scope)` adds
transactional stable-serialization admission and constant live hash workspace;
only the 19-character digest remains charged in the parent. Exact-capacity and
one-byte-below tests verify cleanup and sibling preservation.

Build/typecheck passed; focused tests passed 28; full suite passed 3,561 with 629
skips. `bench/utf8-fingerprint.mjs` compares both kernels after warmup, alternating
execution order. Recorded large ASCII medians were 9.07 ms encoded / 3.83 ms
incremental, and large Unicode 7.55 / 0.75 ms. The small Unicode case regressed
from 0.013 to 0.064 ms. These are kernel samples, excluding serialization and
runtime memory measurements; the removed encoded buffer extent reaches 589,824
bytes in this matrix. Evidence: `docs/benchmarks/utf8-fingerprint-2026-11-14.json`.
Manifest construction still needs scoped normalization and ownership integration;
the optional fingerprint scope alone does not close that boundary.

### 2026-11-14 — admitted manifest construction and retained task inputs

Added query `ownedTaskManifest()` and `createOwnedTaskManifest()` for lower-level
owned task lists. A manifest retains its input owner independently, admits sorted
range/column/partition containers, window descriptors, task identifiers, snapshot
and plan fingerprints, temporary serialization and sorting, and the manifest
records. Input backing is shared under an explicit no-mutation lifetime contract.
The plain query manifest method uses the same admitted path and transfers output
out of accounting at return. Direct legacy `createTaskManifest()` remains an
unscoped utility for caller-controlled inputs.

Manifest construction reuses the normalized task list for plan fingerprinting
instead of allocating another full list, and normalization removes the redundant
range spread before mapping. Prototype-named partition keys remain own data
properties. Tests verify identifier equality with legacy construction, range
ordering, binary predicate lifetime after the original task handle is released,
exact peak capacity and one-byte-below rollback, caller handle preservation on
failure, and plain-result transfer. Object discovery and explain ownership remain
open, as do final runtime and release gates.

Build/typecheck and the full suite passed: 3,563 tests, 629 skips. The existing
real-Parquet serialized golden manifest passes unchanged. Evidence:
`docs/validation/owned-task-manifest-2026-11-14.json`. Internal `slice()` still
uses a plain manifest while scanning and needs a retained lifetime in follow-up.

### 2026-11-14 — slice manifest lifetime and shared cancellation check

`slice()` holds an owned manifest through bookmark validation, row scanning, and
bookmark construction, releasing it in `finally`. Tests cover delayed scan
success/failure/cancellation, early page termination with iterator cleanup,
terminal resumed output, and stale-bookmark rejection before scanning.

The delayed cancellation test exposed a pre-existing shared query-budget gap:
`enforceBudget()` checked resource counters but not the abort signal, allowing a
non-cooperative row scanner's post-abort batch to produce output. The shared
check now calls `throwIfAborted`, covering every existing enforcement site rather
than adding a slice-specific exception. This does not claim that an external
scanner can be forcibly interrupted or that every no-output iterator completion
path has been audited. Returned slice rows/bookmark ownership and broader object
discovery/explain lifetimes remain separate open work.

Validation: build/typecheck passed; focused suites passed 107 tests; full suite
passed 3,567 with 629 skips. Evidence:
`docs/validation/slice-manifest-lifetime-2026-11-14.json`.

### 2026-11-14 — cancellation at scan advancement and empty completion

Added a shared checked async-iteration boundary for row scans, projected borrowed
and owned vector scans, physical column inputs, and window scan inputs. It checks
abort before initial advancement, after each asynchronous result (including empty
completion), and before requesting another value after consumer handoff. Owned
sources supply a discard callback to release a result rejected before handoff.
In-flight advancement is awaited rather than detached; source iterator cleanup
runs before the rejection completes.

Focused tests cover delayed empty completion, delayed values, post-abort owned
allocations, cancellation while the consumer holds a value, and early consumer
return. The slice regression now also covers a delayed scanner that returns no
rows. These changes preserve the existing request scheduling; they do not prove
that arbitrary external adapters stop ongoing I/O immediately. Broader output
ownership and runtime memory acceptance remain open.

Queries with no abort signal return the original iterable without an extra async
generator; an identity test verifies that fast path. Final build/typecheck passed;
full suite passed 3,573 tests with 629 skips. Evidence:
`docs/validation/scan-completion-cancellation-2026-11-14.json`.

### 2026-11-14 — preserve cached transport hints and admit metadata copies

Object discovery cache cloning dropped `rangeReadMode`, so a warm plan could lose
its observed native-versus-emulated range hint. Extracted a shared metadata clone
that preserves all declared `ObjectInfo` fields, independently copies mutable
Dates, and supports scoped admission of list/record/string/date storage before
copying. The cache path now uses this helper. Its scoped form rolls back partial
copies without releasing caller siblings and passes exact-capacity/one-byte-below
tests.

Cold/warm query tests exercise both HEAD and listing discovery for native and
emulated hints, require one discovery across two plans, and inspect what the scan
planner receives. These tests simulate adapter metadata; they are not additional
production cloud measurements. Scoped copying is a prerequisite for owned object
discovery, not a claim that current cache/list collection is fully admitted. The
existing externally supplied planning-cache lifetime remains caller-controlled.

Build/typecheck passed; focused suites passed 90 tests; full suite passed 3,579
with 629 skips. Evidence: `docs/validation/object-info-transport-copy-2026-11-14.json`.

### 2026-11-14 — discovery cancellation and consistent file limits

Discovery checks cancellation before work and after asynchronous cache get/set and
HEAD settlement. Listings use the shared checked iterator, including empty
completion and iterator cleanup. Matching listings reject the first over-limit
object before appending it to retained results. Uncached exact-path planning now
enforces `maxFiles: 0`, matching cached, glob and prefix discovery; previously the
exact-path task planner could return one task at that limit.

Delayed HEAD, empty/value LIST, cache get and cache set tests verify cancellation
before the scan planner runs. Limit tests cover ignored non-matching files,
listing cleanup on overflow, and cold/warm exact/glob/prefix paths at zero and one
file. Cache operations already in flight may complete their side effects before
cancellation is observed; the caller still receives abort and planning does not
continue. Discovery-list allocation ownership remains unfinished.

Build/typecheck passed; focused suites passed 93 tests; full suite passed 3,588
with 629 skips. Evidence: `docs/validation/discovery-boundaries-2026-11-14.json`.

### 2026-11-14 — admitted discovery collection and initial execution integration

Discovery now admits retained list slots and independent object metadata while
listing, copies metadata from cache under admission, admits HEAD conversion
overlap, reserves sorting workspace, and holds admitted cache-write copies until
cache settlement. Hive pruning admits its result list and transient parsed
partition records. Listing admission failure closes the source without consuming
the remaining objects. Task planning, ordinary row projection, columnar projection,
row matching/sorting/top-k collection, and columnar top-k planning retain discovery
charges in their existing scopes. Task discovery stays live until task construction
settles, then releases independently of the returned task owner.

The new charges intentionally change intermediate lifetime assertions: scanner
shutdown can still reference its object metadata, so metadata remains charged
while previously accumulated output is released. Tests now capture that baseline,
verify the remainder of the pool can be used by a sibling during shutdown, and
still require zero final reservations. Tight fixtures targeting later column or
candidate admission now provide enough capacity to reach those stages. Shared
query tests distinguish live reservations from a configured child ceiling.

This is not complete object-plan ownership. Deferred limit/aggregate, window and
some physical input/explain paths still transfer discovered metadata out of
accounting at plan return. Sidecar planning containers and adapter-added schema
plans need separate admission and ownership; cloning only declared ObjectInfo
fields would incorrectly discard Parquet schema-evolution metadata. Existing
external cache retention remains caller-controlled after set() settles. These
boundaries remain required follow-up rather than accepted whole-query coverage.

Build/typecheck passed; full suite passed 3,590 tests with 629 skips. Evidence:
`docs/validation/discovery-admission-2026-11-14.json`. Signal-bound parent scopes
also need a cancellation lifetime audit before claiming every integrated path
holds metadata through in-flight cleanup.

### 2026-11-14 — independent discovery lifetimes across query routes

Deferred limit/aggregate reads, physical column inputs, row matching/ordering/sort
collection, and all window collection routes now use discovery scopes independent
of cancellation-bound operator state. Scopes release in `finally` after in-flight
scan/iterator cleanup settles. Explain construction retains discovery and its task
owner through result construction. The private planner now requires a receiving
scope; no internal query call silently transfers discovered storage at return.

Delayed ordered-row, deferred-column and row-window tests use long object paths
to prove metadata remains charged after abort and while callbacks still reference
it, then require zero reservations after settlement. They exposed that window
scans did not receive discovered ObjectInfo at all. Row and vector window options
now share one helper that carries the object, budget and timing fields, preserving
transport/version metadata while varying only batch size. Existing failed-deferred
output cleanup assertions now subtract captured planning storage while still
proving prior output payloads are released before scanner shutdown.

Sidecar planning containers, adapter-added schema plans, discovery parser/auxiliary
workspace and external cache retention still need separate admission contracts.
Explain's returned presentation data and slice outputs also remain outside owned
output APIs. This integration does not close the full M1/M2 or runtime acceptance
requirements.

Final build/typecheck passed; full suite passed 3,593 tests with 629 skips.
Evidence: `docs/validation/discovery-scan-lifetime-2026-11-14.json`.

### 2026-11-14 — owned adapter object-plan contract

Added optional `ScanAdapter.planOwnedObjects`, preferred over legacy `planObjects`.
It receives a borrowed input owner and the query budget and returns an owner for
its result. Adapters retain input backing referenced by their returned plans.
The query keeps the input handle through callback settlement, adopts the result
into discovery lifetime, and releases both handles on callback failure,
cancellation before handoff, or incompatible-budget rejection. Adapter-specific
metadata survives rather than being discarded by a generic ObjectInfo clone.

Delayed success/failure/cancellation tests verify that added metadata remains
charged and reaches scanner options only on success; incompatible-budget output
is rejected and released without leaking either pool. The built-in Parquet
adapter still uses the legacy planner: its recursive schema-shape/signature work,
per-file descriptors and shared schema sets must be admitted before advertising
this owned capability. This contract alone does not close that implementation gap.

Build/typecheck passed; full suite passed 3,597 tests with 629 skips. Evidence:
`docs/validation/owned-object-plan-contract-2026-11-14.json`.

### 2026-11-14 — iterative admitted Parquet schema-shape construction

Extracted schema-shape construction from the scan adapter. It now creates one
shape record per schema node and uses an explicit traversal stack, eliminating
recursive calls and the prior per-child shape-record spread copy. Optional scoped
construction admits output records, child slots, stack capacity and live frames;
frames release when popped, and failures roll back output/scratch without touching
caller siblings. Element values remain borrowed from metadata, which the caller
must keep live. The existing descriptor path uses the shared builder.

Tests preserve nested shape fields and root-versus-child name behavior, build a
20,000-level synthetic shape without call-stack overflow, and verify exact peak
capacity and one-byte-below rollback. The deep test covers only this builder:
Parquet tree construction and signature serialization are separate remaining
boundaries. Scoped shape construction is not yet wired into an owned Parquet
planner; descriptor/signature storage and multi-file plan ownership remain open.

Final build/typecheck passed; focused suites passed 28 tests; full suite passed
3,600 with 629 skips. Evidence: `docs/validation/parquet-schema-shape-2026-11-14.json`.

### 2026-11-14 — iterative admitted schema-signature serialization

Replaced recursive schema-signature serialization with an explicit traversal
stack and chunk builder. It avoids constructing a complete serialized subtree at
each recursive level. Scoped use admits frame/key directories, key sorting,
escaped scalar strings, chunk slots, and final joined-output overlap. Only the
final signature is retained in the parent; failures dispose temporary traversal
and output ownership. Ancestor tracking rejects cycles while permitting repeated
references to an acyclic shared value.

Tests compare canonical signatures against the prior implementation for nested
schema fields, escaped keys, Unicode/surrogates, undefined fields/array entries,
prototype-named keys, and numeric edge cases. A 20,000-level nested JSON-shaped
schema input serializes without recursive stack overflow. Exact-capacity and
one-byte-below tests verify rollback, including cyclic and unsupported-bigint
failures. These checks cover serialization, not arbitrary Parquet tree creation.
The adapter now uses the iterative serializer, but scoped descriptor construction
and owned multi-file planning integration remain required follow-up.

Build/typecheck passed; focused suites passed 28 tests; full suite passed 3,603
with 629 skips. Evidence: `docs/validation/parquet-schema-signature-2026-11-14.json`.

### 2026-11-14 — owned multi-file Parquet schema planning

Parquet now implements the owned object-plan contract. The output retains its
input owner and admits shared unified column names, per-file presence sets,
planned object records and schema-plan records. Temporary descriptor directories,
compatibility maps, schema shapes, canonical signatures and sorting overlap are
admitted before construction. A shared planner backs both owned query calls and
legacy plain calls; the latter transfer results to caller management at return.

Planning no longer invokes the general Parquet tree builder, which copied a full
root-to-node path array per node. A small iterative topology builder supplies only
element/children data needed by descriptors and validates missing, extra and
invalid child counts. Descriptor tests compare against the previous tree's
canonical signatures and verify exact capacity and transactional rollback.

Generated two-file Parquet fixtures verify independent input retention, exact
planning capacity and one-byte-below rejection, and actual schema-evolution reads
that fill missing columns with nulls. Object-plan options now carry the abort
signal; a delayed owned-read fixture confirms that cancellation waits for the
current file's metadata operation and prevents planning the next file, with zero
reservations after cleanup. These are memory-store fixtures plus existing full
suite cloud/HTTP regressions, not a new production-cloud memory-cap measurement.

Sidecar planning, external cache retention, auxiliary discovery/adapter runtime
allocations, output APIs and final constrained-runtime acceptance remain open.

Build/typecheck passed; targeted suites passed 33 tests; full suite passed 3,611
with 629 skips. Evidence:
`docs/validation/owned-parquet-object-planning-2026-11-14.json`.

### 2026-11-14 — sidecar pruning without copied file metadata

The shared pruning helper now accepts a separate index resolver and optional
container-admission scope. Query planning no longer spreads sidecar fields onto
new object records. Original object identity and transport/version metadata are
preserved, while missing indexes remain conservative. An already ordered input
avoids the previous sorting copy; unordered inputs retain deterministic sorted
results without mutating the caller's array. Sorting workspace, result lists and
records, query lookup entries and pruning arguments are admitted.

The query lookup directory is seeded only with discovered paths, then filled from
matching sidecars. Duplicate index paths preserve last-entry semantics. Unrelated
sidecars no longer create lookup entries: a 10,000-unrelated-index test has the
same measured query peak as the zero-unrelated-index case. Caller-supplied index
storage is external; the test concerns internally allocated lookup storage.

Tests cover sorted/unsorted exact-capacity boundaries, one-byte-below rollback,
lookup failure cleanup, original file identity, missing-index conservatism and
query integration. Spatial literal parsing and expression workspace remain open;
this closes sidecar container construction, not every sidecar allocation or the
whole runtime-memory contract.

Build/typecheck passed; targeted suites passed 105 tests; full suite passed 3,615
with 629 skips. Evidence:
`docs/validation/sidecar-planning-admission-2026-11-14.json`.

### 2026-11-14 — shared admitted spatial literal parsing

Sidecar pruning lazily caches parsed JSON literals for one pruning operation.
Repeated files and repeated literal text share the parsed tree; missing indexes
and short-circuited expressions do not trigger parsing. Malformed reached literals
still fail, with partial planning allocations released. The directory and trees
are scratch allocations, released before the result is handed off. Bbox validation
uses the parsed record directly instead of copying four fields for every file.

The shared owned JSON parser now accepts borrowed UTF16 text as well as UTF8
bytes. Text input forecasts parsed storage directly without an intermediate
UTF8 encoding or decoded text copy. The caller retains the input text. Tests
cover Unicode, escaped surrogate input, exact capacity and one-byte-below failure,
100 files sharing one JSON parse, lazy malformed literals and failure rollback.
Expression traversal and repeated spatial shape validation remain separate open
work; these checks do not prove whole-runtime memory bounds.

Build/typecheck passed; focused tests passed 26; full suite passed 3,618 with 629
skips. Evidence: `docs/validation/sidecar-literal-preparation-2026-11-14.json`.

### 2026-11-14 — allocation-free sidecar comparisons and safe index names

Comparison pruning now passes borrowed column names and scalar values directly
to a shared comparator, eliminating one temporary pair object per file and
comparison. This inspection exposed a correctness bug: literal-left inequalities
were evaluated in the column-left direction. Reversing lt/lte/gt/gte preserves
matching files and handles exact boundaries; eq/ne remain symmetric.

All sidecar column directories now resolve own properties only. Inherited
constructor/toString/__proto__ values are absent metadata and conservatively
retain files. Min/max index construction uses a null-prototype directory so
explicit columns with these names are preserved. Tests cover all six reversed
comparisons, ordered equality boundaries, missing prototype-named indexes across
min/max, bbox and H3, and explicitly indexed prototype-named scalar columns.
Expression traversal workspace and repeated spatial shape validation remain open.

Build/typecheck passed; focused suites passed 98 tests; full suite passed 3,620
with 629 skips. Evidence: `docs/validation/sidecar-comparison-correctness-2026-11-14.json`.

### 2026-11-14 — release checks after owned discovery and sidecar planning

Source d764e98 passed Worker (75), reference (104), required external conformance
(520, one inactive missing-fixture guard), coverage (3,620 passed / 629 skipped),
public build, clean npm consumer, deterministic fixture regeneration, lint,
compatibility, file-size and changeset checks. Typecheck and the full test suite
also passed on this source. Coverage is 91.26% statements, 85.34% branches,
95.77% functions and 93.08% lines; thresholds were not changed.

Evidence: `docs/validation/current-planning-release-gates-2026-11-14.json`.
These gates validate current implementation behavior but do not close remaining
allocation gaps or replace constrained-runtime benchmark refresh and the full
F1–F6/M1–M2/V1–V3 acceptance review.

### 2026-11-14 — current planning mixed-query and Worker runtime matrix

Source 30f7ae8 completed all eight mixed-query scenarios: local memory/HTTP,
64/128 MiB pools, 4/16 MiB caches, two cold/warm rounds with four overlapping
queries and pinned prior output. Maximum admitted allocation increased from
53,954,370 to 53,956,046 bytes. HTTP concurrency remains two; fitting warm caches
require four discovery reads, while smaller caches require 37 reads per round.
Every scenario releases all reservations. This is one run, not a latency claim.

Local workerd native-emulated-R2 and separate localhost-HTTP runs under verified
64 and 128 MiB V8 managed-heap settings completed all scenarios. Every transport
and runtime configuration retained the same success/budget outcomes: one job
succeeds at either pool size; four jobs yield two successes/two budget rejections
at 64 MiB, and four successes at 128 MiB. Final eviction leaves zero reservations.

Evidence: `docs/validation/current-planning-runtime-2026-11-14.json`. Raw heap,
backing and embedder samples remain separate in the artifacts. Sampling is sparse
and cannot establish exact peaks. Managed-heap flags exclude external backing
and are not production Cloudflare total-memory limits; V1 remains open.

### 2026-11-14 — iterative admitted sidecar logical traversal

Sidecar predicate evaluation no longer recurses through logical expressions or
creates every/some callbacks for logical nodes on each file. An explicit frame
stack grows only to the reached logical depth. Frame records and slots are
admitted before allocation and reused across files; the entire workspace is
released with pruning scratch. Short-circuit evaluation and empty AND/OR identities
are preserved, and unreachable spatial literals remain unparsed.

A 20,000-level predicate over two different file indexes verifies stack safety,
frame reuse, exact-capacity success and one-byte-below rollback. Nested mixed
AND/OR tests verify empty operands and malformed short-circuited literals. This
closes recursive logical traversal storage for valid expression trees; repeated
spatial shape validation, caller expression storage and broader allocation gaps
remain separate work.

Build/typecheck passed; focused suites passed 111 tests; full suite passed 3,622
with 629 skips. Evidence: `docs/validation/sidecar-logical-workspace-2026-11-14.json`.

### 2026-11-14 — cached spatial literal shape validation

Prepared sidecar literal records now retain validated bbox/string-array views
alongside the admitted parsed tree. H3 array type validation runs once per literal
text per pruning invocation rather than once per indexed file; invalid shapes
are cached as conservative non-pruning results. The prepared record is admitted
and released with the existing scratch scope. Parsing remains lazy, shared
between functions, and scoped to a single pruning operation.

Operation-count tests verify one array validation across 100 files for both
valid and rejected shapes. Existing exact-capacity, malformed JSON rollback,
short-circuit and Unicode tests continue to cover the shared preparation path.
Per-file index validation and membership comparisons still depend on each file;
this change does not claim constant-time H3 membership or whole-runtime bounds.

Build/typecheck passed; focused suites passed 112 tests; full suite passed 3,623
with 629 skips. Evidence: `docs/validation/sidecar-prepared-shapes-2026-11-14.json`.

### 2026-11-14 — owned planning-cache entry contract

CacheAdapter now exposes optional getOwnedEntry/setOwnedEntry methods, distinct
from existing format-specific metadata-cache methods. Discovery prefers owned
reads and writes when a query scope exists. Returned entries must share the
query pool; ownership lasts through the metadata copy and releases on all exit
paths. Cache writes borrow an admitted entry and metadata until their callback
settles, including cancellation. Persistent retention requires the adapter to
retain ownership in its shared budget and release on eviction. Cache-entry
wrapper allocation is admitted alongside copied metadata.

Tests demonstrate retained storage across two queries after source deletion,
release on eviction, read cancellation/foreign-pool/file-limit rejection, and
write success/failure/cancellation with delayed callback settlement. These tests
exercise an in-process adapter contract, not remote-cache transport behavior.
Concrete generic memory/remote adapter integration, eviction policy and remote
serialization admission remain open; legacy adapters remain outside retained
allocation accounting. No whole-runtime bound is inferred from this interface.

Build/typecheck passed; focused suites passed 102 tests; full suite passed 3,630
with 629 skips. Evidence: `docs/validation/owned-planning-cache-contract-2026-11-14.json`.

### 2026-11-14 — bounded shared-pool planning cache adapter

OwnedEntryCache reuses SharedMemoryCache LRU eviction, shared-pool reclamation
and ownership pins. It charges cache key/directory/record storage alongside the
retained payload so even empty entries consume capacity. memoryPlanningCache
supplies the admitted object-metadata copier, preserving dates and range hints.
Legacy reads/writes copy independently; owned writes retain immutable input in
the shared pool. Replacement invalidates stale entries even if the new entry
cannot be retained. Expiry, clear, disabled caching and limit validation are
explicit contracts. Owned insertion can decline under pressure without failing
the query. Generic value domains supply an admitted clone callback.

Tests cover pin survival across LRU eviction, shared-pool sibling reclamation,
legacy copy isolation, expiry, stale replacement removal, actual cold/warm query
reuse, disabled caching and exact cache capacity versus one byte below including
metadata/key charges. This is in-process storage evidence; remote cache transport,
serialization and the broader allocation audit remain open.

Build/typecheck passed; focused suites passed 14 tests; full suite passed 3,637
with 629 skips. Evidence: `docs/validation/bounded-planning-cache-2026-11-14.json`.

### 2026-11-14 — owned JSON-cache read and parse lifetime

ObjectStoreJsonCache implements getOwnedEntry with an independent read lifetime.
It admits path/range arguments, uses owned range reads when available, and keeps
response bytes charged while the shared owned JSON parser admits decoded text
and parsed-tree overlap. Returned entry ownership includes the parsed tree and
survives internal scope disposal. Missing, expired, invalid, malformed and changed
read extents release all acquired reservations. Budget failures preserve their
typed errors rather than becoming parse errors. Legacy range adapters receive
payload admission before the call, but their internal allocations remain external.

Tests cover Unicode entries, exact capacity/one-byte-below boundaries for owned
and legacy transports, failure cleanup and expiration. Real localhost HTTP tests
exercise native 206 and ignored-range 200 responses with parsed ownership release.
These are actual HTTP transport fixtures, not production-cloud measurements.

The preliminary ObjectStore.head call remains an external metadata boundary;
HTTP head probing may retain a full ignored-range response outside this query
scope. This implementation does not close that gap. Same-size object replacement
between HEAD and read also remains a store-version consistency limitation.
Owned remote writes/serialization and the broader allocation audit remain open.

Build/typecheck passed; focused suites passed 20 tests; full suite passed 3,643
with 629 skips. Evidence: `docs/validation/owned-json-cache-read-2026-11-14.json`.

### 2026-11-14 — owned HEAD capability and discovery routing

ObjectStore exposes optional headOwned(path, scope), returning releasable metadata
or null. A shared reader adopts metadata into the receiving scope and releases
returned owners if adoption fails. Exact-path query discovery and owned JSON-cache
reads use it when available. URI wrappers normalize owned paths; read controls
apply the same concurrency and post-settlement cancellation checks as legacy HEAD,
releasing discarded results. The caching wrapper forwards owned HEAD directly
rather than falling back to its unowned metadata cache.

Tests cover query and JSON-cache routing without legacy HEAD, query-lifetime
metadata retention, URI/cache/control wrapper composition, late completion after
cancellation, foreign-budget rejection and final reservation cleanup. This is a
capability/lifetime contract with instrumented test adapters. HTTP probe admission
still requires its concrete headOwned implementation; forwarding alone does not
close the full-response probe allocation gap or add owned HEAD cache retention.

Build/typecheck passed; focused suites passed 16 tests; full suite passed 3,646
with 629 skips. Evidence: `docs/validation/owned-head-contract-2026-11-14.json`.

### 2026-11-14 — admitted HTTP metadata probes

HttpObjectStore implements headOwned using the shared admitted stream reader.
Tiny range probes, ignored-range full responses and compressed-range full-response
fallback all keep their backing in scratch until metadata construction finishes.
Object metadata, retained header strings and dates are admitted separately and
returned with ownership. Missing responses cancel their body; oversized streams
cancel and release reservations. The owned path never inserts probe backing into
the legacy unaccounted full-response cache.

Tests cover 206/200 exact-capacity boundaries, one-byte-below rejection, compressed
probe fallback and oversized stream cancellation. The existing real HTTP JSON
cache fixtures now exercise this owned preliminary probe as well as owned payload
reads. Probe buffers are released after metadata construction; shared-budget probe
cache retention/reuse still needs implementation and performance review. Legacy
HEAD remains unchanged, and runtime-provided network chunks are external storage.

Build/typecheck passed; focused suites passed 15 tests; full suite passed 3,650
with 629 skips. Evidence: `docs/validation/http-owned-head-2026-11-14.json`.

### 2026-11-14 — shared-budget full HTTP probe reuse

HttpStoreOptions accepts a shared memoryBudget for full-response cache retention.
Owned metadata probes retain ignored-range and compressed-fallback bodies only
when the configured cache shares their allocation pool. Cache key/record overhead
is admitted with payload ownership; optional insertion declines under pressure or
capacity limits. Matching-pool owned range reads copy their requested window from
the retained body, so mutable output cannot corrupt shared cache contents. Cache
clear, successful PUT/DELETE and a new metadata probe invalidate stale backing.

The real localhost ignored-range JSON-cache fixture now uses one request for
probe plus subsequent payload use. Tests cover output isolation, cache clearing
while returned output remains live, zero/undersized retention limits, shared-pool
sibling reclamation and write invalidation. A caller must identify the intended
shared pool; without one, owned probe bodies release rather than escaping into an
unaccounted legacy cache. Automatic propagation of the query pool through every
store factory and broader mixed-workload cache validation remain open.

Review also identified cache-pin capacity as a distinct limit from requested
range capacity. If a child query cannot pin the entire retained backing or admit
the copied range while pinned, the reader releases its pin and falls back to a
bounded transport read. A 512-byte child reads two bytes from a 4 KiB cached object
successfully through this fallback; cache presence does not force that query to
retain the larger backing.

Build/typecheck passed; initial focused suites passed 19 tests, final owned-HEAD
suite passed 9; full suite passed 3,655 with 629 skips. Evidence:
`docs/validation/http-owned-probe-cache-2026-11-14.json`.

### 2026-11-14 — bounded Parquet discovery cache defaults

createParquetLake now uses memoryPlanningCache when caching is enabled and no
custom planning cache is supplied. It inherits the existing shared query pool,
cache capacity and TTL; callers do not configure a second budget. Existing custom
cache overrides remain intact. Real Parquet fixture tests verify warm discovery
reuse, eviction under sibling pool pressure, rediscovery after eviction, zero
capacity and immediate expiry.

The disabled-cache test exposed zero-cost legacy HEAD entries in cachedObjectStore.
HEAD records now include positive key, metadata, date and retained string charges,
including negative lookups. Cached HEAD copies also preserve rangeReadMode, which
was previously dropped. Tests verify exact capacity versus one byte below, zero
capacity, negative-entry charges, defensive transport hints and final cleanup.
This closes default discovery retention gaps; positive-size legacy raw payload
entry overhead and automatic HTTP pool propagation remain separate audit items.

Build/typecheck passed; focused suites passed 120 tests; full suite passed 3,659
with 629 skips. Evidence: `docs/validation/default-planning-cache-2026-11-14.json`.

### 2026-11-14 — raw object-cache entry metadata charges

cachedObjectStore now charges key strings, record/directory storage and byte views
for full-object and range entries, in addition to payload bytes. Empty responses
and cached misses have positive costs, so their entry count cannot grow without
consuming byte capacity. HEAD metadata uses the same shared entry-cost helper.
Owned-read forecasts include cold defensive-copy metadata and match the measured
cold peak in the boundary test. Existing payload-only tests now provide enough
capacity for actual retained metadata and verify measured backing lifetime.

Tests cover 100 distinct negative lookups under a 2 KiB cache, eviction of early
misses, empty objects/ranges, disabled retention, defensive copies, and forecast
agreement. This accounts for retained wrapper storage; caller-created lookup keys,
legacy adapter allocation internals and general runtime heap overhead remain
outside this change's proof. SharedMemoryCache's generic byte-cost API still
requires its callers to provide a complete retention cost.

Build/typecheck passed; focused suites passed 121 tests; full suite passed 3,660
with 629 skips. Evidence: `docs/validation/raw-cache-entry-accounting-2026-11-14.json`.

### 2026-11-14 — cache-accounting mixed-query and compatibility refresh

Source 550ff5c passes the original range-cache matrix and a separate factory-cache
matrix, each with eight scenarios, two rounds, four overlapping queries and pinned
prior output. All jobs complete and final reclamation reaches zero reservations.
The original matrix retains two concurrent HTTP reads and four warm discovery
requests with a fitting 16 MiB range cache.

Factory caching adds raw/decoded/discovery competition for the shared pool. With
16 MiB cache settings, the 64 MiB pool makes 41 cold and 12 warm HTTP requests;
the 128 MiB pool makes 37 cold and zero warm requests. Measured maximum tracked
allocations are 66,960,963 and 69,838,800 bytes respectively. These results show
why a cache setting alone cannot imply that all cache classes fit simultaneously.
The benchmark initially assumed zero warm requests in both cases; the measured
64 MiB contention disproved that assumption. The factory mode now compares cold
and warm requests explicitly. Cleanup releases outputs before assertions and
reclaims caches after round measurements, excluding cleanup pressure from peaks.

Worker tests pass 75, reference tests 104, and required external conformance 520
with one inactive guard skip. Evidence:
`docs/validation/cache-accounting-mixed-runtime-2026-11-14.json`. This is a single
Node benchmark run per case, not a Worker memory-cap or stable latency claim.

### 2026-11-14 — cache replacement and child-pin pressure semantics

SharedMemoryCache now invalidates an existing key before declining a valid but
oversized or unaffordable replacement, for both allocated and owned insertion.
Previously the oversized fast return left stale backing under the same key.
Incompatible allocation pools are rejected before mutation. Independent active
pins still retain the old value until their consumers release
it. Rejected allocated insertions do not invoke their allocation factory, and
rejected owned insertions do not consume the caller's handle. Invalid cost inputs
are still validated before cache mutation.

OwnedEntryCache treats a budget failure while pinning an existing entry as a miss.
The query can rediscover/copy metadata under its own limits instead of being
forced to pin a larger cache entry. Tests cover child-pin rejection, zero leaked
child reservations, stale replacement invalidation and pin survival. An HTTP
adapter regression verifies that a freshly fetched larger object which cannot
be retained does not cause subsequent range reads to return the older object.

Build/typecheck passed; targeted core/Parquet tests passed 85, typed cache 9 and
HTTP 10; final full suite passed 3,664 with 629 skips. Evidence:
`docs/validation/cache-replacement-pressure-2026-11-14.json`.

### 2026-11-14 — release refresh after cache ownership integration

Runtime source 246b165 passes public build/declarations, clean npm consumer
installation (66 packages), Worker (75), reference (104), required external
conformance (520 plus one inactive missing-fixture guard), coverage (3,664 passed /
629 skipped), lint, compatibility, file-size, changeset and deterministic fixture
regeneration checks. Fixture regeneration leaves no tracked changes. Typecheck
and full-suite evidence also cover this source. Coverage thresholds are unchanged:
91.26% statements, 85.39% branches, 95.78% functions, 93.07% lines.

Evidence: `docs/validation/current-cache-ownership-release-gates-2026-11-14.json`.
The audit opening now distinguishes current implementation/release evidence from
older mixed-query and constrained Worker runs, with historical links retained in
an expandable index. These checks do not close the remaining allocation and
whole-runtime acceptance requirements.

### 2026-11-14 — automatic HTTP cache pool propagation

ObjectStore exposes withDefaultMemoryBudget as an immutable configuration
capability. HttpObjectStore binds default pool/cache limits into a separate store
without overriding explicit adapter settings. createParquetLake supplies its
existing pool and cache capacity when caching is enabled. URI, cache and
read-control wrappers preserve the capability. Rebound controlled stores share
the original semaphore and normalized signal, preventing multiple lakes from
multiplying configured transport concurrency or resetting timeout state.

A real written Parquet fixture served by a localhost server ignoring ranges
requires one request on the cold query and zero on the warm query through URI,
cache and read-control wrappers. The same fixture with zero factory capacity
retains no cache bytes and issues fresh reads. Tests also verify immutable binding,
explicit limit precedence, cross-binding concurrency and final shared-pool
reclamation. Caller-created scanners and stores lacking the optional capability
retain their existing configuration; no unrelated pool is silently replaced.

Build/typecheck passed; initial focused suites passed 14 tests, concurrency/queue
checks 5, enabled/disabled real HTTP checks 2; full suite passed 3,668 with 629
skips. Evidence: `docs/validation/default-http-pool-propagation-2026-11-14.json`.

### HTTP unused response cleanup (2026-11-14)

All HTTP failure checks now cancel unread response bodies before returning the typed error. Missing-object reads and metadata probes cancel before returning null; successful PUT/DELETE responses are also discarded. The existing 416 fallback uses the same cleanup helper. Cancellation failures do not replace the operation's HTTP result. This closes a transport resource-lifetime gap without draining potentially unbounded error bodies into memory.

Validation: `pnpm exec tsc -b` passed; `pnpm exec vitest run packages/http` passed 50 tests in 6 files, including existing localhost JSON-cache fixtures. Nine new parameterized tests exercise all public operation families, 404/503, cancellation rejection, zero remaining owned reservations, and successful write-response cleanup. These use native Response/ReadableStream cancellation hooks; they do not measure production cloud socket or isolate memory reclamation. Bounded cache-write serialization and broader runtime allocation gaps remain open.

### Iterative JSON output sizing (2026-11-14)

JSON output sizing now uses an explicit depth stack and ancestor tracking rather than recursive calls. Scoped sizing admits frame, iterator, and ancestor-entry reservation units before descending, releases them when leaving a container, and unwinds all reservations after errors. Shared references remain legal; ancestor cycles remain errors. Array holes, omitted object properties, own-property ordering, and scalar formatting retain their existing output semantics. Scalar roots do not acquire container traversal reservations.

The focused sizing/encoding suites pass 55 tests, including 20,000 nested arrays, exact capacity and one byte below, and cycle cleanup. This establishes deep sizing, not deep encoding: the output writer still uses recursive traversal, and runtime iterator storage is not a measured VM heap cap. Standard cache JSON serialization remains separate because its Date, typed-array, and bigint behavior differs from query output formatting.

Final verification: build and typecheck passed; full suite 3,678 passed / 629 skipped. Three success-fixture budgets increased from 1 KiB to 4 KiB to cover the newly charged traversal; output lifetime assertions and budget-rejection fixtures remain intact. Iterator internals and retained array/set capacity remain runtime headroom concerns rather than measured heap guarantees.

### Iterative owned JSON writing (2026-11-14)

The JSON output writer now traverses containers with explicit admitted frames, removing recursive calls while preserving direct encoding into one pre-admitted UTF-8 backing. Frame reservations overlap the output backing and scalar workspace; completed frames release their reservations, and error cleanup releases all active frames. Object key order, omitted properties, array holes, Unicode escapes, and special query scalar formatting retain the existing contract.

Focused encoding/stream validation: 41 tests passed. New fixtures encode 20,000 alternating object/array levels with a Unicode leaf and compare the actual bytes against independently constructed JSON. Exact-capacity and one-byte-below tests cover both deep sizing and writer/output overlap. The existing 1,000-versus-10,000-element shallow array test now requires identical traversal/scalar overhead, using a 4 KiB allowance for newly admitted frames instead of its prior scalar-only 512 bytes.

These are reservation units, not a total VM heap guarantee: generator internals, retained container capacity, GC, caller-owned values, and stateful mutation remain separate concerns. Standard cache JSON write serialization remains open and cannot reuse query scalar formatting unchanged.

Final verification: build, typecheck, formatting, and full suite passed (3,680 tests / 629 skipped). Two decimal fixtures now allow admitted writer frames in addition to scalar formatting and output backing; their output and release assertions remain intact.

### JSON traversal reservation reuse and measured regression (2026-11-14)

Added `bench/json-output-traversal.mjs`, comparing current built sizing/writing against recursive source revision `5e4d280` with identical inputs and 64 MiB reservation budgets. Each case verifies UTF-8 bytes and zero outstanding reservations, with one warmup and five measured rounds. The 20,000-level input fails with recursive stack overflow and succeeds iteratively. Raw evidence: `json-traversal-benchmark-2026-11-14.json` (source a8e3f63) and `json-traversal-reuse-benchmark-2026-11-14.json` (subsequent working implementation, identified by built-module SHA256 hashes).

The benchmark exposed a significant shallow row-array regression. Initial iterative median was about 2.72 ms versus recursive 0.34 ms for 1,000 objects. Both passes now share `JsonTraversalBudget`: capacity is reserved once per maximum nesting depth and reused for siblings. Charges remain live until traversal completion, accounting for retained stack capacity more conservatively than releasing on each pop. This reduces reservation churn and centralizes disposal/admission logic.

An isolated rerun measured row-array medians of 1.83 ms iterative versus 0.34 ms recursive, wide-numbers 2.39 versus 2.09 ms, and small-row 0.0134 versus 0.0106 ms. Deep iterative encoding took 46.15 ms; recursion could not complete. The shallow-row regression remains OPEN: generator/frame overhead needs further investigation. These sequential kernel measurements do not establish query/cloud performance or whole-heap bounds. Tests cover sibling reuse, high-water retention, failure during depth growth, and idempotent disposal, alongside existing deep/Unicode/exact-capacity suites.

Final validation: build/typecheck passed; full suite 3,682 passed / 629 skipped, 401 passing files.

### Shared JSON container enumeration (2026-11-14)

Sizing and writing now share one module-level container iterator. It preserves array holes and own enumerable object keys without constructing a key array, and borrows one reusable entry record per iterator. Consumers finish using the entry before advancing. This removes the writer's per-container generator-function creation, eliminates per-element sizing entry allocation, and centralizes custom-JSON rejection and enumeration semantics. Existing depth admission covers the iterator/entry workspace; high-water reservations remain until traversal completion.

An isolated rerun of the same kernel benchmark measured median row-array encoding at 0.85 ms (previously 1.83 ms), deep encoding at 21.87 ms (previously 46.15 ms), small-row 0.0106 ms versus recursive 0.0109 ms, and wide-numbers 2.41 ms versus recursive 2.19 ms. The row-array remains about 2.5× slower than recursion (0.34 ms); this regression remains OPEN. Raw samples and built-module hashes are in `json-shared-enumeration-benchmark-2026-11-14.json`. Sequential kernel measurements are not production throughput or heap-cap evidence.

Focused suites passed 73 tests before the final full run. Added byte-level comparison with native JSON.stringify for numeric/empty property keys, inherited and non-enumerable properties, symbols, undefined values, sparse arrays, and repeated aliases. Existing deep/Unicode/cycle/exact-budget tests continue to exercise both passes.

Final verification: build/typecheck and formatting passed; full suite 3,683 passed / 629 skipped in 401 passing files. No claim of completed performance acceptance: the shallow row-array kernel regression remains measurable.

### Worker JSON traversal validation (2026-11-14)

The current runtime source (`d9e53bf`) passes the existing 75 workerd tests, including real R2-backed concurrent JSON/NDJSON/CSV output fixtures. Added direct workerd coverage for 20,000-level NDJSON streaming with a Unicode leaf at 64/128 MiB shared reservation budgets. Each fixture consumes the actual Response stream, compares complete output, reruns at the observed exact reservation peak and one byte below, and checks zero live reservations after success and rejection. A separate low-capacity output test verifies cleanup on admission failure.

The expanded Worker gate passes 78 tests in 22 files. This establishes correct deep traversal and allocation-accounting cleanup in workerd; it does not assert a 64/128 MiB total isolate heap cap. The shallow row-array kernel regression and standard cache-write serialization remain open.

Coverage gate passed unchanged: statements 91.28%, branches 85.39%, functions 95.80%, lines 93.08%. Typecheck passed. Evidence is recorded in `json-worker-coverage-2026-11-14.json`.

### Direct array cursor for JSON traversal (2026-11-14)

Replaced the shared entry generator with a borrowed cursor. Arrays now advance by index, including sparse slots, without generator result objects. Objects retain own-key generator enumeration and read each value once per pass; no materialized key array is introduced. Sizing and writing share this behavior. Writer frame admission increases from eight to ten property units to cover the cursor fields; high-water depth reservations are unchanged.

The established benchmark (`json-cursor-benchmark-2026-11-14.json`, source base b9d2df5 with exact built-module hashes) measures median row-array encoding 0.54 ms versus recursive 0.35 ms, compared with 0.85 ms in the prior shared-generator run. Wide numeric arrays measure 2.09 ms versus recursive 2.20 ms; small rows 0.0117 versus 0.0114 ms; 20,000-level encoding 21.46 ms while recursion overflows. The row-array remains about 1.55× slower; performance acceptance remains open. These single-process sequential kernel samples are not cloud query throughput or whole-heap bounds.

The focused sizing/encoding/stream/decimal suites passed 74 tests before the final frame-charge adjustment. Existing byte-level fixtures cover sparse arrays, repeated aliases, empty/numeric keys, inherited/non-enumerable properties, special scalars and Unicode, and 20,000-level exact-capacity boundaries.

Final verification after the frame-charge adjustment: build/typecheck/formatting passed; full suite 3,683 passed / 629 skipped; workerd 78 passed in 22 files.

### Owned standard-JSON cache writes (2026-11-14)

Added explicit standard JSON scalar semantics to the existing iterative sizing/writing implementation. Query formatting remains the default; the standard mode preserves Date ISO/null behavior, native boxed scalars, typed-array numeric properties, omitted object fields, sparse array nulls, and non-finite number nulls. Date formatting has a reusable admitted workspace. BigInt (primitive or boxed) and arbitrary conversion callbacks are rejected before invocation. This is a durable allocation boundary: opaque callback allocations cannot be forecast. Inputs must remain immutable; legacy plain cache `set` retains native JSON.stringify behavior.

`ObjectStoreJsonCache.setOwnedEntry` now retains its borrowed input, admits wrapper/path/options and output backing, and keeps them live through asynchronous `put` settlement. It applies the same explicit-expiry/default-TTL rules as plain writes. Serialization no longer constructs a complete JSON string followed by a complete UTF-8 copy. Scope disposal releases every serialization owner on success, budget rejection, or transport failure. Transport adapter internal buffering/copies remain external and are NOT claimed bounded by this serialization change.

Validation includes byte-for-byte comparison with native JSON.stringify for ordinary/extended/invalid dates, typed arrays, boxed scalars, omitted fields, non-finite values, and exact/one-byte-below allocation peaks. Callback rejection tests assert zero callback invocations. Cache tests prove pre-transport budget rejection, TTL/key encoding, input/output retention through delayed successful and failed writes, and cleanup after caller-scope disposal. A real localhost HTTP PUT followed by owned GET verifies stored bytes and parsed values, explicitly distinct from in-memory-store tests.

The query kernel benchmark remains similar after adding the standard mode: medians small-row 0.0102 ms vs recursive 0.0102; wide-numbers 2.07 vs 2.17; row-array 0.57 vs 0.34; deep iterative 21.53 ms while recursion fails. Raw evidence `json-standard-mode-benchmark-2026-11-14.json` records base ff4f0c4 and built-module hashes. The shallow row-array performance gap remains open.

Final verification: build/typecheck/formatting passed; full suite 3,693 passed / 629 skipped (402 files), workerd 78 passed (22 files). Evidence: `owned-standard-json-cache-write-2026-11-14.json`.

### Owned write transport and HTTP payload-copy accounting (2026-11-14)

Added optional `ObjectStore.putOwned(path, body, options)`: a borrowed immutable byte owner supplies the budget for adapter-controlled copies and remains valid until settlement. URI, raw-cache, and read-control wrappers forward the capability while preserving normalized paths, pre-abort checks, and before/after-write cache invalidation. Owned JSON cache writes select this capability when present and retain legacy compatibility otherwise.

HTTP now shares write response handling between plain and owned methods. Its owned method retains the input and admits the explicit request-body copy before allocation/sending, copying only the visible view rather than the entire backing buffer. It holds both through successful, HTTP-error, or network-error settlement and then releases them. Successful/error response cleanup remains shared. This closes the known HTTP body-copy gap; request/header allocations, native Fetch copies, opaque network storage, and adapters without owned writes remain external boundaries.

Focused tests prove exact-budget admission and one-byte-below rejection before any fetch, subarray extent handling, delayed lifetime after caller-scope disposal, cleanup for success/HTTP/network errors, wrapper path forwarding, pre-aborted controls, and invalidation of cache entries repopulated during a pending write. A real localhost JSON cache PUT/GET now exercises the URI/cache/read-control chain and owned HTTP write path.

Final verification: build/typecheck/formatting passed; full suite 3,699 passed / 629 skipped (404 files), workerd 78 passed (22 files). Evidence: `owned-http-write-2026-11-14.json`.

### R2 owned write lifetime without a payload copy (2026-11-14)

`R2ObjectStore.putOwned` now retains its supplied byte owner through binding settlement and admits adapter-created write-option records before invoking R2. Plain and owned writes share the same options conversion. Unlike HTTP, R2 already accepts the view directly, so the owned path adds no payload copy. Caller options remain borrowed immutable data; native binding upload buffers remain external.

Unit tests verify reference identity for the supplied subarray (no backing copy), content/custom metadata forwarding, exact and one-byte-below admission before binding invocation, and input lifetime after caller-scope disposal during delayed success/failure. All 14 R2 unit tests pass. A new workerd fixture writes a standard-JSON cache entry containing a date, Unicode path, and typed array through the actual R2 binding, checks stored bytes against native JSON.stringify, reads it back through owned cache parsing, and checks cleanup.

Final verification: build/typecheck/formatting passed; all 14 R2 unit tests and 79 workerd tests (22 files) passed. Evidence: `owned-r2-write-2026-11-14.json`.

### R2 owned metadata lifetime (2026-11-14)

Added R2 `headOwned`: retained metadata records, etag/content-type strings, and upload dates are admitted through the caller scope before constructing the result. Dates are cloned so changes to binding-provided metadata cannot mutate the retained result. Owned metadata reports native range support. Pre/post-binding abort checks reject results arriving after disposal; missing objects return null without a result charge. Binding response allocation itself remains external.

The focused R2 metadata/adapter suites pass 14 tests, including exact capacity/one byte below, owner survival after scope disposal, date isolation, zero-budget missing objects, and late-result rejection. The real R2 JSON cache fixture now makes legacy `head` throw before reading, proving that the cache routes metadata through the new owned capability.

Final verification: build/typecheck/formatting passed; 14 focused R2 tests and all 79 workerd tests passed. Evidence: `owned-r2-metadata-2026-11-14.json`.


### Allocation matrix and release-gate refresh (2026-11-14)

Updated M1's allocation matrix to reflect iterative JSON traversal/scalar admission, bounded planning/cache retention, owned standard-JSON cache I/O, and HTTP/R2 write/metadata ownership. Each row retains its external/runtime boundaries. No acceptance item was narrowed or marked complete; row fallback/spill/state paths, remaining conversion workspace, scheduling/fairness, current mixed-runtime evidence and the JSON kernel regression still require work.

Runtime source `563df61` passes build, clean npm consumer (66 installed packages), current typecheck, 79 Worker tests, 104 reference tests, and required external conformance (520 passed / one inactive missing-fixture guard). Coverage runs 3,705 passing tests / 629 skipped and passes unchanged thresholds: statements 91.29%, branches 85.41%, functions 95.82%, lines 93.09%. Lint initially caught five benchmark JSON formatting errors; formatting was corrected and parsed JSON equality against committed reports verified. Compatibility, large-file, changeset and deterministic fixture-regeneration checks pass; regeneration leaves no tracked fixture changes. Evidence: `current-owned-io-release-gates-2026-11-14.json`.

### F1 row aggregate restore group-limit enforcement (2026-11-14)

The lifecycle review found that public row `aggregateWithState` enforced `maxGroups` for newly created scan groups but not for groups restored from a checkpoint. An incoming state could therefore exceed the requested group limit before scanning began. Restore now checks the validated checkpoint's group count before constructing accumulator groups, matching the existing vector restore contract. Parsing/validation still uses the admitted checkpoint scope; failures unwind it.

Tests cover inline and spilled two-group checkpoints: a limit of one fails with `LAKEQL_GROUP_LIMIT_EXCEEDED` before the scanner runs, while the exact limit of two resumes with correct totals and releases all tracked storage. A real Parquet fixture served over localhost HTTP tests both native 206 and ignored-range 200 responses: rejected restore causes zero new HTTP requests, while exact-limit resume performs reads and returns doubled totals. The checkpoint suite passes 23 tests and the HTTP fixture suite passes two. This closes the demonstrated restore-limit defect under F1/M2; it does not establish full row fallback/spill lifecycle completion or production spill atomicity.

Final verification: build/typecheck/formatting passed; full suite 3,709 passed / 629 skipped (407 files). Evidence: `aggregate-restore-group-limit-2026-11-14.json`.

### F1/F2 restored aggregate buffer enforcement (2026-11-14)

The follow-up lifecycle review found that restored row-aggregate buffer/memory counters were populated but only enforced after a new scanned row. An empty continuation could finish and return state that already exceeded its configured retained-row limit. `AggregateMemory.enforceBudget()` now provides one shared check for restored state before scanning and for per-row updates, covering both buffered-row and serialized operator-memory limits. Allocation admission remains enforced independently by the checkpoint scope.

Inline and spilled median checkpoints containing three values now reject a two-row limit before invoking an empty scanner; an exact three-row limit returns the existing median and releases all reservations. The real Parquet HTTP fixture also checks median checkpoint rejection with no additional requests for both 206 and ignored-range 200 servers. Focused aggregate suites pass 36 tests; both HTTP fixtures pass. This closes the demonstrated restore-validation gap, not all fallback/spill ownership or remote transaction acceptance.

Final verification: build/typecheck passed; full suite 3,711 passed / 629 skipped (407 files). Evidence: `aggregate-restore-buffer-limit-2026-11-14.json`.

### F1 duplicate aggregate restore groups (2026-11-14)

Restore previously accepted repeated group keys and silently replaced the earlier accumulator group in the directory. Row checkpoint restore now rejects duplicates with `LAKEQL_BOOKMARK_INVALID` before constructing the duplicate group; vector restore rejects canonical duplicate keys with `LAKEQL_TYPE_ERROR` before restoring their accumulators. Both reuse the destination directory for membership checks, adding no extra key index or whole-state traversal.

Focused aggregate/checkpoint tests pass 32 cases. New cases cover inline and spilled corrupt checkpoints, scanner non-invocation, zero remaining scope charges, and duplicate vector group snapshots. The real HTTP Parquet fixture now also rejects duplicate checkpoint groups without a new request for either 206 or ignored-range 200 transport. This is a malformed-state correctness/retention fix under F1/V2; it does not close the remaining generic lifecycle and spill-atomicity acceptance work.

Final verification: build/typecheck/formatting passed; full suite 3,714 passed / 629 skipped (407 files). Evidence: `aggregate-duplicate-restore-2026-11-14.json`.

### F1 restored checkpoint scratch lifetime (2026-11-14)

Row aggregate restore previously kept parsed checkpoint objects and owned spill-read buffers in the query-long checkpoint scope. Accumulators already admitted their retained scalar/collection state, but group key records still borrowed the parsed tree. Restore now reads, parses, and compares input state in a child scratch scope; restored group keys are independently detached into the retained scope. Scratch releases before the next scan begins, including failure paths. Unseen first/last accumulator constructors normalize their unused value to null rather than retaining an irrelevant checkpoint reference.

The new lifetime test compares an ordinary checkpoint with the same checkpoint plus 64 KiB of unused JSON data, for both inline and owned spill reads. The padded input produces a higher restore peak but identical live tracked memory at scanner entry. Sum, median, mode, first, last, any and count-distinct results remain unchanged, and final pool usage returns to zero. The checkpoint suite passes 29 tests; existing exact-capacity, cancellation, malformed restore and real HTTP continuation tests are included in full regression validation. Restore-time old/new overlap remains admitted; this change shortens lifetime rather than claiming that overlap or runtime GC disappears.

Final verification: build/typecheck passed; full suite 3,716 passed / 629 skipped (407 files). Evidence: `aggregate-restore-scratch-lifetime-2026-11-14.json`.

### F3 top-k restore scratch before discovery (2026-11-14)

The analogous continuation review found that sort already detaches restored rows and disposes checkpoint scratch before planning. Top-k seeded its bounded owned selector and disposed scratch before scanning, but performed object discovery first. Discovery now follows selector seeding/scratch disposal, so metadata requests do not overlap unused parsed checkpoint and spill-read storage. The existing selector, ownership transfer and comparison behavior are unchanged; no extra row copy or key index was introduced.

Twenty row top-k memory tests pass, including new inline/spilled continuation fixtures. A checkpoint with 64 KiB of extra unused data has a larger restore peak but the same live pool usage at the metadata `head` callback as an ordinary checkpoint. Both continuations return the same selected row and release all reservations. This is a deterministic ownership-boundary check, not a production cloud memory measurement; real transport regressions remain part of the full suite.

Final verification: build/typecheck/formatting passed; full suite 3,718 passed / 629 skipped (407 files). Evidence: `topk-restore-before-discovery-2026-11-14.json`.

### F2 sort spill extent validation (2026-11-14)

Sort checkpoint descriptors carried `byteSize` and `rowCount`, but restoration did not compare either with the loaded run. Restore now verifies byte size before JSON parsing and row count before retaining the decoded run for merge. Descriptor counts/extents must be safe nonnegative integers. Mismatches produce `LAKEQL_BOOKMARK_INVALID` before new scanning; owned read/parse scopes still unwind on rejection.

Fifteen focused sort restore/transaction tests pass. New cases cover changed/truncated run bytes, mismatched row counts, unsafe descriptor integers, valid resumed ordering, no scanner invocation on rejection, and zero remaining pool usage. The real Parquet HTTP fixture now also verifies a bad sort-run descriptor prevents new data requests for both 206 and ignored-range 200 servers. Spill storage in these fixtures is the memory adapter: this proves extent validation and query transport separation, not production remote spill atomicity or detection of same-size/same-count content changes.

Final verification: build/typecheck/formatting passed; full suite 3,723 passed / 629 skipped (408 files). Evidence: `sort-restore-extent-2026-11-14.json`.

### F1/F2/V2 actual R2-bound spill continuation fixtures (2026-11-14)

The built-in spill backend is in-memory; `SpillAdapter` is the extension contract for other stores. Added a test adapter over the workerd R2 binding with direct writes and independently owned reads, deliberately omitting staged/batch publication capabilities. The fixture writes actual SNAPPY Parquet data with two row groups, checkpoints grouped aggregation and sort into R2 objects, then resumes both through the spill-reference path. Its unowned read method throws, proving that continuation uses owned reads. A changed sort run is rejected through the extent checks.

Both 64/128 MiB reservation-pool fixtures pass, with correct aggregate totals/order, input checkpoint bytes charged during writes, zero retained query charges after success/failure, and peak charges within the pool. The full workerd suite passes 81 tests in 23 files. These small correctness fixtures are not memory-pressure or production-network benchmarks. Native R2 upload buffering remains external. No transaction is advertised by this test adapter: it proves the supported nontransactional fallback contract, not atomic multi-object publication. Core's atomic batch guarantees apply only to adapters exposing `beginWriteBatch`; production remote transaction evidence remains separate.

Final verification: typecheck and all 81 workerd tests passed. Evidence: `r2-spill-continuation-2026-11-14.json`.


### V1/V2 current mixed-query benchmark refresh (2026-11-14)

Refreshed the unchanged mixed-query harness against source `4d1dc00`, using a real generated Parquet fixture, four overlapping collect/select/top-k/aggregate jobs, pinned prior output, 32 MiB per-query limits, 64/128 MiB shared pools and 4/16 MiB cache settings. Both shared-range-cache and factory-cache matrices cover memory and localhost HTTP 206 transports with cold/warm rounds. All 128 jobs completed; all scenarios returned tracked usage to zero after cleanup, and HTTP concurrency stayed at two.

The shared-range-cache peak was 53,957,002 bytes. Factory-cache peaks were 66,960,963 bytes under the 64 MiB pool and 69,838,800 under 128 MiB. Factory cache capacities apply per configured cache, while the root pool bounds their combined tracked storage. The 64 MiB result leaves little reservation headroom. HTTP cold/warm request counts match prior reports: shared range cache 37/37 at 4 MiB and 37/4 at 16 MiB; factory caches 37/33 at 4 MiB, 41/12 at 64 MiB pool with 16 MiB caches, and 37/0 at 128 MiB with 16 MiB caches.

Raw reports include source, harness and fixture hashes: `docs/benchmarks/mixed-query-current-2026-11-14.json` and `docs/benchmarks/mixed-query-factory-current-2026-11-14.json`. These are single-run regression observations, not stable latency or fairness evidence. Node includes fixture/server memory and sampled runtime usage exceeds configured reservation pools; samples may miss transient peaks. This does not prove a 64/128 MiB whole-Worker cap. Factory post-measurement reclamation is excluded from round peaks. Build passed before measurement; no runtime source changed in this refresh. Full audit acceptance remains open.


### M2/V1/V2 constrained Worker refresh (2026-11-14)

Reran the existing owned-output Worker harness against source `4d1dc00`: actual 32,768-row Parquet fixture, 256-character payloads, pinned output checksums, cold/warm rounds, one/four jobs, and 64/128 MiB shared reservation pools. Three transport/runtime configurations cover native emulated R2 and localhost HTTP with verified 64/128 MiB V8 managed-heap flags. All 12 scenarios completed. Each four-job/64 MiB round admitted two jobs and rejected two with `LAKEQL_BUDGET_EXCEEDED`; all other jobs succeeded. Every scenario evicted to zero tracked usage.

The maximum tracked reservation was 67,108,545 bytes for the 64 MiB R2 pool, leaving only 319 bytes at that observed peak. At 128 MiB it was 110,920,090 bytes. The constrained 64 MiB managed-heap HTTP run sampled at most 50,877,724 bytes of JS heap; unconstrained R2 sampled up to 101,138,972 bytes. These sparse samples depend on GC timing and cannot establish total Worker memory safety. Managed heap excludes native/external backing and embedder memory; local transports do not establish production cloud latency. No fairness or stable performance claim follows from a single run.

Evidence: `docs/validation/current-worker-refresh-2026-11-14.json`, with source/harness hashes and links to all raw samples. This refresh advances current-revision runtime evidence without closing broader scheduling, allocation-boundary or full audit acceptance requirements.


### F3 JSON scalar dispatch simplification (2026-11-14)

Sizing and encoding previously ran decimal, timestamp, interval and byte-array predicates on every primitive value. These representations are objects, so their checks now live inside the existing object branch in both passes. Primitive dispatch avoids redundant predicates without changing traversal, scalar conversion, reservations, output framing or deep-input support. This is a generic type-dispatch simplification with no query-shape special case or new configuration.

The comparison harness now labels variants reference/current, since its selectable reference may itself be iterative. Against `041c629`, the recorded second run measured median milliseconds: small row 0.01279 to 0.00864, 10,000 numbers 2.19308 to 2.13810, 1,000 rows 0.57434 to 0.53698, and depth 20,000 at 22.64792 to 22.68196. Tracked peaks are identical for all cases; encoded bytes and zero remaining reservations are verified each iteration. An initial run also improved the row-array median (0.59408 to 0.53980). These short sequential kernel measurements are not cloud/query throughput claims, and do not close the remaining regression against recursive output.

Raw evidence: `docs/validation/json-object-dispatch-benchmark-2026-11-14.json`. Its current commit field records the working-tree base; built module hashes identify the measured modification. Build and 58 focused sizing/encoding tests passed before broader verification.

Final verification: build/typecheck passed; 3,723 regular tests passed / 629 skipped, and all 81 Worker tests passed. Evidence: `json-object-dispatch-2026-11-14.json`.


### M2/V1 same-pool recovery after repeated pressure (2026-11-14)

Extended the Worker recovery probe to require successful owned collection after releasing pinned output, without replacing the lake/pool, clearing caches or forcing GC. Previously it only attempted another query while successful outputs remained pinned, accepting either a typed rejection or success. The new phase verifies complete row counts, ID/character checksums and held output contents before release.

Native workerd R2 and localhost HTTP under a verified 64 MiB managed-heap flag each ran four rounds across one/four jobs and 64/128 MiB reservation pools. All eight scenarios completed: 64 primary jobs succeeded and 16 returned typed budget failures. Eight additional retries while prior outputs remained pinned also returned typed budget failures. All eight post-release retries succeeded on the same pool/cache and returned the full 32,768-row fixture; final tracked usage was zero in every scenario. This provides concrete repeated-failure recovery evidence, without proving fairness, cancellation or every operator's self-held-capacity behavior.

Runtime source `717b121`; harness hash and raw reports are recorded in `docs/validation/worker-pressure-recovery-2026-11-14.json`. Recovery requests affect later cache state and elapsed times, so these rounds are not baseline latency comparisons. Local R2 and HTTP remain distinct from production cloud operation, and managed heap does not cover total Worker/native memory. Only the benchmark harness and evidence changed; formatting and executable fixture assertions passed.


### M2 owned metadata cancellation through read controls (2026-11-14)

The read-control review found a concrete asymmetry: owned ranges combined the caller scope and query control signal in a child scope, while owned metadata passed the caller scope directly. Consequently, aborting read controls did not reach an in-flight HTTP metadata probe, and disposing a caller scope did not remove a queued metadata operation promptly.

Controlled owned metadata now uses a signal-bound child scope, retains/transfers successful metadata into the caller scope before disposing temporary read ownership, releases rejected transfers and the adapter-returned independent owner, and preserves null results. The shared semaphore and post-read cancellation checks remain intact. The existing queue fixture now covers both metadata and range operations: cancellation rejects while an unrelated active read remains gated, no queued transport/admission occurs, and the successful active owner remains charged until release. A pending HTTP metadata probe test verifies that query control cancellation reaches the transport while leaving the caller scope usable and with zero tracked storage.

This closes the demonstrated metadata cancellation gap, not general job fairness or whole-pipeline scheduling acceptance.

Final verification: build/typecheck passed; 30 focused tests, 3,725 regular tests (629 skipped), and 81 Worker tests passed. Evidence: `owned-head-read-controls-2026-11-14.json`.


### F6/M2 actual HTTP metadata cancellation and slot recovery (2026-11-14)

Added real localhost HTTP fixtures for cancellation before response headers and during a partial two-byte range response. The body case waits for tracked response storage before cancelling, preventing a header-only cancellation from passing that case. Both queue a second metadata read behind the active request, verify that it has not reached the server, dispose the first scope, observe the server response close, and require the second read to return correct object size. Final shared-pool usage is zero.

Both fixtures pass; typecheck and formatting pass. Runtime source remains `9224149`; only test/evidence files changed. Evidence: `docs/validation/owned-head-http-cancellation-2026-11-14.json`. This verifies native Fetch/socket cancellation and slot recovery together on localhost, without establishing production latency, native R2 cancellation, general fairness or whole-runtime memory limits.


### M2/V2 R2 metadata cancellation settlement contract (2026-11-14)

Added a workerd fixture over the actual R2 bucket that delays delivery of the first real metadata response. R2's binding interface has no abort signal, so the read-control semaphore must retain the active slot until the call settles; otherwise a cancelled query could increase actual binding concurrency. The fixture disposes the first scope, queues another metadata read, checks that only one binding call has occurred, then releases the gate. The late result must reject with `LAKEQL_ABORTED`; the next reader must return the correct object size and release all tracked storage.

The gate controls response delivery after a real binding call, not the native network itself. This is local Worker settlement/ownership evidence, distinct from HTTP socket abort and production R2 latency. Runtime source is `9224149`; no implementation change is required by this fixture.

Final verification: typecheck and all 82 Worker tests passed. Evidence: `docs/validation/r2-metadata-cancellation-2026-11-14.json`.


### M2 scheduler FIFO and cancelled-head progress review (2026-11-14)

Reviewed `MemoryStageScheduler` admission and its callers. Added two adversarial queue cases: an eight-byte active stage in a twelve-byte pool, a twelve-byte waiting head, then twenty one-byte newcomers. Without cancellation, none bypasses the head despite four bytes of free capacity; execution order remains FIFO. With head cancellation, fitting newcomers complete while the original active stage remains gated. Both finish with zero active/queued stages and zero tracked bytes, never exceeding the pool. All 15 scheduler tests pass, including existing recursive-admission rejection, impossible retained-output rejection, cancellation during reclamation, input alias lifetime and scratch guards through settlement.

The implementation's fairness boundary is explicit: `vector-batches.ts` shares numeric schedulers by root pool; `numeric-page-alignment.ts` submits planned numeric replacement stages unless an enclosing stage already supplies admission. `concatScheduledBatches` is another explicit scheduled operation. This does not establish scheduling fairness for every operator/query path, and FIFO may leave free capacity idle behind a larger head. These tests verify the stated FIFO contract rather than a throughput or bounded-wait claim. The existing read semaphore has its own FIFO/cancelled-entry tests; transport slots are distinct from memory-stage admission.

Final verification: 15 scheduler tests and typecheck passed. Evidence: `docs/validation/stage-fifo-admission-2026-11-14.json`. No runtime implementation changed.


### M2/V2 shared scheduled numeric decode instrumentation (2026-11-14)

The scheduler integration review found that direct numeric replacement execution forwarded `options.decode`, while scheduled execution omitted it. Public numeric scans supply this wrapper to record decode time, so scheduled numeric replacement work was omitted from that statistic. The two paths now share one `decodeInto` dispatch function that forwards the same plan, concurrency and instrumentation arguments while varying only the admitted scope. This removes duplicated argument lists without changing planning, reservations or transport scheduling.

Extended the real ZSTD fixture that forces measured decoding under an 8 KiB job cap: direct and scheduled execution must each invoke the decoder wrapper once per numeric column, preserve nullable values, stay within the cap and release all tracked storage. Before the fix, the scheduled case failed with zero callbacks instead of two; afterward all 37 numeric-page-group tests pass. Prior end-to-end wall-clock measurements remain wall-clock observations, but decode-time statistics from affected scheduled paths undercounted work and should not be treated as complete CPU attribution. This is a measurement correctness fix, not a claimed throughput improvement.

Final verification: build/typecheck passed; 37 numeric-page tests, 3,729 regular tests (629 skipped), and 82 Worker tests passed. Evidence: `docs/validation/numeric-decode-dispatch-2026-11-14.json`.


### F4 peer-index boundary and dynamic-frame matrix (2026-11-14)

Reviewed existing RANGE binary-search, GROUPS cursor/index and frame-admission tests. Added an independent linear oracle matrix rather than comparing only cached versus uncached implementations: forty three-row peer groups, ascending/descending ordering, nonmonotone per-row preceding/following offsets, alternating access from both partition ends, RANGE/GROUPS and all four exclusions. It forces adaptive index construction at the exact 164-byte index capacity and verifies the constant-space fallback at 163 bytes, with identical expected frame rows and zero remaining index charges after disposal. The oracle computes bounds and exclusions directly without calling frameSpan or peer lookup.

The focused four-suite run passes 39 tests. Existing suites additionally verify logarithmic RANGE key visits, near-linear sequential GROUPS key visits, typed invalid offsets, timestamp interval ties, independent exclusion filtering, iterator cursor snapshots and bound-expression scratch release across row/run/vector routes. This strengthens F4's local boundary correctness and lower-memory evidence; it does not by itself complete public window lifecycle, current reference comparisons, transport fixtures or full allocation accounting.

Final verification: 39 focused tests and typecheck passed. Evidence: `docs/validation/window-boundary-matrix-2026-11-14.json`. No runtime implementation changed.


### F4/V2 current public dynamic-window reference matrix (2026-11-14)

Extended the existing public CLI/DuckDB differential matrix with sixteen combinations of RANGE/GROUPS, ascending/descending ordering, and EXCLUDE NO OTHERS/CURRENT ROW/GROUP/TIES. Per-row CASE expressions vary both preceding and following offsets. Each case compares sum and count over a real written Parquet fixture with peer ties, a null aggregate value, multiple partitions and a singleton partition. Canonical full result rows are compared, including null sums and zero counts for empty excluded frames.

All 120 reference tests pass across four files, including the sixteen added cases and existing deterministic randomized windows, timestamp intervals, ranking, value/holistic/statistical windows and format reference checks. This refresh supplies current public execution/reference evidence beyond the primitive oracle; fixture size is small and local-file based, so it establishes neither adaptive-index activation nor cloud memory-pressure behavior. Runtime source remains `558e3a5`.

Final verification: all 120 reference tests, typecheck and formatting passed. Evidence: `docs/validation/window-dynamic-reference-2026-11-14.json`. No runtime implementation changed.


### F4/M1 adaptive peer-index view admission (2026-11-14)

The allocation review found that `WindowPeers` admitted only `(groups + 1) * 4` backing bytes before constructing its Uint32Array. It now also reserves the shared `MATERIALIZATION_CHARGES.view` charge before allocation. The optional index still falls back to cursor traversal when the full reservation cannot fit; no new user configuration or query failure is introduced. Disposal releases the combined reservation. Fixed cursor/helper object accounting remains part of the broader allocation review, so this is not complete window heap accounting.

Updated the existing adaptive-index exact/cap-minus-one tests and independent dynamic-frame matrix to use backing plus view: forty groups now require 292 reservation bytes (164 backing plus 128 view), with a 291-byte capacity exercising the fallback. The earlier 164/163-byte evidence is historical and predates this correction. Results, exclusions and traversal behavior are unchanged; only optional-index admission reflects the full modeled typed-array charge.

Final verification: build/typecheck passed; 3,731 regular tests (645 skipped), 120 reference tests and 82 Worker tests passed. The sixteen added regular skips are opt-in reference cases that run successfully in the reference command. Evidence: `docs/validation/window-peer-view-admission-2026-11-14.json`.


### V3 current release-gate refresh after window/read-control changes (2026-11-14)

Source `9206579` passes coverage with 3,731 tests / 645 skipped and unchanged thresholds: statements 91.29%, branches 85.42%, functions 95.82%, lines 93.09%. Build, clean package consumer (66 installed packages), required external conformance (520 passed / one inactive missing-fixture guard), lint, compatibility, large-file, changeset and deterministic fixture-regeneration checks pass. Fixture hashes remain identical and git status is clean after regeneration. Typecheck, 120 reference tests and 82 Worker tests passed on the identical implementation immediately before that commit.

Updated conflicting current-release pointers while retaining historical evidence. Evidence: `docs/validation/current-window-release-gates-2026-11-14.json`. Green release checks do not close the full allocation, lifecycle, runtime headroom, scaling and performance acceptance checklist.


### F5 scalar width scaling and candidate accounting follow-up (2026-11-14)

Expanded `bench/top-k-scalar-workspace.mjs` from one packed-text width to 16/128/1024 bytes, retaining bigint and timestamp96 cases, 100/10,000 rows, top-3, two warmups and seven samples. Every iteration verifies exact selected values and zero final reservations. Tracked incremental peak remains constant by row count for every shape: bigint 140 bytes, timestamp96 524 bytes, packed text 460/908/4492 bytes by width. At 10,000 rows, observed medians were 0.744/1.625 ms for bigint/timestamp96 and 2.803/3.274/4.831 ms for packed widths. Short kernel timings do not establish production throughput or a speedup.

Source review confirms direct scalar reads with reusable workspace rather than a batch-sized key cache. It also identifies a remaining M1 gap: `vectorTopKOwnedBatch` admits only `capacity * 4` before constructing its Uint32Array candidate heap, omitting the standard view charge. These benchmark figures therefore describe current tracked reservations, not complete allocation coverage. A measured comparison against admitted key caching and broader bookkeeping review remain open under F5/M1.

Evidence: `docs/validation/top-k-scalar-widths-2026-11-14.json` and raw `docs/benchmarks/top-k-scalar-widths-2026-11-14.json`; runtime source `9206579`. Harness assertions and formatting passed; no runtime implementation changed.


### F5/M1 top-k candidate view admission (2026-11-14)

`vectorTopKOwnedBatch` now admits `MATERIALIZATION_CHARGES.view + capacity * 4` before constructing its candidate Uint32Array, including the zero-capacity case. Capacity remains limited by actual selected rows and requested offset/limit. Updated scalar comparison exact/cap-minus-one budgets and the sparse-selection oversized-limit test to include the 128-byte view charge. All eleven focused top-k memory tests pass, covering scalar shapes, empty/zero-limit results, ties/nulls, ordering directions and offsets.

This corrects the demonstrated candidate backing/view gap. Temporary subarray selection views and normalized order metadata still require separate review; no complete top-k allocation claim follows from this change. The preceding scalar-width report is historical and excludes the new candidate view charge.

Final verification: build/typecheck, 3,731 regular tests (645 skipped) and 82 Worker tests passed. Refreshed width benchmark keeps workspace constant across row counts, with the expected 128-byte increment per shape. Evidence: `docs/validation/top-k-candidate-view-2026-11-14.json`; raw benchmark `docs/benchmarks/top-k-candidate-view-2026-11-14.json`.


### F5/M1 top-k final selection view reuse (2026-11-14)

Removed the mixed-input path's redundant `heap.subarray(0, size)` view. Candidate capacity is already bounded by actual selected rows and offset+limit, so completed selection fills that capacity. Both monotone and heap paths now share one final gather helper: whole-array selections reuse the admitted heap; sliced selections reserve `MATERIALIZATION_CHARGES.view` before constructing their subarray. Scalar comparison workspace still releases before gather, and input/heap/output overlap remains charged.

Extended exact-capacity/cap-minus-one replay to offsets zero, one and beyond all rows. All thirteen focused top-k tests pass, including existing null/tie/selection/order/limit comparisons. This removes an allocation while accounting for required slices; normalized ordering metadata and broader gather/operator bookkeeping remain separate M1 work. No cloud throughput or complete heap-bound claim is made.

Final verification: build/typecheck, 3,733 regular tests (645 skipped) and 82 Worker tests passed. Evidence: `docs/validation/top-k-selection-view-2026-11-14.json`.


### F5/M1 shared vector order normalization and admission (2026-11-14)

The shared vector order normalizer previously serialized `[column, direction, nulls]` for every input term into a deduplication Set, and owned top-k ran normalization outside its scope. It now maps each borrowed column name to a four-bit direction/null-order mask, avoiding per-term JSON strings while preserving the first occurrence of each equivalent term. The same helper serves owned and legacy vector sort/top-k paths. Owned top-k admits the normalized array and unique records in its operation scope and admits Map/entry scratch in a child scope released before candidate selection. Invalid-column/normalization errors unwind the owned scope.

Updated exact-budget tests to include retained normalized terms and scratch overlap. A new Unicode/quoted-column case repeats all four normalized variants one hundred times, verifies identical output and identical tracked peaks, then checks exact capacity and one byte below. All fourteen focused tests pass. For a single selected numeric row, normalization scratch now dominates peak admission; the test reflects that overlap rather than treating the heap as the only workspace. Caller-owned term arrays and borrowed column strings remain input storage, not cloned allocations.

The fused projection fixture now admits the larger normalization scratch peak while retaining its exact final-storage assertion. Final verification: build/typecheck, 3,734 regular tests (645 skipped) and 82 Worker tests passed. Evidence: `docs/validation/vector-order-normalization-2026-11-14.json`. No measured speedup is claimed.


### F5 current top-k scaling after metadata admission (2026-11-14)

Refreshed scalar-width and numeric owned/borrowed kernels on source `a8ba6db`. All ten scalar cases verify exact output and zero final reservations; tracked workspace remains constant from 100 to 10,000 rows for each shape: bigint top-3 604 bytes, timestamp96 988 bytes, packed widths 16/128/1024 at 924/1372/4956 bytes. These include candidate-view and normalized-order charges added since earlier reports.

All twelve numeric cases (10,000/100,000 rows, ascending/descending/mixed distribution, limits 10/1,000) pass output equality and release checks. Peak incremental tracked storage is 664 bytes for top-10 and 20,464 bytes for top-1,000, invariant by row count/distribution. At 100,000 mixed rows, observed current borrowed/owned medians were 18.13/2.96 ms for top-10 and 34.95/6.62 ms for top-1,000. This compares two current implementations, not a before/after improvement; input creation is excluded and the owned result is released during timing. Native heap/GC, cloud transport and alternative admitted key-cache strategies remain outside these measurements.

Raw reports and conditions: `docs/benchmarks/top-k-normalized-workspace-2026-11-14.json`, `docs/benchmarks/vector-top-k-current-2026-11-14.json`; summary `docs/validation/top-k-normalized-workspace-2026-11-14.json`. No runtime implementation changed.


### F5 measured direct packed reads versus admitted decoded-key cache (2026-11-14)

Added an experimental benchmark using the same owned top-k operator over either packed UTF8 input or an explicitly admitted string-vector key cache. Both original input and cache remain charged through selection; cache construction is included in timing. Cache storage is admitted in bulk before decoding, avoiding per-key scope bookkeeping as a confounding cost. The ASCII fixed-width fixture gives a known decoded string bound. Twelve width/row-count/limit scenarios run two warmups and five samples for each variant, checking exact output and zero final reservations each time.

Cached keys have lower observed medians in eleven of twelve cases. For 10,000 rows and top-3, direct/cached medians are 2.396/1.233 ms at width 16, 2.577/1.821 ms at width 128, and 4.463/5.190 ms at width 1024. The memory tradeoff is substantial: cache incremental tracked peaks are approximately 1.1/3.4/21.3 MB by width, while direct reads use roughly 1–5 KB. Top-100 can benefit more from decoded reuse, but sequential short kernel timings are not a production performance guarantee.

No production policy changed. These measurements support investigating a bounded headroom/workload-aware choice, not unconditional whole-batch caching. Nulls, dictionary encodings, multiple keys, reuse across jobs, concurrency and Worker-native costs remain unmeasured here. Evidence: `docs/validation/top-k-key-cache-2026-11-14.json`, with raw samples and harness hash in `docs/benchmarks/top-k-key-cache-2026-11-14.json`. Runtime source `a8ba6db`; benchmark assertions and formatting passed.


### F5 nullable decoded-key cache tradeoff matrix (2026-11-14)

Extended the experimental key-cache benchmark to no-null, nulls-first and nulls-last cases with every seventh row null, yielding 36 scenarios/two variants. Expected results come from an independent nullable string sort; top-100 on 100 rows also verifies null outputs. Cached construction skips null payload decoding and explicitly admits/copies validity storage. Every iteration verifies exact values and zero final reservations.

Cached medians were lower in 35/36 scenarios in this run; 10,000-row, 1024-byte, nulls-first top-3 was slightly slower (2.793 versus 2.642 ms). The earlier non-null wide top-3 result reversed in this run, demonstrating that these sequential microbenchmarks cannot select a stable production threshold. Memory conclusions are more deterministic: nullable wide caches retain roughly 18.3 MB versus about 5 KB of direct workspace. No production default changed. Dictionary/multiple-key/Unicode payload/reuse/concurrency evidence remains open.

Evidence: `docs/validation/top-k-key-cache-nullable-2026-11-14.json` and raw `docs/benchmarks/top-k-key-cache-nullable-2026-11-14.json`, including the executed harness hash. Runtime source remains `a8ba6db`; all benchmark assertions and formatting pass.


### F1/V2 restored aggregate counter invariants (2026-11-14)

Checkpoint validation previously accepted any numeric count for count/average/variance states; mode counts accepted nonnegative integers without checking safe representability. All restored row aggregate counters now share a nonnegative safe-integer predicate. This rejects negative, fractional and unsafe counters before accumulator construction or new scanning, avoiding invalid continuation arithmetic. Sum/mean/value semantics are unchanged.

Added inline and owned-spill regression matrices covering count, average, variance and mode with each invalid counter class. All twenty-four malformed restore combinations reject with `LAKEQL_BOOKMARK_INVALID`, never invoke the scanner and leave zero tracked storage. All 31 checkpoint tests pass, including valid continuation and ownership cases. These are memory-adapter malformed-state fixtures; actual transport operation remains covered by separate HTTP/R2 continuation suites. Broader aggregate lifecycle and allocation acceptance remains open.

Final verification: build/typecheck, 3,736 regular tests (645 skipped) and 82 Worker tests passed. Evidence: `docs/validation/aggregate-restore-counters-2026-11-14.json`.


### F1/V2 shared row/vector restored-counter validation (2026-11-14)

The analogous vector restore path also accepted arbitrary numeric counters. Extracted the nonnegative safe-integer predicate into `aggregate-count.ts` and reused it from row checkpoint validation and vector state restoration. Counter-bearing vector states validate before construction; mode entry counts validate in the existing restoration loop, with no added whole-snapshot pass. Public vector errors use the existing validation-error family; row checkpoint errors remain bookmark-invalid.

Seven operation cases cover count, average, all four variance/stddev variants and mode. Each rejects negative, fractional, unsafe, NaN and infinite counters through direct and scoped restore; valid zero/one/maximum-safe counters round-trip. Scoped failures dispose cleanly. The two focused row/vector suites pass 40 tests. This aligns counter invariants without changing valid accumulator arithmetic or claiming full restore-shape validation.

Final verification: build/typecheck, 3,743 regular tests (645 skipped) and 82 Worker tests passed. Evidence: `docs/validation/vector-restore-counters-2026-11-14.json`.


### F1/F2 current aggregate cardinality and reservation scaling (2026-11-14)

Refreshed the public grouped-continuation harness on source `1d43a68`, adding an output-path override so historical evidence is preserved. At 2,000/4,000/8,000 unique groups, batch sizes 128 and 512 each use one reduction session, exactly one append per batch, zero inter-batch checkpoints, correct per-group counts, closed scanners and zero final reservations. Tracked peaks are 3,213,686 / 6,331,270 / 12,634,102 bytes, independent of batch size in these cases. Observed medians grow from 46.85 to 209.13 ms (batch 128) and 42.43 to 201.68 ms (batch 512): approximately 4.5–4.8× time for 4× groups, consistent with near-linear growth over this range but not an asymptotic proof.

The owned-median diagnostic verifies the expected median and zero cleanup at 1,024/8,192/65,536 rows; growth reservation calls are 6/10/38, rather than one per appended value. It uses explicit Node GC and one timing sample, so retained-heap/timing observations are diagnostic only. Public grouping uses a synthetic owned vector scanner, not cloud reads. These checks strengthen incremental accounting evidence without closing every update/merge/restore/spill lifecycle path.

Evidence: `docs/validation/aggregate-scaling-current-2026-11-14.json` and its raw report links/harness hashes. All executable assertions and formatting passed; no runtime implementation changed.

### F4/M1/M2 window output and failure lifecycle matrix

`window-lifecycle-memory.test.ts` now exercises row, sorted-run and vector-batch
execution with two interleaved partitions, cross-input peers, an empty run/batch,
null and negative aggregate values, and compatible prefix/full sort keys. Literal
expected dense ranks and cumulative sums provide an independent result oracle.
Each route measures its tracked peak, rejects with one byte occupied at that cap,
verifies failure cleanup without releasing a sibling job's pinned allocation,
and then succeeds three times on the same pool after pressure is released.
Returned output remains charged until explicit scope disposal; execution scratch
is absent from the retained output charge. Inputs remain unchanged.

All 63 tests across six focused window suites pass. This is in-memory kernel
lifecycle evidence, not a cloud or public scanner fixture, and a measured
reservation peak is not a VM heap bound. Fixed helper-object accounting and the
broader F4/M1 acceptance work remain open. See
`validation/window-lifecycle-memory-2026-11-14.json`.

### F4/M1 fixed peer metadata admission

`WindowPeers` now reserves its fixed instance and three reusable cursor records
before constructing cursors whenever a budget is provided. The explicit charge
is 1,536 bytes (four objects and twenty properties); borrowed partition keys
remain with their existing owner. Optional index admission uses remaining
headroom after that fixed charge. Disposal releases both charges idempotently.
The standalone segment-tree entry point now creates its default peer helper with
the supplied budget and disposes it on success or failure; caller-provided helpers
remain caller-owned. Unsupported and empty standalone inputs return before
constructing a helper.

Boundary tests cover exact fixed admission, one-byte pressure, sibling isolation,
retry and double disposal. Existing dynamic-frame independent oracles now test
1,828/1,827-byte total capacities (1,536 fixed plus 292/291 index headroom).
Ranking tests still bound peer-key visits; their previous zero-budget assumption
is replaced by the exact fixed charge. Tree fallback and failure tests preserve
their previous optional-workspace headroom and verify helper cleanup.

This closes the identified peer-helper gap, not all window helper/VM allocation
coverage. See `validation/window-peer-metadata-2026-11-14.json` for validation.

### F4/M1/V1 owned window output across native R2 cache eviction

The existing real-Parquet native R2 fixture now retains owned output from four
concurrent window queries at both 64 and 128 MiB shared pool caps. It evicts the
entire range cache before comparing every output against independently computed
rank, distinct-count, mode, quantile, lag/default, last-value, running-count and
cumulative-distribution values. The pool remains charged while results are held
and returns to zero after all owners release. This directly exercises published
window strings after backing-cache eviction, beyond the earlier plain-row checks.
The fixture has 100 rows in ten row groups with 1,024-character strings; it proves
operation and ownership, not near-cap runtime headroom.

All 82 Worker tests pass. All 120 real-Parquet/DuckDB reference tests also pass on
runtime `c950498`, including the dynamic RANGE/GROUPS exclusion matrix. Those
reference tests use local files; the R2 test uses a native local workerd binding,
not production cloud service measurements. See
`validation/window-owned-r2-2026-11-14.json`.

### F6 range-prefetch acceptance trace and current cloud headroom check

The current source keeps borrowed demand copying in `sliceEntry`; prefetch uses
`ensureRange` and releases its cache lease without calling that copy operation.
`range-cache.test.ts` instruments source `ArrayBuffer.slice` for local and shared
caches: prefetch makes zero discarded copies, while demand returns independent
mutable output. Owned prefetch/demand tests separately assert coalesced backing
charges, output charges, pinned lifetime and cross-job reuse.

The requirement trace is:

| Contract | Evidence |
| --- | --- |
| Copy-free prefetch and independent demand buffers | `range-cache.test.ts`, instrumented copy counts and mutation isolation |
| Bounded concurrency and early-stop planning | `range-cache.test.ts`, `owned-vector-batches.test.ts` |
| Coalescing, pending deduplication, per-job ownership | `range-cache-owned.test.ts`, `range-cache-wrapper.test.ts` |
| File/version separation and failed pending-read retry | `range-cache-wrapper.test.ts`, v1/v2 wrappers and physical-read count |
| Cancellation and transport error propagation | `range-cache-owned.test.ts`, last-waiter abort and settle-before-error cases |
| Real HTTP range semantics and full fixture correctness | `range-cache-http.test.ts`, 206 responses, request/byte/concurrency assertions and local-reference comparison |

All 107 tests across these five suites pass on runtime `4160696`. A fresh run
of `bench/cloud-range-validation.mjs` against the public R2 fixture fails at its
legacy 128 KiB remaining-headroom scenario: `LAKEQL_BUDGET_EXCEEDED`, purpose
`raw compact metadata`, root limit 67,108,864 bytes and attempted total 67,134,270.
That aborted run emits no complete report and must not be counted as a passing
cloud matrix. Its pool cap is enforced; the historical 128 KiB success claim is
not current. The harness now accepts explicit `LAKEQL_CLOUD_QUERY_HEADROOM` for
measured comparisons, retaining the same 128 KiB default and assertions. This
is a benchmark parameter, not a change to query defaults.

With 4 MiB of explicitly available headroom, the live R2 run completes all 52
scenarios, including 40 ordinary-query combinations (five query kinds, two pool
caps, one/four concurrent jobs, cold/warm cache). The maximum tracked incremental
charge is 397,512 bytes; all warm ordinary-query cases issue zero data reads.
Coalesced overlap remains four physical reads / 4,096 data bytes, with one
additional two-byte metadata probe; measured parallelism reaches exactly one or
four as configured. Every result and final pool cleanup assertion passes.
Single-run ordinary-query elapsed times range from 121.675 to 1,047.026 ms;
these network timings are descriptive, not a before/after speed claim.

See `benchmarks/cloud-range-headroom4m-2026-11-14.json` and
`validation/range-prefetch-current-2026-11-14.json`. This does not establish the
minimum safe headroom, total VM memory, or a fix for the 128 KiB scenario.
