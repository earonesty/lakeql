---
"lakeql": minor
"lakeql-lance": patch
---

Add shared and per-query allocation budgets, owned cloud range reads and cache pins, and scoped Parquet scan, filtering, projection, and top-k execution. Preserve bounded parallel range coalescing, cross-job deduplication, cancellation, and warm-cache reuse with ownership-aware adapters.

Reduce repeated aggregate accounting, window peer lookup, nested vector rebuilding, and projection intermediates. Use measured input order and available allocation headroom for bounded execution choices. Preserve raw Parquet binary data as byte arrays, including invalid UTF8, and fix dictionary-writer fallback encoding metadata.

Allocation tracking covers instrumented backing buffers, not a complete JavaScript or Worker heap cap. Legacy borrowed adapters, decoder/codec workspaces, returned JS rows, and other documented execution paths retain accounting limitations; see the performance and memory audit for verification and outstanding coverage.

Preserve all source columns when `select` is omitted or empty, including filtered, ordered, computed-projection, and task-planned queries. Predicate/order dependencies no longer accidentally become an implicit output projection; explicit column selections still enable pruning.

Admit ZSTD per-call typed workspace alongside page output and validate actual
decoded lengths, including concatenated and unknown-size frames. The decoder
adaptation retains upstream licensing and releases completed frame workspace and replaced block tables; module constants and JavaScript objects remain outside tracked limits.

Share the fixed-output ZSTD decoder with Lance deletion reads, charge temporary codec buffers to the Lance read budget, and reject decompressed length mismatches.

Keep Lance parallel range reservations charged until every sibling read settles, including when another response fails or is truncated.

Allow Lance operations to share a MemoryBudget pool and enforce maxAllocatedMemoryBytes per operation, with cleanup on dataset-open failure.

Use owned object-store range reads in Lance when available, admitting transport buffers against the shared operation budget without double-charging logical range reservations.

Admit Lance manifest-copy allocations before construction and reuse admitted backing buffers for parsing views, removing redundant copies.

Adapt Lance range coalescing to available shared memory and remaining byte limits, falling back to exact reads when optional gaps would exceed capacity.

Bound Lance pending range work as well as active transport concurrency, stopping new reads after failure while awaiting active siblings before cleanup.

Drain active Lance fragment and file work before operation cleanup on failure, with bounded callback windows and no admission of further siblings after an observed error.

Bound Iceberg snapshot and manifest hydration callbacks while preserving shared manifest deduplication and waiting for active siblings before failure cleanup.

Wait for every Parquet column iterator to finish cleanup before propagating a closer failure and exiting the read scope.

Bound owned and borrowed Parquet cache read-ahead callbacks, defaulting to four with an optional maxConcurrentPrefetchReads setting, while preserving allocation-pressure fallback and draining active reads on failure.

Decode unannotated fixed-width PLAIN binary columns as packed vectors with implicit offsets, preserving owned page backing and admitting nullable index buffers.

Keep eligible text and binary Parquet dictionaries in packed owned backing, avoiding per-value view arrays and repeated dictionary-vector construction while preserving cache and output lifetimes.

Share dictionary predicate mask expansion across comparison and spatial/H3 kernels, releasing temporary dictionary masks immediately after expansion or failed output admission.

Evaluate compound predicate masks incrementally, reusing private mask backing and releasing consumed operands while preserving SQL null semantics and source columns.

Use compact admitted branch-choice vectors for CASE expressions, releasing consumed WHEN masks and selecting values directly while preserving first-match and SQL null semantics.

Preserve direct column-expression vector aliases in projections, avoiding redundant decoding and output copies while retaining source vector shape and existing input ownership.

Reuse scalar IN candidate workspace across rows and dynamically admit sorted numeric literal lookups when comparison savings and available allocation headroom justify them. Preserve null/negation semantics and fall back when optional scratch cannot fit; release lookup workspace before returning the predicate mask.

Release all-present Parquet definition levels before value decoding and reuse normalized byte-wide levels as nullable packed binary/text validity. Preserve output ownership after decoder disposal while reducing mandatory workspace peaks.

Consume exact owned compact numeric/boolean/dictionary scratch after nullable page expansion and reuse normalized byte levels as final validity. Add conditional exact-allocation release without releasing borrowed aliases or independently retained owners.

Register byte-stream-split numeric output as the exact owner of its transposed allocation so nullable expansion releases copied scratch without guessing at borrowed backing aliases.

Index exact allocation ownership to avoid quadratic retain/release searches across large scopes, preserving insertion-order behavior for duplicate values and independent retained handles.

Reuse scope ownership handles as exact-index nodes to reduce per-owner bookkeeping, keeping internal links non-enumerable. Add a reproducible Node heap benchmark documenting the remaining gap between typed payload charges and total ownership overhead.

Share immutable ancestor chains across a budget's reservations instead of allocating one chain per handle, reducing live ownership bookkeeping while preserving shared-parent admission and retention semantics.

Share composite backing pins across same-budget retained handles instead of cloning every backing reservation. Preserve per-child admission, rollback and reentrant eviction protection for cross-budget transfers; release backing only after the last composite handle.

Reuse immutable scope backing snapshots while ownership is unchanged, invalidating them on allocation/adoption/release and disposal while preserving independent output lifetimes and excluding pending I/O.

Preserve schema-declared maps recursively through nested lists, structs and map values before vector inference, fixing absent keys becoming null-valued struct fields. Verify public projected list-of-map reads and owned exact-budget boundaries against real external Parquet fixtures and DuckDB.

Keep early-stop Parquet reads page-driven instead of prefetching complete column
chunks. Avoid speculative owned range expansion when the configured cache cannot
retain allocations from the query's pool; compatible caches preserve coalescing
and reuse.

Release owned numeric decompression scratch once output no longer aliases it,
while preserving backing for aligned PLAIN views and packed payloads.

Admit fixed-width owned concat output across all columns before construction,
including validity masks, while keeping input/output overlap charged.

Include shared-dictionary concat in atomic output planning, accounting for shared
index buffers, independent validity masks, and retained input ownership.

Concatenate direct packed UTF-8 and binary vectors through admitted payload copies
without row materialization, preserving packed output and detaching source backing.

Concatenate compatible struct fields recursively with parent validity and atomic
planned output admission, retaining ownership of nested shared dictionaries.

Preserve list/map schemas during concat with typed child concatenation and rebased
offsets. Keep empty maps distinct from structs, normalize null collection batches,
and retain direct backing when null normalization introduces dictionary aliases.

Preserve an existing nested dictionary when concatenating null batches, avoiding
map-to-struct normalization and keeping temporary indices out of shared reuse caches.

Concatenate differing nested dictionaries through admitted typed selections and
recursive child operators, preserving empty maps and null dictionary leaves
without materializing whole rows or unused collection payload.

Admit distinct fixed-width dictionary concat outputs across all planned columns
before construction, sharing shape/nullability measurement with the typed kernel.
Avoid selected-row planning scans for dictionary chains without validity masks.

Add an internal bounded whole-stage scheduler with shared-pool admission,
cancellation-safe input guards, and deterministic rejection when consumer-retained
outputs prevent progress. Automatic query/decoder integration remains separate.

Share parent stage admission through cancellation-isolated child scopes so
controlled owned range reads do not reserve their output twice. Preserve pending
I/O charges and reject allocations beyond the original stage extent.

Propagate existing admission through Parquet page transport, iteration-lifetime
decoding, and ZSTD frame workspace without independently reserving those buffers.

Share remaining stage admission with independently owned range-cache transport.
Keep deduplicated reads alive for surviving consumers after the initiator cancels,
including delayed adapter scratch, while preserving one total allocation extent.

Count serialized operator budgets directly without constructing JSON strings or
UTF8 copies. Size columnar top-k batches without whole-batch row materialization,
while preserving existing serialized-budget boundaries.

Size nested list/struct/dictionary vectors directly and resolve map keys before
sizing surviving values, avoiding nested payload materialization and serialization
of overwritten map entries during budget checks.

Allow Parquet struct columns through generic vector/fallback decoding while preserving recursive leaf conversion checks. Compare nested schema shapes during multi-file planning instead of treating all groups as the same type.

Apply shared read concurrency controls to object discovery as well as payload reads, including HTTP adapters that implement discovery with range GETs. Cancel queued discovery through the same read semaphore.

Decode Parquet LZ4_RAW pages into exact-size admitted output buffers without codec scratch allocation, validating malformed lengths and back-references. Support the codec in generic numeric page admission and Worker execution.

Read deprecated Parquet LZ4 with validated Hadoop block/chunk framing or historical raw blocks using one admitted output allocation. Preserve independent chunk back-reference bounds and exact decoded length checks.

Normalize unknown owned Parquet scan failures to LAKEQL_PARQUET_READ_ERROR with source path and original cause, while preserving budget/cancellation errors and releasing owned allocations on malformed fixtures.

Reject timestamp values outside signed 64-bit vector storage instead of silently wrapping their epoch counts; preserve exact boundary values in all supported timestamp units.

Select timestamp batch precision from all values to preserve fractional epochs
independently of row order. Reject mixed UTC/local timestamp semantics and invalid
Dates explicitly rather than silently coercing them into a shared representation.

Plan mixed-unit timestamp concatenation with exact typed output admission and
write scaled epochs directly, avoiding intermediate arrays of timestamp objects.

Correct negative fractional timestamp formatting and format epochs beyond native
Date range with bounded Gregorian calendar conversion and signed extended years.

Decode Parquet FIXED_LEN_BYTE_ARRAY(12) timestamps as signed little-endian epoch
counts. Automatically select eight- or twelve-byte typed timestamp storage based
on batch values, and admit mixed-width concatenation before allocating output.
Preserve wide epochs through sorting, filtering, grouping and JSON output; verify
the Apache fixture through local and concurrent HTTP range/cache scans.

Released memory ownership handles now drop strong payload references. Reading
`value` after `release()` raises a validation error; retain an independent handle
before releasing when later access is needed. Composite backing is cleared after
the last owner, and adopted-handle cleanup remains idempotent across producer and
scope disposal order.

Add generic row-materialization planning and explicitly owned materialized batches.
Output reservations precede decoding and row construction; copied outputs detach
from input ownership while borrowed values retain their source. Reservation units
are an accounting model, not a portable VM heap measurement. Supported owned-vector projection/filter `toArray()` paths now admit collected
output and the final array before returning rows to caller management.
`QueryResult.toOwnedArray()` retains those reservations until explicit release;
unsupported execution paths reject this capability before scanning. Native string
output is charged independently without retaining unrelated decoded pages.
Remaining fallback paths and whole-runtime GC headroom are not yet covered.

Preserve late materialization for owned filtered/limited query output using a
bounded positional vector gather, avoiding query-sized reference maps. Carry
observed HTTP range behavior through planning: endpoints that ignore ranges use
eager output reads and avoid splitting demanded chunks when allocation headroom
permits. Tight budgets still select page windows. Validate native and ignored
HTTP ranges, cold/warm concurrent jobs, offsets, failure cleanup, and Worker
binary ownership lifetimes.

Admit final materialized rows from owned vector top-k before construction, and
return completed columnar top-k arrays directly from `toArray()` to avoid a second
collector array. Plain returned rows transfer to caller management; deferred
rank-reference and legacy borrowed sort paths retain documented limitations.

Derive omitted owned-scan projections from the Parquet schema so supported columns
use packed vectors by default, avoiding premature JS text materialization. Keep
capability-based fallback for unsupported shapes. Accept multi-group padding in
bit-packed definition levels while bounding decoded output by declared page rows
and validating encoded input extents; verify against DuckDB and exact-budget tests.

Indexed footer acquisition and owned row-group decoding now reserve their selected
raw compact metadata structures before construction. Oversized initial footer reads
can retry with exact footer bytes when envelope admission runs out of headroom.
Converted metadata objects remain a separate accounting gap.

Owned metadata parsing also admits decoded names, paths, key/value and CRS strings
before UTF-8 conversion, retaining their charges with the metadata owner.
Textual metadata statistics receive the same pre-decode admission, preserving
BOM semantics and propagating budget rejection.
Owned column metadata conversion now admits new object and mapped-array containers
before constructing them, while preserving borrowed histogram/crypto ownership.
Row-group objects, sorting metadata, and streamed column-list slots now receive
pre-construction admission under the same metadata ownership scope.
Failed output collection releases accumulated chunks before awaiting asynchronous
producer closure, allowing sibling allocations to progress during remote cleanup.
Cancellation releases accumulated owned output immediately while a producer read
is pending, with listener cleanup when collection terminates.
Owned footer envelopes now admit schema/key-value containers and result objects
before construction, retaining charges with the metadata owner.
Logical metadata conversion admits recognized object shapes and shares fixed
lookup tables across schema fields while preserving unknown raw metadata.
Fixed-width metadata statistics now reserve conversion charges before decoding;
checked decimal fallback propagates memory-budget failures.
Single-batch owned collection hands off the existing array through independent
ownership instead of allocating a duplicate result array.
Materialization reserves its full peak requirement before conversion, then releases
transient descriptor and UTF-8 view charges while retaining output payload ownership.
Materialization preflight enumerates own fields directly, avoiding repeated
Object.entries descriptor arrays before admission for nested output shapes.
Preserve prototype-like column names in generic vector construction, projection,
and selected row materialization, verified through owned Parquet queries.
Owned materialization failures synchronously release previously collected output
before asynchronous scan cleanup, preserving original errors and allowing sibling
jobs to progress within the existing shared allocation cap.
Checked scalar decimal statistics admit numeric output and bounded exponent-formatting
scratch before conversion, releasing scratch after completion.
Indexed schema-leaf copies now use admitted, single-pass construction with rollback
and source lifetime ownership; footer narrowing includes their memory requirements.
GeoParquet JSON trees are admitted before parsing and released after annotation
construction; retained CRS strings and annotations follow indexed-source ownership.
Root-column marking skips complete nested schema subtrees and preserves existing
logical annotations, with encoded-footer parity and allocation rollback coverage.
Owned full-metadata fallback now admits decoded metadata and checked statistics,
with transactional rollback and an exact-footer retry when speculative backing
leaves insufficient decoding headroom. Metadata owners retain these charges
through cache reuse and eviction.
Fixed-byte statistic conversion now admits temporary views and FLOAT16 numeric
results, preserving borrowed payload aliases and releasing conversion scratch.
Row-group parsing releases temporary raw records after conversion while separately
retaining forwarded metadata aliases, reducing full-metadata retention across
many groups with explicit admission for the conversion overlap.
Retained raw metadata aliases now transfer existing parser admission instead of
requiring duplicate capacity during conversion, preserving the lower retained
footprint without increasing indexed metadata peak allocation.
Fallback string/binary vectors now build one admitted output array instead of
slice/map intermediates and admit new binary view headers while retaining existing
payload aliases. Incompatible decoded value kinds produce typed read errors.
Scoped flat delta byte-array pages now use admitted prefix/suffix scratch and
packed payload reconstruction, with shared v1/v2 level handling and explicit
expansion validation before allocation.
Reconstructed packed delta output releases tracked decompression scratch promptly;
length-only and PLAIN payloads preserve their borrowed backing ownership.
Fallback page compaction reuses already-dense arrays and admits exact output
storage only when null placeholders must be removed.
Scoped DATE/timestamp conversion admits result arrays and temporal values before
parsing and avoids the upstream intermediate Array.from copy.
Both fallback page versions now route non-dictionary logical conversion through
the shared scoped temporal/decimal converter.
Legacy INT96 temporal conversion now admits result storage and bounded arithmetic
scratch while preserving nanosecond precision across the Unix epoch.
Scoped PLAIN INT96 decoding validates extents and admits carrier arrays and
arithmetic scratch, preserving all high/low bits without low-word sign extension.

Admit PLAIN binary dictionary carrier arrays and borrowed view headers before
construction, release offset scratch promptly, and reject missing fixed widths
and truncated payloads without committing reader state.

Route flat logical binary PLAIN data pages through admitted physical decoding
and retain decompression backing while binary carrier views borrow it.

Admit FLOAT16 logical output before conversion, eliminate its intermediate array
copy, and validate physical widths before allocation.

Add rollback-safe admitted JSON conversion and correct JSON scalar-vector
capability checks so heterogeneous objects are not silently returned as null.
Mixed object/array batches use generic union vectors with struct null normalization.

Admit inferred string-vector arrays while preserving single-pass expression
construction, and roll back inference allocations when a value shape is rejected.

Add dense union storage traversal, slicing, shared gathers, retained-byte and
materialization accounting. Infer recursive mixed-shape vectors with admitted routing and child storage;
operator capabilities remain explicit.

Plan and admit union concatenation using bounded-fanout routing over retained
children, including mixed null and dictionary inputs, without payload inspection
for shape detection.

Route flat JSON scans through admitted logical conversion and generic union
vectors using replayable null/dictionary expansion, with validated physical
counts and indices and no row-sized reconstruction array.

Release union source-row routing and child-start scratch after child construction,
retaining only the tags and child-local offsets needed by the finished vector.

Pack union concatenation trees into full lower levels to avoid sparse intermediate
nodes; retain explicit admission and source ownership. Incremental accumulation
compaction remains a separately tracked performance issue.

Bound selective ownership traversal with admitted frames and release deep
composite histories iteratively, including cleanup after third-party release errors.

Compact deeply accumulated owned union concat routing automatically using a
row-count-derived depth threshold, admitted flattening, and alias-safe selective
backing retention. Dictionary-selected histories remain a separate compaction case.

Expose the generic bounded-fanout union concatenation planner for shared-payload
Parquet row-fragment normalization; internal nested decoding now has admitted
page, dictionary, row-sealing, and cross-column alignment stages.

Select the scoped nested Parquet pipeline for supported owned reads, preserving
nested null/list/map structure, bounded column concurrency, decoded-cache reuse,
and retained output lifetimes. Interpret Spark INT96 using writer metadata and
its signed microsecond arithmetic. Omitted-value maps produce null map values.

Accept legacy optional MAP key annotations while rejecting actual null keys, and
ignore unused zero dictionary offsets on columns without dictionary encoding.

Adapt nested output assembly to available memory by shrinking row windows after
allocation admission failures, preserving the requested batch-size upper bound
and avoiding repeated cloud reads. Release unused header-probe backing on nested
decoded-cache hits.

Use compact nested pages for schema assembly, avoiding unused nullable leaf-slot
expansion. Keep decoded page cache shapes distinct while sharing dictionaries.

Release nested decompression buffers after physical reconstruction when no output
aliases them, while preserving backing ownership for numeric and binary views.

Reject buffering aggregate growth before adding an over-limit item, preserving
existing collection contents on serialized-byte or buffered-row budget failure.

Own deferred top-k candidates, late row materialization and projected output;
transfer only requested winner payloads after offsets. Admit read-window planning
and shared deferred-join containers, reject malformed late scans, and preserve
cache reuse by sharing the lake allocation pool with configured caches by default.
Reuse object planning when emulated-range reads require eager fallback.

Preserve complete INT96 physical values in public and fallback Parquet reads, sharing automatic Spark-writer interpretation with owned decoding. Bundle the pinned reader fix so clean npm installations receive the same behavior.

Preserve dynamic JSON object members through vector inference, batching, gathering, and concatenation: missing keys remain absent rather than becoming null-valued struct fields. Admit JSON map storage before construction and preserve prototype-named own properties during public-row normalization.

Read key-only Parquet maps as maps with null values, and preserve empty unannotated repeated fields as empty arrays across public, column, and owned readers. Keep optional nulls distinct and validate map shape before using map assembly.

Validate Parquet schemas in one iterative pass, avoiding repeated ancestor-path copies and call-stack exhaustion on deep selected or skipped subtrees. Preserve projection and exact decimal error paths.

Admit temporary schema-validation directories against caller budgets in owned scans, task execution, and indexed planning; release them on every exit and skip traversal for empty projections.

Close suspended row iterators when NDJSON, JSON, or CSV encoding fails, releasing producer-owned allocations without masking the original error. Share cleanup across failure and cancellation races.

Decode ALP FLOAT/DOUBLE pages through shared public and owned reader dispatch, preserving exception bits and admitting output buffers before allocation. Validate page extents and exception positions before decoding.

Decode BSON logical columns with admitted nonrecursive framing and value construction, exact integer/datetime values and explicit tagged BSON-specific values. Share conversion across public, owned, packed-leaf and dictionary paths; reject malformed framing, duplicate keys and invalid UTF-8.

Decode Brotli pages into fixed output with admitted reusable workspace and decoder metadata. Reject oversized pages before output allocation, validate full input consumption, and release scratch allocations on success and failure.

Admit scalar filter temporaries across ordinary row batches, row aggregates, ordered selection and checkpointed sorting; release predicate scratch before yielding and on failure or cancellation.

Admit projected values and result directories in checkpointed top-k/sort and per-row aggregate projection. Avoid intermediate selected-row copies and preserve ownership during spill.

Omit unsafe truncated writer statistics and dependent column indexes instead of pruning matching long strings. Preserve null counts/offset indexes and bundle the patched writer for npm consumers.

Bound ordinary row projection by output batch size and collect fallback rows with explicit ownership. Preserve selected mutable values across scanner reuse, retain known immutable projection backing without duplicate payload charges, and release failed collected output before asynchronous cleanup.

Require safe integer query and aggregate row bounds, positive safe batch/group capacities, and a safe `offset + limit` sum. Reject invalid bounds before scans or vector selection allocation; retain support for large safe limits over small inputs.

Apply the same exact-integer contract to restored top-k bounds (including their sum), slice/resumable batch capacities, and policy row caps. Invalid restored state rejects before discovery; invalid policy caps cannot be hidden by a smaller explicit limit.

Bound slice accumulation using the shared owned row collector. Detach selected rows before borrowed scanners advance or close, admit payload and collection arrays, and release failed collection before asynchronous producer cleanup. Preserve pagination/lookahead behavior and transfer completed plain slices to caller management.
