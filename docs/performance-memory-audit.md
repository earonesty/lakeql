# Performance and memory budget audit

Latest package/Worker evidence: `docs/validation/current-package-worker-gates-2026-11-14.json`. Latest coverage/reference/conformance evidence: `docs/validation/deferred-spill-gates-2026-11-14.json`. Prior gate: `docs/validation/task-planning-cleanup-2026-11-14.json`. Prior full release-command refresh: `docs/validation/task-materialization-gates-2026-11-14.json`. Prior coverage gate: `docs/validation/task-ownership-release-gates-2026-11-14.json`. Latest HTTP fixture evidence: `docs/validation/borrowed-deferred-http-2026-11-14.json`. Latest scaling evidence: `docs/validation/column-top-k-scaling-2026-11-14.json`. Latest implementation evidence: `docs/validation/borrowed-projected-admission-2026-11-14.json`. Projection caller unification: `docs/validation/projection-caller-unification-2026-11-14.json`. Projection descriptors: `docs/validation/projection-directory-admission-2026-11-14.json`. Reduction projection lineage: `docs/validation/reduction-projection-lineage-2026-11-14.json`. Query fallback checkpoints: `docs/validation/query-fallback-checkpoints-2026-11-14.json`. Scoped physical reductions: `docs/validation/scoped-reduction-checkpoints-2026-11-14.json`. Aggregate planning: `docs/validation/query-aggregate-column-planning-2026-11-14.json`. Deferred aggregation: `docs/validation/borrowed-deferred-aggregate-2026-11-14.json`. Columnar top-k: `docs/validation/borrowed-column-top-k-2026-11-14.json`. Column planning: `docs/validation/top-k-column-planning-2026-11-14.json`. Borrowed top-k: `docs/validation/borrowed-deferred-top-k-2026-11-14.json`. Commit cancellation: `docs/validation/checkpoint-commit-cancellation-2026-11-14.json`. Window batch integration: `docs/validation/window-spill-batch-2026-11-14.json`. Sort integration: `docs/validation/sort-spill-batch-2026-11-14.json`. Batch contract: `docs/validation/spill-write-batch-contract-2026-11-14.json`. Staged query checkpoints: `docs/validation/staged-query-checkpoints-2026-11-14.json`. Staged spill contract: `docs/validation/staged-spill-contract-2026-11-14.json`. Spill accounting: `docs/validation/spill-accounting-2026-11-14.json`. In-flight metadata cancellation: `docs/validation/task-metadata-inflight-cancellation-2026-11-14.json`. Planning cancellation: `docs/validation/task-planning-cancellation-2026-11-14.json`. Physical projection: `docs/validation/task-physical-projection-2026-11-14.json`. Aggregate column planning: `docs/validation/aggregate-column-planning-2026-11-14.json`. Grouping admission: `docs/validation/task-grouping-admission-2026-11-14.json`. Grouped concurrency: `docs/validation/grouped-task-concurrency-2026-11-14.json`. Mixed HTTP tasks: `docs/validation/mixed-task-http-2026-11-14.json`. Range-grouping baseline: `docs/validation/task-range-grouping-2026-11-14.json`. Owned planning: `docs/validation/owned-work-unit-planning-2026-11-14.json`. Work-unit topology: `docs/validation/work-unit-topology-2026-11-14.json`. Planning metadata: `docs/validation/work-unit-planning-metadata-2026-11-14.json`. Row-window iteration: `docs/validation/task-row-window-iteration-2026-11-14.json`. Planner workspace: `docs/validation/materialization-planner-workspace-2026-11-14.json`. Dynamic scalar workspace: `docs/validation/task-dynamic-scalar-workspace-2026-11-14.json`. Residual selection: `docs/validation/task-residual-selection-2026-11-14.json`. Column vectors: `docs/validation/task-column-vectors-2026-11-14.json`. Row transformations: `docs/validation/task-row-transform-2026-11-14.json`. Metadata lifetime: `docs/validation/task-metadata-ownership-2026-11-14.json`. Partition constants: `docs/validation/constant-partition-vectors-2026-11-14.json`. Grouped task ownership: `docs/validation/owned-group-fanin-2026-11-14.json`. Task scan: `docs/validation/owned-group-task-scan-2026-11-14.json`. Scalar task ownership: `docs/validation/owned-scalar-aggregate-tasks-2026-11-14.json`. Merge capability: `docs/validation/reduction-checkpoint-merge-2026-11-14.json`. Immutable reuse: `docs/validation/window-task-immutable-index-2026-11-14.json`. Latest execution evidence: `docs/validation/window-task-http-cache-2026-11-14.json`. Scheduler admission: `docs/validation/work-unit-scheduler-admission-2026-11-14.json`. Owned runner: `docs/validation/owned-window-task-runner-2026-11-14.json`. Parquet ownership: `docs/validation/owned-parquet-window-tasks-2026-11-14.json`. Partition admission: `docs/validation/window-task-partition-admission-2026-11-14.json`. Failure lifetime: `docs/validation/work-unit-failure-drain-2026-11-14.json`. Task merge admission: `docs/validation/window-task-sparse-buckets-2026-11-14.json`. Last full release gates: `docs/validation/release-refresh-2026-11-14.json`. Full release acceptance remains open.

LakeQL has further opportunities to remove repeated whole-state work from per-row
execution. The highest-value work is shared incremental memory accounting, followed
by bounded selection and indexed window-frame lookup. Improvements must preserve
parallel range reads and respect configured memory limits, including allocations
made to accelerate execution.

Schema traversal findings: `docs/validation/schema-validation-2026-11-13.json`.
Stream/output admission and scalar-workspace benchmarks: `docs/validation/stream-lifetime-2026-11-13.json`.
The original findings describe upstream `main` snapshot `b54d4ed`, retrieved
November 12, 2026. Implementation on `perf/memory-bounded-execution` now includes
measured kernel improvements, owned read/cache primitives, and local-HTTP/public-R2
validation. The original findings below remain baseline descriptions; the progress
sections distinguish completed changes from remaining gaps. A whole-query or
whole-Worker allocation cap is **not yet implemented or proven**.

Worker heap stress **contradicts a whole-heap interpretation of the configured
pool**. An earlier unconstrained twenty-round owned-output run reached about 311 MB sampled JS
heap with four jobs per round and a 64 MiB pool, despite bounded live reservations
and output release after every round. Diagnostic GC after final eviction reduced
that heap to about 4.1 MB. Default packed scans improve short-run behavior but do
not solve delayed reclamation. These are local workerd/native-emulated-R2
observations without production Cloudflare memory enforcement; sparse samples
are lower bounds, not exact peaks or production-OOM predictions. Evidence:
`docs/benchmarks/worker-heap-packed-repeated-2026-11-13.json`.
A subsequent constrained managed-heap experiment completed the same workload
under verified 64/128 MiB V8 heap settings; this distinguishes runtime GC policy
from a persistent leak, but does not cap external backing or prove production
Cloudflare total-memory safety. See the constrained-runtime section below.

Window buffer chunking and measurements: `docs/validation/window-key-ledger-2026-11-13.json` (shared quantile/mode/distinct ledger).

Window quantile buffer admission and real HTTP/Worker fixture evidence: `docs/validation/window-quantile-admission-2026-11-13.json`. Mode/distinct key admission and fixture evidence: `docs/validation/window-key-admission-2026-11-13.json`; scalar payload ownership remains open.

Historical checkpoint (November 13, 2026): coverage and fixture regeneration
passed before ALP, BSON and Brotli integration.
Release-gate evidence: `docs/validation/release-gate-review-2026-11-13.json`.
Window evidence: `docs/validation/window-offset-cursors-2026-11-13.json`,
`docs/validation/window-frame-iterator-2026-11-13.json`, and
`docs/validation/window-segment-budget-2026-11-13.json`.

External conformance reports **517 passed, one skipped**. ALP and BSON decode;
Brotli decodes bounded pages and rejects oversized pages at the requested cap.
Conformance inspects page extents rather than filenames to require that rejection.
Generic decimal operators, broader allocation coverage and final release gates
remain unfinished. These checks do not establish whole-runtime memory safety.

Supported public owned nested reads now use admitted schema planning, bounded
column blocks, batch-level assembly and shared decoded-page/dictionary caches.
Real fixtures and HTTP/Worker-R2 tests cover continuation, windows, null/empty
containers, retained output, cache eviction and cancellation. The measured
1,024-row small-payload case improved from about 197 ms local/212 ms HTTP for the
initial per-row design to about 3 ms local/16 ms HTTP with bounded blocks and
probe-body reuse. Small 128-row cases still trail legacy decoding. See the
successive `nested-reader-*` benchmark reports for samples and allocation limits;
these results do not establish adaptive scheduling or all-workload speedups.

The latest 20-round, 65,536-row owned-output matrices complete under both 64/128 MiB
V8 managed-heap settings. Single jobs succeed 20/20; four-job rounds succeed 20/80
with a 64 MiB allocation pool and 40/80 with a 128 MiB pool. Failed jobs reject at
admission. Single-job tracked peak is 55,350,721 bytes. Synchronous release of failed
collected output before async scan cleanup restored the observed two-job completion
rate at 128 MiB without retries or weaker limits. Checksums, retained owner lifetime,
and zero usage after eviction pass. Evidence:
`docs/benchmarks/worker-heap-early-failure-{64,128}-2026-11-13.json`.
These local HTTP/workerd measurements are not production total-memory guarantees.

Default owned scans and task planning now use indexed metadata unless callers
supply a custom parsed-metadata cache or explicitly choose cached mode. Budgeted
multi-file schema discovery uses the indexed envelope, and scans reuse matching
planned ObjectInfo. Full and indexed parsing share projections that omit unused
metadata payloads. Row-group owners keep borrowed footer backing alive across
source disposal; demand and prefetch iterators release owners on all exits.

An earlier real-R2 public scan matrix passed all eight scenarios (64/128 MiB
configured pools, one/four concurrent jobs, auto/cached metadata): seven cold
scan-range reads, zero warm scan-range reads, and one discovery probe per job.
Indexed tracked peaks are 12,076 bytes for one job and 29,872 for four, with
6,144 retained cache bytes and zero after final eviction. Evidence:
`docs/benchmarks/cloud-scan-owned-metadata-2026-11-13.json`. These small-fixture
tracked allocations predate the newer metadata charges and do not prove a
64/128 MiB whole-Worker cap.

Owned metadata now admits selected raw compact trees, decoded text, converted
column/row-group/envelope containers, recognized logical objects, and fixed-width
statistics. Borrowed footer aliases retain their backing owners. Remaining gaps
include parser/planner scratch, some conversion paths, derived structures and
complete runtime validation. The earlier largest indexed fixture retained about
281 KB of JS heap with a roughly 6 MB immediate parse delta; those historical
figures are neither current peak measurements nor portable bounds. Releasing
ownership does not force garbage collection.

Owned scan buffers and scoped top-k/projection paths propagate allocation budgets.
Supported public columnar grouped and ungrouped aggregates now use owned continuation
state, independently admitted checkpoints and result storage. The current public
reproducer rejects 16-byte pools for grouped count, median and count-distinct at
128/1,024 rows; sufficient pools return correct values and release all reservations.
The unsafe historical result is archived separately. Borrowed legacy fallback,
unsupported expressions, spill/resumable state and the complete output lifecycle
remain open. Owned deferred aggregation avoids query-wide row-reference retention.

Public R2 checks cover ordinary queries with shared caches and concurrent jobs.
The largest remaining work is complete allocation coverage: remaining decoder and
metadata conversions, fallback operators, codec/runtime workspace, compiled plans
and caller-returned outputs are not covered end to end. Byte-aware scheduling
covers compatible grouped numeric scans; integration across all operators, measured
batch sizing and near-cap mixed-workload validation remain incomplete. A changeset
exists; external conformance resolution, final gates and requirement-by-requirement
acceptance remain outstanding.

### Current allocation ownership matrix

This consolidates implemented ownership boundaries and known gaps. “Admitted” means
explicit reservation under the library accounting model, not measured total heap.
The referenced tests establish their specific paths; they do not prove every query
route uses those paths. Closing the gaps remains required under M1/M2.

| Storage category | Owner and admission boundary | Release or transfer | Evidence / remaining gap |
| --- | --- | --- | --- |
| Shared and per-query capacity | `MemoryBudget` roots/children; `MemoryScope` reserves before owned allocation | Scope disposal, explicit release or retained/adopted owner | `memory-scope.test.ts`; scope/VM bookkeeping and delayed GC are not a total-heap cap |
| Raw range reads and cached backing | Owned range/cache reservations before supported body collection; pinned handles protect aliases | Read scope disposal; cache eviction releases its handle; consumers retain independent pins | `range-cache-owned.test.ts`, `range-cache-http.test.ts`, `range-cache-wrapper.test.ts`; preserve versioning, deduplication and cancellation; broader concurrent scheduling remains open |
| Decoded pages, dictionaries and header descriptors | Shared decoded cache plus query pins; `cached-page-window.ts` admits descriptor/key storage before parsing | Cache eviction and query/output owner release | `decoded-column-cache.test.ts`, `cached-page-window.test.ts`; ordinary whole-chunk routes may still read before lookup; complete converter/codec workspace remains open |
| Metadata and schema plans | Owned full/indexed footer and selected metadata scopes; borrowed footer aliases retain backing | Envelope/row-group owner disposal and iterator cleanup | Dated metadata fixture/HTTP/R2 evidence below; remaining parser/planner and conversion scratch must be inventoried and closed |
| Scan vectors and nested assembly | Owned scanners and bounded column-block/assembly scopes | Batch owner transfer, deferred-window release and scan cleanup | `query-owned-scan.test.ts`, Parquet owned-output and nested fixtures; unsupported encodings/fallback allocations remain open |
| Group and buffering aggregate state | `OwnedGroupDirectory` / `OwnedAggregateState` with shared arena and incremental ledger; admit growth before mutation | Continuation disposal; independently admitted checkpoint/result transfer | Owned state/directory tests and public admission/scaling harnesses; borrowed legacy, spill/resumable and unsupported expression routes remain open |
| Parquet task scans and fan-in | Task metadata scope; admitted row transformation output; shared dictionary partition vectors; scoped selections/gathers; owned aggregate/window partials | Per-yield output disposal, drained scheduler failure, independent final owner release | `task-execution.test.ts`, `task-batch.test.ts`, HTTP task/cache fixtures; owned work-unit planning and scoped grouping now cover supported task topology; borrowed adapter/parser preallocation, callback-created storage and runtime overhead remain open |
| Predicate masks and scalar temporaries | Scoped mask backing, per-row/per-candidate scalar scopes, fixed bigint/timestamp fast-path workspace | Consume/reuse private masks; release scratch at last consumer | Predicate/CASE/expression/bigint/timestamp memory tests; compiled plan containers, unscoped conversion callers and runtime overhead remain open |
| Sort/top-k replacements and row buffers | Incremental buffer accounting and scoped owned selection/projection workspaces | Eviction/replacement release; result transfer | `buffer-memory.test.ts`, `query-memory.test.ts`, `serialized-size.test.ts`, `vector-top-k-memory.test.ts`; full row fallback and replacement-overlap matrix still needs final review |
| Window indexes and execution state | Charged shared boundary/index metadata and pressure-sensitive alternatives | Operator/workspace teardown | Window memory/reference/scaling evidence below; whole-window lifecycle coverage remains separate and incomplete |
| Serialized stream output | JSON direct buffers; CSV cell directory/backing and final buffer admitted before allocation | Shared `ownedOutputStream` releases queued owners on handoff, cancellation or failure; caller-retained chunks leave library ownership | Encoder/lifetime tests, real HTTP/native-R2 fixtures and clean package smoke checks; scalar formatting, traversal and runtime workspace remain open. CPU tradeoffs: `stream-lifetime-2026-11-13.json` |
| Returned results and retained cache/output | Owned APIs transfer independent handles; plain Row[] APIs end library ownership at return | Explicit owner release for owned results; caller/GC lifetime for ordinary arrays | `query-owned-output.test.ts` and cache-pinning fixtures; caller-retained ordinary results and total process memory cannot be inferred from pool usage |

CASE plan admission is now integrated into the supported owned aggregate route.
Materialization traversal is scoped in owned, window-vector and task paths. Scalar task predicates choose batch workspace when it fits within half the available capacity, otherwise one-row workspace; the local benchmark records the CPU/memory tradeoff. Standalone unscoped planners and runtime overhead remain outside this admission claim.
Remaining expression/conversion contracts and public fallback/state routes still
require implementation. The mixed-operator concurrency
matrix and all V3 gates must still be satisfied; the matrix does not replace them.

Callers retaining planned tasks across jobs can use
`planParquetTaskWorkUnitsOwned(store, task, { memoryBudget, maxRowGroupsPerTask })`.
Its `OwnedMemory<TaskInput[]>` remains charged until `release()`; retain a handle
while queued or running tasks still depend on the plan. Footer metadata releases
before return. The existing `planParquetTaskWorkUnits` returns a plain array and
ends library ownership at return.

### Finite acceptance checklist

These items trace the original findings and full goal. They are acceptance units,
not a declaration that existing primitives satisfy every integration requirement.
New work must close one of these items or fix a demonstrated regression; an
adjacent optimization alone is not a reason to expand the implementation.

| ID | Required outcome | Concrete acceptance evidence / remaining action |
| --- | --- | --- |
| F1 | Incremental group-by accounting | Recheck general/encoded update, merge, restore/reset and spill paths against `vector-group-by-memory.test.ts`, `aggregate-memory.test.ts` and cardinality scaling evidence. Record each lifecycle path's result. Supported public columnar reductions now use owned continuation. Public scaling now verifies zero inter-batch checkpoints and near-linear measured growth. Borrowed deferred aggregation shares bounded gathers and the owned continuation; columnar fallback checkpoints now retain scoped ownership. Broader row fallback, spill and resumable-state paths remain open. |
| F2 | Incremental row-buffer accounting | Recheck DISTINCT, sort runs, row fallback, aggregate snapshots and windows against shared serialized-size rules, pool limits and `buffer-memory`, `query-memory`, `serialized-size` tests. Identify any remaining full-state scans. |
| F3 | Bounded stable row top-k | Candidate/checkpoint ownership implemented; borrowed deferred top-k now shares admitted selection and detached output with the owned route. Rank/output column lists and execution order-term normalization now have scoped admission; borrowed columnar merges/output share owned operations. Query-builder configuration and broader output review remain open. Evidence: `docs/validation/row-top-k-acceptance-2026-11-13.json`; current gates: `docs/validation/checkpoint-release-baseline-2026-11-13.json`. |
| F4 | Reusable window boundaries | Verify RANGE/GROUPS/peers across dynamic bounds and exclusions, charged shared metadata, lower-memory behavior, and reference/scaling comparisons. |
| F5 | Vector top-k preparation | Verify redundant sorting/normalization removal; document measured direct-access versus admitted key-cache choices by shape and width. No unconditional unbounded key cache. |
| F6 | Copy-free bounded prefetch | Verify ensure-cache operation avoids discarded copies; preserve cache versioning, coalescing, pending-read deduplication and pinned backing. Compare requests, bytes, copies and latency. |
| M1 | Shared and per-query allocation contract | Consolidate a current allocation matrix covering cache, rows/vectors, state/indexes, temporary replacements, reads, decode, metadata and returned outputs. Each category needs its owner, admission point, release/transfer path, tests and remaining untracked storage. Close implementation gaps rather than treating reservations as total heap. |
| M2 | Memory-responsive execution | Verify batch/read scheduling, lower-memory operator choices and typed failure at indivisible limits. Exercise cancellation, fairness and self-held-capacity deadlock cases. Document smart defaults and any required tradeoff configuration. |
| V1 | End-to-end constrained concurrent operation | Run the mapped workloads at 64/128 MiB pools with multiple jobs, cold/warm caches, pinned outputs and concurrent read/decode. Distinguish local, real HTTP, workerd/R2 and production-cloud evidence; include runtime memory observations and headroom limits. |
| V2 | Correctness and complexity evidence | Tie edge-case/fixture tests, reference comparisons and scaling counters to F1–F6/M1–M2. Archive benchmark conditions and changes, not just passing test counts. |
| V3 | Final release and scope review | Re-run applicable gates on the final implementation, account for all conformance outcomes and compatibility changes, remove obsolete paths, and review every checklist item against current evidence. Final release acceptance remains unverified; no exception is assumed. |

Format-support gaps are tracked under V3. They must not silently become an
unbounded feature program, nor be waived to claim completion. Their relationship
to the promised read/validation contract must be resolved explicitly. The original
engine allowance requires runtime headroom; no exact RSS guarantee is substituted
for the stated reservation/live-allocation invariant.

### Current acceptance status

| Requirement | Current evidence | Remaining acceptance work |
| --- | --- | --- |
| Remove repeated whole-state accounting | Incremental vector groups and row aggregate states; exact serialized boundaries and scaling regressions | End-to-end allocation accounting is a separate requirement |
| Bounded selection and window lookup | Stable owned top-k, scoped selections, shared gather workspace lifetime, binary RANGE boundaries, monotone/adaptive GROUPS; reference comparisons | Whole-window allocation coverage remains separate |
| Shared/per-query allocation limits | Pool/child admission, ownership transfer, owned scan lifetimes and scoped top-k/projection; supported public grouped-state admission gap closed, owned deferred gathers, exact-cap and cap-minus-one fixture checks | Remaining decoder/operator/output lifecycle integration and complete heap coverage |
| Cloud transport bounds | Owned HTTP, native R2, signed S3-style HTTP; strict range/body and cancellation tests; public R2 ordinary-query matrix | Larger/adversarial mixed-workload coverage and whole-runtime memory proof |
| Parallel range quality | Coalescing, cross-wrapper pending-read sharing, cross-job cache reuse, cancellation, lazy planning, headroom-aware prefetch | Scheduling across remaining operators and broader mixed-job stress |
| Decode allocation admission | Scoped numeric/boolean/dictionary-index decoders, packed PLAIN and flat delta strings/binary, admitted fallback vector arrays, decompression, owned batches, bounded nested block assembly | Remaining encodings/conversions, fallback allocations, metadata/runtime overhead and codec workspace |
| Cache reuse within caps | Owned data and full/indexed metadata, raw-alias reservation transfer, cross-job pins, eviction-safe output; zero warm data reads on public R2 | Remaining metadata/converter workspace, all normal query paths |
| Smart defaults | Available-headroom admission, exact-range fallback, optional prefetch decline, default pool | Measured decode/batch scheduling through the complete execution pipeline |
| Release readiness | Unit, reference, workerd, transport and targeted benchmark evidence; changeset written | Final release gates, allocation audit and review |

The sections below preserve the original findings and dated implementation evidence.
A completed primitive or targeted test must not be read as completion of its wider
integration requirement.

## Evidence from the recent performance PRs

- [PR 19](https://github.com/earonesty/lakeql/pull/19), merged October 20,
  removed the O(n × k) worst-candidate rescan in vector top-k. Its reported benchmark
  for 200,000 rows and k = 1,000 improved from 8,751 ms to 97 ms. The merged
  implementation periodically sorts and trims candidates, retaining O(k) indices.
  Unlike the original PR description, it reads keys during comparisons and does
  not build a batch-sized key cache.
- [PR 20](https://github.com/earonesty/lakeql/pull/20), merged October 30,
  replaced repeated serialization of buffering aggregate state with incremental
  serialized-size accounting. Reported 50,000-row budgeted benchmarks improved
  from 29,174 ms to 8 ms for `count_distinct` and from 332,761 ms to 9 ms for
  `mode`. These are PR-reported measurements, not reruns for this audit.

The common failure is an individually reasonable operation—finding the worst
candidate, measuring a buffer, discovering frame boundaries—repeated over all
retained state for every incoming row. A memory cap limits the eventual state size
but does not make that repeated work cheap.

## Findings and proposed changes

### Group by repeatedly measures all groups

In [vector-group-by.ts](https://github.com/earonesty/lakeql/blob/b54d4ed/packages/core/src/vector-group-by.ts),
both the general and encoded update paths call `enforceGroupByMemoryBudget` after
each row. When a byte budget is configured, `estimateVectorGroupByMemoryBytes`
walks every group and aggregate state. With a fixed aggregate specification,
the accounting work is O(n × g), becoming quadratic when group count grows with
row count. PR 20 fixes accounting within aggregates but leaves this outer scan.

Maintain a group-by total from group creation and changes in aggregate charges.
Update it through every mutation, including merge, restore, reset, and spill
transitions. An unmeasured restored state may require one full measurement;
subsequent updates should apply deltas. Preserve existing accounting decisions
while separately defining any more complete allocation accounting contract.

Priority: highest. Verify high-cardinality count/sum as well as buffering
aggregates, with and without byte limits.

### Row execution serializes growing buffers

In [query.ts](https://github.com/earonesty/lakeql/blob/b54d4ed/packages/core/src/query.ts),
`addDistinctRow` copies the full seen set and serializes it after each new value.
Ordered row execution likewise estimates the retained rows after each insertion;
sort-run accumulation estimates the growing run. `estimateOperatorMemoryBytes`
converts the value to JSON-safe form, serializes it, and UTF-8 encodes the result.
Several callers evaluate this estimator before entering the function that checks
whether a memory limit exists, so the work also occurs without that limit.

Use shared accounting primitives for append, replacement, removal, and restored
state. Preserve JSON-safe serialization semantics where serialized size remains
the metric, including escaping, Unicode, separators, and supported special values.
Skip optional accounting when neither enforcement nor diagnostics consumes it.
Do not skip accounting merely because a query has no local limit if a shared pool
still governs it. Review row aggregate snapshots and window buffer accounting
under the same contract rather than fixing each call site independently.

Priority: highest. Test growing DISTINCT sets, unbounded sorts, bounded top-k,
sort runs, and row fallback execution.

### Row top k still rescans retained candidates

In [query-sort.ts](https://github.com/earonesty/lakeql/blob/b54d4ed/packages/core/src/query-sort.ts),
`addOrderedMatch` searches all retained candidates to find the worst for each
incoming row once the buffer fills. This is the direct O(n × k) analogue of PR 19.

Introduce a bounded selection operator, such as a heap, with O(log k) updates,
explicit encounter-order tie semantics, and incremental byte charges. Share
ordering and selection contracts across row and vector execution where useful;
retain representation-specific accessors and explicit vector-shape capabilities.
Account for keys, indices, row references, and replacement allocations. A bound
on candidate count alone is insufficient when k or row width is large.

Priority: high. Establish the intended tie contract against full sorting before
changing behavior; incidental replacement order must not become an undocumented
semantic difference. Cover offsets, nulls, descending order, and multiple keys.

### Window frame discovery can dominate aggregation

In [window-frame.ts](https://github.com/earonesty/lakeql/blob/b54d4ed/packages/core/src/window-frame.ts),
GROUPS frames rebuild peer groups and search them for each output row. RANGE
offset frames scan from the start of the partition for their endpoints. Peer-span
lookup repeatedly traverses peers, which is expensive for large equal-key groups.
[window-segment-tree.ts](https://github.com/earonesty/lakeql/blob/b54d4ed/packages/core/src/window-segment-tree.ts)
calls frame lookup per row, so faster aggregate queries do not eliminate this
potential quadratic cost.

Prepare reusable peer boundaries and range lookup metadata per compatible
partition/order specification. Binary search can find ordered range endpoints;
monotone cursors are appropriate only when bound semantics guarantee monotonicity.
Share metadata across compatible expressions and charge it to the memory pool.
When it does not fit, choose a lower-memory strategy rather than allocating it
outside the budget. Avoid materializing entire frames where an endpoint or
aggregate query suffices.

Priority: high. Cover large peer groups, GROUPS frames, numeric and timestamp
RANGE offsets, ascending/descending order, exclusions, and row-dependent bounds.

### Vector top k repeats sorting and normalization

In [vector-sort.ts](https://github.com/earonesty/lakeql/blob/b54d4ed/packages/core/src/vector-sort.ts),
`vectorTopKBatch` sorts the already ordered result of `vectorTopKIndices` and calls
`normalizeVectorOrderBy` inside its comparator. Remove the redundant sort once
the ordered-result contract is covered, and prepare comparison metadata once.

A batch-sized key cache is a valid optimization if its allocation is admitted
under the user's limits. It is not inherently undesirable, but neither batch size
nor O(k) complexity establishes a byte bound. Compare direct vector access with
budgeted key caching, accounting for variable-width values and shared backing
storage. Do not introduce an unconditional O(n) cache to accelerate bounded top-k.

Priority: medium. The redundant sort is concrete; the benefit of key caching
requires measurement by vector shape and key width.

### Range prefetch copies data it discards

In [range-cache.ts](https://github.com/earonesty/lakeql/blob/b54d4ed/packages/parquet/src/range-cache.ts),
`prefetchRetainableRanges` calls `slice` and discards its result. Cached reads
return an `ArrayBuffer.slice` copy, including the prefetch path. Retainable ranges
are already deduplicated and selected against a byte allowance; removing the copy
should preserve those properties.

Share an internal operation that ensures a range is cached without producing a
caller-owned copy. Keep the public read ownership contract intact. Account for
pending reads, completed cache entries, and returned buffers separately, while
counting shared backing allocations once. Preserve coalescing, in-flight
deduplication, cache identity/versioning, and the object-store concurrency cap.

Priority: medium. Measure temporary allocation volume as well as requests,
bytes fetched, and latency.

## Proposed memory contract for concurrent jobs

The deployment target discussed for this audit is a configurable 64–128 MiB
engine allowance, depending on query concurrency and cache reuse. These are user
configuration targets, not assertions about platform limits or safe runtime
defaults. The engine allowance must leave headroom for the application, runtime,
libraries, and garbage collection.

Use a shared memory pool for jobs that share an execution environment and cache,
with per-query limits beneath it. A query must satisfy both its own allowance and
the shared pool. Separate isolates cannot coordinate through an ordinary in-memory
pool; define that ownership boundary explicitly.

The proposed invariant is that outstanding reservations plus charged live engine
allocations never exceed the configured pool limit. The categories include:

- Shared cache backing allocations, counted once across jobs.
- Query-owned rows, vectors, aggregate state, indices, and sort/frame metadata.
- Temporary materialization, gather, concatenation, serialization, and decode
  buffers, including overlap while replacing an old allocation.
- In-flight range buffers and any decode expansion reserved for consumption.

Reserve before allocating. Transfer reservations into live charges without double
counting; release them on consumption, cancellation, error, and disposal. Cache
eviction cannot release a backing allocation's charge while a query still holds
it. Releasing a logical charge does not force immediate JavaScript garbage
collection, so this is an engine accounting contract, not an exact RSS guarantee.

Serialized-size accounting is useful for deterministic operator limits, but does
not by itself bound heap use: maps, objects, strings, allocator overhead, and
temporary copies differ from their serialized representation. Specify whether
existing `maxMemoryBytes` semantics are preserved or migrated, and document any
new pool setting and its scope. Never silently substitute a different metric.

Batch size and read concurrency should be admitted against available bytes and
request slots. Preserve useful parallelism rather than forcing serial reads.
When an optimization will not fit, reduce its working set, choose a lower-memory
operator, spill where the runtime supports it, or return a typed budget error.
Waiting for capacity must handle cancellation, fairness, and deadlock: a query
must not wait indefinitely for memory that only its own retained state can free.

## Implementation order

1. Inventory allocations and existing accounting boundaries across operators,
   caches, decode, and reads. Specify pool ownership, per-query limits, reservation
   lifecycle, and compatibility with existing budget metrics.
2. Build shared accounting primitives and eliminate whole-state rescans in group
   by and row buffers. Cover merge and restore before broad adoption.
3. Introduce bounded selection and reusable window lookup metadata within that
   contract. Express operator capabilities generically, not by query-specific
   planner branches.
4. Remove redundant sorting and prefetch copies; evaluate budget-admitted key
   caching and other allocation/performance tradeoffs with measurements.
5. Validate end-to-end workloads under concurrent query and shared-cache pressure.

Each change should be independently reviewable while contributing to the same
durable contract. The implementation order prioritizes the largest architectural
payoff rather than isolated micro-optimizations.

## Validation and acceptance criteria

Correctness tests must compare optimized paths with reference execution for ties,
nulls, offsets, multiple keys, vector shapes, and window boundaries. Exercise exact
budget limits and one-byte-over failures, plus cloning, merging, restoring,
spilling, cancellation, and failed reads. Preserve documented result and budget
semantics; report intentional compatibility changes explicitly.

Complexity regressions should count comparisons, serialization volume, state
visits, and copied bytes as input grows. Wall-clock benchmarks supplement these
tests but cannot replace them. Include bounded and unbounded configurations,
high group cardinality, wide strings/binary values, large peer groups, and varying
batch sizes and k values. Distinguish expected complexity from measured speedups.

Memory tests should inspect peak reservations and live charges under 64 MiB and
128 MiB configured pools, with multiple active queries, cold and warm caches,
pinned cache entries, and simultaneous reads/decode. Include replacement overlap
and error cleanup. Supplement accounting assertions with observed runtime memory
where available; identify untracked allocations and required headroom.

Range-read regressions must preserve the request concurrency cap, demonstrate
parallel reads when capacity exists, and verify coalescing, deduplication,
cancellation, requests issued, and bytes fetched. Faster execution must not rely
on full-object downloads, excessive read amplification, uncharged buffers, or
serializing previously parallel reads.

## Implementation progress

The historical execution log is preserved in chronological companion files. It records
what was verified at each checkpoint; current completion requirements and latest
evidence remain in this document. Earlier passing checkpoints do not establish
completion of the current checklist.

- [Historical execution log, part 1](performance-memory-audit-history-1.md)
- [Historical execution log, part 2](performance-memory-audit-history-2.md)
- [Historical execution log, part 3](performance-memory-audit-history-3.md)
