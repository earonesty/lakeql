# Performance and memory budget audit

PR review repair tracking: [thread inventory](validation/pr21-review-repairs.json). The broad audit is paused while outstanding PR feedback is repaired. The first batch charges implicit Parquet range caches to the lake pool, verifies eviction/re-read under sibling pressure, validates median/quantile checkpoint entries before copying, admits decimal-negation signs, documents zero-operand scale semantics, and restores assignment on ordinary decoded-row keys while preserving `__proto__` handling. Release setup now fetches package-check fixtures. Benchmark repairs cover runtime teardown/contract consistency, source fingerprints, timing boundaries, working-directory independence, selected-case completeness and nested routing-depth inspection. Remaining review threads stay pending in the inventory; this is not completion of the audit.

Explicit scalar comparison context: [evidence and alternatives](validation/scalar-context-2026-11-14.json). Ordering passes borrowed state directly into the existing scalar workspace, removing the captured loop binding without changing payload admission or result semantics. Native single-job profile estimates fall from 170–187 MB to 66–72 MB of total managed allocation per round; these are allocation-volume estimates, not live peaks. Context identity, recoverable consumer failure and existing scalar lifetime/budget tests pass. All 4,111 regular/coverage tests (645 skipped), 95 Worker tests, 520 required conformance cases (one guard skipped), 120 references and release gates pass. All 16 runtime scenarios and 52 public-R2 scenarios pass at unchanged caps/headroom. Isolated complete text queries carry roughly 3–7% CPU overhead; a simpler callback-helper experiment had similar numeric allocation savings but a larger long-prefix regression. Numeric/text kernels are mixed. Runtime backing storage still exceeds intended caps, so this is an allocation-volume improvement, not a whole-runtime-cap or universal speedup claim.

Balanced top-k candidate runs: [evidence](validation/topk-runs-2026-11-14.json). Adjacent runs merge by measured row count, preserving stable ties while avoiding repeated growing-prefix copies; for positive k, settled candidates stay below 2k rows, with transient overlap admitted separately. A logarithmic directory is charged before construction. Parent disposal and reducer failures release internal owners; output survives parent shutdown. Explicit legacy serialized-memory or buffered-row limits retain eager prefix checks. Existing physical vector-shape placement and admitted concat/top-k implementations remain authoritative. Fourteen unit cases prove ordering, bounded retention, logarithmic full-limit merge volume, exact admission and error cleanup. All 4,110 regular/coverage tests, 95 Worker tests, 520 required conformance cases (one guard skipped), 120 reference tests and release gates pass. All 16 real-fixture runtime scenarios and 52 public-R2 scenarios pass. Native four-job numeric/text sampled backing peaks fall from 514/713 MB to 283/139 MB at the 64 MiB pool, but HTTP still reaches 828 MB. No whole-runtime-cap or controlled wall-clock speedup claim; runtime reclamation remains open.

Full-limit top-k runtime stress: [evidence](validation/topk-runtime-2026-11-14.json). The real 32,768-row Parquet fixture now exercises descending numeric and text ordering, with an independent order-sensitive result digest, retained outputs, pinned retries and post-release recovery. All 16 native-R2/constrained-heap-HTTP scenarios complete their correctness and admission checks; every final admission returns to zero. Nevertheless, sampled backing storage reaches 712,852,467 bytes on native R2 and 1,264,245,421 bytes on HTTP, both in 64 MiB/four-job scenarios. These are separate inspector counters, not additive process-memory measurements. A managed-heap limit does not bound external backing storage, and diagnostic GC is not a production remedy. M2/V1 remain open: trace allocation churn and establish runtime headroom before claiming 64–128 MiB compatibility. Earlier scan-only results do not cover this workload. A matched four-round native scan control now peaks at 23,316,278 bytes of sampled backing storage; top-k allocation profiling estimates 0.96–1.02 GB of managed-object allocation per single-job round. Source inspection confirms repeated concat/re-sort of growing candidates and unconditional serialized-size scans despite no legacy size limit. This wildcard fixture does not use deferred gathering. The next architectural target is a generic retained top-k continuation with stable ordering and explicit vector-shape capabilities, validated against full-limit and small-k scaling.

Gather descriptor ownership: [evidence](validation/gather-descriptors-2026-11-14.json). Both gather plans now admit batch, column-directory/key and vector descriptors before construction; an independent reservation protects them through reentrant disposal. Container-only ownership integrates with existing compaction provenance, preserving the original 512-iteration memory ceilings. Compaction compares complete retained costs with a separate overlap-headroom check. Wide/empty exact-cap tests and a baseline-failing control pass. Initial real Parquet timings are 287.81 → 307.27 ms and 457.65 → 460.11 ms, with modeled peaks increasing by 3,108 bytes in both fixture sets. All 4,096 regular tests (645 skipped), 95 Worker tests, 520 required conformance cases (one guard skipped), 120 references and release gates pass. Local and native R2 fixtures verify default compaction and forced shared fallback through scanner shutdown. All 52 public-R2 scenarios pass with unchanged 512 KiB headroom, 396,118-byte maximum incremental admission and zero warm data requests. No speedup or whole-runtime-cap claim.

Scalar comparison metadata admission: [evidence](validation/scalar-workspace-metadata-2026-11-14.json). Reusable scalar workspace state now admits 512 modeled bytes before construction, preserves its charge through active/reentrant disposal and releases it at teardown. Owned top-k avoids this workspace for allocation-free scalar shapes and zero candidate capacity. Exact/one-byte-short creation, reclamation cancellation and input-owner preservation tests pass; disabling the new admission makes both boundary controls fail. The existing scalar payload and fixed-stage lifetime tests include the metadata charge. All 4,088 regular tests (645 skipped), 95 Worker tests, 520 required conformance cases (one guard skipped), 120 references and release gates pass. All 52 public-R2 scenarios preserve 384,214-byte maximum incremental admission, zero warm data requests and at most four concurrent reads. Real-query timings are mixed: 288.60 → 286.82 ms for fast-prefix keys; Unicode fallback comparisons are 479.43 → 499.35 ms and 668.99 → 565.64 ms. No speedup or whole-runtime-cap claim; broader M1/M2/runtime acceptance remains open.

Bounded adaptive packed-text ordering: [evidence and tradeoffs](validation/packed-text-prefix-review-2026-11-14.json). Valid UTF8 prefixes compare directly in UTF16 order with no decoded-key cache; long or malformed prefixes retain the admitted scalar path. A shape-specific, admitted operation-local counter stops repeated unsuccessful probes. Four no-decode budget cases fail against the baseline and pass with the implementation. Final six-case Parquet top-k timing improves from 519 to 353 ms; complete long-prefix ASCII/Unicode controls remain near parity (501 → 501 ms; 509 → 497 ms). Isolated fallback kernels still vary, including regressions, so this is not a universal speedup claim. All 4,085 regular tests (645 skipped), 95 native Worker tests, 520 required conformance cases (one guard skipped), 120 references and release gates pass. Four-job native R2 fixtures now include ASCII/BMP/supplementary keys at both pool sizes; all 52 public-R2 range scenarios preserve 384,214-byte maximum incremental admission under 512 KiB headroom and zero warm data requests. Whole-runtime caps remain unproven.

Runtime refresh and fingerprint arithmetic: [runtime evidence](validation/utf8-fingerprint-runtime-2026-11-14.json), [hash evidence](validation/utf8-fingerprint-words-2026-11-14.json). Current native R2 and constrained-heap HTTP matrices preserve 64 successful primary jobs, 16 typed budget rejections, eight rejected pinned retries and eight successful post-release retries, with zero final admission. Native 64 MiB/four-job managed heap still samples 71,285,472 bytes; whole-runtime caps remain unproven, and GC-dependent differences are not a causal reduction claim. A byte-compatible FNV implementation uses two-word arithmetic for ASCII and the established UTF8 path thereafter, avoiding a full encoded copy and selecting automatically from content. Large ASCII kernels improve around 1.5–1.7×; general Unicode stays on the prior algorithm. Earlier all-input numeric variants were rejected. All 4,077 regular tests (645 skipped), 95 Worker tests, 520 conformance cases (one guard skipped), 120 references, release gates and 52 public-R2 scenarios pass. Native workerd measures about 1.20× for 1 KiB ASCII and parity for 64 KiB ASCII; Node’s larger ASCII gain is not assumed portable. Complete-query comparisons are near parity (309.61 → 308.08 ms and 296.89 → 300.15 ms), with unchanged modeled peak and no end-to-end speedup claim.

Footer-envelope entry scratch: [evidence](validation/footer-envelope-scratch-2026-11-14.json). Owned schema/key-value conversion now shares a per-range scratch helper with column metadata. Each raw entry releases after conversion; unknown logical unions transfer retained aliases before release. The 128-column/key-value fixture drops retained modeled storage from 361,786 to 150,966 bytes (58%). The first width-matrix comparison carries about 5% CPU overhead; complete metadata-enabled query timing is near parity. Generated Parquet metadata, exact/one-byte-short budgets, opaque logical alias retention and conversion failure checks pass. All 4,071 regular tests (645 skipped), 95 Worker tests, 520 conformance cases (one guard skipped), 120 references and release gates pass. The per-entry regression fails on ba8ce18 and passes after exact candidate restoration. All 52 public-R2 scenarios pass with unchanged 512 KiB headroom and 384,214-byte peak incremental admission. Repeated width-matrix medians show about 2–5% CPU overhead (2.2856 → 2.3954 ms and 2.5113 → 2.5636 ms). Complete-query comparisons vary in direction (305.93 → 301.78 ms and 307.59 → 317.10 ms); this is a memory improvement, not a claimed speedup. Runtime/parser overhead and fixed-stage consumption remain separate limitations.

Bounded JSON cursor experiment rejected: [evidence](validation/json-cursor-snapshot-2026-11-14.json). A depth-admitted 16-key snapshot improves row-array and mixed-sibling kernels but regresses the 17-property fallback boundary by about 14%; single-pass bounded collection still regresses it by 16%. It also needs 320 extra modeled bytes per active object. Complete real-Parquet JSON/NDJSON comparisons are near parity (181.43 → 180.64 ms and 181.60 → 182.93 ms). The original candidate passed 4,076 regular tests, 95 Worker tests, 520 conformance cases, 120 references and release checks, but performance acceptance failed. Both candidates are rejected and production is restored to 5ac8e35. Retained benchmarks cover 16/17/128-property boundaries, real streamed output, byte parity and zero cleanup. Per-property suspension remains a measured target; adding key snapshots is not an accepted solution. Whole-runtime caps remain open.

JSON short-string scan experiment: [decision and evidence](validation/json-short-string-decision-2026-11-14.json). Current-source profiling attributes about 24% of sampled self time to cursor/generator advancement and 10% to ASCII detection. Skipping the ASCII precheck for short strings has inconsistent row-array benefits and regresses small rows/short ASCII; the experiment is rejected and production restored. The benchmark now covers string-length boundaries, Unicode, escapes and lone surrogates with byte parity and zero charged cleanup. Cursor/generator overhead remains the next measured JSON target; no runtime-cap claim is made.

Borrowed range-cache consolidation: [evidence](validation/borrowed-range-key-workspace-2026-11-14.json). Borrowed private/shared caches now share retained-entry accounting, eviction, key leases and prefetch admission. The separate payload-only private cache is removed; private identities omit redundant file paths, while shared identities remain complete. Key/pending workspace lasts through settlement; planning forecasts payload, metadata, selected descriptors and concurrent key leases before transport. Five regressions fail on bb4d44b and pass on the candidate, including real Parquet rows, previously uncharged private retention, exact total-pool prefetch boundaries and tiny-cache demand fallback. The HTTP fixture still fetches exactly 384 bytes across three ranges at concurrency two, with explicit metadata/planning allowance and zero charged cleanup. Private maxBytes now includes retained metadata, matching shared caches; borrowed source/output ownership remains caller-managed. Repeated local borrowed-path comparisons measure 33.45 → 38.89 ms and 33.86 → 39.95 ms (16–18% overhead). This is a memory-accounting correctness tradeoff, not a speedup; the prior private implementation ignored its configured pool. Static identity measurement and synchronous hit release reduce overhead while preserving pending-read leases. Final validation passes 4,068 regular tests (645 skipped), 95 Worker tests, 520 conformance cases (one guard skipped), 120 references and all release checks. All 52 public-R2 scenarios pass at unchanged 512 KiB headroom; diagnostic borrowed-cache planning is explicitly charged and cleaned up.

Metadata-key workspace: [evidence](validation/metadata-key-workspace-2026-11-14.json). Owned metadata identities are constructed only inside admitted cache operations and released before awaited footer decoding. Shared borrowed metadata operations use asynchronous admission held through adapter settlement. Shared-cache scratch uses its configured domain; owned custom adapters fall back to the caller budget. Unused no-cache identities are not constructed. Four real-Parquet oversized-identity regressions fail on c9a9db1 and pass on the candidate with matching rows; adapter lookup/insertion failures release all charges. Three generic async-key tests cover exact/one-byte-short admission and settlement success/failure. All 4,063 regular tests (645 skipped), 95 Worker tests, 520 conformance cases (one guard skipped) and 120 references pass. All 52 public-R2 scenarios preserve four maximum concurrent reads, zero warm ordinary requests and 420,310-byte peak incremental admission under unchanged 512 KiB headroom. Metadata-enabled complete queries verify 512 matching rows, one warm metadata hit and 1,024 warm decoded hits with zero final charges. Two comparisons record 301.8 → 307.6 ms and 306.0 → 302.6 ms; timing direction varies, and modeled peak remains 2,614,821 bytes. All release gates pass. Borrowed raw-range keys and custom borrowed metadata adapters without an admission contract remain outside this change, and whole-runtime acceptance remains open.

Pending range-key workspace: [evidence](validation/pending-range-key-workspace-2026-11-14.json). Pending identity/record workspace and prefetch selected keys/directories are admitted before construction, remain charged through asynchronous settlement, and release independently of initiating-query disposal. Optional demand falls back to exact owned reads when workspace cannot fit; prefetch declines. Retention forecasts also account for cache-child budgets and distinguish ancestor versus sibling admission. Seven new tests pass; six fail on 0fe61cf. Existing exact payload/cache child caps remain unchanged, with explicit parent planning allowances; 57 focused range tests cover root boundaries, cancellation, cross-wrapper deduplication, all-64 selection at concurrency two, and smaller selection under total-pool pressure. The initial 13 constrained failures are resolved. Correctness gates pass 4,054 regular tests (645 skipped), 95 Worker tests, 520 conformance cases (one guard skipped) and 120 references. All 52 public-R2 scenarios retain four maximum concurrent reads, zero warm ordinary requests and a 420,310-byte peak incremental admission under unchanged 512 KiB headroom. Two complete decoded-enabled query comparisons record 297.3 → 300.8 ms and 308.0 → 303.7 ms; modeled peak is 2,614,074 → 2,614,371 bytes, with identical rows, 1,024 warm decoded hits and zero final charges. Timing direction varies, so no performance gain/regression is claimed. All release gates pass. Metadata/borrowed-key admission and whole-runtime acceptance remain open.

File-cache identity correction: [evidence](validation/file-cache-identity-2026-11-14.json). Five real-Parquet regressions reproduce collisions in borrowed/owned raw-range entries, shared pending reads and borrowed/owned metadata caches. Different path/version field boundaries return the first file bytes or metadata for the second file; metadata collisions suppress the expected second-file rows. All five fail on 69825d9 and pass with a shared versioned structured identity helper. The focused suite passes 106 tests, including absent/empty/sentinel version tags and existing cancellation/priority fixtures. Correctness gates pass 4,047 regular tests (645 skipped), 95 Worker tests, 520 conformance cases (one guard skipped) and 120 references. One reference case timed out during concurrent coverage; the unchanged command passed after competing tests finished. All 52 public-R2 scenarios pass with at most four concurrent reads, zero warm ordinary requests and 420,310-byte peak incremental admission under unchanged 512 KiB headroom. The first local decoded-enabled comparison records 292.3 → 302.5 ms and 2,614,030 → 2,614,074 modeled peak bytes; the repeat records 305.3 → 298.9 ms, so timing direction varies and no speedup/regression claim is made. All release checks pass. Temporary raw-key workspace and pending-directory admission remain open; this identity correction does not claim to close those allocation gaps.

Decoded-cache temporary key workspace: [evidence](validation/decoded-key-workspace-2026-11-14.json). decoded batch, page, dictionary, nested-page and page-header identities now defer serialization until synchronous optional workspace admission. Row-group prefixes compose deferred plans; existing serialized identities are preserved. Scratch uses the configured cache budget (caller budget only as fallback), releases before awaited decoding, and is excluded from retained payload ownership. Allocation-free JSON sizing keeps ASCII estimates tight. The initial focused suite passes 142 tests across nine files; three additional cases cover small-cache reuse, composed identities and numeric sizing boundaries. Two real Parquet regressions, covering column and owned vector readers with a 128 KiB identity under a 64 KiB pool, pass on the candidate and fail against 9155bcd because the baseline serializes the oversized identity; both baseline reads still return correct values. Candidate cleanup returns charged bytes to zero. The first decoded-enabled external-fixture comparison records 301.5 → 307.7 ms total cold/warm median and 3,124,138 → 2,614,030 modeled peak bytes at the same 64 MiB pool/16 MiB cache. Both variants check all 512 rows, zero warm requests, 1,024 warm decoded hits and zero final charges. Two broader exact-budget fixture failures were traced to missing key workspace in the fixture allowance; retained cache capacity and speculative-read protections remain unchanged. The repeat comparison records 297.8 → 301.3 ms, with the same 510,108-byte (16.3%) modeled peak reduction. Timing carries about 1–2% accounting overhead; no speedup or runtime-heap claim is made. Final checks pass 4,041 regular tests (645 skipped), 95 Worker tests, 520 conformance cases (one guard skipped), 120 references and all release gates. All 52 public-R2 scenarios preserve at most four concurrent reads, zero warm ordinary requests and a 420,180-byte peak incremental admission at unchanged 512 KiB headroom. Raw range/metadata key workspace and whole-runtime memory limits remain open.

Shared-cache metadata admission: [evidence](validation/shared-cache-metadata-admission-2026-11-14.json). Generic cache insertion now admits retained keys and directory records separately from payload pins, bounds zero-byte entries, validates cache limits centrally, and preserves protected entries before optional insertion can reclaim them. Planning and HTTP probe caches no longer need duplicate metadata ownership wrappers. Prefetch includes entry metadata before starting a read; exact and one-byte-short pool/cache tests cover ordinary and admitted-stage scopes. All 12 generic-cache regressions fail on the baseline. Final core checks pass 53 tests; decoded/HTTP/Parquet integration and range fixtures pass. Insertion kernels carry additional accounting overhead. Two complete cold/warm external-Parquet comparisons are near parity (216.9 → 219.5 ms and 224.5 → 220.7 ms), with zero warm requests, 512 matching cold/warm rows and zero final charges; modeled peak rises 322,500 → 323,992 bytes. All 4,026 regular tests (645 skipped), 95 Worker tests, 520 conformance cases (one guard skipped), 120 references and release gates pass. All 52 public-R2 scenarios pass, with at most four ordinary concurrent reads, zero warm ordinary requests and 420,180-byte peak incremental admission at unchanged 512 KiB headroom. The four-window owned transport fixture explicitly allows 4,096 payload bytes plus metadata (7,818 bytes total); its 64/128 MiB pool caps are unchanged. Transient key construction, remaining allocation integration and total-runtime caps remain open.

Decoded-cache identity correctness: [evidence](validation/decoded-cache-key-identity-2026-11-14.json). Delimiter-joined identities allowed different column projections and file identities to collide. A real Parquet fixture reproduced wrong columns and values on a warm read. Versioned structured keys preserve field and ordered column-array boundaries, including escaped names and absent versus empty version tags. Three negative controls fail on the baseline; all 25 focused tests pass, with two distinct cold projections followed by correct warm hits. Final gates pass 4,012 regular tests (645 skipped), 95 Worker tests, 520 conformance cases (one guard skipped), 120 references and full release checks. All 52 public-R2 scenarios pass: ordinary reads remain bounded at four, incremental admission peaks at 415,448 bytes under 512 KiB headroom, and warm ordinary queries issue zero requests. Remaining key-construction admission and whole-runtime limits remain open.

Nested page cache-key admission: [evidence](validation/nested-page-cache-key-2026-11-14.json). the nested Parquet page reader previously constructed variable-sized JSON keys before reserving their storage. The candidate admits escaped-string and temporary array workspace before construction, preserves the existing key format, and skips optional lookup/insertion when workspace is insufficient or would consume a nonrefundable parent-stage extent. Key scratch is a child scope kept through insertion but excluded from retained page owners. An oversized-key real nested-Parquet regression fails against the prior implementation and passes with identical rows and zero cleanup. Nine focused tests and the latest TypeScript build pass. Final validation passes 4,008 regular tests (645 skipped), 95 Worker tests, 520 conformance cases (one guard skipped), 120 references, full release gates and 52 public-R2 scenarios at 512 KiB headroom. Other decoded-cache key prefixes and parser/converter allocations remain separate M1 gaps.

Bounded scalar-text cache rejected: [decision](validation/bounded-scalar-cache-rejected-2026-11-14.json). A separately admitted eight-slot cache, active operand pins and workspace-before-cache reclamation passed focused lifetime/pressure checks but regressed complete DuckDB-fixture top-k comparisons from 425 → 1,335 ms and 421 → 1,345 ms. Every case slowed, with unchanged overall query admission peaks. The candidate and its otherwise-unused reclamation tier were removed; the published same-entry operand reuse remains. The rejected patch and raw comparisons are retained for reproducibility. Simulated hit rates alone did not predict actual performance. F5 broader acceptance and runtime-cap work remain open.


Top-k dictionary reuse trace: [evidence](validation/top-k-dictionary-reuse-trace-2026-11-14.json). Eighteen current-operator cases show repeated heap-candidate decoding even for mostly unique input: an eight-entry LRU simulation predicts roughly 51–52% dense-case hits at cardinality 8,192. Simple modulo placement collapses under strided selections (as low as 0.5% hits); mixed-bit placement avoids that collision pattern but trails LRU. These are decode-count simulations with stable-sort/cleanup checks, not timing or admission proof. Runtime is unchanged. Next evaluate separately admitted bounded lazy payload reuse, preserving active operand pins and releasing optional storage before mandatory growth/gather.

Shared dictionary comparison operand: [evidence](validation/same-dictionary-scalar-2026-11-14.json). Equal packed-text dictionary entries with matching outer null state decode once within scalar comparison; comparator validation, secondary ordering and stable ties remain intact, with no retained cache. A baseline-proven 592-byte rejection now succeeds and releases to zero. All 4,004 regular tests (645 skipped), 95 Worker tests, 520 conformance cases (one guard skipped), 120 references, full release gates and 52 public-R2 scenarios pass. Two DuckDB-fixture comparisons show about 10–14% improvement for 16-value dictionaries at batch size 2,048; aggregate timings are near parity and other cases vary. F5 adaptive preparation and whole-runtime acceptance remain open.

Top-k scalar lifetime and reuse review: [evidence](validation/top-k-scalar-lifetime-review-2026-11-14.json). Two new regressions confirm that parent-stage disposal during comparison and scope disposal inside decoding keep live scalar storage charged until settlement, rejecting competing admission and releasing to zero. All 23 focused tests pass; runtime is unchanged. Materialization’s reference-only string cache cannot be reused directly for top-k: materialization pre-admits output strings, whereas comparison uses two-value scratch. Adaptive top-k preparation must separately admit retained payload and relinquish optional capacity before mandatory wider reads/gathers, preserving direct-access success under pressure. F5 remains open.

Optional dictionary materialization reuse: [evidence](validation/optional-dictionary-materialization-2026-11-14.json). Owned output reserves mandatory storage first, then lazily caches immutable packed dictionary strings only when selected-row count predicts reuse and separately admitted reference directories fit. Parent stage limits, nulls, UTF-8/BOM/replacement behavior and direct fallback are preserved. All 3,998 regular tests (645 skipped), 95 Worker tests, 520 required conformance cases (one guard skipped), 120 references, full release gates, pressure/recovery probes and 52 public-R2 scenarios pass. Independent DuckDB fixtures show the 16-value/2,048-row-batch case improving from about 10 ms to 5.4–5.8 ms in two comparisons; other timings vary. Native four-job/64 MiB-pool managed heap samples 76,745,572 bytes; sparse samples do not prove a before/after heap improvement or whole-runtime caps. Nested dictionary reuse and observed-repetition policy remain opportunities; full audit acceptance stays open.

Dictionary materialization reuse experiment: [evidence](validation/materialization-dictionary-reuse-2026-11-14.json). Two twelve-case local comparisons evaluate decoded-string reuse with an admitted dictionary reference directory and direct fallback one byte below workspace capacity. Repeated dictionaries benefit in both runs. Dense unique-dictionary timings vary between runs without saving decoder calls; sparse unique dictionaries regress in both runs, making full-cardinality preparation wasteful. Every output matches existing UTF-8 decoding, including BOM and malformed bytes. Source/output heap is excluded from this policy experiment; production code is unchanged. Next investigate bounded lazy reuse driven by observed index repetition rather than unconditional preparation.

Post-collector runtime profile: [evidence](validation/incremental-collector-allocation-profile-2026-11-14.json). Four-round native workerd samples including collected allocations identify string materialization and typed-array subviews as leading allocation-volume sources. After measured work, release, eviction and diagnostic GC, managed heap returns to 5.1–5.4 MB and backing storage to 366,064 bytes in every scenario, with zero tracked allocations. Production cannot rely on forced GC; sampled volume is not a live-memory or peak guarantee. Next investigate bounded decode/materialization reuse and avoidable views while preserving UTF-8/binary semantics and pre-allocation admission. Runtime code is unchanged.

Incremental collector: [validation evidence](validation/incremental-collector-2026-11-14.json). One growing, admitted output directory replaces linked batch directories, releasing separable source containers during append. The 1,024-row/1,024-batch diagnostic reduces admitted peak from 512,576 to 234,128 bytes, with obsolete arrays collectible before finish. A real 40,000-row local Parquet owned-output comparison reduces peak from 19,601,203 to 9,361,859 bytes and alternating-pair median from 4.667 to 4.443 seconds; all outputs release to zero. [Query evidence](benchmarks/incremental-collector-owned-query-2026-11-14.json), [baseline retention](benchmarks/incremental-collector-retention-baseline-2026-11-14.json), [current retention](benchmarks/incremental-collector-retention-current-2026-11-14.json). Failed private-array growth stays charged for reuse; active append retains accumulated payload through reentrant disposal without rescanning previous owners. All 66 focused tests and TypeScript build pass. All 3,993 regular tests (645 skipped), 95 Worker tests, 520 required conformance cases (one guard skipped), 120 DuckDB reference cases, build/type checks, package consumers, lint, compatibility, large-file, changeset and fixture gates pass. Two fixed-byte-array reference boundaries now follow declining optional-compaction peaks until mandatory admission rejects, preserving values and zero-cleanup checks on every attempt. Native R2 and managed-heap-limited HTTP pressure/recovery probes pass, as do 52 public-R2 scenarios at 512 KiB query headroom. Native four-job/64 MiB-pool managed heap still samples 111,056,976 bytes; whole-runtime caps remain unproven.

Workspace-aware container transfer: [evidence](validation/container-transfer-headroom-2026-11-14.json). Optional compaction now checks free workspace before allocation, preserving reusable cache entries and falling back to complete ownership when traversal cannot fit. A baseline 176-byte transfer rejection now succeeds at a 176-byte peak; mandatory collection/slice boundaries still reject one byte below. Two native-R2 cases preserve binary backing with and without sibling pressure. All 3,984 regular/coverage tests (645 skipped), 95 Worker tests, 520 required conformance cases (one guard skipped), 120 references, 52 public-R2 scenarios and full release gates pass. The local query comparison is near parity (2.268 → 2.254 seconds). Native 64 MiB-pool managed heap still samples 97,992,664 bytes; whole-runtime acceptance remains open.

Separable output containers: [evidence](validation/separate-row-containers-2026-11-14.json). Materialization now distinguishes container-only storage from payload, allowing collection and vector windows to release obsolete arrays without mutating siblings or losing borrowed-buffer pins. All 64 obsolete batch arrays collect while final output remains held; retained admission falls from 188,736 to 168,256 bytes for 1,024 numeric rows. The measured collection peak rises 512 bytes for ownership traversal. The engine uses combined ownership for streaming/direct output and separate ownership for replaced directories; final local stream timings are 2.4% and 4.2% slower, an explicit tradeoff. All 3,980 regular/coverage tests (645 skipped), 93 Worker tests, 520 required conformance cases (one guard skipped), 120 references, 52 public-R2 scenarios and complete release gates pass. Native 64 MiB-pool managed heap still samples 120,900,472 bytes; whole-runtime acceptance remains open.

Confirmed output-directory retention gap: [evidence](validation/row-directory-retention-2026-11-14.json). Materialized row arrays and payload share indivisible ownership. After collection into one final array, all 64 source arrays remain reachable for 1,024 rows split across 64 batches; retained admission is 188,736 bytes versus 163,972 for one batch. Vector windows show the same retained-container pattern in accounting. Both local diagnostic runs reproduce exactly, and all charges/references release with final output. This baseline finding is addressed by the separable-container implementation above; broader collection peak/runtime work remains open.

Window planning lifetime review: [evidence](validation/window-planning-lifecycle-2026-11-14.json). Six new row/run/vector cases verify that sort-planning metadata does not transfer into empty output and that partition-evaluation failures clean up beside a pinned sibling before same-scope retry. The suspected retention issue was disproved; production code is unchanged. All 3,971 regular tests (645 skipped), 120 references and targeted native R2 fixtures pass. Complete window lifecycle and whole-runtime acceptance remain open.

Column-first materialization planning: [evidence](validation/materialization-column-traversal-2026-11-14.json). Unmasked immutable batches enumerate their column directory twice rather than once per row; selected batches retain one mask scan and O(rows + selected × columns) work without allocating an index buffer. All twelve planning cases improve in this run with unchanged forecasts/peaks. Real many-page query timings are mixed (4.9% slower, repeat 0.7% faster); no end-to-end speedup is claimed. All 3,965 regular/coverage tests (645 skipped), 93 Worker tests, 520 required conformance cases (one guard skipped), 120 references and full release gates pass. Native R2 still samples 159,536,644 managed-heap bytes at a 64 MiB pool; whole-runtime acceptance remains open.

Leaf materialization traversal: [evidence](validation/materialization-leaf-traversal-2026-11-14.json). Worker allocation sampling exposes roughly 4 MB per single-job round at cycle-set deletion in flat materialization. Leaf shapes now skip those mutations; exact regression counters fall from 32,768 to zero while recursive cycle checks and depth/output forecasts remain unchanged. Numeric/packed-text planning improves locally; the many-page query is about 0.9% slower in this comparison, so no end-to-end speedup is claimed. Unprofiled native R2/HTTP pressure and same-pool recovery pass, but native R2 still samples 85,693,032 managed-heap bytes at a 64 MiB pool. All 3,958 regular/coverage tests (645 skipped), 93 Worker tests, 520 required conformance cases (one guard skipped), 120 references and complete release gates pass. Whole-runtime acceptance remains open.

Current retention/runtime refresh (`fe2b5ec`): [evidence](validation/retention-runtime-refresh-2026-11-14.json). All 128 mixed-operator memory/HTTP jobs and all 52 public R2 scenarios pass, preserving at most four reads, zero warm ordinary-query data reads and a 416,968-byte incremental admission peak with 512 KiB headroom. Native R2 and managed-heap-constrained HTTP complete 64/80 primary jobs; sixteen correctly reject under pressure, all eight pinned retries reject, and all eight retries after release succeed using the same pool/cache. Final admission is zero. **Whole-runtime caps remain unproven:** including post-release/eviction snapshots, native R2 managed heap reaches 97,687,612 bytes at a 64 MiB pool and 179,062,576 bytes at 128 MiB. Diagnostic GC only after all measured rounds reduces managed heap to roughly 5 MB and backing to 366,064 bytes. Production cannot rely on that GC. These sparse samples are not statistical before/after evidence, and separate runtime counters are not assumed additive.

Indexed-source retention: [evidence](validation/indexed-source-retention-2026-11-14.json). A baseline probe retains footer buffers, metadata and input sources after explicit/parent disposal despite zero counters. A separate handle closure now detaches active state on source or parent closure; all six observed objects collect. Independently acquired row groups survive source, parent and abort closure in local and native R2 fixtures. Public R2 planning completes six fitting trials and eighteen typed rejections, all within cap and cleaning up to zero. All 3,951 regular/coverage tests (645 skipped), 93 Worker tests, 520 required conformance cases (one inactive guard skipped), 120 references and complete release gates pass. Whole-runtime acceptance remains open.

Released cache leases: [evidence](validation/released-cache-retention-2026-11-14.json). Both reserved and owned entry payloads stayed reachable through expired leases after eviction and zero reservation counters. Release now detaches payload and pin references; both payloads collect while the expired leases remain held. Released value access rejects, with live sibling ownership and detached/idempotent release preserved. Shared accessors avoid the initial per-lease closure overhead; final timings are close and mixed. Live public R2 preserves coalescing, warm cache reuse and an exact 4,144-byte engine-buffer peak. All 3,949 regular/coverage tests (645 skipped), 90 Worker tests, 520 required conformance cases (one inactive guard skipped), 120 reference cases and complete release gates pass. Whole-runtime caps remain unproven.

Measured-plan retention: [evidence](validation/measured-plan-retention-2026-11-14.json). Both compressed inputs stayed reachable through decoder closures after plan disposal and zero reservation counters. Disposal now detaches those references; both buffers collect in a Node forced-GC probe while the disposed plan remains held. Independent output ownership is preserved, and throwing input releases drain once with the first error preserved. All 3,947 regular/coverage tests (645 skipped), 90 Worker tests, 520 required conformance cases (one guard skipped), 120 reference cases and complete release checks pass. This addresses a concrete retention gap without establishing whole-runtime caps.

Measured shared-pool numeric fallback: [evidence](validation/numeric-shared-pressure-2026-11-14.json). A baseline-proven conservative ZSTD admission failure now falls back to measured inputs after reclamation and active-stage progress. Seven new cases cover exact bounds, ordinary scanner integration, queue/execution failures and preserved parallel reads. The actual workerd R2 fixture succeeds with 8,192 bytes of modeled shared headroom and rejects at one byte, with zero final charges. All 3,943 regular/coverage tests (645 skipped), 88 Worker tests, 520 required conformance cases (one guard skipped), 120 references and complete release checks pass. Pressure measurement reads inputs serially; normal admission retains parallel reads. This does not establish whole-runtime caps or broader operator integration.

Scheduler failure settlement: [evidence](validation/scheduler-failure-settlement-2026-11-14.json). A baseline negative control exposed early output publication followed by an unhandled cleanup exception and stranded scheduler bookkeeping. Output now publishes only after cleanup; failures discard output, remove job slots and advance queued work. Settlement defers reentrant draining until released capacity is available. All 23 focused cases and the native R2 recovery fixture pass; live public R2 retains one/two concurrent HTTP reads at exact 64/128-byte range-buffer budgets. Microbenchmarks are mixed, including about 0.6 µs extra per empty stage. Complete release checks pass: 3,936 regular/coverage tests (645 skipped), 87 Worker tests, 520 required conformance cases (one inactive guard skipped), 120 references, full build/types, clean consumer and repository gates. Broader operator scheduling and whole-runtime headroom remain open.

Scope release detachment: [evidence](validation/scope-release-detachment-2026-11-14.json). Released handles now detach their scope/backing references, and disposed children drop their parent allocation source. All three baseline-retained objects collect in a forced-GC reachability probe while expired handles remain held. Disposal drains later owners and children after a callback throws, preserving the first error; registration releases incoming ownership if snapshot cleanup fails or closes the scope. Pending I/O retains its charge until settlement. The real page-count comparison is 2.537 → 2.509 seconds, with no observed regression in this run. Validation passes 3,929 regular/coverage tests (645 skipped), 86 Worker tests and 520 required conformance cases (one inactive guard skipped), plus full build/types, clean consumer, 120 references and release checks. The first Worker run timed out on one row-expression case; its isolated run and complete unchanged retry pass. This does not prove whole-runtime caps.

Shared reservation implementations: [evidence](validation/shared-reservation-handles-2026-11-14.json). Reservation handles share method implementations and bind callbacks lazily, preserving detached-method behavior. Fresh-process Node diagnostics reduce median retained heap for 32,768 handles from 31.0 MB to 10.0 MB with identical admitted bytes. Final deep-JSON encoding measures 28.951 → 15.886 ms; ordinary shapes remain mixed. The real many-page Parquet query measures 2.731 → 2.627 seconds. Native R2 repeated pressure/recovery passes with zero final admission, but the 64 MiB/four-job case still samples 89,782,672 bytes of managed heap. Full release checks pass: 3,923 regular/coverage tests (645 skipped), 85 Worker tests, 520 required conformance cases (one inactive guard skipped), 120 references, build/types, clean consumer, lint and fixture/compatibility/size/changeset checks. Neither sparse heap samples nor diagnostic GC establish a production runtime cap.

Shared workspace capacity: [evidence](validation/shared-workspace-capacity-2026-11-14.json). JSON scalar formatting now admits only workspace growth, fixing a baseline-proven failure inside a sufficient fixed stage. Materialization and JSON traversal share the same exclusive-capacity helper. Traversal removes its depth-sized reservation directory; at 20,000 levels the modeled peak drops 320,064 bytes, with a measured 7–11% encoding slowdown across two isolated runs. Ordinary shapes are mixed. This explicit memory/CPU tradeoff does not close the historical JSON regression or whole-runtime headroom gap. Full coverage passes 3,920 tests (645 skipped), all 85 Worker tests and 520 required conformance cases pass (one inactive guard skipped), and build/types/lint pass with unchanged thresholds/timeouts.

Materialization traversal capacity: [evidence](validation/materialization-traversal-capacity-2026-11-14.json). Planning now reuses admitted ancestry depth across sibling cells and merges exclusive reservations, avoiding per-cell reservation construction and repeated consumption of fixed stage allowance. At 32,768 rows/depth four, reservation calls fall from 131,073 to five, with unchanged 320-byte planning peak and unchanged output forecasts; the local planning median falls from 51.126 to 5.302 ms. An initial scope-owned variant regressed the one-row-per-page fixture and was replaced by merged reservations; the final comparison measures 3.024 versus 3.153 seconds. Native R2 pressure/recovery passes, but managed-heap samples still exceed the admission pool. Final validation passes 3,913 regular/coverage tests (645 skipped), 84 Worker tests and 520 required conformance cases (one inactive guard skipped), plus build/types/lint. Coverage thresholds and conformance timeouts are unchanged; no whole-runtime-cap claim is made.

Current concurrency/runtime refresh (`4f20d13`): [validation](validation/current-concurrent-runtime-2026-11-14.json). All 52 live public R2 scenarios pass with 512 KiB shared query headroom, zero warm ordinary-query data reads and at most four concurrent reads. The maximum incremental admitted charge is 416,968 bytes. Separate local memory/HTTP mixed-query matrices complete all 128 jobs with four overlapping operators and pinned prior outputs. Native workerd/R2 and managed-heap-constrained HTTP matrices complete 64/80 primary jobs; the other 16 correctly reject on budget pressure. All eight retries after releasing pinned output succeed with the same pool/cache, and all pools clean up to zero. **Whole-runtime caps remain unproven:** native R2 sampled managed heap reaches 108,379,340 bytes at a 64 MiB admission pool. A separate post-measurement GC diagnostic reduces managed heap to roughly 5 MiB and backing to 366,064 bytes, supporting pending reclamation rather than large retained state. Production cannot rely on forced GC; measured counters are sparse and are not assumed additive.

Physical identity projections: [validation](validation/physical-identity-projection-2026-11-14.json). The generic CPU project operator now borrows its input when mappings and column order are unchanged, avoiding redundant directories and descriptor owners. Public `vectorProjectBatch()` still returns a new directory; subsets, reordering, aliases and missing-column errors retain existing behavior. On the real 40,000-row many-page Boolean fixture, a clean alternating-process comparison reduces the local median from 4.023 to 3.221 seconds, with admitted peak 802,047 to 801,657 bytes and zero after release. All 3,907 regular tests (645 skipped), build/types, and 520 required conformance cases pass. Initial timeout failures are retained in the diagnostic; the five-second timeout and value assertions are unchanged. All 84 native Worker tests and lint pass. This is local query evidence, not cloud throughput or a total-heap guarantee.

Shared packed-range access: [validation](validation/packed-range-access-2026-11-14.json). Gather/concat and selected packed execution now share scalar range access, removing per-row range tuples and rejecting invalid payload extents before gather admission. A baseline negative control confirms the previous gather planner accepted an out-of-bounds range. Fixed-width traversal counts remain unchanged. Two warmed 12-case local comparisons favor the new path in 9/12 and 11/12 cases; binary improves in all cases in both runs, while some timing differences remain noisy. A separate copy probe rejects a blanket scalar-loop replacement for native copying. Build/types/lint and 3,907 regular tests (645 skipped) pass with the identity-projection follow-up. All 84 Worker tests also pass on the combined runtime. Initial conformance timeout evidence is in [the diagnostic](validation/page-count-query-diagnostic-2026-11-14.json); the generic projection follow-up above now passes all 520 required cases. Native copy-view scratch and output/planner metadata remain open.

Gather view admission: [validation](validation/gather-view-admission-2026-11-14.json). Both shared and compact gathers now admit retained typed-array views, including empty output and timestamp96 planes. View charges follow their actual arrays so repeated union compaction releases obsolete charges. Scoped gather uses atomic stage admission; compact selection includes view cost. Local and native R2 fixtures cover small/shared and large/compact output after scanner shutdown. The 18-case local allocation matrix verifies exact caps and zero cleanup: 32 text columns share 4,128 bytes for 1,000 selected rows. Build/types/lint, all 84 Worker tests and 3,885 regular/coverage tests (645 skipped) pass. Unchanged coverage thresholds pass at 91.34% statements, 85.45% branches, 95.88% functions and 93.15% lines. Descriptor/planner metadata and temporary copy views remain open.

Predicate mask/view admission: [validation](validation/predicate-mask-view-admission-2026-11-14.json). Sixteen predicate mask producers now share one allocator that admits backing plus the existing typed-view charge. Wider CASE choices and selected-index construction also charge their views. Exact-overlap tests preserve mask reuse, scratch cleanup, independent retained owners and optional IN/decimal fallbacks. Logical predicates remain at two live masks through 128 operands (200,256-byte peak for 100,000 rows). Gather-output retained views are addressed by the follow-up above; other vector-conversion view admission remains open. Build/types, 3,882 regular tests (645 skipped), and all 82 Worker tests pass.

Metadata-only null predicates: [validation](validation/vector-null-mask-2026-11-14.json). Column `IS NULL`/`IS NOT NULL` now follow validity and dictionary/union indirection without decoding text or building nested values. A negative control restores three failures from the removed loop; iterative traversal also rejects cycles without a visited set. The largest local list fixture eliminates 28,753,920 decoded bytes and reduces its median from 5.249 ms to 0.066 ms. This removes a demonstrated untracked payload allocation; existing mask/view bookkeeping remains a separate M1 item. Real HTTP and native R2 fixtures cover filtering, pagination, concurrent owned outputs and cache eviction. Build/types, 3,874 regular tests (645 skipped), and all 82 Worker tests pass. The final Worker rerun serialized file startup after two dynamic-import timeouts, without changing timeouts or in-test query concurrency.

Scalar memo experiment rejected: [decision and evidence](validation/top-k-scalar-reuse-decision-2026-11-14.json). A one-value comparison memo preserves all 72 exact direct capacities and improves all 32 nulls-last shape cases, but loses 27/32 nulls-first cases. The prototype and its optional metadata/fallback branches were removed from production; the patch and benchmark are archived for reproduction. Inspect direct nullable-comparison work before adding more caching policy.

Top-k preparation cardinality/pressure evidence: [validation](validation/top-k-key-cache-cardinality-2026-11-14.json). Two alternating-order 72-scenario runs vary 64/1,024/10,000 dictionary entries against 10,000/1,000/100 selected rows. Small dictionaries win all 24 cases in each run; 10,000-entry dictionaries with 100 selected rows lose all eight. Every exact-direct-cap cache trial rejects while direct succeeds (144 trials across both runs), confirming that optional preparation needs a direct fallback and a work estimate, not only a fit check. No production default changed.

Top-k preparation shape evidence: [validation](validation/top-k-key-cache-shapes-2026-11-14.json). A 64-scenario alternating-order Unicode/null/multi-key/reuse matrix checks all 1,024 runs against a stable row oracle. Caching 64-entry dictionaries wins all 32 dictionary cases (median speedup ratio 2.93×; 10,960–83,904 extra tracked bytes). Packed caches win 29/32 but use up to 11,845,616 extra bytes and lose small-k cases. Production remains direct-access pending dictionary cardinality/selection-density and pressure evidence; sequential retained-result reuse is not a concurrent cloud workload.

Bounded vector projection follow-up: [validation](validation/bounded-vector-projection-2026-11-14.json). Oversized scanner batches are sliced before projection/deferred gathers with admitted metadata and preserved source ownership/physical offsets. A 1,000-row expanding projection streams under a 128 KiB pool; real HTTP 206/200 borrowed/owned scanner tests verify bounded output and filtered pagination. Build/types, 3,856 regular tests (645 skipped), and 82 Worker tests pass. The concurrent Worker run timed out twice; its complete isolated rerun passed without timeout changes. Third-party scanner allocations before yield remain the scanner’s responsibility.

Top-k stream lifetime follow-up: [validation](validation/top-k-borrowed-stream-lifetime-2026-11-14.json). A new real-Parquet test demonstrated zero live charge while a columnar top-k iterator still retained output. Borrowed batching now keeps the owned result through yields, admits temporary reference arrays and reuses a complete single batch. Build/types, 3,831 regular tests (645 skipped), and 82 Worker tests pass, including HTTP 206/200 and four concurrent native R2 streams at both pool caps.

Row aggregate finalization follow-up: [validation](validation/row-aggregate-finalization-2026-11-14.json). Ordinary row-fallback `aggregate()` no longer builds a discarded checkpoint; shared execution preserves restore/limits and explicit spill behavior. At 1,000 groups, alternating-order medians are 89.35 ms for rows versus 124.78 ms with checkpoint emission, with charged peak 7,506,596 versus 14,083,466 bytes. Build/types, 3,819 regular tests (645 skipped), and 82 Worker tests pass; real HTTP 206/200 tests cover repeated grouped spill continuation. Full release gates below predate this runtime change.

Prefetch acceptance refresh: [validation](validation/prefetch-acceptance-refresh-2026-11-14.json). Controlled copy-volume/timing evidence and 107 focused tests verify the original F6 mechanism. A current live R2 run passes all 52 scenarios with 512 KiB headroom; maximum incremental charge is 398,024 bytes and warm ordinary queries issue zero data reads. Broader scheduling and runtime-headroom acceptance remain open.

Owned slice/bookmark follow-up: [validation](validation/owned-slice-bookmark-2026-11-14.json). `ownedSlice()` retains rows and detached continuation metadata until release; plain slices share the same construction path. Bookmark normalization/sorting scratch is admitted before construction. Build/types, 3,817 regular tests (645 skipped), and 82 Worker tests pass, including four concurrent native R2 owners at each pool cap. The broader allocation and release audit remains open.

Native R2 slice follow-up: [validation](validation/slice-native-r2-2026-11-14.json). Four concurrent slice/resume jobs at each 64/128 MiB pool cap preserve first-page data across resumption and cache eviction; 82 Worker tests and type checks pass. The 100-row fixture validates lifecycle, not near-cap headroom. Bookmark ownership must cover sorted projection/window metadata and retained nested expressions, not just its outer object.

Slice collection follow-up: [validation](validation/slice-output-collection-2026-11-14.json). Selected payload and collection arrays now remain admitted across borrowed batch lifetimes through the shared owned collector. Build/types, 3,813 regular tests (645 skipped), 82 Worker tests and 12 HTTP fixture cases pass. Plain returned slices are caller-managed; bookmark metadata and explicitly owned public slice output remain review items.

Restored row-limit follow-up: [validation](validation/safe-restored-row-limits-2026-11-14.json). Top-k snapshot sums, slice/resumable capacities and policy caps now require exact safe integers. Build, types, 3,801 regular tests (645 skipped), and 82 Worker tests pass. Slice payload/collection ownership remains an explicit F3 gap.

JSON row-profile follow-up: [sampled profile](validation/json-row-profile-2026-11-14.json), reproduced with `pnpm exec tsc -b` then `LAKEQL_JSON_PROFILE_REPORT=/tmp/json-profile.json node bench/json-output-profile.mjs`. After 1,000 warmup encodes, 20,000 encodes of 1,000 Unicode-bearing rows attribute about 23% sampled self time to cursor advancement/key generation, 0.8% to cursor construction and 2% to GC. This supports investigating enumeration overhead before further frame pooling; sampled/inlined attribution is not a causal performance proof. Every encode releases all tracked charges, and each phase checks bytes against native JSON. Runtime is unchanged; this local Node kernel profile does not establish Worker/cloud throughput or total heap bounds. Any enumeration change must retain bounded admission before key storage, own-property ordering, sparse arrays, cycle/custom-toJSON rejection and deep traversal.

JSON traversal measurement refresh: [balanced baseline](validation/json-balanced-baseline-2026-11-14.json) and [frame-reuse decision](validation/json-frame-reuse-decision-2026-11-14.json). The harness now alternates variant order across four warmup/eight measured rounds and includes mixed sibling shapes. Current row-array median is 0.542 ms versus 0.337 ms recursively; depth 20,000 succeeds only with iterative traversal. A depth-frame/cursor reuse experiment improved mixed siblings but regressed ordinary row arrays in two runs, so it was removed. Production runtime remains unchanged; JSON performance acceptance remains open. Profile the ordinary row path before adding further reset/pooling complexity.

Current implementation: `docs/validation/default-http-pool-propagation-2026-11-14.json`.

HTTP response cleanup follow-up: [validation](validation/http-response-cleanup-2026-11-14.json); build and all 50 HTTP adapter tests passed. Full acceptance remains open.

JSON sizing follow-up: [validation](validation/iterative-json-sizing-2026-11-14.json); iterative admitted traversal, build/typecheck and 3,678 tests passed (629 skipped). Output writing is also iterative in the follow-up below; cache-write serialization was open at that revision; see the owned cache-write follow-up below.

JSON writing follow-up: [validation](validation/iterative-json-writing-2026-11-14.json); iterative admitted output writing, build/typecheck and 3,680 tests passed (629 skipped).

JSON traversal performance follow-up: [initial measurement](validation/json-traversal-benchmark-2026-11-14.json), [reservation-reuse measurement](validation/json-traversal-reuse-benchmark-2026-11-14.json). Shared high-water admission reduces churn, the 1,000-row encoding kernel was about 5.5× slower than recursion at that revision. The shared-enumeration follow-up below improves this further. Build/typecheck and 3,682 tests passed (629 skipped).

Shared JSON enumeration follow-up: [benchmark](validation/json-shared-enumeration-benchmark-2026-11-14.json). One iterator implementation now serves sizing and writing; row-array median improves to 0.85 ms but remains about 2.5× slower than the recursive reference, so the regression remains open.

Current JSON runtime checks: [Worker and coverage evidence](validation/json-worker-coverage-2026-11-14.json), 78 Worker tests passed; coverage thresholds passed unchanged. Deep NDJSON exact-capacity tests exercise workerd directly; total isolate heap bounds remain unproven.

Direct JSON cursor follow-up: [benchmark](validation/json-cursor-benchmark-2026-11-14.json). Array traversal avoids iterator-result allocation; row-array median is 0.54 ms versus 0.35 ms recursively (about 1.55×, still open). Build/typecheck, 3,683 regular tests (629 skipped), and 78 Worker tests passed.

JSON object dispatch follow-up: [benchmark](validation/json-object-dispatch-benchmark-2026-11-14.json). Object-only query scalar checks now run inside the object branch in both sizing and encoding. Against the preceding iterative implementation, row-array median improved from 0.574 ms to 0.537 ms with unchanged tracked peaks; the broader regression against recursion remains open. The report identifies the base commit and built module hashes for the measured working-tree change.

Owned cache-write follow-up: [validation](validation/owned-standard-json-cache-write-2026-11-14.json), [query benchmark](validation/json-standard-mode-benchmark-2026-11-14.json). Standard JSON serialization is now admitted through write settlement for immutable data values; custom conversion callbacks are rejected. Adapter-internal write buffering remains external. Build/typecheck, 3,693 tests (629 skipped), and 78 Worker tests passed.

Owned HTTP transport follow-up: [validation](validation/owned-http-write-2026-11-14.json). `putOwned` now admits HTTP payload copies and survives URI/cache/control composition. Build/typecheck, 3,699 tests (629 skipped), and 78 Worker tests passed. Native Fetch/request-header allocations and legacy adapters remain external.

Owned R2 transport follow-up: [validation](validation/owned-r2-write-2026-11-14.json). R2 retains the supplied byte owner without adding a payload copy and admits write-option records. Build/typecheck, all 14 R2 unit tests, and 79 Worker tests passed, including a real-binding JSON cache round trip. Native upload buffering remains external.

Owned R2 metadata follow-up: [validation](validation/owned-r2-metadata-2026-11-14.json). Retained metadata and cloned dates are now admitted; late results are rejected. Build/typecheck, 14 focused tests, and 79 Worker tests passed. Binding response allocations remain external.

Latest complete release checks: [scheduler-settlement gate refresh](validation/scheduler-failure-settlement-2026-11-14.json), runtime source identified by its recorded SHA256. Coverage runs 3,936 tests (645 skipped); unchanged thresholds pass at 91.38% statements, 85.53% branches, 95.90% functions and 93.18% lines. Full bundle/declaration build, types, clean 66-package consumer, 520 required conformance cases, 120 references, 87 Worker cases, lint, deterministic fixtures, compatibility, large-file and changeset checks pass. No timeouts or assertions changed. Earlier [scope-lifecycle evidence](validation/scope-release-detachment-2026-11-14.json) covers `25c924e`. Full finite acceptance remains open; these gates do not establish complete allocation coverage or whole-runtime headroom.

F1 restore follow-up: [group-limit validation](validation/aggregate-restore-group-limit-2026-11-14.json). Restored row aggregates now honor `maxGroups` before accumulator construction or new reads. Build/typecheck and 3,709 tests passed (629 skipped); full acceptance remains open.

F1/F2 buffer-restore follow-up: [validation](validation/aggregate-restore-buffer-limit-2026-11-14.json). Restored aggregate buffer/memory counters are now checked before scanning, including empty continuations, through the same enforcement method used for updates. Build/typecheck and 3,711 tests passed (629 skipped).

F1 malformed-restore follow-up: [validation](validation/aggregate-duplicate-restore-2026-11-14.json). Row/vector restore now rejects duplicate groups instead of silently replacing state, using the existing directory. Build/typecheck and 3,714 tests passed (629 skipped), including real HTTP checkpoint fixtures.

F1 restore-lifetime follow-up: [validation](validation/aggregate-restore-scratch-lifetime-2026-11-14.json). Parsed checkpoint/spill buffers now release before scanning; restored keys have independent ownership. Inline/spill tests show unchanged scanner-entry memory despite 64 KiB extra checkpoint data. Build/typecheck and 3,716 tests passed (629 skipped).

F3 discovery-lifetime follow-up: [validation](validation/topk-restore-before-discovery-2026-11-14.json). Top-k now releases checkpoint scratch before metadata discovery; sort already releases it before planning. Build/typecheck and 3,718 tests passed (629 skipped).

F2 sort-spill follow-up: [validation](validation/sort-restore-extent-2026-11-14.json). Run byte sizes and row counts are checked before merge/new scanning; unsafe descriptors are rejected. Build/typecheck and 3,723 tests passed (629 skipped). Remote spill atomicity remains unproven.

R2 spill continuation follow-up: [validation](validation/r2-spill-continuation-2026-11-14.json). Real SNAPPY Parquet queries resume aggregate/sort state from the workerd R2 binding at 64/128 MiB reservation budgets, using owned spill reads. Typecheck and 81 Worker tests passed. These small fixtures do not prove memory-pressure behavior, production-network performance, or atomic multi-object publication.
Earlier cache-ownership release checks (source `246b165`):
`docs/validation/current-cache-ownership-release-gates-2026-11-14.json`.

Latest mixed-query measurements (source `4d1dc00`):
[shared range cache](benchmarks/mixed-query-current-2026-11-14.json) and [factory caches](benchmarks/mixed-query-factory-current-2026-11-14.json). All 128 jobs completed across memory/HTTP, cold/warm, and 64/128 MiB reservation pools; final tracked usage returned to zero and HTTP concurrency stayed at two. The 64 MiB factory-cache peak was 66,960,963 bytes, close to its reservation limit. These Node measurements do not establish whole-Worker memory caps or stable latency improvements.
Latest constrained Worker measurements (source `4d1dc00`):
[current Worker refresh](validation/current-worker-refresh-2026-11-14.json). All 12 scenarios completed with expected typed budget rejection under four-job/64 MiB pressure and zero final tracked usage. Managed-heap limits exclude external buffers and total Worker accounting.
These sources cover different revisions and scopes. Full audit acceptance remains open.

<details>
<summary>Historical implementation and validation evidence</summary>

Earlier mixed-query evidence: `docs/validation/cache-accounting-mixed-runtime-2026-11-14.json`. Earlier constrained Worker runtime evidence: `docs/validation/current-planning-runtime-2026-11-14.json`. Earlier package/Worker evidence: `docs/validation/current-planning-release-gates-2026-11-14.json`. Earlier coverage/reference/conformance evidence: `docs/validation/current-planning-release-gates-2026-11-14.json`. Prior gate: `docs/validation/task-planning-cleanup-2026-11-14.json`. Prior full release-command refresh: `docs/validation/task-materialization-gates-2026-11-14.json`. Prior coverage gate: `docs/validation/task-ownership-release-gates-2026-11-14.json`. Earlier HTTP fixture evidence: `docs/validation/borrowed-deferred-http-2026-11-14.json`. Earlier scaling evidence: `docs/validation/column-top-k-scaling-2026-11-14.json`. Earlier implementation evidence: `docs/validation/cache-replacement-pressure-2026-11-14.json`. Raw cache accounting: `docs/validation/raw-cache-entry-accounting-2026-11-14.json`. Discovery defaults: `docs/validation/default-planning-cache-2026-11-14.json`. HTTP probe cache: `docs/validation/http-owned-probe-cache-2026-11-14.json`. HTTP probes: `docs/validation/http-owned-head-2026-11-14.json`. Owned HEAD contract: `docs/validation/owned-head-contract-2026-11-14.json`. JSON cache reads: `docs/validation/owned-json-cache-read-2026-11-14.json`. Bounded cache: `docs/validation/bounded-planning-cache-2026-11-14.json`. Cache contract: `docs/validation/owned-planning-cache-contract-2026-11-14.json`. Spatial shapes: `docs/validation/sidecar-prepared-shapes-2026-11-14.json`. Logical workspace: `docs/validation/sidecar-logical-workspace-2026-11-14.json`. Sidecar comparisons: `docs/validation/sidecar-comparison-correctness-2026-11-14.json`. Spatial literals: `docs/validation/sidecar-literal-preparation-2026-11-14.json`. Sidecar containers: `docs/validation/sidecar-planning-admission-2026-11-14.json`. Parquet schema plans: `docs/validation/owned-parquet-object-planning-2026-11-14.json`. Schema signatures: `docs/validation/parquet-schema-signature-2026-11-14.json`. Schema shapes: `docs/validation/parquet-schema-shape-2026-11-14.json`. Owned object plans: `docs/validation/owned-object-plan-contract-2026-11-14.json`. Discovery lifetime: `docs/validation/discovery-scan-lifetime-2026-11-14.json`. Discovery admission: `docs/validation/discovery-admission-2026-11-14.json`. Discovery boundaries: `docs/validation/discovery-boundaries-2026-11-14.json`. Metadata copies: `docs/validation/object-info-transport-copy-2026-11-14.json`. Scan cancellation: `docs/validation/scan-completion-cancellation-2026-11-14.json`. Slice manifests: `docs/validation/slice-manifest-lifetime-2026-11-14.json`. Owned manifests: `docs/validation/owned-task-manifest-2026-11-14.json`. Queued tasks: `docs/validation/queued-task-ownership-2026-11-14.json`. Window topology: `docs/validation/window-task-topology-admission-2026-11-14.json`. Object/window read plans: `docs/validation/object-window-read-planning-2026-11-14.json`. Projected read plans: `docs/validation/projected-read-planning-2026-11-14.json`. Shared deferred limits: `docs/validation/shared-deferred-limit-2026-11-14.json`. GPU scoped selection: `docs/validation/webgpu-scoped-selection-2026-11-14.json`. GPU pipeline lifetime: `docs/validation/webgpu-pipeline-dispatch-lifetime-2026-11-14.json`. GPU shared cache: `docs/validation/webgpu-pipeline-pool-2026-11-14.json`. GPU bounded cache: `docs/validation/webgpu-pipeline-cache-2026-11-14.json`. GPU selection backing: `docs/validation/webgpu-selection-backing-2026-11-14.json`. GPU cleanup: `docs/validation/webgpu-partial-buffer-cleanup-2026-11-14.json`. Borrowed projection admission: `docs/validation/borrowed-projected-admission-2026-11-14.json`. Projection caller unification: `docs/validation/projection-caller-unification-2026-11-14.json`. Projection descriptors: `docs/validation/projection-directory-admission-2026-11-14.json`. Reduction projection lineage: `docs/validation/reduction-projection-lineage-2026-11-14.json`. Query fallback checkpoints: `docs/validation/query-fallback-checkpoints-2026-11-14.json`. Scoped physical reductions: `docs/validation/scoped-reduction-checkpoints-2026-11-14.json`. Aggregate planning: `docs/validation/query-aggregate-column-planning-2026-11-14.json`. Deferred aggregation: `docs/validation/borrowed-deferred-aggregate-2026-11-14.json`. Columnar top-k: `docs/validation/borrowed-column-top-k-2026-11-14.json`. Column planning: `docs/validation/top-k-column-planning-2026-11-14.json`. Borrowed top-k: `docs/validation/borrowed-deferred-top-k-2026-11-14.json`. Commit cancellation: `docs/validation/checkpoint-commit-cancellation-2026-11-14.json`. Window batch integration: `docs/validation/window-spill-batch-2026-11-14.json`. Sort integration: `docs/validation/sort-spill-batch-2026-11-14.json`. Batch contract: `docs/validation/spill-write-batch-contract-2026-11-14.json`. Staged query checkpoints: `docs/validation/staged-query-checkpoints-2026-11-14.json`. Staged spill contract: `docs/validation/staged-spill-contract-2026-11-14.json`. Spill accounting: `docs/validation/spill-accounting-2026-11-14.json`. In-flight metadata cancellation: `docs/validation/task-metadata-inflight-cancellation-2026-11-14.json`. Planning cancellation: `docs/validation/task-planning-cancellation-2026-11-14.json`. Physical projection: `docs/validation/task-physical-projection-2026-11-14.json`. Aggregate column planning: `docs/validation/aggregate-column-planning-2026-11-14.json`. Grouping admission: `docs/validation/task-grouping-admission-2026-11-14.json`. Grouped concurrency: `docs/validation/grouped-task-concurrency-2026-11-14.json`. Mixed HTTP tasks: `docs/validation/mixed-task-http-2026-11-14.json`. Range-grouping baseline: `docs/validation/task-range-grouping-2026-11-14.json`. Owned planning: `docs/validation/owned-work-unit-planning-2026-11-14.json`. Work-unit topology: `docs/validation/work-unit-topology-2026-11-14.json`. Planning metadata: `docs/validation/work-unit-planning-metadata-2026-11-14.json`. Row-window iteration: `docs/validation/task-row-window-iteration-2026-11-14.json`. Planner workspace: `docs/validation/materialization-planner-workspace-2026-11-14.json`. Dynamic scalar workspace: `docs/validation/task-dynamic-scalar-workspace-2026-11-14.json`. Residual selection: `docs/validation/task-residual-selection-2026-11-14.json`. Column vectors: `docs/validation/task-column-vectors-2026-11-14.json`. Row transformations: `docs/validation/task-row-transform-2026-11-14.json`. Metadata lifetime: `docs/validation/task-metadata-ownership-2026-11-14.json`. Partition constants: `docs/validation/constant-partition-vectors-2026-11-14.json`. Grouped task ownership: `docs/validation/owned-group-fanin-2026-11-14.json`. Task scan: `docs/validation/owned-group-task-scan-2026-11-14.json`. Scalar task ownership: `docs/validation/owned-scalar-aggregate-tasks-2026-11-14.json`. Merge capability: `docs/validation/reduction-checkpoint-merge-2026-11-14.json`. Immutable reuse: `docs/validation/window-task-immutable-index-2026-11-14.json`. Earlier execution evidence: `docs/validation/window-task-http-cache-2026-11-14.json`. Scheduler admission: `docs/validation/work-unit-scheduler-admission-2026-11-14.json`. Owned runner: `docs/validation/owned-window-task-runner-2026-11-14.json`. Parquet ownership: `docs/validation/owned-parquet-window-tasks-2026-11-14.json`. Partition admission: `docs/validation/window-task-partition-admission-2026-11-14.json`. Failure lifetime: `docs/validation/work-unit-failure-drain-2026-11-14.json`. Task merge admission: `docs/validation/window-task-sparse-buckets-2026-11-14.json`. Last full release gates: `docs/validation/release-refresh-2026-11-14.json`. Full release acceptance remains open.

</details>

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

Current source `12527c2` mixed-query validation completes four overlapping jobs
across local-memory/real-HTTP, 64/128 MiB pools and cold/warm 4/16 MiB caches.
HTTP concurrency stays at two; fitting warm caches need only discovery probes.
Local workerd/emulated-R2 collectors reach 63.99 MiB tracked at a 64 MiB pool:
two of four jobs complete and two reject with budget errors, with zero reservations
after release. All four complete at 128 MiB. The 64 MiB warm scenario samples
81.26 MiB managed heap, so reservations still do not establish a runtime cap.
A separate localhost-HTTP workerd run with a 64 MiB V8 managed-heap flag completes
with the same job outcomes and samples at most 55.1 MiB managed heap. Transport,
sampling, external-memory and production-runtime limitations remain explicit in
`docs/validation/current-mixed-runtime-2026-11-14.json`; V1 remains open.

JSON frame-reuse experiment: [evidence](validation/json-frame-reuse-experiment-2026-11-14.json). Reusing frame/cursor objects cuts construction from 4,002 to six for 1,000 mixed rows, but the isolated comparison slows small-row encoding from 5.4 to 8.1 µs for only about 4% row-array improvement. The candidate is rejected and archived; production source is unchanged. Ordinary JSON performance and whole-runtime headroom remain open.

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
| Metadata and schema plans | Owned full/indexed footer and selected metadata scopes; directory view/backing share one admission forecast; raw schema, key/value and column scratch releases after each conversion; unknown logical aliases transfer to output ownership; borrowed footer aliases retain backing | Envelope/row-group owner disposal and iterator cleanup | `column-metadata-scratch-2026-11-14.json`, `footer-directory-view-2026-11-14.json`, and dated HTTP/R2 evidence below; remaining parser/planner and conversion scratch must be inventoried and closed |
| Scan vectors and nested assembly | Owned scanners and bounded column-block/assembly scopes | Batch owner transfer, deferred-window release and scan cleanup | `query-owned-scan.test.ts`, Parquet owned-output and nested fixtures; unsupported encodings/fallback allocations remain open |
| Group and buffering aggregate state | `OwnedGroupDirectory` / `OwnedAggregateState` with shared arena and incremental ledger; admit growth before mutation | Continuation disposal; independently admitted checkpoint/result transfer | Owned state/directory tests and public admission/scaling harnesses; borrowed legacy, spill/resumable and unsupported expression routes remain open |
| Parquet task scans and fan-in | Task metadata scope; admitted row transformation output; shared dictionary partition vectors; scoped selections/gathers; owned aggregate/window partials | Per-yield output disposal, drained scheduler failure, independent final owner release | `task-execution.test.ts`, `task-batch.test.ts`, HTTP task/cache fixtures; owned work-unit planning and scoped grouping now cover supported task topology; borrowed adapter/parser preallocation, callback-created storage and runtime overhead remain open |
| Scoped GPU selection and reusable pipelines | Selection admits upload/device backing, modeled descriptors/views and output records; active pipeline metadata is admitted before compilation, pinned through dispatch, and optionally retained in the shared root pool | Async completion releases workspace; output scope owns the mask/result; LRU eviction, pressure reclamation, failure and backend close release cache handles | `webgpu/src/backend.test.ts`, `pipeline-cache.test.ts`; required-accelerator selection is restored. Scoped reduction/vector-distance, compiler scratch and opaque driver/runtime memory remain separate gaps |
| Predicate masks and scalar temporaries | Scoped mask backing, per-row/per-candidate scalar scopes, fixed bigint/timestamp fast-path workspace | Consume/reuse private masks; release scratch at last consumer | Predicate/CASE/expression/bigint/timestamp memory tests; metadata-only column null predicates eliminate text/nested materialization, with cycle-safe indirection and HTTP/native-R2 evidence (`vector-null-mask-2026-11-14.json`). Shared CPU mask producers, wider CASE choices and selected-index construction now admit their typed views (`predicate-mask-view-admission-2026-11-14.json`). Retained gather views now use stage admission and array provenance (`gather-view-admission-2026-11-14.json`). Other converter views, compiled plan containers, unscoped conversion callers and runtime overhead remain open |
| Gathered vector output | Shared/compact forecasts admit backing and retained typed views before construction; compact selection measures their combined cost | Array-associated charges survive output transfer and release with obsolete backing during compaction | `gather-view-admission-2026-11-14.json`; exact/empty boundaries, repeated compaction and local/native R2 shared/compact fixtures pass. Per-row selected range tuples are removed and range validation is shared (`packed-range-access-2026-11-14.json`). Gather batch/directory/vector descriptors now share forecast, pre-construction admission, independently held construction lifetime and compaction provenance (`gather-descriptors-2026-11-14.json`). Descriptor coverage in other constructors, plan records and temporary copy views remains open |
| Sort/top-k replacements and row buffers | Incremental buffer accounting and scoped owned selection/projection workspaces | Eviction/replacement release; result transfer | `buffer-memory.test.ts`, `query-memory.test.ts`, `serialized-size.test.ts`, `vector-top-k-memory.test.ts`; scalar workspace metadata is admitted before construction and held through active disposal (`scalar-workspace-metadata-2026-11-14.json`); full row fallback and replacement-overlap matrix still needs final review |
| Window indexes and execution state | Charged shared boundary/index metadata and pressure-sensitive alternatives | Operator/workspace teardown | Window memory/reference/scaling evidence below; whole-window lifecycle coverage remains separate and incomplete |
| Serialized stream output | JSON direct buffers, scalar formatting and iterative cursor traversal use admitted high-water depth capacity; CSV cell directory/backing and final buffer admitted before allocation | Shared `ownedOutputStream` releases queued owners on handoff, cancellation or failure; traversal capacity releases at completion; caller-retained chunks leave library ownership | `json-output.workerd.ts`, encoder/stream/decimal fixtures, real HTTP/native-R2 output tests; 20,000-level exact-capacity Worker checks pass. Runtime iterator/GC overhead remains external. Row-array JSON kernel remains about 1.6× slower than recursive reference: `json-standard-mode-benchmark-2026-11-14.json` |
| Planning/cache entry retention | Factory planning cache and generic `OwnedEntryCache` retain values and entry metadata in the shared pool; HTTP ignored-range probe cache inherits factory defaults when not explicitly configured | LRU eviction, expiration, shared-pool pressure and clear release cache handles; active reader pins remain valid | `owned-entry-cache.test.ts`, `http-default-pool.test.ts`, replacement/pool-isolation fixtures; custom cache implementations and unadmitted lookup-key construction remain external/gaps |
| Object-store JSON cache I/O | Owned reads admit payload/parser overlap; owned writes admit standard-JSON wrapper/path/output/workspace and retain immutable input through settlement | Parsed owner transfers to caller; serialized backing/input release after `putOwned` or legacy `put` settles | `object-store-json-cache-owned.test.ts`, real HTTP round trips, real R2 binding fixture; custom conversions rejected by owned serializer. Legacy `set`, native transport storage and non-capable adapters remain external |
| HTTP/R2 write and metadata ownership | HTTP admits its request payload copy; R2 borrows the existing view and admits options records. Both admit retained metadata; HTTP also admits body probes | Owned results transfer to caller; upload input/copy survives pending I/O and releases on success/failure; disposed metadata scopes reject late results | HTTP/R2 owned-put/head tests, URI/cache/control composition, real HTTP and workerd fixtures. Native Fetch/R2 buffering, request/header creation and binding response allocations remain external |
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
| F1 | Incremental group-by accounting | Recheck general/encoded update, merge, restore/reset and spill paths against `vector-group-by-memory.test.ts`, `aggregate-memory.test.ts` and cardinality scaling evidence. Record each lifecycle path's result. Supported public columnar reductions now use owned continuation. Current public scaling verifies zero inter-batch checkpoints and near-linear measured growth over 2,000–8,000 groups; owned median growth uses 6/10/38 reservation calls over 1,024/8,192/65,536 values (`aggregate-scaling-current-2026-11-14.json`). Borrowed deferred aggregation shares bounded gathers and the owned continuation; columnar fallback checkpoints now retain scoped ownership. Row checkpoint restore now enforces `maxGroups` before rebuilding groups or scanning, verified with inline/spilled state and real HTTP Parquet fixtures (`aggregate-restore-group-limit-2026-11-14.json`). Restored row counters reject negative/fractional/unsafe values before scanning (`aggregate-restore-counters-2026-11-14.json`); vector restore shares the invariant with direct/scoped boundary coverage (`vector-restore-counters-2026-11-14.json`). Owned distinct/mode updates now reuse normalized keys through existing merge helpers; scalar, formatting-failure and budget-rollback cases pass (`aggregate-key-reuse-2026-11-14.json`). Row fallback now shares one execution lifecycle with separate row-only/checkpoint finalizers, removing discarded final snapshots from ordinary `aggregate()`. Repeated grouped spill continuation through real HTTP 206/200 covers scalar and buffering aggregates; restored buffer limits reject before new reads. Broader state ownership and runtime integration remain open. |
| F2 | Incremental row-buffer accounting | Recheck DISTINCT, sort runs, row fallback, aggregate snapshots and windows against shared serialized-size rules, pool limits and `buffer-memory`, `query-memory`, `serialized-size` tests. Direct iterator-count regressions now prove one restored-value measurement per accounting epoch for all five buffering aggregates, replacing an obsolete serializer-call proxy; the source trace identifies intentional restore/partition scans (`row-accounting-traversal-2026-11-14.json`). Broader physical/spill integration remains open. |
| F3 | Bounded stable row top-k | Candidate/checkpoint ownership implemented; borrowed deferred top-k now shares admitted selection and detached output with the owned route. Rank/output column lists and execution order-term normalization now have scoped admission; borrowed columnar merges/output share owned operations. Query, aggregate-result and vector bounds now share safe-integer/combined-extent validation; invalid caps reject before I/O or selection allocation, with valid safe-boundary coverage (`safe-row-limits-2026-11-14.json`). Restored top-k bounds, slice/resumable capacities and policy caps now also reject unsafe integers; top-k snapshot sums must remain exact. `QueryResult.slice()` now uses the shared owned collector with detached selected row payload and admitted per-batch/final directories. Exact-cap, mutation, cancellation and delayed-cleanup checks cover the collector; real Parquet, HTTP 206/200 and four-job native R2 pagination cover bookmarks and cache eviction (`slice-native-r2-2026-11-14.json`). `ownedSlice()` now retains rows and detached bookmark backing, including nested expression literals and operator bytes; bookmark normalization/sorting scratch and slice arguments/result metadata are admitted. Plain returned slices transfer to caller management, matching `toArray()`. Exact overlap, mutation, pressure and four-job native R2 owner lifetimes are covered in `owned-slice-bookmark-2026-11-14.json`. Columnar top-k borrowed batching now holds the owned result through each yield and admits temporary reference arrays; the prior suspended-iterator regression reported zero live charge despite retained rows. Single-batch output reuses its admitted array. Broader query configuration ownership remains open. Evidence: `docs/validation/row-top-k-acceptance-2026-11-13.json`; current gates: `docs/validation/checkpoint-release-baseline-2026-11-13.json`. |
| F4 | Reusable window boundaries | Independent dynamic RANGE/GROUPS oracle now covers all exclusions, both directions and arbitrary access at exact peer-index capacity and one byte below (`window-boundary-matrix-2026-11-14.json`). Peer-index admission now includes its typed-array view charge; the earlier index-only exact/fallback capacities were 292/291 bytes for forty groups (`window-peer-view-admission-2026-11-14.json`). Existing search/cursor tests bound key visits. Current public CLI/DuckDB dynamic-frame matrix adds 16 combinations and all 120 reference tests pass (`window-dynamic-reference-2026-11-14.json`). Row/run/vector kernel lifecycle now verifies exact-cap rejection/recovery, retained output and sibling isolation (`window-lifecycle-memory-2026-11-14.json`). Peer helpers now admit 1,536 bytes of fixed instance/cursor metadata; forty-group total exact/fallback capacities are 1,828/1,827 bytes (`window-peer-metadata-2026-11-14.json`). Native R2 now verifies four concurrent owned window outputs survive cache eviction at both pool caps; all 120 local Parquet/DuckDB reference tests pass on `c950498` (`window-owned-r2-2026-11-14.json`). A later six-case row/run/vector review confirms planning metadata is excluded from returned output and partition failures clean up before same-scope retry beside a pinned sibling; production code is unchanged (`window-planning-lifecycle-2026-11-14.json`). Complete public window lifecycle, remaining shared metadata and broader scaling remain required. |
| F5 | Vector top-k preparation | Verify redundant sorting/normalization removal; document measured direct-access versus admitted key-cache choices by shape and width. No unconditional unbounded key cache. Current bigint/timestamp96/packed-width scaling holds tracked workspace constant across row counts (`top-k-scalar-widths-2026-11-14.json`); candidate-heap view admission is corrected in `top-k-candidate-view-2026-11-14.json`. Final selection views now reuse the heap or admit slicing, with redundant intermediate views removed (`top-k-selection-view-2026-11-14.json`). Owned normalized ordering metadata is now admitted and shared normalization avoids serialized deduplication keys (`vector-order-normalization-2026-11-14.json`). Current scalar-width and numeric scaling, including new metadata charges, pass (`top-k-normalized-workspace-2026-11-14.json`). An admitted decoded-key-cache comparison now quantifies a speed/memory tradeoff for packed ASCII widths (`top-k-key-cache-2026-11-14.json`). Nullable ordering expands this to 36 scenarios (`top-k-key-cache-nullable-2026-11-14.json`), with timing variability preventing a stable policy threshold. Unicode packed/dictionary shapes, two ordering terms and one/four selections sharing preparation now have 64-scenario alternating-order evidence (`top-k-key-cache-shapes-2026-11-14.json`): 64-entry dictionary caches win 32/32, packed caches 29/32 with substantially higher memory. Dictionary cardinality/selection density and exact-cap pressure now have two 72-scenario runs (`top-k-key-cache-cardinality-2026-11-14.json`): eager preparation is rejected as a universal default, with direct access required as a low-memory fallback. Retained gather views now have exact admission and provenance-aware release (`gather-view-admission-2026-11-14.json`), with real local/native R2 shared and compact fixture coverage. Bounded adaptive prefix comparison is now implemented without retained decoded keys: flat and dictionary shapes, Unicode/long-prefix controls, independent ordering oracles, exact-budget fallback and four-job native R2 fixtures pass (`packed-text-prefix-review-2026-11-14.json`). The policy admits its fixed state only for supported ordering shapes and stops unsuccessful probes automatically. Gathered descriptors now have independent construction/retention admission and complete retained-cost compaction decisions (`gather-descriptors-2026-11-14.json`). Balanced adjacent candidate runs now avoid growing-prefix rebuilds (`topk-runs-2026-11-14.json`); explicit borrowed scalar comparison context removes per-comparison capture with unchanged admission (`scalar-context-2026-11-14.json`). Complete F5 acceptance and remaining operator bookkeeping still require integration review alongside M1. |
| F6 | Copy-free bounded prefetch | Original mechanism and transport-preservation checks verified on `8db3935`: 107 focused tests cover independent demand copies, private/shared ensure-cache prefetch, identity/versioning, pending deduplication, live pins, cancellation and concurrency. Alternating-order controlled measurements remove 1–4 MiB of discarded copies per prefetch with the same four reads/bytes and concurrency two; median timings improve in all four local cases. Current live public R2 validation passes 52 scenarios at 64/128 MiB pools with 512 KiB headroom, zero warm ordinary-query data reads and 398,024-byte maximum incremental charge. See `prefetch-acceptance-refresh-2026-11-14.json`. This verifies the original copy-removal contract; broader cross-operator scheduling and whole-runtime/headroom acceptance remain open under M2/V1, without changing those requirements. |
| M1 | Shared and per-query allocation contract | Consolidate a current allocation matrix covering cache, rows/vectors, state/indexes, temporary replacements, reads, decode, metadata and returned outputs. Each category needs its owner, admission point, release/transfer path, tests and remaining untracked storage. Owned row-group metadata now releases raw scratch per column while transferring retained aliases; real 1/8/32-column tests keep scratch above retained output constant at 3,478 bytes. The 32-column peak falls from 261,222 to 160,150 bytes, with a measured CPU tradeoff (`column-metadata-scratch-2026-11-14.json`). Alternating-order measurements through 128 columns confirm constant scratch, about 40% lower wide-schema peak and about 4% slower 128-column decoding (`column-metadata-balanced-2026-11-14.json`). Close remaining implementation gaps rather than treating reservations as total heap. |
| M2 | Memory-responsive execution | Verify batch/read scheduling, lower-memory operator choices and typed failure at indivisible limits. Owned collection now verifies eight successful same-pool/cache retries after releasing pinned outputs following repeated typed failures, through R2 and constrained HTTP (`worker-pressure-recovery-2026-11-14.json`). Scheduler FIFO/cancelled-head progress now has adversarial coverage (`stage-fifo-admission-2026-11-14.json`); its integration is numeric replacement stages and explicit scheduled concat, not all query paths. Retained-input progress now verifies rejection after active work settles and advancement of fitting aliased-input work without double charging (`stage-retained-input-progress-2026-11-14.json`). Failure settlement now verifies failed output publication, reentrant queue draining, cancellation pinning failures, partial input transfer and job reuse, with native R2 recovery and live HTTP range concurrency (`scheduler-failure-settlement-2026-11-14.json`). Measured-input cancellation now covers either input, retained charges until settlement, no later reads/decode, and same-pool native R2 retry with a sibling pin retained (`measured-input-cancellation-2026-11-14.json`). All 3,945 regular tests and 90 Worker tests pass; production code is unchanged. Exercise broader cancellation, fairness and operator self-held-capacity deadlock cases across integrations. Document smart defaults and any required tradeoff configuration. |
| V1 | End-to-end constrained concurrent operation | The broad `4f20d13` matrix distinguishes 52 live R2/HTTP scenarios, 128 mixed local/HTTP jobs, and native workerd/R2 versus a verified 64 MiB managed-heap HTTP runtime (`current-concurrent-runtime-2026-11-14.json`). Pinned outputs, cold/warm reuse, typed failure, post-release recovery and zero admitted cleanup pass. Native R2 samples exceed the configured admission pool (108,379,340 managed-heap bytes at a 64 MiB pool); post-measurement GC returns heap/backing near baseline. The later `c2eae26` native R2 replay samples 89,782,672 managed-heap bytes at the 64 MiB/four-job setting, with successful release/retry recovery (`shared-reservation-handles-2026-11-14.json`). Sparse GC-dependent samples do not prove a statistical heap reduction. Whole-runtime headroom and 64/128 MiB runtime acceptance remain unproven. |
| V2 | Correctness and complexity evidence | Tie edge-case/fixture tests, reference comparisons and scaling counters to F1–F6/M1–M2. Archive benchmark conditions and changes, not just passing test counts. |
| V3 | Final release and scope review | Re-run applicable gates on the final implementation, account for all conformance outcomes and compatibility changes, remove obsolete paths, and review every checklist item against current evidence. Final release acceptance remains unverified; no exception is assumed. |

Format-support gaps are tracked under V3. They must not silently become an
unbounded feature program, nor be waived to claim completion. Their relationship
to the promised read/validation contract must be resolved explicitly. The original
engine allowance requires runtime headroom; no exact RSS guarantee is substituted
for the stated reservation/live-allocation invariant.

Owned metadata cancellation follow-up: controlled HEAD now binds query/caller cancellation to its read scope and transfers successful metadata into the caller scope. Queued disposal and pending HTTP cancellation are covered alongside real HTTP ownership fixtures; see `validation/owned-head-read-controls-2026-11-14.json`. Real HTTP header/partial-body cancellation and queued-slot recovery also pass (`validation/owned-head-http-cancellation-2026-11-14.json`). R2 settlement coverage verifies that non-abortable binding calls retain the read slot until settlement, then reject late metadata and admit the next reader (`validation/r2-metadata-cancellation-2026-11-14.json`; 82 Worker tests passed).

Scheduled numeric decode instrumentation follow-up: direct and scheduled replacements now share dispatch, fixing omitted decode-time callbacks in the scheduled path. The measured ZSTD regression fails before the fix and passes afterward; prior affected decode-time breakdowns undercount work (wall-clock timings are unaffected). See `validation/numeric-decode-dispatch-2026-11-14.json`.

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
| Release readiness | Refreshed gates pass on explicit scalar comparison context: 4,111 regular/coverage tests (645 skipped), build, types, clean consumer, 95 Worker tests, 120 reference tests, 520 required conformance tests (one inactive guard), unchanged coverage thresholds, lint, deterministic fixtures and repository checks; see `scalar-context-2026-11-14.json`. All 52 public-R2 scenarios also pass | Complete allocation/scope audit remains open; refresh gates after further implementation changes |

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
