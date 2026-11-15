# Performance rebuild

PR #21 is closed. Its branch, `perf/memory-bounded-execution`, remains an
archive of experiments and evidence, not a source tree to merge wholesale.
The replacement starts from main at `b54d4ed` (which already includes #19/#20).

## Design direction

Unify bounded selection and merge ordering around one priority queue in the
existing sort implementation. Ordinary row ordering and deferred ranking currently
scan all retained candidates to find the worst item; sort-run merging maintains a
separate heap. Share the ordering machinery instead of adding another execution
framework. Preserve encounter order for equal row keys and the existing explicit
path/row tie-break for deferred references. Restore checkpoints through the same
selector used for new input.

Vector top-k already uses bounded sort-and-truncate. Keep that representation and
remove redundant re-sorting/normalization rather than wrapping it in an ownership
framework. Existing physical shape placement remains authoritative.

## Required evidence before a replacement PR

- One ordering implementation replaces duplicated worst-candidate/heap routines.
- Selection retains at most offset + limit candidates; no full-input key cache.
- Existing serialized memory and buffered-row limits keep their meaning. No
  whole-runtime 64–128 MiB guarantee is inferred from serialized-size accounting.
- Independent full-sort comparisons cover ties, nulls, multiple keys, offsets,
  zero limits, skewed input, restored checkpoints and spill continuation.
- Operation counts demonstrate logarithmic candidate maintenance as k grows;
  measured timings supplement correctness and scaling tests.
- Real Parquet fixtures exercise row and deferred cloud reads separately from
  local files. Request concurrency, coalescing, cancellation and cache reuse are
  preserved; allocation savings cannot depend on downloading the whole object.
- A concise final findings section records changes, measured tradeoffs and
  remaining memory limitations. Keep raw experimental output outside the PR.

## Boundaries

This PR will not recreate shared allocation scopes, converters, format-support
expansion, decoder forks, diagnostic frameworks or an exhaustive audit program.
Those were separate architectural changes interleaved into #21. New scope must
justify itself by simplifying this design, not by repairing that abandoned tree.

No production changes have yet been selected solely because their old tests pass.
The final implementation must be assessed against main and the requirements above.
