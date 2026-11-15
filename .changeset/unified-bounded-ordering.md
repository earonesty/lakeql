---
"lakeql": patch
---

Share priority-queue ordering across bounded row selection, deferred ranking and
sort-run merging. Preserve earlier equal-key rows, reuse the same selector for
restored checkpoints, and remove redundant sorting and ordering-term copies.
Candidate retention remains bounded by offset + limit; existing serialized-size
and buffered-row budget semantics are preserved.
