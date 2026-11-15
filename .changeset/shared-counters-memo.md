---
"lakeql": minor
---

Add shared running counters and bounded `SharedMemoryCache.getOrCompute` memoization.
Replace repeated buffer and row/vector aggregate accounting scans with retained totals.
Share scalar and correlated subquery preparation between SQL library and CLI,
deduplicate equivalent plans within query lifetime, and expose `memoMaxBytes`.
Preserve existing serialized-size budget semantics, cache priority and TTL behavior.
