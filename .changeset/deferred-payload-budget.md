---
"lakeql": patch
---

Enforce existing operator byte and row limits while retaining deferred payloads for
ordered top-k and filtered LIMIT queries. Account for selected references and
materialized result entries incrementally, and release discarded offset references
before payload reads. Oversized retained payloads now raise a budget error.
