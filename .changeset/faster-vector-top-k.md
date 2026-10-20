---
"lakeql-core": patch
---

Improve vector top-k sorting performance by caching sort keys and periodically sorting and trimming candidates while preserving row-index tie breaking.
