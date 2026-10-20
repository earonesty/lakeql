---
"lakeql-core": patch
---

Improve vector top-k sorting performance by periodically sorting and trimming candidates while preserving row-index tie breaking. Keep retained row indices bounded by top-k, avoid per-term batch-sized key caches, and deduplicate equivalent sort terms.
