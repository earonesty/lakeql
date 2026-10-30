---
"lakeql-core": patch
---

Keep buffering aggregates (count_distinct, approx_count_distinct, mode, median, quantile) linear under a memory budget by tracking their exact serialised size as values arrive instead of re-serialising the whole buffer per value, and stop mode copying its keys per row without a budget.
