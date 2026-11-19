---
"lakeql-lance": patch
---

Load overlapping R-tree pages in bounded concurrent waves, parallelize structural column/page projection, share page-directory reads within each operation, and cache decoded spatial metadata and numeric pages by snapshot. Add deterministic network-wave regression checks for latency dependencies, I/O amplification, pruning, concurrency, and memory limits.
