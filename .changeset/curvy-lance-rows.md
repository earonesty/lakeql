---
"lakeql-lance": minor
"lakeql-core": patch
"lakeql": minor
---

Add snapshot-safe projected materialization for stable Lance row IDs through bounded
object-store range reads, a broad scalar/binary/date/timestamp type matrix, sparse
deletion vectors, bounded official BTree exact-key and range lookup, and typed Lance
compatibility and snapshot errors. Add bounded IVF_FLAT vector search for L2,
cosine, and dot metrics with explicit dimension, partition, and candidate limits.
Support Lance dictionary-encoded UTF-8 projections and add a reproducible public
USPTO scattered-row HTTP range benchmark with physical I/O reporting.
