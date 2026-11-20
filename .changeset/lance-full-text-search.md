---
"lakeql-lance": minor
---

Search official Lance `INVERTED` full-text indexes from browsers and workers: BM25-ranked match (OR/AND, fuzzy, prefix), phrase, and boolean queries through `LanceDataset.ftsIndexes()` and `searchRows()`. The reader decodes Lance's FST token sets, bit-packed posting blocks, and positions with bounded range requests, reproduces the index's English analyzer pipeline (including a Snowball stemmer and ASCII folding), skips rows deleted after indexing, and enforces explicit work limits. Rankings and scores are verified against pylance 8.0.0, and deterministic network-wave tests bound dependent round trips, requests, bytes, and memory.
