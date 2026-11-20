# Lance Full-Text Search Benchmark

`bench/lance-fts.mjs` runs `LanceDataset.searchRows` against an official Lance
`INVERTED` index through HTTP byte ranges. Its local server answers only requests that carry a
`Range` header; any full-object GET fails the run.

```sh
pnpm build
node bench/lance-fts.mjs --dataset packages/lance/fixtures/fts-inverted-v2.0.lance \
  --version 5 --index text_idx --query "quick brown fox" --kind match --limit 10
node bench/lance-fts.mjs --base-url https://example.com/ --path catalog.lance --version 3 \
  --index title_idx --query "river delta" --limit 20 --select id,title
```

## Fixture

`packages/lance/fixtures/fts-inverted-v2.0.lance` is written by `pylance 8.0.0` with
`packages/lance/scripts/generate_fts_fixture.py`: 1,200 rows in three fragments, indexed with
`create_scalar_index("text", "INVERTED", with_position=True)`; 400 rows appended and added as a
second index partition with `optimize_indices()`; then rows with `id % 22 = 0` deleted after
indexing. `expected.json` records pylance's ranking and BM25 score for 19 queries. The index
files total about 444 KB.

## Loopback result (not public-network latency)

Measured with `maxConcurrentReads: 8` on a loopback HTTP server. "Requests" and "bytes" include
row materialization of 10 rows, which uses the existing take path; the index share is in the
CI contract below.

| Query | Cache | Requests | Physical bytes | Peak memory | Postings decoded | Matches |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| match `quick brown fox` | cold | 63 | 93,808 | 203,903 | 2,093 | 1,264 |
| match `quick brown fox` | warm | 47 | 28,494 | 195,644 | 2,093 | 1,264 |
| match `zebra` | cold | 65 | 89,890 | 55,928 | 201 | 201 |
| match `zebra` | warm | 49 | 24,576 | 39,948 | 201 | 201 |
| phrase `quick brown fox` | cold | 71 | 161,305 | 58,427 | 2,093 | 31 |
| phrase `quick brown fox` | warm | 55 | 95,991 | 50,168 | 2,093 | 31 |

Wall time on loopback was 23–79 ms and says nothing about remote latency; it is recorded only
so regressions in decode cost are visible. Record public-network timings separately from the
CI thresholds, and note that peak memory above is *accounted* operation memory, not process RSS.

## CI work contracts

`packages/lance/src/fts-performance.test.ts` replays the same fixture through the network-wave
store (each event-loop turn is one round trip) with `get` and `list` forbidden. For the index
files alone:

| Contract | Ceiling |
| --- | --- |
| Cold dependent index waves (concurrency 1 / 2 / 8 / 32) | 36 / 20 / 12 / 12 |
| Cold index requests, bytes | 36, under 90,000 B (index is 444 KB) |
| Warm dependent index waves (concurrency 1 / 2 / 8 / 32) | 16 / 9 / 5 / 5 |
| Warm index requests, bytes | 16, under 9,000 B |
| Rare-term warm query | under 4,500 B and under 2% of the index |
| Phrase query (172 candidate documents) | at most 32 index requests, 10 waves |
| Concurrency | `trace.peak <= min(concurrency, 8)` |
| Accounted memory (scoring state is charged per candidate document before it is built) | under 256 KiB |

A warm query's dependency chain is posting offsets and lengths, block offsets, block bytes,
then `_rowid` of the ranked documents; it never fetches a token set or the document-length
column again. The tests also assert exact posting-entry and candidate counts, and that each
explicit limit and operation budget fails the query before more data is read.
