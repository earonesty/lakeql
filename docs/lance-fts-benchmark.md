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
| match `quick brown fox` | cold | 71 | 103,760 | 105,514 | 2,093 | 1,264 |
| match `quick brown fox` | warm | 55 | 38,446 | 70,658 | 2,093 | 1,264 |
| match `zebra` | cold | 66 | 98,822 | 67,248 | 201 | 201 |
| match `zebra` | warm | 50 | 33,508 | 19,646 | 201 | 201 |
| phrase `quick brown fox` | cold | 84 | 170,363 | 346,410 | 2,093 | 31 |
| phrase `quick brown fox` | warm | 68 | 105,049 | 311,554 | 2,093 | 31 |

Each search resolved 18–20 row ids: the ten best candidates of each of the two partitions are
read and checked against deletion files before ranking, where an earlier revision read the ten
final row ids after ranking and rescanned the postings whenever one turned out to be deleted.
That is the 2–4 KB of extra bytes per search compared with that revision.

Wall time on loopback was 15–67 ms and says nothing about remote latency; it is recorded only
so regressions in decode cost are visible. Record public-network timings separately from the
CI thresholds, and note that peak memory above is *accounted* operation memory, not process RSS.

Accounted memory covers more than payload bytes: the index's decoded file directories (34,856
bytes for this fixture, reserved from their encoded size before they are decoded and reported as
`search.directoryBytes`), the query plan (`search.planBytes`, 1.6–3.2 KB here), every read plan
while its read runs, and every result container, including empty ones. An earlier revision of
this report showed lower peaks for the same requests and bytes because those structures were
allocated without being charged; the reads themselves are unchanged. The phrase peak is one
batch of 172 candidates of a three-term phrase: 516 position lists whose read plans, containers
and decoded positions are each admitted before they exist.

Cold searches include one pass over every document length (6,400 bytes for 1,600 documents) to
compute the corpus token total, which the format does not persist. It is skipped entirely when
no query term is in the dictionary, and cached with the opened index otherwise.

## CI work contracts

`packages/lance/src/fts-performance.test.ts` replays the same fixture through the network-wave
store (each event-loop turn is one round trip) with `get` and `list` forbidden. For the index
files alone:

| Contract | Ceiling |
| --- | --- |
| Cold dependent index waves (concurrency 1 / 2 / 8 / 32) | 40 / 22 / 13 / 13 |
| Cold index requests, bytes | 40, under 90,000 B (index is 444 KB) |
| Warm dependent index waves (concurrency 1 / 2 / 8 / 32) | 24 / 12 / 5 / 5 |
| Warm index requests, bytes | 24, under 18,000 B |
| Rare-term warm query | under 13,000 B and under 3.4% of the index |
| Nearby warm query | same rows, scores and total as an independent cold run; under 18,000 B, 5 waves |
| Absent term, cold | no document length, posting or row id read; 0 rows decoded |
| Corpus total in 16 chunks, concurrency 8 | at most 3 waves longer than in 2 chunks (10 if read in sequence) |
| Phrase query (172 candidate documents) | at most 40 index requests, 10 waves |
| Concurrency | `trace.peak <= min(concurrency, 8)` |
| Accounted memory, cold / warm / phrase | under 128 / 80 / 352 KiB |
| Fixed 96 KiB budget, `limit` 5, one-block batches, 201 to 1,400+ matches | identical results to an unbatched search, peaks within 8 KiB of each other |
| `limit` 3 to 400 with `maxBufferedRows` = `limit`, past deleted hits | identical results to an unbounded search under work limits of exactly one scan (2,093 postings, 1,264 documents) |
| 95% of rows treated as deleted, `limit` 10 | exactly one scan's postings and documents; at most 8 extra waves unbounded, 40 at `maxBufferedRows` 10 |
| Work limits | fail at exactly `limit + 1` candidates, before the batch is built |

A warm query's dependency chain is posting offsets and lengths, then per batch block offsets,
block bytes and the scored documents' lengths, then `_rowid` of the ranked documents. A cached
token set is never fetched again, and the only index structure that stays resident is the file
directories: the rest of the peak is the largest batch. `fts-regressions.test.ts` additionally
runs a search on a context it owns and asserts that, once the last page is released, the
operation is charged for exactly the directories and the plan and nothing else. The tests also assert exact posting-entry and candidate counts, identical
rankings with one-block batches for every recorded pylance query, and that each explicit limit
and operation budget fails the query before more data is read.
