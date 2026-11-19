# Funds demo range-read measurements

Measured November 18, 2026 against the released `fccf7bc` build and this change.
Chromium loaded the built UI and read the real TigZig Parquet endpoint directly;
no HTTP responses were mocked. Cold comparisons used fresh browser contexts and
alternated the two builds. These are network samples, not a universal speed guarantee.

## Findings and changes

- The old range cache required identical range keys. A 457,038-byte column-chunk
  prefetch was followed by a 457,015-byte fetch starting 23 bytes later, despite
  already retaining those bytes. Covering retained/in-flight ranges now serve
  subranges, with the same TTL, LRU, priority, byte and entry limits. The local
  and shared variants now use one implementation; no separate range index can
  outlive cache eviction.
- Aligning known column chunks to 256 KiB fetched unrelated columns. Prefetching
  now merges only overlapping/touching chunks that fit the configured retention
  and entry budgets. Metadata tail reads no longer round backwards into data.
- Deferred scanners repeated head probes after planning had already established
  a versioned object. They now reuse that planned size/ETag. Unversioned or
  mismatched listing hints still refresh head; abort and read budgets remain active.
- Chrome queued same-URL HTTP range-cache fills: sendStart for later requests
  followed earlier response headers. Disabling the HTTP cache allowed independent
  requests onto the wire together. The demo uses `cache: no-store` and shares one
  bounded 8 MiB Parquet byte/metadata cache across selections. Each query starts
  with a fresh version probe; changed ETags cannot hit the previous version's keys.
  Superseded selections abort their query and fetches.

The browser read cap remains four. The selected fund's phases have at most two
independent reads after adjacent chunks are merged; issuing four just to occupy
slots would add unnecessary work. The native read-concurrency regression also
continues to verify parallel execution stays within the configured four-read cap.

## Results

For scheme 122639, fresh-context pairs were 8.97 s vs 4.45 s and 7.67 s vs 4.33 s.
Transfer fell from 2,100,628 bytes to 815,074 bytes (about 61% less), with 10 vs 6
requests and zero full downloads. A separate same-page selection sequence gave:

| Selection | Released time | Updated time | Released transfer / requests | Updated transfer / requests |
| --- | --- | --- | --- | --- |
| Parag Parikh (first) | 8.44 s | 4.34 s | 2.00 MiB / 10 | 796.0 KiB / 6 |
| HDFC Flexi Cap | 8.65 s | 2.07 s | 2.44 MiB / 12 | 647.9 KiB / 3 |
| Aditya Birla | 4.06 s | 1.93 s | 2.52 MiB / 12 | 649.5 KiB / 3 |
| Franklin India | 3.58 s | 2.39 s | 2.53 MiB / 12 | 655.0 KiB / 3 |
| Parag Parikh (repeat) | 4.39 s | 699 ms | 2.00 MiB / 10 | 2 bytes / 1 |

Observation counts, latest NAVs, fund names, chart paths, and displayed ledger
rows matched for every selection. Rapid switching cancelled the superseded fetch
and rendered the final selected fund without stale output or page errors.
The source was 179,257,059 bytes during these measurements and refreshes regularly.

## Validation and bounds

Full `pnpm check` passes: 1,018 regular tests (97 skipped), 10 native Worker/R2
checks, 7 external conformance tests (1 skipped), 81 reference comparisons,
coverage, fixture verification, package-install smoke, lint, build and types.
Site build passes. Regressions cover covering/in-flight reads, failed retries,
truncated bodies, overlapping chunks, eviction, TTL, ETag/path isolation, versioned
planning, cancellation, physical read budgets and real HTTP 206/200 behavior.

The cache limit accounts for retained raw ranges and encoded metadata, not total
browser/Worker heap. Decoded batches and caller-held results have separate lives.
The demo still caps output at 12,000 rows. Arbitrary range-ignoring HTTP servers
can still incur the existing full-response fallback; this change does not claim
a whole-process memory ceiling or alter that fallback.
