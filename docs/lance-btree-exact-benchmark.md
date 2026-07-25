# Million-row Lance BTree exact lookup

This benchmark measures one pinned-snapshot exact lookup against an official Lance 2.0 dataset
written by `pylance 8.0.0`. The dataset contains 1,000,000 rows, a 245-row
`page_lookup.lance`, and 4,096-row BTree leaves. The selected parcel-key index consists of a
15,629-byte lookup file and a 34,000,953-byte page-data file.

The measured query looked up `50000:000000550000`, projected `parcel_key`, and disabled full-object
reads. R2 measurements used the same private fixture under `lakeql-bench/parcels-1m.lance/`.

| Reader plan | Cold requests | Physical bytes | R2 wall time |
| --- | ---: | ---: | ---: |
| Remote midpoint probes | 44 | 19,623 | 4.6–5.7 s |
| Candidate leaf fetched once | 22 | 158,479 | 1.9–3.0 s |
| Candidate leaf plus bounded metadata-tail reads | 14 | 289,221 | 1.37–2.01 s |
| Shared decoded index/leaf cache | 4 | 67,716 | 0.53–0.54 s |

The request reduction deliberately trades roughly 270 KiB per cold lookup for fewer object-store
round trips. The decoded index cache has an independent configurable byte ceiling; the benchmark
uses 8 MiB. Cold cache population decoded 4,342 rows and the warm lookup decoded only the projected
row.

On a local HTTP range server, the 14-request cold plan took 35–102 ms and shared-cache trials took
about 8 ms. Direct filesystem midpoint probes were faster than an uncached full-leaf decode in the
microbenchmark. A future storage-cost planner can therefore select between full-leaf and midpoint
operators without changing the snapshot cache or reader contracts; `indexCacheBytes` controls only
retained decoded state, not that physical-operator choice.

Run the checked-in benchmark with:

```sh
pnpm bench:lance-btree -- \
  --dataset /tmp/parcels-1m.lance \
  --version 3 \
  --key 50000:000000550000 \
  --trials 3
```

`--base-url` can replace `--dataset` for an HTTP range endpoint.
