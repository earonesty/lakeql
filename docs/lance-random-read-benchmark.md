# Lance scattered-row benchmark

This benchmark measures LakeQL materializing 32 known-positive, widely scattered USPTO trademark
rows from a Lance 2.0 dataset. It exercises the intended post-search path: immutable snapshot,
stable row IDs, projected columns, and bounded HTTP range reads. It does not scan a key column.

## Dataset

- Source: the public MarkWatch July V3 serial-layout
  [Parquet object](https://pub-cc21dc8afbef4216b7b0e3e63213bfb9.r2.dev/markwatch/july-v3/serials/marks.parquet)
  listed by the public
  [manifest](https://pub-cc21dc8afbef4216b7b0e3e63213bfb9.r2.dev/markwatch/manifest.json).
- Source SHA-256: `7c40d5753b05f6f42a072726328bd1029a0d4ea50ea4f071b3b232bcf420156d`.
- Source rows: 682,517.
- Producer: official `pylance 8.0.0`.
- Lance storage version: 2.0 with stable row IDs and V2 manifest paths.
- Layout: at most 65,536 rows per file and 4,096 rows per group.
- Projection: `serial`, `mark_text`, `owner_name`, `status`, and `source_url`.

`bench/lance-prepare-uspto.py` downloads the public Parquet object, writes the Lance dataset with
official tooling, chooses 32 evenly scattered rows, and records the stable IDs, expected
projections, and hashes in the generated `benchmark.json`.

## Recorded result

The recorded run used Node.js 24.14.1 on July 23, 2026. A local HTTP server enforced byte-range
requests and rejected full-object GETs. Each trial opened a fresh dataset and used a 3-second hard
elapsed budget, 16 MiB byte budget, 512-request budget, 32 MiB memory budget, eight concurrent
reads, and 32-row output/decoding limits.

| Measurement | Trial 1 | Trial 2 | Trial 3 |
| --- | ---: | ---: | ---: |
| Total elapsed | 603.94 ms | 334.34 ms | 229.33 ms |
| Wall elapsed | 605.20 ms | 334.65 ms | 229.58 ms |
| Metadata elapsed | 45.59 ms | 7.66 ms | 5.98 ms |
| Snapshot metadata | 3,074 B | 3,074 B | 3,074 B |
| Data metadata | 9,064 B | 9,064 B | 9,064 B |
| Logical bytes | 19,883 B | 19,883 B | 19,883 B |
| Physical bytes | 20,877 B | 20,877 B | 20,877 B |
| Range requests | 346 | 346 | 346 |
| Fragments touched | 11 | 11 | 11 |
| Pages touched | 55 | 55 | 55 |
| Peak decoded memory | 6,688 B | 7,744 B | 7,200 B |
| Full-object GETs | 0 | 0 | 0 |

All 32 projected rows matched the projections recorded by official Lance tooling. The local run
met both the 3-second hard budget and the desired one-second retrieval target. Loopback latency is
not a substitute for a public R2 latency result: the request count makes coalescing and metadata
caching important on a real network. The result establishes correct proportional I/O and provides
an honest reproducible baseline; use the remote mode below to measure a deployed copy in its target
region.

## Reproduction

```sh
uv run --with pylance==8.0.0 --with pyarrow --with numpy \
  python bench/lance-prepare-uspto.py --output /tmp/uspto.lance
pnpm bench:lance -- --dataset /tmp/uspto.lance --trials 3 \
  --output /tmp/lance-results.json
```

To benchmark the same generated dataset after uploading it to an HTTP range-capable object store:

```sh
pnpm bench:lance -- \
  --base-url https://example.invalid/ \
  --path markwatch/uspto.lance \
  --manifest /tmp/uspto.lance/benchmark.json \
  --trials 3 \
  --output /tmp/lance-remote-results.json
```

No generated benchmark dataset is checked into Git, and this workflow does not deploy anything.
The checked-in compatibility fixtures remain small and independently reproducible.
